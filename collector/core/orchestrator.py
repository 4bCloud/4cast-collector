"""
Collection Orchestrator — runs all collectors across all accounts and regions in parallel.

Region discovery strategy:
  1. Collect Cost Explorer FIRST (before discovering regions)
  2. Extract regions with meaningful billing activity from by_region_30d
  3. Use those regions for regional collectors
  4. If CE returned data but no billable region, scan only the account default region
  5. Fallback to ec2:DescribeRegions only if CE data is unavailable
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import aioboto3
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)

from collector.collectors.anomaly_detection import AnomalyDetectionCollector
from collector.collectors.api_gateway import APIGatewayCollector
from collector.collectors.aurora import AuroraCollector
from collector.collectors.aurora_dsql import AuroraDSQLCollector
from collector.collectors.cloudfront import CloudFrontCollector
from collector.collectors.cloudwatch_logs import CloudWatchLogsCollector
from collector.collectors.compute_optimizer import ComputeOptimizerCollector
from collector.collectors.cost_explorer import CostExplorerCollector
from collector.collectors.documentdb import DocumentDBCollector
from collector.collectors.dynamodb import DynamoDBCollector
from collector.collectors.ebs import EBSCollector
from collector.collectors.ec2 import EC2Collector
from collector.collectors.ecr import ECRCollector
from collector.collectors.ecs import ECSCollector
from collector.collectors.efs import EFSCollector
from collector.collectors.eks import EKSCollector
from collector.collectors.elasticache import ElastiCacheCollector
from collector.collectors.elb import ELBCollector
from collector.collectors.keyspaces import KeyspacesCollector
from collector.collectors.kinesis import KinesisCollector
from collector.collectors.lambda_functions import LambdaCollector
from collector.collectors.lightsail_databases import LightsailDatabasesCollector
from collector.collectors.memorydb import MemoryDBCollector
from collector.collectors.neptune import NeptuneCollector
from collector.collectors.network import NetworkCollector
from collector.collectors.rds import RDSCollector
from collector.collectors.redshift import RedshiftCollector
from collector.collectors.s3 import S3Collector
from collector.collectors.savings_plans import SavingsPlansCollector
from collector.collectors.secretsmanager import SecretsManagerCollector
from collector.collectors.sqs import SQSCollector
from collector.collectors.timestream import TimestreamCollector
from collector.collectors.trusted_advisor import TrustedAdvisorCollector
from collector.core.settings import settings

console = Console()
ACCOUNT_CREDENTIAL_REFRESH_WINDOW = timedelta(minutes=5)

GLOBAL_COLLECTORS = [
    AnomalyDetectionCollector,
    TrustedAdvisorCollector,
    ComputeOptimizerCollector,
    S3Collector,
    SavingsPlansCollector,
    CloudFrontCollector,
]

REGIONAL_COLLECTORS = [
    EC2Collector,
    RDSCollector,
    AuroraCollector,
    EBSCollector,
    NetworkCollector,
    ElastiCacheCollector,
    MemoryDBCollector,
    NeptuneCollector,
    RedshiftCollector,
    TimestreamCollector,
    KeyspacesCollector,
    AuroraDSQLCollector,
    LightsailDatabasesCollector,
    CloudWatchLogsCollector,
    DocumentDBCollector,
    ECSCollector,
    EKSCollector,
    ECRCollector,
    EFSCollector,
    ELBCollector,
    DynamoDBCollector,
    KinesisCollector,
    SQSCollector,
    APIGatewayCollector,
    LambdaCollector,
    SecretsManagerCollector,
]


class CollectionOrchestrator:
    """Runs all collectors across all accounts and regions concurrently."""

    def __init__(
        self,
        accounts: list[dict],
        regions: list[str] | None = None,
    ) -> None:
        self.accounts = accounts
        self.forced_regions = regions
        self._account_semaphore = asyncio.Semaphore(settings.max_concurrent_accounts)

    async def run(self) -> dict:
        """Collect from all accounts in parallel and return merged payload."""

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            overall = progress.add_task("Collecting from accounts...", total=len(self.accounts))
            tasks = [self._collect_account(account, progress, overall) for account in self.accounts]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        accounts_data = {}
        coverage_accounts = {}
        for account, result in zip(self.accounts, results):
            if isinstance(result, Exception):
                console.print(f"[red]✗[/red] Account [bold]{account['id']}[/bold] failed: {result}")
                accounts_data[account["id"]] = {"_error": str(result)}
                coverage_accounts[account["id"]] = _account_failure_coverage(str(result))
            else:
                accounts_data[account["id"]] = result
                coverage_accounts[account["id"]] = result.get("collection_coverage") or {}

        return {
            "meta": {
                "collected_at": datetime.now(UTC).isoformat(),
                "account_count": len(self.accounts),
                "collector_version": "0.2.0",
            },
            "accounts": accounts_data,
            "collection_coverage": _coverage_summary(coverage_accounts),
        }

    async def _discover_regions_from_ce(self, ce_data: dict) -> list[str]:
        """
        Extract active regions from Cost Explorer billing data.

        A few cents of spend is enough to consider a region active. This avoids
        scanning all enabled regions for low-spend/dev accounts.
        """
        billing_regions: set[str] = set()

        for item in ce_data.get("by_region_30d", []):
            key = item.get("key", "")
            amount = float(item.get("amount") or 0)

            if not key or key in ("NoRegion", "global"):
                continue

            if "-" not in key:
                continue

            if amount >= settings.active_region_min_spend_usd:
                billing_regions.add(key)

        return sorted(billing_regions)

    async def _discover_regions_fallback(self, session: aioboto3.Session) -> list[str]:
        """Fallback: ec2:DescribeRegions when CE has no data."""
        fallback = ["us-east-1", "us-east-2", "sa-east-1"]
        try:
            async with session.client("ec2", region_name="us-east-1") as ec2:
                response = await ec2.describe_regions(
                    Filters=[
                        {"Name": "opt-in-status", "Values": ["opt-in-not-required", "opted-in"]}
                    ]
                )
                regions = [r["RegionName"] for r in response.get("Regions", [])]
                return regions if regions else fallback
        except Exception:
            return fallback

    async def _collect_account(self, account: dict, progress: Progress, overall_task: int) -> dict:
        """Collect all data for a single account across all active regions."""
        async with self._account_semaphore:
            account_id = account["id"]
            account_name = account.get("name", account_id)

            await _refresh_account_credentials_if_needed(account)
            session = aioboto3.Session(
                aws_access_key_id=account.get("access_key_id"),
                aws_secret_access_key=account.get("secret_access_key"),
                aws_session_token=account.get("session_token"),
                region_name=account.get("region", "us-east-1"),
            )

            api_semaphore = asyncio.Semaphore(settings.max_concurrent_apis_per_account)

            async with api_semaphore:
                ce_collector = CostExplorerCollector(
                    account_id=account_id,
                    account_name=account_name,
                    session=session,
                )
                try:
                    ce_data = await asyncio.wait_for(
                        ce_collector.collect(),
                        timeout=settings.collection_timeout_seconds,
                    )
                except Exception:
                    ce_data = {}

            if self.forced_regions:
                active_regions = self.forced_regions
                active_regions_source = "manual"
            else:
                active_regions = await self._discover_regions_from_ce(ce_data)
                active_regions_source = "cost_explorer"

                if not active_regions:
                    if ce_data.get("by_region_30d"):
                        default_region = account.get("region", "us-east-1") or "us-east-1"
                        active_regions = [default_region]
                        active_regions_source = "default_region"
                        console.print(
                            f"[dim]  {account_name}: no billable region above "
                            f"${settings.active_region_min_spend_usd:.2f}; "
                            f"using default region {default_region}[/dim]"
                        )
                    else:
                        active_regions = await self._discover_regions_fallback(session)
                        active_regions_source = "ec2_describe_regions_fallback"

            console.print(
                f"[dim]  {account_name}: scanning {len(active_regions)} region(s) "
                f"({', '.join(active_regions)}) "
                f"[source: {active_regions_source}][/dim]"
            )

            async def run_global(cls: type) -> tuple[str, dict]:
                async with api_semaphore:
                    collector = cls(
                        account_id=account_id,
                        account_name=account_name,
                        session=session,
                    )
                    if hasattr(collector, "set_active_regions"):
                        collector.set_active_regions(active_regions)
                    try:
                        data = await asyncio.wait_for(
                            collector.collect(),
                            timeout=settings.collection_timeout_seconds,
                        )
                        return collector.name, data
                    except TimeoutError:
                        return collector.name, {"_error": "timeout"}
                    except Exception as exc:
                        return collector.name, {"_error": str(exc)}

            global_results = await asyncio.gather(*[run_global(cls) for cls in GLOBAL_COLLECTORS])

            async def run_regional(cls: type, region: str) -> tuple[str, str, dict]:
                async with api_semaphore:
                    await _refresh_account_credentials_if_needed(account)
                    regional_session = aioboto3.Session(
                        aws_access_key_id=account.get("access_key_id"),
                        aws_secret_access_key=account.get("secret_access_key"),
                        aws_session_token=account.get("session_token"),
                        region_name=region,
                    )
                    collector = cls(
                        account_id=account_id,
                        account_name=account_name,
                        session=regional_session,
                    )
                    try:
                        data = await asyncio.wait_for(
                            collector.collect(),
                            timeout=settings.collection_timeout_seconds,
                        )
                        return collector.name, region, data
                    except TimeoutError:
                        return collector.name, region, {"_error": "timeout"}
                    except Exception as exc:
                        return collector.name, region, {"_error": str(exc)}

            regional_tasks = [
                run_regional(cls, region)
                for cls in REGIONAL_COLLECTORS
                for region in active_regions
            ]
            regional_results = await asyncio.gather(*regional_tasks)

            account_data: dict = {
                "account_id": account_id,
                "account_name": account_name,
                "active_regions": active_regions,
                "active_regions_source": active_regions_source,
                "cost_explorer": ce_data,
            }
            coverage: dict = {
                "account_id": account_id,
                "account_name": account_name,
                "global": {},
                "regional": {},
            }
            coverage["global"]["cost_explorer"] = _coverage_entry(
                status="ok" if not ce_data.get("_error") else "error",
                message=ce_data.get("_error"),
            )

            for collector_name, data in global_results:
                account_data[collector_name] = data
                coverage["global"][collector_name] = _coverage_entry(
                    status=_status_from_collector_data(data),
                    message=data.get("_error") if isinstance(data, dict) else None,
                )

            regional_by_collector: dict = {}
            for collector_name, region, data in regional_results:
                if collector_name not in regional_by_collector:
                    regional_by_collector[collector_name] = {}
                if collector_name not in coverage["regional"]:
                    coverage["regional"][collector_name] = {}
                regional_by_collector[collector_name][region] = data
                coverage["regional"][collector_name][region] = _coverage_entry(
                    status=_status_from_collector_data(data),
                    message=data.get("_error") if isinstance(data, dict) else None,
                )

            account_data.update(regional_by_collector)
            account_data["collection_coverage"] = _coverage_summary({account_id: coverage})

            progress.advance(overall_task)
            console.print(
                f"[green]✓[/green] [bold]{account_name}[/bold] "
                f"[dim]({account_id} · {len(active_regions)} regions)[/dim]"
            )

            return account_data


async def _refresh_account_credentials_if_needed(account: dict) -> bool:
    refresh = account.get("_refresh_credentials")
    if not callable(refresh):
        return False

    expires_at = _parse_credential_expiration(account.get("credential_expires_at"))
    if expires_at and expires_at > datetime.now(UTC) + ACCOUNT_CREDENTIAL_REFRESH_WINDOW:
        return False

    creds = await asyncio.to_thread(refresh)
    account["access_key_id"] = creds["AccessKeyId"]
    account["secret_access_key"] = creds["SecretAccessKey"]
    account["session_token"] = creds["SessionToken"]

    expiration = creds.get("Expiration")
    parsed_expiration = _parse_credential_expiration(expiration)
    account["credential_expires_at"] = (
        parsed_expiration.isoformat() if parsed_expiration is not None else str(expiration or "")
    )
    return True


def _parse_credential_expiration(value: object) -> datetime | None:
    if isinstance(value, datetime):
        expiration = value
    elif isinstance(value, str) and value:
        try:
            expiration = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None

    if expiration.tzinfo is None:
        expiration = expiration.replace(tzinfo=UTC)
    return expiration.astimezone(UTC)


def _status_from_collector_data(data: dict) -> str:
    if not isinstance(data, dict):
        return "ok"
    if data.get("_error") == "timeout":
        return "timeout"
    if data.get("_error"):
        return "error"
    return "ok"


def _coverage_entry(*, status: str, message: object | None = None) -> dict:
    entry = {"status": status}
    if message:
        entry["message"] = str(message)
    return entry


def _account_failure_coverage(message: str) -> dict:
    return {
        "status": "error",
        "message": message,
    }


def _coverage_summary(accounts: dict) -> dict:
    summary = {
        "total": 0,
        "ok": 0,
        "failed": 0,
        "timeout": 0,
    }
    for account_coverage in accounts.values():
        _count_entries(account_coverage, summary)
    return {
        "summary": summary,
        "accounts": accounts,
    }


def _count_entries(value: object, summary: dict[str, int]) -> None:
    if isinstance(value, dict) and set(value.keys()).issubset({"status", "message"}):
        status = value.get("status")
        summary["total"] += 1
        if status == "ok":
            summary["ok"] += 1
        elif status == "timeout":
            summary["timeout"] += 1
            summary["failed"] += 1
        elif status in {"error", "failed"}:
            summary["failed"] += 1
        return
    if isinstance(value, dict):
        for item in value.values():
            _count_entries(item, summary)
