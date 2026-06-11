import asyncio
import argparse
import logging
import signal
import socket
import time
from typing import Any

from redis.asyncio import Redis
from rich.console import Console

from collector.core.orchestrator import CollectionOrchestrator
from collector.core.settings import settings
from collector.core.api_client import ApiClient, build_failed_result
from collector.core.postgres_queue import PostgresJobQueue, ClaimedPostgresJob
from collector.core.progress import publish_scan_progress
from collector.core.contract import build_worker_scan_result
from collector.store.evidence import write_local_evidence, write_evidence_artifact
from collector.auth.assume_role import AssumeRoleAuth
from collector.knowledge.pricing import AWSPricingEngine
from collector.analyzer.cost_attribution import build_cost_attribution
from collector.analyzer.pricing_coverage import build_pricing_coverage_audit

console = Console()
log = logging.getLogger(__name__)

class CollectorWorker:
    def __init__(self):
        self.worker_id = socket.gethostname()
        self.redis = Redis.from_url(settings.redis_url)
        self.api = ApiClient(worker_id=self.worker_id)
        self.pg_queue = PostgresJobQueue(
            settings.effective_postgres_jobs_database_url,
            stages=settings.worker_stage_list,
            claimant=self.worker_id,
        )
        self._shutting_down = False
        self._active_tasks = set()

    async def run(self):
        self._install_signal_handlers()
        console.print(f"Collector worker ready. ID={self.worker_id} (Postgres Queue)")
        
        while not self._shutting_down:
            try:
                job = await self.pg_queue.claim_next()
            except Exception as exc:
                log.error("Failed to claim next job: %s", exc)
                await asyncio.sleep(settings.worker_idle_sleep_seconds)
                continue

            if job is None:
                await asyncio.sleep(settings.worker_idle_sleep_seconds)
                continue
            
            task = asyncio.create_task(self._run_job(job))
            self._active_tasks.add(task)
            task.add_done_callback(self._active_tasks.discard)

        if self._active_tasks:
            console.print(f"Draining {len(self._active_tasks)} tasks...")
            await asyncio.gather(*self._active_tasks, return_exceptions=True)
            
        await self.close()

    async def _run_job(self, claimed: ClaimedPostgresJob):
        job_id = claimed.payload.get("job_id") or claimed.payload.get("scan_job_id")
        await self.pg_queue.mark_running(claimed.db_job_id)
        
        # Simple background heartbeat
        heartbeat = asyncio.create_task(self._do_heartbeat(claimed.db_job_id))
        
        try:
            success = await self._process_job(str(job_id), claimed.payload)
            if success:
                await self.pg_queue.mark_succeeded(claimed.db_job_id)
            else:
                await self.pg_queue.mark_failed(claimed.db_job_id, "Collection failed")
        except Exception as exc:
            log.error("Job %s crashed: %s", job_id, exc)
            await self.pg_queue.mark_failed(claimed.db_job_id, str(exc))
        finally:
            heartbeat.cancel()

    async def _do_heartbeat(self, db_job_id: str):
        while True:
            await asyncio.sleep(settings.worker_heartbeat_interval_seconds)
            try:
                await self.pg_queue.heartbeat(db_job_id)
            except Exception: pass

    async def _process_job(self, job_id: str, job: dict[str, Any]) -> bool:
        started_at = time.time()
        tenant_id = str(job.get("tenant_id", "unknown"))
        scan_id = str(job.get("scan_id", "unknown"))
        
        console.print(f"[cyan]→[/cyan] Starting collection job {job_id} (tenant={tenant_id})")
        
        try:
            await self.api.update_status(job_id, "running")
            await self._publish_progress(job, job_id, "running", message="Discovering accounts...")
            
            # 1. AUTH & DISCOVERY
            auth_info = await self.api.fetch_job_auth(job_id)
            aws_auth = auth_info.get("aws", {})
            
            auth_engine = AssumeRoleAuth(
                role_arn=aws_auth.get("role_arn", ""),
                external_id=aws_auth.get("external_id", ""),
                aws_access_key_id=auth_info.get("access_key_id", ""),
                aws_secret_access_key=auth_info.get("secret_access_key", ""),
                aws_session_token=auth_info.get("session_token", ""),
            )
            accounts = await auth_engine.get_accounts()
            if not accounts:
                raise RuntimeError(f"Could not assume role or discover accounts for {aws_auth.get('role_arn')}")

            # 2. RAW COLLECTION
            await self._publish_progress(job, job_id, "running", message=f"Collecting from {len(accounts)} account(s)...")
            orchestrator = CollectionOrchestrator(accounts, regions=job.get("regions"))
            collection = await orchestrator.run()
            
            # 3. PRICING & ENRICHMENT
            await self._publish_progress(job, job_id, "running", message="Fetching AWS pricing...")
            # Use management account session for pricing (Global Service)
            pricing_engine = AWSPricingEngine() 
            pricing = await pricing_engine.fetch_for_collection(collection)
            collection["aws_pricing"] = pricing
            
            await self._publish_progress(job, job_id, "running", message="Building cost attribution...")
            collection["cost_attribution"] = build_cost_attribution(collection)
            collection["pricing_coverage"] = build_pricing_coverage_audit(collection)
            
            # 4. STORE ARTIFACT
            evidence_artifact = await write_evidence_artifact(
                tenant_id=tenant_id,
                scan_id=scan_id,
                collection=collection
            )
            
            result = build_worker_scan_result(
                scan_id=scan_id,
                collection=collection,
                started_at=started_at,
                finished_at=time.time(),
                tenant_id=tenant_id,
                status="succeeded"
            )
            if evidence_artifact:
                result["artifacts"].append(evidence_artifact)
                
            await self.api.submit_worker_result(job_id, result)
            await self._publish_progress(job, job_id, "succeeded")
            console.print(f"[green]✓[/green] Completed collection job {job_id}")
            return True
            
        except Exception as exc:
            console.print(f"[red]✗[/red] Job {job_id} failed: {exc}")
            await self.api.submit_worker_result(job_id, build_failed_result(job, exc))
            await self._publish_progress(job, job_id, "failed", message=str(exc))
            return False

    async def _publish_progress(self, job, job_id, status, message=None):
        try:
            await publish_scan_progress(
                self.redis,
                tenant_id=str(job.get("tenant_id", "")),
                scan_id=str(job.get("scan_id", "")),
                job_id=job_id,
                stage="collect",
                status=status,
                message=message
            )
        except Exception as exc: log.debug("Progress publish failed: %s", exc)

    def _install_signal_handlers(self):
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: setattr(self, "_shutting_down", True))

    async def close(self):
        await self.api.close()
        await self.pg_queue.close()
        await self.redis.aclose()

async def main():
    parser = argparse.ArgumentParser(description="4Cast Collector")
    parser.add_argument("--worker", action="store_true", help="Run in worker mode")
    parser.add_argument("--account-id")
    parser.add_argument("--role-arn")
    parser.add_argument("--external-id")
    parser.add_argument("--output", default="evidence.json.zst")
    
    args = parser.parse_args()
    
    if args.worker or settings.worker_mode:
        worker = CollectorWorker()
        await worker.run()
    else:
        # CLI mode logic...
        if not args.account_id: parser.error("--account-id is required")
        # Simplified CLI auth
        accounts = [{"id": args.account_id, "name": args.account_id, "role_arn": args.role_arn, "external_id": args.external_id}]
        orchestrator = CollectionOrchestrator(accounts)
        result = await orchestrator.run()
        write_local_evidence(result, args.output)
        print(f"Artifact saved to {args.output}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
