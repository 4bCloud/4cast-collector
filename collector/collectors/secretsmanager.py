"""
Secrets Manager Collector — READ-ONLY.

FinOps signals:
- Secrets that appear unused for a long period.
- Secrets without rotation enabled.
- Secrets replicated to other regions.

IAM required:
- secretsmanager:ListSecrets
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from collector.collectors.base import BaseCollector

UNUSED_SECRET_DAYS = 90


class SecretsManagerCollector(BaseCollector):
    name = "secretsmanager"

    async def collect(self) -> dict:
        secrets: list[dict] = []
        unused_secrets: list[dict] = []
        no_rotation_secrets: list[dict] = []
        replicated_secrets: list[dict] = []

        async with self.session.client("secretsmanager") as sm:
            paginator = sm.get_paginator("list_secrets")
            async for page in paginator.paginate():
                for item in page.get("SecretList", []):
                    secret = self._normalize_secret(item)
                    secrets.append(secret)
                    if secret.get("is_potentially_unused"):
                        unused_secrets.append(secret)
                    if secret.get("rotation_enabled") is False:
                        no_rotation_secrets.append(secret)
                    if secret.get("replica_regions"):
                        replicated_secrets.append(secret)

        return {
            "secrets": secrets,
            "total_secrets": len(secrets),
            "unused_secrets": unused_secrets,
            "no_rotation_secrets": no_rotation_secrets,
            "replicated_secrets": replicated_secrets,
            "price_source": "See aws_pricing.secretsmanager and Cost Explorer service spend.",
            "cost_note": (
                "Secrets Manager cost is driven by secret count, replicas and API calls. "
                "Unused-secret cleanup should be validated against application ownership and rotation requirements."
            ),
        }

    def _normalize_secret(self, item: dict[str, Any]) -> dict:
        name = str(item.get("Name") or "unknown-secret")
        arn = str(item.get("ARN") or name)
        last_accessed = item.get("LastAccessedDate")
        created = item.get("CreatedDate")
        changed = item.get("LastChangedDate")
        deleted = item.get("DeletedDate")
        rotation_enabled = bool(item.get("RotationEnabled"))
        replica_regions = [
            str(rep.get("Region") or "")
            for rep in (item.get("ReplicationStatus") or [])
            if rep.get("Region")
        ]

        last_access_age_days = self._age_days(last_accessed)
        created_age_days = self._age_days(created)
        deleted_pending = deleted is not None
        potentially_unused = (
            not deleted_pending
            and (
                (last_access_age_days is not None and last_access_age_days >= UNUSED_SECRET_DAYS)
                or (last_accessed is None and created_age_days is not None and created_age_days >= UNUSED_SECRET_DAYS)
            )
        )

        return {
            "name": name,
            "arn": arn,
            "description": item.get("Description"),
            "kms_key_id": item.get("KmsKeyId"),
            "owning_service": item.get("OwningService"),
            "created_date": self._iso(created),
            "last_changed_date": self._iso(changed),
            "last_accessed_date": self._iso(last_accessed),
            "deleted_date": self._iso(deleted),
            "created_age_days": created_age_days,
            "last_access_age_days": last_access_age_days,
            "rotation_enabled": rotation_enabled,
            "rotation_lambda_arn": item.get("RotationLambdaARN"),
            "replica_regions": replica_regions,
            "is_pending_deletion": deleted_pending,
            "is_potentially_unused": potentially_unused,
            "price_source": "See aws_pricing.secretsmanager",
        }

    def _age_days(self, value: Any) -> int | None:
        if not value:
            return None
        if isinstance(value, datetime):
            dt = value
        else:
            try:
                dt = datetime.fromisoformat(str(value).replace("+0000", "+00:00"))
            except ValueError:
                return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max((datetime.now(timezone.utc) - dt).days, 0)

    def _iso(self, value: Any) -> str | None:
        if not value:
            return None
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc).isoformat()
        return str(value)
