"""S3 Collector — READ-ONLY."""
from __future__ import annotations

from collector.collectors.base import BaseCollector


class S3Collector(BaseCollector):
    name = "s3"

    async def collect(self) -> dict:
        buckets = []
        buckets_without_lifecycle = []
        buckets_without_intelligent_tiering = []
        buckets_with_multipart_uploads = []

        async with self.session.client("s3") as s3:
            response = await self._safe_call(s3.list_buckets())
            if not response or "_error" in response:
                return {"_error": response.get("_error") if response else "failed"}

            for bucket in response.get("Buckets", []):
                name = bucket["Name"]

                # Check lifecycle policy
                lc = await self._safe_call(s3.get_bucket_lifecycle_configuration(Bucket=name))
                has_lifecycle = bool(lc and "_error" not in lc and lc.get("Rules"))

                # Check intelligent tiering
                it = await self._safe_call(s3.list_bucket_intelligent_tiering_configurations(Bucket=name))
                has_intelligent_tiering = bool(
                    it and "_error" not in it and it.get("IntelligentTieringConfigurationList")
                )

                location = await self._safe_call(s3.get_bucket_location(Bucket=name))
                region = self._normalize_region((location or {}).get("LocationConstraint"))

                versioning = await self._safe_call(s3.get_bucket_versioning(Bucket=name))
                versioning_status = (
                    versioning.get("Status")
                    if versioning and "_error" not in versioning
                    else None
                )

                multipart = await self._safe_call(s3.list_multipart_uploads(Bucket=name))
                multipart_uploads = []
                if multipart and "_error" not in multipart:
                    multipart_uploads = multipart.get("Uploads") or []

                bucket_entry = {
                    "name": name,
                    "region": region,
                    "has_lifecycle": has_lifecycle,
                    "has_intelligent_tiering": has_intelligent_tiering,
                    "versioning_status": versioning_status,
                    "incomplete_multipart_upload_count": len(multipart_uploads),
                }
                buckets.append(bucket_entry)

                if not has_lifecycle:
                    buckets_without_lifecycle.append(name)
                if not has_intelligent_tiering:
                    buckets_without_intelligent_tiering.append(name)
                if multipart_uploads:
                    buckets_with_multipart_uploads.append(bucket_entry)

        total = len(response.get("Buckets", []))
        return {
            "total_buckets": total,
            "buckets": buckets,
            "without_lifecycle_policy": buckets_without_lifecycle,
            "without_lifecycle_count": len(buckets_without_lifecycle),
            "without_intelligent_tiering": buckets_without_intelligent_tiering,
            "with_incomplete_multipart_uploads": buckets_with_multipart_uploads,
            "with_incomplete_multipart_uploads_count": len(buckets_with_multipart_uploads),
        }

    def _normalize_region(self, location: str | None) -> str:
        if not location:
            return "us-east-1"
        if location == "EU":
            return "eu-west-1"
        return str(location)
