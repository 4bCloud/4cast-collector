"""
ECR Collector — READ-ONLY.

Amazon ECR (Elastic Container Registry) hidden cost:
- Images accumulate indefinitely without lifecycle policies
- Each GB stored = $0.10/month (check aws_pricing)
- CI/CD pushing daily without cleanup = hundreds of GB

IAM required: ecr:DescribeRepositories, ecr:DescribeImages,
              ecr:GetLifecyclePolicy
"""
from __future__ import annotations

from datetime import datetime, timezone

from collector.collectors.base import BaseCollector

OLD_IMAGE_DAYS = 90  # images older than this are candidates for cleanup


class ECRCollector(BaseCollector):
    name = "ecr"

    async def collect(self) -> dict:
        repositories = []
        total_size_bytes = 0

        async with self.session.client("ecr") as ecr:
            paginator = ecr.get_paginator("describe_repositories")
            async for page in paginator.paginate():
                for repo in page.get("repositories", []):
                    enriched = await self._enrich_repo(repo, ecr)
                    repositories.append(enriched)
                    total_size_bytes += enriched.get("total_size_bytes", 0)

        no_lifecycle = [r for r in repositories if not r.get("has_lifecycle_policy")]
        old_images_repos = [r for r in repositories if r.get("old_images_count", 0) > 0]

        return {
            "repositories":           repositories,
            "total_repositories":     len(repositories),
            "total_size_gb":          round(total_size_bytes / 1024**3, 3),
            "repos_without_lifecycle": no_lifecycle,
            "repos_with_old_images":  old_images_repos,
            "price_source":           "See aws_pricing.ecr",
            "recommendation": (
                "Enable ECR lifecycle policies to automatically delete old images. "
                "Common policy: keep last 10 images per tag prefix, "
                "delete untagged images after 1 day."
            ),
        }

    async def _enrich_repo(self, repo: dict, ecr) -> dict:
        repo_name = repo.get("repositoryName", "")
        repo_arn  = repo.get("repositoryArn", "")

        # Check lifecycle policy
        has_lifecycle = False
        try:
            await ecr.get_lifecycle_policy(repositoryName=repo_name)
            has_lifecycle = True
        except Exception:
            pass

        # Get images
        images = []
        total_size = 0
        old_images = []
        old_images_size = 0
        untagged_images = 0
        untagged_images_size = 0
        cutoff = datetime.now(timezone.utc).timestamp() - (OLD_IMAGE_DAYS * 86400)

        try:
            img_paginator = ecr.get_paginator("describe_images")
            async for page in img_paginator.paginate(repositoryName=repo_name):
                for img in page.get("imageDetails", []):
                    pushed_at = img.get("imagePushedAt")
                    size      = img.get("imageSizeInBytes", 0)
                    total_size += size

                    is_old = pushed_at and pushed_at.timestamp() < cutoff
                    is_untagged = not img.get("imageTags")

                    img_entry = {
                        "digest":    img.get("imageDigest", "")[:20],
                        "tags":      img.get("imageTags", []),
                        "size_mb":   round(size / 1024 / 1024, 1),
                        "pushed_at": str(pushed_at),
                        "is_old":    is_old,
                        "is_untagged": is_untagged,
                    }
                    images.append(img_entry)
                    if is_old or is_untagged:
                        old_images.append(img_entry)
                        old_images_size += size
                        if is_untagged:
                            untagged_images += 1
                            untagged_images_size += size
        except Exception:
            pass

        return {
            "repository_name":    repo_name,
            "repository_uri":     repo.get("repositoryUri", ""),
            "repository_arn":     repo_arn,
            "region":             repo_arn.split(":")[3] if repo_arn and ":" in repo_arn else "",
            "registry_id":        repo.get("registryId", ""),
            "total_images":       len(images),
            "total_size_bytes":   total_size,
            "total_size_gb":      round(total_size / 1024**3, 3),
            "has_lifecycle_policy": has_lifecycle,
            "old_images_count":   len(old_images),
            "old_images_size_gb":  round(old_images_size / 1024**3, 3),
            "untagged_images_count": untagged_images,
            "untagged_images_size_gb": round(untagged_images_size / 1024**3, 3),
            "old_images":         old_images,
            "encryption":         repo.get("encryptionConfiguration", {}).get("encryptionType"),
            "price_source":       "See aws_pricing.ecr",
        }
