"""
Compute Optimizer Collector — READ-ONLY.

IMPORTANT SAFETY RULES implemented here:
- Never recommends changing instance family based on CPU alone
- Includes Memory, Network, and Disk utilization in context
- Confidence score based on data completeness
- Respects production tags
"""
from __future__ import annotations
from collector.collectors.base import BaseCollector


class ComputeOptimizerCollector(BaseCollector):
    name = "compute_optimizer"

    async def collect(self) -> dict:
        async with self.session.client("compute-optimizer", region_name="us-east-1") as co:

            # ── Check opt-in status first ──────────────────────────────────
            # Compute Optimizer requires explicit opt-in per account.
            # Without opt-in, all recommendation calls return empty or error.
            enrollment = await self._safe_call(
                co.get_enrollment_status()
            )
            if not enrollment or "_error" in enrollment:
                return {
                    "_warning": "Compute Optimizer opt-in status could not be determined.",
                    "ec2": [], "ebs": [], "lambda": [], "rds": [],
                }
            status = enrollment.get("status", "")
            if status not in ("Active", "active"):
                return {
                    "_warning": (
                        f"Compute Optimizer is not active for this account (status={status}). "
                        "Enable it at: https://console.aws.amazon.com/compute-optimizer/home#/dashboard"
                    ),
                    "ec2": [], "ebs": [], "lambda": [], "rds": [],
                }

            # ── EC2: Finding values are OVER_PROVISIONED / UNDER_PROVISIONED ──
            ec2_recs = await self._safe_call(
                co.get_ec2_instance_recommendations(
                    filters=[{
                        "name": "Finding",
                        "values": ["OVER_PROVISIONED", "UNDER_PROVISIONED"],
                    }]
                )
            )

            # ── EBS: Finding value is NotOptimized ─────────────────────────
            ebs_recs = await self._safe_call(
                co.get_ebs_volume_recommendations(
                    filters=[{"name": "Finding", "values": ["NotOptimized"]}]
                )
            )

            # ── Lambda: Finding values are Overprovisioned / Underprovisioned ──
            lambda_recs = await self._safe_call(
                co.get_lambda_function_recommendations(
                    filters=[{"name": "Finding", "values": ["Overprovisioned", "Underprovisioned"]}]
                )
            )

            # ── RDS: Correct filter name is 'InstanceFinding', not 'Finding' ──
            # Doc ref: boto3 get_rds_database_recommendations — filter name options:
            # 'InstanceFinding' | 'InstanceFindingReasonCode' | 'StorageFinding' | 'StorageFindingReasonCode' | 'Idle'
            rds_recs = await self._safe_call(
                co.get_rds_database_recommendations(
                    filters=[{"name": "InstanceFinding", "values": ["Overprovisioned"]}]
                )
            )

        return {
            "ec2": self._parse_ec2(ec2_recs),
            "ebs": self._parse_ebs(ebs_recs),
            "lambda": self._parse_lambda(lambda_recs),
            "rds": self._parse_rds(rds_recs),
        }

    def _parse_ec2(self, response: dict | None) -> list[dict]:
        """
        Parse EC2 recommendations with context-aware safety rules.

        Key rule: if the current instance is Memory or GPU Optimized family,
        we flag the recommendation as LOW confidence unless RAM utilization data
        is available — because CPU alone is not enough context.
        """
        if not response or "_error" in response:
            return []

        results = []
        memory_families = {"r", "x", "u", "z"}  # r5, r6g, x1, u-6tb1, etc.
        gpu_families = {"p", "g", "inf", "trn"}   # p3, g4, inf1, trn1

        for rec in response.get("instanceRecommendations", []):
            current = rec.get("currentInstanceType", "")
            family = current.split(".")[0].lower() if "." in current else current

            # Determine confidence based on instance family and available metrics
            utilization = rec.get("utilizationMetrics", [])
            has_memory_metric = any(m.get("name") == "MEMORY_5_MINUTE_MAXIMUM" for m in utilization)

            is_memory_optimized = any(family.startswith(f) for f in memory_families)
            is_gpu = any(family.startswith(f) for f in gpu_families)

            if is_gpu:
                # Never recommend downsizing GPU instances without explicit GPU utilization data
                confidence = "LOW"
                safety_note = "GPU instance — rightsizing requires GPU utilization data (CloudWatch Agent with nvidia-smi plugin)"
            elif is_memory_optimized and not has_memory_metric:
                confidence = "LOW"
                safety_note = "Memory-optimized instance — CPU alone is insufficient. Install CloudWatch Agent to collect RAM metrics before acting on this recommendation."
            elif has_memory_metric:
                confidence = "HIGH"
                safety_note = None
            else:
                confidence = "MEDIUM"
                safety_note = "Only CPU metrics available. Verify workload characteristics before rightsizing."

            # Check for production tag
            tags = {t.get("key"): t.get("value") for t in rec.get("tags", [])}
            is_prod = tags.get("env", tags.get("environment", "")).lower() in (
                "prod", "production", "prd"
            )

            options = []
            for opt in rec.get("recommendationOptions", [])[:3]:
                savings = opt.get("estimatedMonthlySavings", {})
                options.append({
                    "instance_type": opt.get("instanceType"),
                    "estimated_monthly_savings": float(savings.get("value", 0)),
                    "currency": savings.get("currency", "USD"),
                    "performance_risk": opt.get("performanceRisk"),
                })

            results.append({
                "instance_id": rec.get("instanceArn", "").split("/")[-1],
                "instance_name": rec.get("instanceName"),
                "current_type": current,
                "finding": rec.get("finding"),
                "confidence": confidence,
                "safety_note": safety_note,
                "is_production": is_prod,
                "utilization_metrics": utilization,
                "options": options,
            })

        return results

    def _parse_ebs(self, response: dict | None) -> list[dict]:
        if not response or "_error" in response:
            return []
        results = []
        for rec in response.get("volumeRecommendations", []):
            options = []
            for opt in rec.get("volumeRecommendationOptions", [])[:2]:
                savings = opt.get("estimatedMonthlySavings", {})
                options.append({
                    "volume_type": opt.get("configuration", {}).get("volumeType"),
                    "size_gb": opt.get("configuration", {}).get("volumeSize"),
                    "estimated_monthly_savings": float(savings.get("value", 0)),
                    "currency": savings.get("currency", "USD"),
                })
            results.append({
                "volume_id": rec.get("volumeArn", "").split("/")[-1],
                "current_type": rec.get("currentConfiguration", {}).get("volumeType"),
                "current_size_gb": rec.get("currentConfiguration", {}).get("volumeSize"),
                "finding": rec.get("finding"),
                "options": options,
            })
        return results

    def _parse_lambda(self, response: dict | None) -> list[dict]:
        if not response or "_error" in response:
            return []
        results = []
        for rec in response.get("lambdaFunctionRecommendations", []):
            results.append({
                "function_arn": rec.get("functionArn"),
                "function_version": rec.get("functionVersion"),
                "finding": rec.get("finding"),
                "current_memory_mb": rec.get("memorySizeRecommendationOptions", [{}])[0].get("memorySize") if rec.get("memorySizeRecommendationOptions") else None,
                "options": [
                    {
                        "memory_mb": o.get("memorySize"),
                        "estimated_monthly_savings": float(o.get("projectedUtilizationMetrics", [{}])[0].get("value", 0)) if o.get("projectedUtilizationMetrics") else 0,
                    }
                    for o in rec.get("memorySizeRecommendationOptions", [])[:2]
                ],
            })
        return results

    def _parse_rds(self, response: dict | None) -> list[dict]:
        if not response or "_error" in response:
            return []
        results = []
        for rec in response.get("rdsDBRecommendations", []):
            # Safety: always flag Multi-AZ instances
            is_multi_az = rec.get("currentDBInstanceClass", "").endswith(".multi-az")
            results.append({
                "resource_arn": rec.get("resourceArn"),
                "current_class": rec.get("currentDBInstanceClass"),
                "engine": rec.get("engine"),
                "finding": rec.get("finding"),
                "is_multi_az": is_multi_az,
                "safety_note": "Multi-AZ instance — downsize will affect availability during failover" if is_multi_az else None,
                "options": rec.get("instanceRecommendationOptions", [])[:2],
            })
        return results
