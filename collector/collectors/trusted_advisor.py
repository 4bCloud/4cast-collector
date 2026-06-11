"""
Trusted Advisor Collector — READ-ONLY.

IMPORTANT: Requires Business, Enterprise On-Ramp, or Enterprise Support plan.
Accounts without these plans will receive SubscriptionRequiredException.
This collector degrades gracefully — missing plan is logged, not a crash.

Also note: check IDs are validated dynamically against available checks
because not all check IDs are available in all accounts/plans.
"""
from __future__ import annotations
from collector.collectors.base import BaseCollector


class TrustedAdvisorCollector(BaseCollector):
    name = "trusted_advisor"

    # Cost optimization check IDs we want to retrieve.
    # IDs are stable identifiers (safer than names per AWS docs).
    # We validate availability at runtime via describe_trusted_advisor_checks.
    COST_CHECK_IDS = {
        "Qch7DwouX1",  # Low Utilization Amazon EC2 Instances
        "hjLMh88uM8",  # Idle Load Balancers
        "DAvU99Dc4C",  # Unassociated Elastic IP Addresses
        "Z4AUBRNSmz",  # Underutilized Amazon EBS Volumes
        "di4V5KHUMJ",  # Underutilized Amazon Redshift Clusters
        "1iG5NDGVre",  # Amazon RDS Idle DB Instances
        "G31sQ1E9U",   # Amazon EC2 Reserved Instance Lease Expiration
        "7ujm6yhn5t",  # Amazon EC2 Reserved Instance Optimization
    }

    async def collect(self) -> dict:
        # Trusted Advisor API only works in us-east-1
        async with self.session.client("support", region_name="us-east-1") as support:

            # ── Step 1: Validate plan and available checks ─────────────────
            available_checks = await self._safe_call(
                support.describe_trusted_advisor_checks(language="en")
            )

            if not available_checks or "_error" in available_checks:
                error_msg = (available_checks or {}).get("_error", "unknown error")
                # SubscriptionRequiredException means no Business/Enterprise plan
                if "SubscriptionRequiredException" in str(error_msg) or "Subscription" in str(error_msg):
                    return {
                        "_warning": (
                            "Trusted Advisor checks are not available for this account. "
                            "A Business, Enterprise On-Ramp, or Enterprise Support plan is required. "
                            "See: https://aws.amazon.com/premiumsupport/plans/"
                        ),
                        "findings": [],
                        "plan_required": True,
                    }
                return {"_error": error_msg, "findings": []}

            # Build set of check IDs actually available in this account
            available_ids = {
                c["id"] for c in available_checks.get("checks", [])
                if c.get("category") == "cost_optimizing"
            }

            # Only request checks we want AND that are actually available
            checks_to_run = self.COST_CHECK_IDS & available_ids
            if not checks_to_run:
                return {
                    "_warning": "No cost optimization Trusted Advisor checks available for this account.",
                    "findings": [],
                }

            # ── Step 2: Retrieve results for each available check ──────────
            findings = []
            for check_id in checks_to_run:
                result = await self._safe_call(
                    support.describe_trusted_advisor_check_result(
                        checkId=check_id, language="en"
                    )
                )
                if result and "_error" not in result:
                    check_result = result.get("result", {})
                    if check_result.get("status") in ("warning", "error"):
                        findings.append({
                            "check_id": check_id,
                            "status": check_result.get("status"),
                            "resources_flagged": check_result.get("resourcesSummary", {}).get("resourcesFlagged", 0),
                            "estimated_monthly_savings": self._extract_savings(check_result),
                            "flagged_resources": check_result.get("flaggedResources", [])[:20],
                        })

        return {
            "findings": findings,
            "total_checks_run": len(checks_to_run),
            "total_checks_with_issues": len(findings),
        }

    def _extract_savings(self, check_result: dict) -> float:
        """Try to extract estimated monthly savings from Trusted Advisor metadata."""
        try:
            for resource in check_result.get("flaggedResources", []):
                metadata = resource.get("metadata", [])
                if metadata:
                    for val in reversed(metadata):
                        if val and "$" in str(val):
                            cleaned = str(val).replace("$", "").replace(",", "").strip()
                            try:
                                return float(cleaned)
                            except ValueError:
                                continue
        except Exception:
            pass
        return 0.0
