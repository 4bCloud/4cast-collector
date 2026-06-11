"""
Savings Plans Collector — READ-ONLY (GLOBAL).

Analyzes Savings Plans coverage and purchase opportunities:
- Current coverage percentage (what % of eligible spend is covered)
- Utilization (are existing SPs being fully used?)
- Purchase recommendations from AWS Cost Explorer
- On-Demand spend eligible for Savings Plans not yet covered

IAM required: ce:GetSavingsPlansUtilization,
              ce:GetSavingsPlansCoverage,
              ce:GetSavingsPlansPurchaseRecommendation
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from collector.collectors.base import BaseCollector


class SavingsPlansCollector(BaseCollector):
    name = "savings_plans"
    PAYMENT_OPTIONS = ["NO_UPFRONT", "PARTIAL_UPFRONT", "ALL_UPFRONT"]
    TERMS = ["ONE_YEAR", "THREE_YEARS"]
    SAVINGS_PLAN_TYPES = ["COMPUTE_SP", "EC2_INSTANCE_SP"]
    RESERVED_INSTANCE_SERVICES = [
        "Amazon Elastic Compute Cloud - Compute",
        "Amazon Relational Database Service",
        "Amazon ElastiCache",
        "Amazon Redshift",
        "Amazon OpenSearch Service",
    ]

    def set_active_regions(self, regions: list[str]) -> None:
        self.active_regions = [region for region in regions if region]

    async def collect(self) -> dict:
        end_date   = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=30)
        start_str  = start_date.isoformat()
        end_str    = end_date.isoformat()

        utilization   = {}
        coverage      = {}
        recommendations = []
        commitment_options = []
        errors = []

        async with self.session.client("ce", region_name="us-east-1") as ce:

            # ── Utilization — are existing SPs being used? ─────────────────
            util_resp = await self._safe_call(
                ce.get_savings_plans_utilization(
                    TimePeriod={"Start": start_str, "End": end_str},
                )
            )
            if util_resp and not util_resp.get("_error"):
                total = util_resp.get("Total", {})
                utilization = {
                    "utilized_hours":    total.get("UtilizationPercentage", "0"),
                    "total_commitment":  total.get("TotalCommitment", {}).get("Amount", "0"),
                    "used_commitment":   total.get("UsedCommitment", {}).get("Amount", "0"),
                    "unused_commitment": total.get("UnusedCommitment", {}).get("Amount", "0"),
                    "net_savings":       total.get("NetSavings", {}).get("Amount", "0"),
                }

            # ── Coverage — what % of eligible spend is covered? ────────────
            cov_resp = await self._safe_call(
                ce.get_savings_plans_coverage(
                    TimePeriod={"Start": start_str, "End": end_str},
                    Granularity="MONTHLY",
                )
            )
            if cov_resp and not cov_resp.get("_error"):
                coverages = cov_resp.get("SavingsPlansCoverages", [])
                if coverages:
                    cov = coverages[-1].get("Coverage", {})
                    coverage = {
                        "coverage_percentage":    cov.get("CoveragePercentage", "0"),
                        "on_demand_cost":         cov.get("OnDemandCost", "0"),
                        "spend_covered_by_sp":    cov.get("SpendCoveredBySavingsPlans", "0"),
                        "total_cost":             cov.get("TotalCost", "0"),
                    }

            recommendation_regions = self._recommendation_regions()

            # ── Savings Plans purchase recommendations ─────────────────────
            for region in recommendation_regions:
                for sp_type in self.SAVINGS_PLAN_TYPES:
                    for term in self.TERMS:
                        for payment in self.PAYMENT_OPTIONS:
                            rec_resp = await self._safe_call(
                                self._get_savings_plan_recommendation(
                                    ce,
                                    self._savings_plan_request(
                                        sp_type=sp_type,
                                        term=term,
                                        payment=payment,
                                        region=region,
                                    ),
                                )
                            )
                            if rec_resp and rec_resp.get("_error"):
                                errors.append(
                                    {
                                        "type": "SAVINGS_PLAN",
                                        "plan_type": sp_type,
                                        "term": term,
                                        "payment": payment,
                                        "region": region,
                                        "error": rec_resp.get("_error"),
                                    }
                                )
                                continue
                            for option in self._savings_plan_options(
                                sp_type, term, payment, rec_resp or {}, region=region
                            ):
                                commitment_options.append(option)
                                if sp_type == "COMPUTE_SP":
                                    recommendations.append(option)

            # ── Reserved Instance purchase recommendations ──────────────────
            for region in recommendation_regions:
                for service in self.RESERVED_INSTANCE_SERVICES:
                    for term in self.TERMS:
                        for payment in self.PAYMENT_OPTIONS:
                            rec_resp = await self._safe_call(
                                self._get_reservation_recommendation(
                                    ce,
                                    self._reservation_request(
                                        service=service,
                                        term=term,
                                        payment=payment,
                                        region=region,
                                    ),
                                )
                            )
                            if rec_resp and rec_resp.get("_error"):
                                errors.append(
                                    {
                                        "type": "RESERVED_INSTANCE",
                                        "service": service,
                                        "term": term,
                                        "payment": payment,
                                        "region": region,
                                        "error": rec_resp.get("_error"),
                                    }
                                )
                                continue
                            for option in self._reservation_options(
                                service, term, payment, rec_resp or {}, region=region
                            ):
                                commitment_options.append(option)

        # Determine coverage quality
        cov_pct = float(coverage.get("coverage_percentage", 0))
        util_pct = float(utilization.get("utilized_hours", 0))
        unused = float(utilization.get("unused_commitment", "0"))

        return {
            "utilization":      utilization,
            "coverage":         coverage,
            "recommendations":  recommendations,
            "commitment_options": sorted(
                commitment_options,
                key=lambda item: float(item.get("estimated_monthly_savings", 0) or 0),
                reverse=True,
            ),
            "recommendation_errors": errors[:10],
            "recommendation_source": "AWS Cost Explorer purchase recommendations",
            "recommendation_lookback_days": 30,
            "recommendation_regions": recommendation_regions,
            "unsupported_terms_note": "AWS Cost Explorer purchase recommendations support 1-year and 3-year terms; 2-year terms are not offered by these APIs.",
            "coverage_quality": (
                "GOOD"   if cov_pct >= 80 else
                "MEDIUM" if cov_pct >= 50 else
                "POOR"
            ),
            "has_unused_commitment":  unused > 10,
            "unused_commitment_note": (
                f"${unused:.2f}/month in purchased Savings Plans is going unused — "
                "you're paying for capacity you're not using."
                if unused > 10 else None
            ),
            "coverage_note": (
                f"Only {cov_pct:.0f}% of eligible On-Demand spend is covered by Savings Plans. "
                "Consider purchasing additional coverage to reduce costs."
                if cov_pct < 70 else None
            ),
        }

    def _recommendation_regions(self) -> list[str | None]:
        # Cost Explorer purchase recommendation APIs do not currently accept
        # REGION as a request filter for these recommendation calls. Keep the
        # AWS quote intact and split only when the response details include
        # region-level data.
        return [None]

    async def _get_savings_plan_recommendation(self, ce, request: dict) -> dict:
        merged: dict = {}
        next_token = None
        while True:
            page_request = dict(request)
            page_request.setdefault("PageSize", 1000)
            if next_token:
                page_request["NextPageToken"] = next_token
            page = await ce.get_savings_plans_purchase_recommendation(**page_request)
            if not merged:
                merged = page
            else:
                current = merged.setdefault("SavingsPlansPurchaseRecommendation", {})
                incoming = page.get("SavingsPlansPurchaseRecommendation") or {}
                current.setdefault(
                    "SavingsPlansPurchaseRecommendationSummary",
                    incoming.get("SavingsPlansPurchaseRecommendationSummary") or {},
                )
                current.setdefault("SavingsPlansPurchaseRecommendationDetails", []).extend(
                    incoming.get("SavingsPlansPurchaseRecommendationDetails") or []
                )
            next_token = page.get("NextPageToken")
            if not next_token:
                return merged

    async def _get_reservation_recommendation(self, ce, request: dict) -> dict:
        merged = {"Recommendations": []}
        next_token = None
        while True:
            page_request = dict(request)
            page_request.setdefault("PageSize", 1000)
            if next_token:
                page_request["NextPageToken"] = next_token
            page = await ce.get_reservation_purchase_recommendation(**page_request)
            merged["Recommendations"].extend(page.get("Recommendations") or [])
            next_token = page.get("NextPageToken")
            if not next_token:
                return merged

    def _region_filter(self, region: str | None) -> dict | None:
        if not region:
            return None
        return {"Dimensions": {"Key": "REGION", "Values": [region]}}

    def _savings_plan_request(
        self,
        *,
        sp_type: str,
        term: str,
        payment: str,
        region: str | None,
    ) -> dict:
        request = {
            "SavingsPlansType": sp_type,
            "TermInYears": term,
            "PaymentOption": payment,
            "LookbackPeriodInDays": "THIRTY_DAYS",
            "AccountScope": "LINKED",
        }
        return request

    def _reservation_request(
        self,
        *,
        service: str,
        term: str,
        payment: str,
        region: str | None,
    ) -> dict:
        request = {
            "Service": service,
            "TermInYears": term,
            "PaymentOption": payment,
            "LookbackPeriodInDays": "THIRTY_DAYS",
            "AccountScope": "LINKED",
        }
        return request

    def _savings_plan_option(
        self,
        sp_type: str,
        term: str,
        payment: str,
        response: dict,
        region: str | None = None,
    ) -> dict | None:
        options = self._savings_plan_options(sp_type, term, payment, response, region=region)
        if not options:
            return None
        if len(options) == 1:
            return options[0]
        return self._aggregate_savings_plan_options(sp_type, term, payment, options)

    def _savings_plan_options(
        self,
        sp_type: str,
        term: str,
        payment: str,
        response: dict,
        region: str | None = None,
    ) -> list[dict]:
        recommendation = response.get("SavingsPlansPurchaseRecommendation") or response
        summary = recommendation.get("SavingsPlansPurchaseRecommendationSummary") or {}
        if not summary:
            return []

        details = recommendation.get("SavingsPlansPurchaseRecommendationDetails") or []
        grouped = self._group_savings_plan_details(details)
        if grouped:
            options = [
                option
                for option in (
                    self._savings_plan_option_from_details(
                        sp_type,
                        term,
                        payment,
                        summary,
                        region,
                        region_details,
                    )
                    for region, region_details in grouped.items()
                )
                if option
            ]
            if options:
                return options

        monthly_savings = self._money(summary.get("EstimatedMonthlySavingsAmount"))
        hourly_commitment = self._money(summary.get("HourlyCommitmentToPurchase"))
        upfront = sum(self._money(d.get("UpfrontCost")) for d in details)
        recurring = self._money(summary.get("EstimatedTotalCost"))
        regions = sorted(
            {
                str((d.get("SavingsPlansDetails") or {}).get("Region"))
                for d in details
                if (d.get("SavingsPlansDetails") or {}).get("Region")
            }
        )
        if region and not regions:
            regions = [region]
        account_ids = sorted({str(d.get("AccountId")) for d in details if d.get("AccountId")})
        if monthly_savings <= 0 and hourly_commitment <= 0:
            return []

        return [{
            "type": "SAVINGS_PLAN",
            "service": self._sp_type_label(sp_type),
            "plan_type": sp_type,
            "term": term,
            "term_label": self._term_label(term),
            "payment": payment,
            "payment_label": self._payment_label(payment),
            "hourly_commitment": hourly_commitment,
            "upfront_cost": upfront,
            "estimated_monthly_commitment": recurring,
            "estimated_monthly_savings": monthly_savings,
            "estimated_annual_savings": round(monthly_savings * 12, 2),
            "estimated_roi": self._money(summary.get("EstimatedROI")),
            "current_on_demand_spend": self._money(summary.get("CurrentOnDemandSpend")),
            "account_ids": account_ids,
            "regions": regions,
            "region_scope": "regional_filter" if region else "aws_summary",
            "recommended_count": int(float(summary.get("TotalRecommendationCount") or len(details) or 0)),
            "currency": summary.get("CurrencyCode") or "USD",
        }]

    def _group_savings_plan_details(self, details: list[dict]) -> dict[str, list[dict]]:
        grouped: dict[str, list[dict]] = {}
        for detail in details:
            region = str((detail.get("SavingsPlansDetails") or {}).get("Region") or "").strip()
            if not region:
                continue
            grouped.setdefault(region, []).append(detail)
        if len(grouped) <= 1:
            return {}
        return grouped

    def _savings_plan_option_from_details(
        self,
        sp_type: str,
        term: str,
        payment: str,
        summary: dict,
        region: str,
        details: list[dict],
    ) -> dict | None:
        monthly_savings = round(
            sum(self._money(d.get("EstimatedMonthlySavingsAmount")) for d in details),
            2,
        )
        hourly_commitment = round(
            sum(self._money(d.get("HourlyCommitmentToPurchase")) for d in details),
            3,
        )
        current_on_demand = round(
            sum(
                self._money(
                    d.get("CurrentOnDemandSpend")
                    or d.get("CurrentMonthlyOnDemandSpend")
                    or d.get("EstimatedMonthlyOnDemandCost")
                )
                for d in details
            )
            or sum(self._money(d.get("CurrentAverageHourlyOnDemandSpend")) for d in details) * 730,
            2,
        )
        upfront = round(sum(self._money(d.get("UpfrontCost")) for d in details), 2)
        recurring = round(sum(self._money(d.get("EstimatedTotalCost")) for d in details), 2)
        if recurring <= 0 and current_on_demand > 0 and monthly_savings > 0:
            recurring = round(max(current_on_demand - monthly_savings, 0), 2)
        if hourly_commitment <= 0 and recurring > 0:
            hourly_commitment = round(recurring / 730, 3)

        if monthly_savings <= 0 and hourly_commitment <= 0:
            return None

        return {
            "type": "SAVINGS_PLAN",
            "service": self._sp_type_label(sp_type),
            "plan_type": sp_type,
            "term": term,
            "term_label": self._term_label(term),
            "payment": payment,
            "payment_label": self._payment_label(payment),
            "hourly_commitment": hourly_commitment,
            "upfront_cost": upfront,
            "estimated_monthly_commitment": recurring,
            "estimated_monthly_savings": monthly_savings,
            "estimated_annual_savings": round(monthly_savings * 12, 2),
            "estimated_roi": self._money(summary.get("EstimatedROI")),
            "current_on_demand_spend": current_on_demand,
            "account_ids": sorted({str(d.get("AccountId")) for d in details if d.get("AccountId")}),
            "regions": [region],
            "region_scope": "aws_detail",
            "recommended_count": int(float(len(details) or 0)),
            "currency": summary.get("CurrencyCode") or "USD",
        }

    def _aggregate_savings_plan_options(
        self,
        sp_type: str,
        term: str,
        payment: str,
        options: list[dict],
    ) -> dict:
        monthly_savings = round(
            sum(float(option.get("estimated_monthly_savings") or 0) for option in options),
            2,
        )
        upfront = round(sum(float(option.get("upfront_cost") or 0) for option in options), 2)
        recurring = round(
            sum(float(option.get("estimated_monthly_commitment") or 0) for option in options),
            2,
        )
        hourly = round(sum(float(option.get("hourly_commitment") or 0) for option in options), 3)
        current_on_demand = round(
            sum(float(option.get("current_on_demand_spend") or 0) for option in options),
            2,
        )
        regions = sorted({r for option in options for r in option.get("regions", [])})
        account_ids = sorted({a for option in options for a in option.get("account_ids", [])})
        return {
            "type": "SAVINGS_PLAN",
            "service": self._sp_type_label(sp_type),
            "plan_type": sp_type,
            "term": term,
            "term_label": self._term_label(term),
            "payment": payment,
            "payment_label": self._payment_label(payment),
            "hourly_commitment": hourly,
            "upfront_cost": upfront,
            "estimated_monthly_commitment": recurring,
            "estimated_monthly_savings": monthly_savings,
            "estimated_annual_savings": round(monthly_savings * 12, 2),
            "estimated_roi": round((monthly_savings * 12 / upfront * 100), 2) if upfront > 0 else 0.0,
            "current_on_demand_spend": current_on_demand,
            "account_ids": account_ids,
            "regions": regions,
            "recommended_count": sum(int(option.get("recommended_count") or 0) for option in options),
            "regional_breakdown": options,
            "currency": options[0].get("currency") or "USD",
        }

    def _reservation_option(
        self,
        service: str,
        term: str,
        payment: str,
        response: dict,
        region: str | None = None,
    ) -> dict | None:
        options = self._reservation_options(service, term, payment, response, region=region)
        if not options:
            return None
        if len(options) == 1:
            return options[0]
        return self._aggregate_reservation_options(service, term, payment, options)

    def _reservation_options(
        self,
        service: str,
        term: str,
        payment: str,
        response: dict,
        region: str | None = None,
    ) -> list[dict]:
        recommendations = response.get("Recommendations") or []
        if not recommendations:
            return []

        details = []
        for recommendation in recommendations:
            details.extend(recommendation.get("RecommendationDetails") or [])

        if not details:
            return []

        grouped: dict[str, list[dict]] = {}
        for detail in details:
            region = self._region_from_reservation_detail(detail) or "global"
            reservation_type = self._reservation_type_label(detail) or "General reservation"
            key = f"{region}|{reservation_type}"
            grouped.setdefault(key, []).append(detail)

        return [
            option
            for option in (
                self._reservation_option_from_details(
                    service,
                    term,
                    payment,
                    self._reservation_type_label(group[0]) or "General reservation",
                    group,
                    region=region,
                )
                for group in grouped.values()
            )
            if option
        ]

    def _reservation_option_from_details(
        self,
        service: str,
        term: str,
        payment: str,
        reservation_type: str,
        details: list[dict],
        region: str | None = None,
    ) -> dict | None:
        monthly_savings = round(
            sum(self._money(detail.get("EstimatedMonthlySavingsAmount")) for detail in details),
            2,
        )
        upfront = round(sum(self._money(detail.get("UpfrontCost")) for detail in details), 2)
        recurring = round(
            sum(self._money(detail.get("RecurringStandardMonthlyCost")) for detail in details),
            2,
        )
        current_on_demand = round(
            sum(self._money(detail.get("EstimatedMonthlyOnDemandCost")) for detail in details),
            2,
        )
        regions = sorted(
            {
                region
                for detail in details
                if (region := self._region_from_reservation_detail(detail))
            }
        )
        reservation_types = sorted(
            {
                label
                for detail in details
                if (label := self._reservation_type_label(detail))
            }
        )
        account_ids = sorted({str(d.get("AccountId")) for d in details if d.get("AccountId")})
        recommended_count = int(
            sum(float(detail.get("RecommendedNumberOfInstancesToPurchase") or 0) for detail in details)
            or sum(float(detail.get("RecommendedNumberOfCapacityUnitsToPurchase") or 0) for detail in details)
            or len(details)
        )
        if monthly_savings <= 0 and upfront <= 0 and recurring <= 0:
            return None
        if region and not regions:
            regions = [region]

        return {
            "type": "RESERVED_INSTANCE",
            "service": service,
            "term": term,
            "term_label": self._term_label(term),
            "payment": payment,
            "payment_label": self._payment_label(payment),
            "upfront_cost": upfront,
            "estimated_monthly_commitment": recurring,
            "estimated_monthly_savings": monthly_savings,
            "estimated_annual_savings": round(monthly_savings * 12, 2),
            "estimated_roi": round((monthly_savings * 12 / upfront * 100), 2) if upfront > 0 else 0.0,
            "current_on_demand_spend": current_on_demand,
            "recommended_count": recommended_count,
            "account_ids": account_ids,
            "regions": regions,
            "region_scope": "regional_filter" if region else "aws_detail",
            "reservation_type": reservation_type,
            "reservation_types": reservation_types or [reservation_type],
            "currency": (
                details[0].get("CurrencyCode")
                if details and details[0].get("CurrencyCode")
                else "USD"
            ),
        }

    def _aggregate_reservation_options(
        self,
        service: str,
        term: str,
        payment: str,
        options: list[dict],
    ) -> dict:
        monthly_savings = round(
            sum(float(option.get("estimated_monthly_savings") or 0) for option in options),
            2,
        )
        upfront = round(sum(float(option.get("upfront_cost") or 0) for option in options), 2)
        recurring = round(
            sum(float(option.get("estimated_monthly_commitment") or 0) for option in options),
            2,
        )
        current_on_demand = round(
            sum(float(option.get("current_on_demand_spend") or 0) for option in options),
            2,
        )
        regions = sorted({r for option in options for r in option.get("regions", [])})
        account_ids = sorted({a for option in options for a in option.get("account_ids", [])})
        reservation_types = sorted(
            {t for option in options for t in option.get("reservation_types", [])}
        )
        return {
            "type": "RESERVED_INSTANCE",
            "service": service,
            "term": term,
            "term_label": self._term_label(term),
            "payment": payment,
            "payment_label": self._payment_label(payment),
            "upfront_cost": upfront,
            "estimated_monthly_commitment": recurring,
            "estimated_monthly_savings": monthly_savings,
            "estimated_annual_savings": round(monthly_savings * 12, 2),
            "estimated_roi": round((monthly_savings * 12 / upfront * 100), 2) if upfront > 0 else 0.0,
            "current_on_demand_spend": current_on_demand,
            "recommended_count": sum(int(option.get("recommended_count") or 0) for option in options),
            "account_ids": account_ids,
            "regions": regions,
            "reservation_types": reservation_types,
            "reservation_breakdown": options,
            "currency": options[0].get("currency") or "USD",
        }

    def _region_from_reservation_detail(self, detail: dict) -> str | None:
        instance_details = detail.get("InstanceDetails") or {}
        for key in (
            "EC2InstanceDetails",
            "RDSInstanceDetails",
            "RedshiftInstanceDetails",
            "ElastiCacheInstanceDetails",
            "ESInstanceDetails",
            "MemoryDBInstanceDetails",
        ):
            region = (instance_details.get(key) or {}).get("Region")
            if region:
                return str(region)
        reserved_capacity = detail.get("ReservedCapacityDetails") or {}
        region = (reserved_capacity.get("DynamoDBCapacityDetails") or {}).get("Region")
        return str(region) if region else None

    def _reservation_type_label(self, detail: dict) -> str | None:
        instance_details = detail.get("InstanceDetails") or {}
        detail_fields = (
            ("EC2InstanceDetails", ("InstanceType", "Family", "Platform", "Tenancy")),
            (
                "RDSInstanceDetails",
                ("InstanceType", "Family", "DatabaseEngine", "DeploymentOption", "LicenseModel"),
            ),
            ("RedshiftInstanceDetails", ("NodeType", "Family")),
            ("ElastiCacheInstanceDetails", ("NodeType", "ProductDescription")),
            ("ESInstanceDetails", ("InstanceClass", "InstanceSize")),
            ("MemoryDBInstanceDetails", ("NodeType",)),
        )
        for key, fields in detail_fields:
            values = [
                str(value)
                for field in fields
                if (value := (instance_details.get(key) or {}).get(field))
            ]
            if values:
                return " / ".join(values)
        return None

    def _money(self, value: object) -> float:
        try:
            return round(float(value or 0), 2)
        except (TypeError, ValueError):
            return 0.0

    def _term_label(self, term: str) -> str:
        return "1 year" if term == "ONE_YEAR" else "3 years"

    def _payment_label(self, payment: str) -> str:
        return {
            "NO_UPFRONT": "No upfront",
            "PARTIAL_UPFRONT": "Partial upfront",
            "ALL_UPFRONT": "All upfront",
        }.get(payment, payment.replace("_", " ").title())

    def _sp_type_label(self, sp_type: str) -> str:
        return {
            "COMPUTE_SP": "Compute Savings Plan",
            "EC2_INSTANCE_SP": "EC2 Instance Savings Plan",
            "SAGEMAKER_SP": "SageMaker Savings Plan",
        }.get(sp_type, sp_type.replace("_", " ").title())
