"""
Cost Explorer Collector — reads cost and usage data.

READ-ONLY:
- Uses GetCostAndUsage only.
- Does not modify any AWS resource.

Important behavior:
- The executive reporting period is the last closed calendar month.
- Tax is excluded by default to match the usual Cost Explorer "Tax excluded" view.
- Current month-to-date is collected separately and must not be mixed with the
  last closed month total.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

from collector.collectors.base import BaseCollector


class CostExplorerCollector(BaseCollector):
    name = "cost_explorer"

    EXCLUDED_SERVICES = ["Tax"]
    USAGE_ATTRIBUTION_SERVICES = [
        "EC2 - Other",
        "Amazon Virtual Private Cloud",
        "Amazon Elastic Load Balancing",
        "Amazon Simple Storage Service",
        "AmazonCloudWatch",
        "AWS Lambda",
        "Amazon Simple Queue Service",
        "Amazon EC2 Container Registry (ECR)",
    ]

    async def collect(self) -> dict:
        today = datetime.now(timezone.utc).date()

        periods = self._build_periods(today)

        # CRITICAL: Filter by this specific account_id.
        # Without this, the management/master account returns costs for ALL
        # linked accounts in the organization (consolidated billing).
        # Each account must query only its own costs.
        ce_filter = self._build_account_filter(self.account_id, self.EXCLUDED_SERVICES)

        async with self.session.client("ce", region_name="us-east-1") as ce:
            # Last closed month — source of truth for executive monthly total
            total_last_closed = await self._safe_call(
                ce.get_cost_and_usage(
                    TimePeriod={
                        "Start": periods["last_closed_month"]["start"],
                        "End": periods["last_closed_month"]["end"],
                    },
                    Granularity="MONTHLY",
                    Metrics=["UnblendedCost"],
                    Filter=ce_filter,
                )
            )

            by_service_last_closed = await self._safe_call(
                ce.get_cost_and_usage(
                    TimePeriod={
                        "Start": periods["last_closed_month"]["start"],
                        "End": periods["last_closed_month"]["end"],
                    },
                    Granularity="MONTHLY",
                    Metrics=["UnblendedCost", "UsageQuantity"],
                    GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}],
                    Filter=ce_filter,
                )
            )

            by_region_last_closed = await self._safe_call(
                ce.get_cost_and_usage(
                    TimePeriod={
                        "Start": periods["last_closed_month"]["start"],
                        "End": periods["last_closed_month"]["end"],
                    },
                    Granularity="MONTHLY",
                    Metrics=["UnblendedCost"],
                    GroupBy=[{"Type": "DIMENSION", "Key": "REGION"}],
                    Filter=ce_filter,
                )
            )

            # Current month-to-date — collected separately for context only.
            # Do not use this as "monthly spend" because the month is not closed.
            total_mtd = {}
            by_service_mtd = {}
            by_region_mtd = {}

            if periods["current_month_to_date"]["enabled"]:
                total_mtd = await self._safe_call(
                    ce.get_cost_and_usage(
                        TimePeriod={
                            "Start": periods["current_month_to_date"]["start"],
                            "End": periods["current_month_to_date"]["end"],
                        },
                        Granularity="MONTHLY",
                        Metrics=["UnblendedCost"],
                        Filter=ce_filter,
                    )
                )

                by_service_mtd = await self._safe_call(
                    ce.get_cost_and_usage(
                        TimePeriod={
                            "Start": periods["current_month_to_date"]["start"],
                            "End": periods["current_month_to_date"]["end"],
                        },
                        Granularity="MONTHLY",
                        Metrics=["UnblendedCost", "UsageQuantity"],
                        GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}],
                        Filter=ce_filter,
                    )
                )

                by_region_mtd = await self._safe_call(
                    ce.get_cost_and_usage(
                        TimePeriod={
                            "Start": periods["current_month_to_date"]["start"],
                            "End": periods["current_month_to_date"]["end"],
                        },
                        Granularity="MONTHLY",
                        Metrics=["UnblendedCost"],
                        GroupBy=[{"Type": "DIMENSION", "Key": "REGION"}],
                        Filter=ce_filter,
                    )
                )

            # Monthly trend — closed months plus current MTD as a separate partial bucket.
            total_trend = await self._safe_call(
                ce.get_cost_and_usage(
                    TimePeriod={
                        "Start": periods["trend"]["start"],
                        "End": periods["trend"]["end"],
                    },
                    Granularity="MONTHLY",
                    Metrics=["UnblendedCost"],
                    Filter=ce_filter,
                )
            )

            services_with_spend = self._service_names_from_response(by_service_last_closed)
            usage_type_last_closed = {}
            for service_name in self.USAGE_ATTRIBUTION_SERVICES:
                if service_name not in services_with_spend:
                    continue
                service_filter = self._build_account_service_filter(
                    self.account_id,
                    self.EXCLUDED_SERVICES,
                    service_name,
                )
                usage_type_last_closed[service_name] = await self._get_cost_and_usage_all_pages(
                    ce,
                    TimePeriod={
                        "Start": periods["last_closed_month"]["start"],
                        "End": periods["last_closed_month"]["end"],
                    },
                    Granularity="MONTHLY",
                    Metrics=["UnblendedCost", "UsageQuantity"],
                    GroupBy=[
                        {"Type": "DIMENSION", "Key": "USAGE_TYPE"},
                        {"Type": "DIMENSION", "Key": "OPERATION"},
                    ],
                    Filter=service_filter,
                )

            currency = self._detect_currency(
                by_service_last_closed,
                fallback_response=total_last_closed,
            )

        last_closed_total = self._parse_total_amount(total_last_closed)
        current_mtd_total = self._parse_total_amount(total_mtd)

        by_service_last_closed_items = self._parse_cost_results(by_service_last_closed)
        by_region_last_closed_items = self._parse_cost_results(by_region_last_closed)
        by_service_mtd_items = self._parse_cost_results(by_service_mtd)
        by_region_mtd_items = self._parse_cost_results(by_region_mtd)
        usage_type_last_closed_items = {
            service_name: self._parse_usage_type_results(response, service_name)
            for service_name, response in usage_type_last_closed.items()
        }

        return {
            "billing_summary": {
                "currency": currency,
                "cost_metric": "UnblendedCost",
                "tax_mode": "excluded",
                "excluded_services": self.EXCLUDED_SERVICES,
                "reporting_period": {
                    "type": "last_closed_month",
                    "start": periods["last_closed_month"]["start"],
                    "end": periods["last_closed_month"]["end"],
                    "display": periods["last_closed_month"]["display"],
                    "note": "AWS Cost Explorer End date is exclusive.",
                },
                "last_closed_month_total": round(last_closed_total, 6),
                "current_month_to_date": {
                    "enabled": periods["current_month_to_date"]["enabled"],
                    "start": periods["current_month_to_date"]["start"],
                    "end": periods["current_month_to_date"]["end"],
                    "display": periods["current_month_to_date"]["display"],
                    "total": round(current_mtd_total, 6),
                    "note": "Partial current month. Do not compare directly with a full month.",
                },
                "service_breakdown_period": {
                    "start": periods["last_closed_month"]["start"],
                    "end": periods["last_closed_month"]["end"],
                    "display": periods["last_closed_month"]["display"],
                    "matches_reporting_period": True,
                    "total_from_services": round(
                        sum(item.get("amount", 0) for item in by_service_last_closed_items),
                        6,
                    ),
                },
                "region_breakdown_period": {
                    "start": periods["last_closed_month"]["start"],
                    "end": periods["last_closed_month"]["end"],
                    "display": periods["last_closed_month"]["display"],
                    "matches_reporting_period": True,
                    "total_from_regions": round(
                        sum(item.get("amount", 0) for item in by_region_last_closed_items),
                        6,
                    ),
                },
            },
            # New explicit fields
            "by_service_last_closed_month": by_service_last_closed_items,
            "by_region_last_closed_month": by_region_last_closed_items,
            "by_service_month_to_date": by_service_mtd_items,
            "by_region_month_to_date": by_region_mtd_items,
            "by_usage_type_last_closed_month": usage_type_last_closed_items,
            "total_trend_monthly": self._parse_trend(total_trend),
            # Backward-compatible aliases used by existing code.
            # They now intentionally point to the last closed month, not a rolling 30d range.
            "by_service_30d": by_service_last_closed_items,
            "by_region_30d": by_region_last_closed_items,
            "by_usage_type_30d": usage_type_last_closed_items,
            "total_trend_90d": self._parse_trend(total_trend),
            "currency": currency,
        }

    def _build_periods(self, today: date) -> dict[str, Any]:
        """Build all Cost Explorer periods using AWS exclusive End dates."""
        current_month_start = today.replace(day=1)

        last_closed_end = current_month_start
        last_closed_start = self._add_months(current_month_start, -1)

        trend_start = self._add_months(current_month_start, -3)
        trend_end = today

        mtd_enabled = today > current_month_start
        mtd_start = current_month_start
        mtd_end = today

        return {
            "last_closed_month": {
                "start": last_closed_start.isoformat(),
                "end": last_closed_end.isoformat(),
                "display": self._display_period(last_closed_start, last_closed_end),
            },
            "current_month_to_date": {
                "enabled": mtd_enabled,
                "start": mtd_start.isoformat(),
                "end": mtd_end.isoformat(),
                "display": self._display_period(mtd_start, mtd_end) if mtd_enabled else None,
            },
            "trend": {
                "start": trend_start.isoformat(),
                "end": trend_end.isoformat(),
                "display": self._display_period(trend_start, trend_end),
            },
        }

    def _add_months(self, d: date, months: int) -> date:
        """Return date moved by whole calendar months, preserving day when possible."""
        month_index = d.month - 1 + months
        year = d.year + month_index // 12
        month = month_index % 12 + 1
        day = min(d.day, self._days_in_month(year, month))
        return date(year, month, day)

    def _days_in_month(self, year: int, month: int) -> int:
        if month == 12:
            next_month = date(year + 1, 1, 1)
        else:
            next_month = date(year, month + 1, 1)
        return (next_month - timedelta(days=1)).day

    def _display_period(self, start: date, exclusive_end: date) -> str:
        """Human display for AWS Cost Explorer's exclusive End date."""
        inclusive_end = exclusive_end - timedelta(days=1)
        return f"{start.isoformat()} to {inclusive_end.isoformat()}"

    def _build_account_filter(self, account_id: str, excluded_services: list[str]) -> dict:
        """
        Build CE filter scoped to this specific account only.
        Without LINKED_ACCOUNT filter, management account returns ALL org costs.
        """
        account_filter = {"Dimensions": {"Key": "LINKED_ACCOUNT", "Values": [account_id]}}
        if not excluded_services:
            return account_filter
        return {"And": [account_filter, self._exclude_services_filter(excluded_services)]}

    def _build_account_service_filter(
        self,
        account_id: str,
        excluded_services: list[str],
        service_name: str,
    ) -> dict:
        filters = [
            {"Dimensions": {"Key": "LINKED_ACCOUNT", "Values": [account_id]}},
            {"Dimensions": {"Key": "SERVICE", "Values": [service_name]}},
        ]
        if excluded_services:
            filters.append(self._exclude_services_filter(excluded_services))
        return {"And": filters}

    def _exclude_services_filter(self, services: list[str]) -> dict:
        """Build Cost Explorer filter excluding services such as Tax."""
        return {
            "Not": {
                "Dimensions": {
                    "Key": "SERVICE",
                    "Values": services,
                }
            }
        }

    def _detect_currency(
        self,
        response: dict | None,
        fallback_response: dict | None = None,
    ) -> str:
        """Extract billing currency from Cost Explorer response."""
        for candidate in (response, fallback_response):
            if not candidate or "_error" in candidate:
                continue

            try:
                results = candidate.get("ResultsByTime", [])
                if not results:
                    continue

                groups = results[0].get("Groups", [])
                if groups:
                    metrics = groups[0].get("Metrics", {})
                    cost_data = metrics.get("UnblendedCost", {})
                    return cost_data.get("Unit", "USD")

                total = results[0].get("Total", {})
                cost_data = total.get("UnblendedCost", {})
                if cost_data:
                    return cost_data.get("Unit", "USD")
            except (KeyError, IndexError, TypeError):
                continue

        return "USD"

    def _parse_total_amount(self, response: dict | None) -> float:
        """Parse non-grouped Cost Explorer total amount."""
        if not response or "_error" in response:
            return 0.0

        total = 0.0
        for period in response.get("ResultsByTime", []):
            cost = period.get("Total", {}).get("UnblendedCost", {})
            total += float(cost.get("Amount", 0) or 0)

        return total

    def _parse_cost_results(self, response: dict | None) -> list[dict]:
        """Parse grouped Cost Explorer results into a clean list."""
        if not response or "_error" in response:
            return []

        items = []
        for period in response.get("ResultsByTime", []):
            for group in period.get("Groups", []):
                cost = group.get("Metrics", {}).get("UnblendedCost", {})
                usage = group.get("Metrics", {}).get("UsageQuantity", {})

                amount = float(cost.get("Amount", 0) or 0)
                if amount == 0:
                    continue

                items.append(
                    {
                        "key": group.get("Keys", ["unknown"])[0],
                        "amount": amount,
                        "unit": cost.get("Unit", "USD"),
                        "usage_quantity": float(usage.get("Amount", 0) or 0),
                        "usage_unit": usage.get("Unit", "N/A"),
                        "period_start": period.get("TimePeriod", {}).get("Start"),
                        "period_end": period.get("TimePeriod", {}).get("End"),
                    }
                )

        return sorted(items, key=lambda x: x["amount"], reverse=True)

    def _parse_usage_type_results(self, response: dict | None, service_name: str) -> list[dict]:
        """Parse Cost Explorer usage type/operation groups for one service."""
        if not response or "_error" in response:
            return []

        items = []
        for period in response.get("ResultsByTime", []):
            for group in period.get("Groups", []):
                keys = group.get("Keys", [])
                cost = group.get("Metrics", {}).get("UnblendedCost", {})
                usage = group.get("Metrics", {}).get("UsageQuantity", {})
                amount = float(cost.get("Amount", 0) or 0)
                if amount == 0:
                    continue
                items.append(
                    {
                        "service": service_name,
                        "usage_type": keys[0] if len(keys) > 0 else "unknown",
                        "operation": keys[1] if len(keys) > 1 else "unknown",
                        "amount": amount,
                        "unit": cost.get("Unit", "USD"),
                        "usage_quantity": float(usage.get("Amount", 0) or 0),
                        "usage_unit": usage.get("Unit", "N/A"),
                        "period_start": period.get("TimePeriod", {}).get("Start"),
                        "period_end": period.get("TimePeriod", {}).get("End"),
                    }
                )

        return sorted(items, key=lambda x: x["amount"], reverse=True)

    def _service_names_from_response(self, response: dict | None) -> set[str]:
        """Return service names with non-zero spend from a CE SERVICE-grouped response."""
        return {
            item["key"]
            for item in self._parse_cost_results(response)
            if float(item.get("amount") or 0) > 0
        }

    async def _get_cost_and_usage_all_pages(self, ce, **kwargs: Any) -> dict:
        """Call GetCostAndUsage and merge paginated ResultsByTime groups."""
        merged: dict | None = None
        next_token = None
        while True:
            params = dict(kwargs)
            if next_token:
                params["NextPageToken"] = next_token
            page = await self._safe_call(ce.get_cost_and_usage(**params))
            if not page or page.get("_error"):
                return page or {}
            if merged is None:
                merged = {k: v for k, v in page.items() if k != "NextPageToken"}
            else:
                for idx, period in enumerate(page.get("ResultsByTime", [])):
                    if idx >= len(merged.get("ResultsByTime", [])):
                        merged.setdefault("ResultsByTime", []).append(period)
                    else:
                        merged["ResultsByTime"][idx].setdefault("Groups", []).extend(
                            period.get("Groups", [])
                        )
            next_token = page.get("NextPageToken")
            if not next_token:
                break
        return merged or {}

    def _parse_trend(self, response: dict | None) -> list[dict]:
        """Parse monthly cost trend."""
        if not response or "_error" in response:
            return []

        trend = []
        for period in response.get("ResultsByTime", []):
            cost = period.get("Total", {}).get("UnblendedCost", {})
            amount = float(cost.get("Amount", 0) or 0)

            trend.append(
                {
                    "period_start": period.get("TimePeriod", {}).get("Start"),
                    "period_end": period.get("TimePeriod", {}).get("End"),
                    "display": self._display_period(
                        date.fromisoformat(period.get("TimePeriod", {}).get("Start")),
                        date.fromisoformat(period.get("TimePeriod", {}).get("End")),
                    ),
                    "amount": amount,
                    "unit": cost.get("Unit", "USD"),
                    "is_partial_current_month": self._is_current_month_period(
                        period.get("TimePeriod", {}).get("Start"),
                        period.get("TimePeriod", {}).get("End"),
                    ),
                }
            )

        return trend

    def _is_current_month_period(self, start: str | None, end: str | None) -> bool:
        if not start or not end:
            return False

        today = datetime.now(timezone.utc).date()
        current_month_start = today.replace(day=1)

        try:
            start_date = date.fromisoformat(start)
            end_date = date.fromisoformat(end)
        except ValueError:
            return False

        return start_date == current_month_start and end_date <= today
