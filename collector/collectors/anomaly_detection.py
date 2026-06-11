"""Anomaly Detection Collector — READ-ONLY."""
from __future__ import annotations
from datetime import date, timedelta
from collector.collectors.base import BaseCollector


class AnomalyDetectionCollector(BaseCollector):
    name = "anomaly_detection"

    async def collect(self) -> dict:
        today = date.today()
        start = (today - timedelta(days=90)).isoformat()
        end = today.isoformat()

        async with self.session.client("ce", region_name="us-east-1") as ce:
            anomalies = await self._safe_call(
                ce.get_anomalies(
                    DateInterval={"StartDate": start, "EndDate": end},
                    TotalImpact={"NumericOperator": "GREATER_THAN", "StartValue": 10},
                )
            )

        if not anomalies or "_error" in anomalies:
            return {"anomalies": [], "_error": anomalies.get("_error") if anomalies else None}

        parsed = []
        for a in anomalies.get("Anomalies", []):
            impact = a.get("Impact", {})
            parsed.append({
                "anomaly_id": a.get("AnomalyId"),
                "start_date": a.get("AnomalyStartDate"),
                "end_date": a.get("AnomalyEndDate"),
                "max_impact": float(impact.get("MaxImpact", 0)),
                "total_impact": float(impact.get("TotalImpact", 0)),
                "total_actual_spend": float(impact.get("TotalActualSpend", 0)),
                "total_expected_spend": float(impact.get("TotalExpectedSpend", 0)),
                "service": a.get("RootCauses", [{}])[0].get("Service") if a.get("RootCauses") else None,
                "region": a.get("RootCauses", [{}])[0].get("Region") if a.get("RootCauses") else None,
                "feedback": a.get("Feedback"),
            })

        return {
            "anomalies": sorted(parsed, key=lambda x: x["total_impact"], reverse=True),
            "total_anomaly_impact": sum(a["total_impact"] for a in parsed),
        }
