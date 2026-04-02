"""Cloud infrastructure billing service for Mission Control.

Primary source: BigQuery Cloud Billing Export table configured via settings.
This service intentionally keeps cloud infrastructure cost separate from LLM/event
analytics to avoid mixing spend categories in Mission Control.
"""

from __future__ import annotations

import logging
import os
from calendar import monthrange
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import google.auth
from google.auth.transport.requests import AuthorizedSession

from app.core.config import get_settings

logger = logging.getLogger(__name__)


class CloudBillingService:
    """Read Cloud Infrastructure spend from BigQuery billing export."""

    _session: Optional[AuthorizedSession] = None
    _project_id: Optional[str] = None
    _auth_ready: bool = False

    @staticmethod
    def _unavailable(reason: str) -> Dict[str, Any]:
        return {
            "current_month_cloud_cost": 0.0,
            "projected_month_end_cloud_cost": 0.0,
            "daily_cloud_cost_series": [],
            "cloud_cost_breakdown": [],
            "is_available": False,
            "unavailable_reason": reason,
            "last_data_timestamp": None,
            "billing_data_lag_hours": None,
            "billing_source": "gcp_bigquery_billing_export",
        }

    @staticmethod
    def _parse_export_table(export_table: str) -> bool:
        # Expected format: project.dataset.table
        parts = str(export_table or "").strip().split(".")
        return len(parts) == 3 and all(bool(part.strip()) for part in parts)

    def _get_session(self) -> AuthorizedSession:
        if self._session is None:
            settings = get_settings()
            if settings.GOOGLE_APPLICATION_CREDENTIALS:
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = settings.GOOGLE_APPLICATION_CREDENTIALS
            try:
                credentials, project_id = google.auth.default(
                    scopes=["https://www.googleapis.com/auth/cloud-platform"]
                )
            except Exception as exc:
                raise RuntimeError(
                    "ADC unavailable: provide GOOGLE_APPLICATION_CREDENTIALS or run with GCP default service identity."
                ) from exc
            self._project_id = (
                str(settings.GCP_BILLING_PROJECT_ID or "").strip() or str(project_id or "").strip()
            )
            if not self._project_id:
                raise RuntimeError(
                    "GCP billing project id is missing. Set GCP_BILLING_PROJECT_ID or ensure ADC project is available."
                )
            self._session = AuthorizedSession(credentials)
            self._auth_ready = True
        return self._session

    def _query_bigquery(
        self,
        *,
        query: str,
        billing_account_id: str,
    ) -> List[Dict[str, Any]]:
        session = self._get_session()
        project_id = str(self._project_id or "").strip()
        if not project_id:
            raise RuntimeError("Missing GCP billing project id for BigQuery queries")
        url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/queries"
        payload = {
            "query": query,
            "useLegacySql": False,
            "parameterMode": "NAMED",
            "queryParameters": [
                {
                    "name": "billing_account_id",
                    "parameterType": {"type": "STRING"},
                    "parameterValue": {"value": billing_account_id},
                }
            ],
        }
        response = session.post(url, json=payload, timeout=45)
        response.raise_for_status()
        body = response.json() if response.content else {}
        schema_fields = (body.get("schema") or {}).get("fields") or []
        rows = body.get("rows") or []
        parsed: List[Dict[str, Any]] = []
        for row in rows:
            values = (row.get("f") or [])
            item: Dict[str, Any] = {}
            for idx, field in enumerate(schema_fields):
                key = str((field or {}).get("name") or "")
                value = None
                if idx < len(values):
                    value = values[idx].get("v")
                item[key] = value
            if item:
                parsed.append(item)
        return parsed

    async def get_cloud_infra_cost_metrics(self) -> Dict[str, Any]:
        settings = get_settings()
        export_table = str(settings.GCP_BILLING_EXPORT_TABLE or "").strip()
        billing_account_id = str(settings.GCP_BILLING_ACCOUNT_ID or "").strip()
        if not export_table:
            return self._unavailable(
                "Missing billing export configuration: set GCP_BILLING_EXPORT_TABLE (project.dataset.table)."
            )
        if not self._parse_export_table(export_table):
            return self._unavailable(
                "Invalid GCP_BILLING_EXPORT_TABLE format. Expected project.dataset.table."
            )
        try:
            self._get_session()
        except Exception as exc:
            return self._unavailable(str(exc))

        daily_query = f"""
        SELECT
          FORMAT_DATE('%Y-%m-%d', DATE(usage_start_time)) AS date,
          ROUND(SUM(CAST(cost AS NUMERIC)), 4) AS cost
        FROM `{export_table}`
        WHERE DATE(usage_start_time) >= DATE_TRUNC(CURRENT_DATE(), MONTH)
          AND (@billing_account_id = '' OR billing_account_id = @billing_account_id)
        GROUP BY date
        ORDER BY date ASC
        """
        # Service-level breakdown uses the standard Cloud Billing Export field
        # service.description when present in the export schema.
        service_breakdown_query = f"""
        SELECT
          COALESCE(NULLIF(TRIM(service.description), ''), 'Unknown Service') AS service,
          ROUND(SUM(CAST(cost AS NUMERIC)), 4) AS cost
        FROM `{export_table}`
        WHERE DATE(usage_start_time) >= DATE_TRUNC(CURRENT_DATE(), MONTH)
          AND (@billing_account_id = '' OR billing_account_id = @billing_account_id)
        GROUP BY service
        ORDER BY cost DESC
        LIMIT 50
        """
        last_data_query = f"""
        SELECT
          MAX(usage_end_time) AS last_data_timestamp
        FROM `{export_table}`
        WHERE DATE(usage_start_time) >= DATE_TRUNC(CURRENT_DATE(), MONTH)
          AND (@billing_account_id = '' OR billing_account_id = @billing_account_id)
        """
        try:
            daily_rows = self._query_bigquery(query=daily_query, billing_account_id=billing_account_id)
        except Exception as exc:
            logger.warning("cloud_billing_query_unavailable error=%s", exc)
            return self._unavailable(str(exc))

        daily_cloud_cost_series: List[Dict[str, Any]] = []
        current_month_cloud_cost = 0.0
        for row in daily_rows:
            day = str(row.get("date") or "").strip()
            cost_value = float(row.get("cost") or 0.0)
            if not day:
                continue
            daily_cloud_cost_series.append({"date": day, "cost": round(cost_value, 4)})
            current_month_cloud_cost += cost_value
        current_month_cloud_cost = round(current_month_cloud_cost, 2)

        now = datetime.now(timezone.utc)
        days_in_month = monthrange(now.year, now.month)[1]
        month_elapsed_days = max(1, now.day)
        projected_month_end_cloud_cost = round(
            (current_month_cloud_cost / float(month_elapsed_days)) * float(days_in_month),
            2,
        )

        cloud_cost_breakdown: List[Dict[str, Any]] = []
        unavailable_reason: Optional[str] = None
        last_data_timestamp: Optional[str] = None
        billing_data_lag_hours: Optional[float] = None
        try:
            breakdown_rows = self._query_bigquery(
                query=service_breakdown_query,
                billing_account_id=billing_account_id,
            )
            for row in breakdown_rows:
                service = str(row.get("service") or "Unknown Service").strip() or "Unknown Service"
                cost_value = round(float(row.get("cost") or 0.0), 4)
                cloud_cost_breakdown.append({"service": service, "cost": cost_value})
        except Exception as exc:
            logger.warning("cloud_billing_service_breakdown_unavailable error=%s", exc)
            unavailable_reason = (
                "Service-level cloud cost breakdown unavailable from configured billing export."
            )
        try:
            last_rows = self._query_bigquery(
                query=last_data_query,
                billing_account_id=billing_account_id,
            )
            if last_rows:
                raw_last_ts = str((last_rows[0] or {}).get("last_data_timestamp") or "").strip()
                if raw_last_ts:
                    normalized = raw_last_ts.replace(" ", "T")
                    if not normalized.endswith("Z") and "+" not in normalized:
                        normalized = f"{normalized}Z"
                    last_dt = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
                    last_data_timestamp = last_dt.astimezone(timezone.utc).isoformat()
                    lag = (datetime.now(timezone.utc) - last_dt.astimezone(timezone.utc)).total_seconds() / 3600.0
                    billing_data_lag_hours = round(max(0.0, lag), 2)
        except Exception as exc:
            logger.warning("cloud_billing_last_data_timestamp_unavailable error=%s", exc)

        logger.info(
            "cloud_billing_fetch_succeeded current_month_cloud_cost=%.2f last_data_timestamp=%s",
            current_month_cloud_cost,
            last_data_timestamp or "",
        )
        return {
            "current_month_cloud_cost": current_month_cloud_cost,
            "projected_month_end_cloud_cost": projected_month_end_cloud_cost,
            "daily_cloud_cost_series": daily_cloud_cost_series,
            "cloud_cost_breakdown": cloud_cost_breakdown,
            "is_available": True,
            "unavailable_reason": unavailable_reason,
            "last_data_timestamp": last_data_timestamp,
            "billing_data_lag_hours": billing_data_lag_hours,
            "billing_source": "gcp_bigquery_billing_export",
        }


cloud_billing_service = CloudBillingService()

