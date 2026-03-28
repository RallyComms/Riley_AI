import uuid
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from neo4j import AsyncGraphDatabase

from app.core.config import get_settings
from app.services.analytics_contract import (
    canonical_campaign_id,
    classify_chat_type,
    infer_provider_from_model,
    normalize_event_type,
    now_iso_utc,
    resolve_riley_display_identity,
    safe_metadata_json,
)

logger = logging.getLogger(__name__)


class GraphService:
    """Service responsible for Neo4j graph database operations.

    Provides graph query capabilities to understand relationships
    between clients, campaigns, assets, and staff.
    """

    def __init__(self, driver: Optional[Any] = None) -> None:
        settings = get_settings()

        self._driver = driver or AsyncGraphDatabase.driver(
            settings.NEO4J_URI,
            auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD),
        )

    def _require_team_chat_author_id(self, author_id: Optional[str], *, campaign_id: str, thread_id: Optional[str] = None) -> str:
        """Validate author IDs for Team Chat write paths."""
        normalized = str(author_id or "").strip()
        if normalized and normalized.lower() not in {"unknown", "unknown_user", "null", "none"}:
            return normalized
        logger.error(
            "team_chat_message_write_missing_author_id campaign_id=%s thread_id=%s author_id=%s",
            campaign_id,
            thread_id or "",
            author_id,
        )
        raise ValueError("Team chat message requires a valid author_id")

    def _coerce_team_chat_read_identity(
        self,
        *,
        author_id: Optional[str],
        author_display_name: Optional[str],
        author_fallback_level: Optional[int],
        campaign_id: str,
        message_id: Optional[str],
        thread_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Enforce Team Chat read contract + observability for degraded identity."""
        normalized_author_id = str(author_id or "").strip()
        if not normalized_author_id:
            logger.error(
                "team_chat_message_missing_author_id_read campaign_id=%s thread_id=%s message_id=%s",
                campaign_id,
                thread_id or "",
                message_id,
            )
            raise ValueError("Team chat message missing author_id at read time")

        fallback_level_value = int(author_fallback_level or 4)
        normalized_display = str(author_display_name or "").strip() or normalized_author_id
        if fallback_level_value > 1:
            logger.warning(
                "team_chat_identity_degraded user_id=%s fallback_level=%s message_id=%s",
                normalized_author_id,
                fallback_level_value,
                message_id,
            )

        return {
            "author_id": normalized_author_id,
            "author_display_name": normalized_display,
            "author_fallback_level": fallback_level_value,
        }

    @property
    def driver(self) -> Any:
        """Return the Neo4j driver instance."""
        return self._driver

    async def close(self) -> None:
        """Close the Neo4j driver connection."""
        if self._driver:
            await self._driver.close()

    async def ensure_mission_control_schema(self) -> None:
        """Create high-value indexes/constraints for Mission Control analytics paths."""
        statements = [
            # AnalyticsEvent write/read keys
            """
            CREATE CONSTRAINT analytics_event_event_id_unique IF NOT EXISTS
            FOR (e:AnalyticsEvent) REQUIRE e.event_id IS UNIQUE
            """,
            """
            CREATE INDEX analytics_event_occurred_at_idx IF NOT EXISTS
            FOR (e:AnalyticsEvent) ON (e.occurred_at)
            """,
            """
            CREATE INDEX analytics_event_campaign_id_idx IF NOT EXISTS
            FOR (e:AnalyticsEvent) ON (e.campaign_id)
            """,
            """
            CREATE INDEX analytics_event_user_id_idx IF NOT EXISTS
            FOR (e:AnalyticsEvent) ON (e.user_id)
            """,
            """
            CREATE INDEX analytics_event_actor_user_id_idx IF NOT EXISTS
            FOR (e:AnalyticsEvent) ON (e.actor_user_id)
            """,
            """
            CREATE INDEX analytics_event_source_entity_idx IF NOT EXISTS
            FOR (e:AnalyticsEvent) ON (e.source_entity)
            """,
            """
            CREATE INDEX analytics_event_source_event_type_raw_idx IF NOT EXISTS
            FOR (e:AnalyticsEvent) ON (e.source_event_type_raw)
            """,
            # Rollup keys used by Mission Control queries
            """
            CREATE CONSTRAINT analytics_daily_system_rollup_event_date_unique IF NOT EXISTS
            FOR (r:AnalyticsDailySystemRollup) REQUIRE r.event_date IS UNIQUE
            """,
            """
            CREATE CONSTRAINT analytics_daily_campaign_rollup_key_unique IF NOT EXISTS
            FOR (r:AnalyticsDailyCampaignRollup) REQUIRE (r.event_date, r.campaign_id) IS UNIQUE
            """,
            """
            CREATE CONSTRAINT analytics_daily_user_rollup_key_unique IF NOT EXISTS
            FOR (r:AnalyticsDailyUserRollup) REQUIRE (r.event_date, r.campaign_id, r.user_id) IS UNIQUE
            """,
            """
            CREATE CONSTRAINT analytics_daily_provider_rollup_key_unique IF NOT EXISTS
            FOR (r:AnalyticsDailyProviderRollup) REQUIRE (r.event_date, r.provider, r.model) IS UNIQUE
            """,
        ]
        async with self._driver.session() as session:
            for statement in statements:
                try:
                    result = await session.run(statement)
                    await result.consume()
                except Exception:
                    logger.exception("mission_control_schema_statement_failed")

    async def _label_exists(self, label: str) -> bool:
        """Return True if the Neo4j label exists in current DB."""
        async with self._driver.session() as session:
            result = await session.run(
                """
                CALL db.labels() YIELD label
                WHERE label = $label
                RETURN count(*) > 0 AS exists
                """,
                label=label,
            )
            record = await result.single()
            return bool(record and record.get("exists"))

    async def append_analytics_event(
        self,
        *,
        event_id: str,
        source_event_type_raw: str,
        source_entity: str,
        campaign_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        client_id: Optional[str] = None,
        is_global: Optional[bool] = None,
        user_id: Optional[str] = None,
        actor_user_id: Optional[str] = None,
        occurred_at: Optional[str] = None,
        object_id: Optional[str] = None,
        provider: Optional[str] = None,
        model: Optional[str] = None,
        status: Optional[str] = None,
        latency_ms: Optional[int] = None,
        cost_estimate_usd: Optional[float] = None,
        pricing_version: Optional[str] = None,
        cost_confidence: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Append/merge a normalized analytics event contract row."""
        canonical_id = canonical_campaign_id(
            campaign_id=campaign_id,
            tenant_id=tenant_id,
            client_id=client_id,
            is_global=is_global,
        )
        metadata_obj = dict(metadata or {})
        resolved_user_display = resolve_riley_display_identity(
            display_name=metadata_obj.get("user_display_name"),
            username=metadata_obj.get("user_username"),
            email=metadata_obj.get("user_email"),
            user_id=user_id,
        )
        resolved_actor_display = resolve_riley_display_identity(
            display_name=metadata_obj.get("actor_display_name"),
            username=metadata_obj.get("actor_username"),
            email=metadata_obj.get("actor_email"),
            user_id=actor_user_id,
        )
        metadata_obj.setdefault("user_display_name_normalized", resolved_user_display)
        metadata_obj.setdefault("actor_display_name_normalized", resolved_actor_display)
        chat_type = classify_chat_type(
            source_entity=source_entity,
            is_private_thread=metadata_obj.get("is_private_thread"),
        )
        normalized_type, feature_area, object_type = normalize_event_type(
            source_event_type_raw=source_event_type_raw,
            source_entity=source_entity,
            chat_type=chat_type,
        )
        if chat_type and "chat_type" not in metadata_obj:
            metadata_obj["chat_type"] = chat_type
        target_object_id = str(object_id or "").strip() or None
        if target_object_id is None and metadata_obj.get("request_id"):
            target_object_id = str(metadata_obj.get("request_id") or "").strip() or None
        resolved_provider = provider or infer_provider_from_model(model)
        input_tokens: Optional[int] = None
        output_tokens: Optional[int] = None
        total_tokens: Optional[int] = None

        def _coerce_int(value: Any) -> Optional[int]:
            if isinstance(value, bool):
                return None
            if isinstance(value, (int, float)):
                return int(value)
            if isinstance(value, str):
                text = value.strip()
                if not text:
                    return None
                try:
                    return int(float(text))
                except Exception:
                    return None
            return None

        input_tokens = _coerce_int(metadata_obj.get("input_tokens"))
        output_tokens = _coerce_int(metadata_obj.get("output_tokens"))
        total_tokens = _coerce_int(metadata_obj.get("total_tokens"))
        if total_tokens is None and input_tokens is not None and output_tokens is not None:
            total_tokens = input_tokens + output_tokens

        async with self._driver.session() as session:
            query = """
            MERGE (e:AnalyticsEvent {event_id: $event_id})
            ON CREATE SET
                e.created_at = datetime()
            SET
                e.event_type_normalized = $event_type_normalized,
                e.source_event_type_raw = $source_event_type_raw,
                e.occurred_at = $occurred_at,
                e.campaign_id = $campaign_id,
                e.user_id = $user_id,
                e.actor_user_id = $actor_user_id,
                e.feature_area = $feature_area,
                e.object_type = $object_type,
                e.object_id = $object_id,
                e.provider = $provider,
                e.model = $model,
                e.status = $status,
                e.latency_ms = $latency_ms,
                e.cost_estimate_usd = $cost_estimate_usd,
                e.input_tokens = $input_tokens,
                e.output_tokens = $output_tokens,
                e.total_tokens = $total_tokens,
                e.pricing_version = $pricing_version,
                e.cost_confidence = $cost_confidence,
                e.source_entity = $source_entity,
                e.metadata_json = $metadata_json,
                e.updated_at = datetime()
            """
            await session.run(
                query,
                event_id=event_id,
                event_type_normalized=normalized_type,
                source_event_type_raw=source_event_type_raw,
                occurred_at=occurred_at or now_iso_utc(),
                campaign_id=canonical_id,
                user_id=user_id,
                actor_user_id=actor_user_id,
                feature_area=feature_area,
                object_type=object_type,
                object_id=target_object_id,
                provider=resolved_provider,
                model=model,
                status=status,
                latency_ms=latency_ms,
                cost_estimate_usd=cost_estimate_usd,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                total_tokens=total_tokens,
                pricing_version=pricing_version,
                cost_confidence=cost_confidence,
                source_entity=source_entity,
                metadata_json=safe_metadata_json(metadata_obj),
            )

    async def rebuild_analytics_daily_rollups(self, *, days_back: int = 30) -> Dict[str, int]:
        """Rebuild v1 daily analytics rollups from AnalyticsEvent."""
        async with self._driver.session() as session:
            campaign_query = """
            MATCH (e:AnalyticsEvent)
            WHERE e.occurred_at >= $start_iso
            WITH
                date(datetime(e.occurred_at)) as event_date,
                coalesce(e.campaign_id, "unknown_campaign") as campaign_id,
                count(e) as event_count,
                count(DISTINCT coalesce(e.user_id, e.actor_user_id, "")) as active_users,
                coalesce(sum(toFloat(e.cost_estimate_usd)), 0.0) as total_cost_estimate_usd,
                sum(CASE WHEN (e.feature_area = "chat" OR e.source_entity IN ["Message", "TeamMessage", "ThreadMessage"]) THEN 1 ELSE 0 END) as chat_events,
                sum(CASE WHEN e.source_entity = "RileyReportJob" AND e.object_id IS NOT NULL THEN 1 ELSE 0 END) as report_events,
                coalesce(sum(CASE WHEN e.latency_ms IS NOT NULL THEN toFloat(e.latency_ms) ELSE 0.0 END), 0.0) as latency_sum_ms,
                sum(CASE WHEN e.latency_ms IS NOT NULL THEN 1 ELSE 0 END) as latency_count
            MERGE (r:AnalyticsDailyCampaignRollup {event_date: toString(event_date), campaign_id: campaign_id})
            SET
                r.event_count = event_count,
                r.active_users = active_users,
                r.total_cost_estimate_usd = total_cost_estimate_usd,
                r.chat_events = chat_events,
                r.report_events = report_events,
                r.latency_sum_ms = latency_sum_ms,
                r.latency_count = latency_count,
                r.updated_at = datetime()
            RETURN count(r) as count_rows
            """
            user_query = """
            MATCH (e:AnalyticsEvent)
            WHERE e.occurred_at >= $start_iso
            WITH
                date(datetime(e.occurred_at)) as event_date,
                coalesce(e.campaign_id, "unknown_campaign") as campaign_id,
                coalesce(e.user_id, e.actor_user_id, "unknown_user") as user_id,
                count(e) as event_count,
                coalesce(sum(toFloat(e.cost_estimate_usd)), 0.0) as total_cost_estimate_usd,
                sum(CASE WHEN (e.feature_area = "chat" OR e.source_entity IN ["Message", "TeamMessage", "ThreadMessage"]) THEN 1 ELSE 0 END) as chat_events,
                sum(CASE WHEN e.source_entity = "RileyReportJob" AND e.object_id IS NOT NULL THEN 1 ELSE 0 END) as report_events
            MERGE (r:AnalyticsDailyUserRollup {
                event_date: toString(event_date),
                campaign_id: campaign_id,
                user_id: user_id
            })
            SET
                r.event_count = event_count,
                r.total_cost_estimate_usd = total_cost_estimate_usd,
                r.chat_events = chat_events,
                r.report_events = report_events,
                r.updated_at = datetime()
            RETURN count(r) as count_rows
            """
            system_query = """
            MATCH (e:AnalyticsEvent)
            WHERE e.occurred_at >= $start_iso
            WITH
                date(datetime(e.occurred_at)) as event_date,
                count(e) as event_count,
                count(DISTINCT coalesce(e.campaign_id, "unknown_campaign")) as active_campaigns,
                count(DISTINCT coalesce(e.user_id, e.actor_user_id, "unknown_user")) as active_users,
                coalesce(sum(toFloat(e.cost_estimate_usd)), 0.0) as total_cost_estimate_usd,
                coalesce(sum(CASE WHEN e.latency_ms IS NOT NULL THEN toFloat(e.latency_ms) ELSE 0.0 END), 0.0) as latency_sum_ms,
                sum(CASE WHEN e.latency_ms IS NOT NULL THEN 1 ELSE 0 END) as latency_count,
                sum(CASE WHEN (e.feature_area = "chat" OR e.source_entity IN ["Message", "TeamMessage", "ThreadMessage"]) THEN 1 ELSE 0 END) as chat_events,
                sum(CASE WHEN e.source_entity = "RileyReportJob" AND e.object_id IS NOT NULL THEN 1 ELSE 0 END) as report_events,
                sum(CASE WHEN (e.source_event_type_raw = "worker_failed")
                              OR (e.source_event_type_raw = "ingestion_failed")
                              OR (e.source_event_type_raw = "preview_generation_failed")
                              OR (e.source_entity = "RileyReportJob" AND e.status = "failed")
                         THEN 1 ELSE 0 END) as failure_events,
                sum(CASE WHEN e.source_event_type_raw = "report_provider_fallback_triggered" THEN 1 ELSE 0 END) as provider_fallback_triggered,
                sum(CASE WHEN e.source_event_type_raw = "report_provider_fallback_succeeded" THEN 1 ELSE 0 END) as provider_fallback_successes,
                sum(CASE WHEN e.source_event_type_raw = "report_provider_fallback_failed" THEN 1 ELSE 0 END) as provider_fallback_failures,
                sum(CASE WHEN e.source_event_type_raw = "reranker_failed" THEN 1 ELSE 0 END) as reranker_failures,
                sum(CASE WHEN e.source_entity = "RileyReportJob" AND e.status = "complete" AND e.object_id IS NOT NULL THEN 1 ELSE 0 END) as report_successes,
                sum(CASE WHEN e.source_entity = "RileyReportJob" AND e.status = "failed" AND e.object_id IS NOT NULL THEN 1 ELSE 0 END) as report_failures
            MERGE (r:AnalyticsDailySystemRollup {event_date: toString(event_date)})
            SET
                r.event_count = event_count,
                r.active_campaigns = active_campaigns,
                r.active_users = active_users,
                r.total_cost_estimate_usd = total_cost_estimate_usd,
                r.latency_sum_ms = latency_sum_ms,
                r.latency_count = latency_count,
                r.chat_events = chat_events,
                r.report_events = report_events,
                r.failure_events = failure_events,
                r.provider_fallback_triggered = provider_fallback_triggered,
                r.provider_fallback_successes = provider_fallback_successes,
                r.provider_fallback_failures = provider_fallback_failures,
                r.reranker_failures = reranker_failures,
                r.report_successes = report_successes,
                r.report_failures = report_failures,
                r.avg_latency_ms = CASE WHEN latency_count = 0 THEN 0.0 ELSE latency_sum_ms / toFloat(latency_count) END,
                r.updated_at = datetime()
            RETURN count(r) as count_rows
            """
            provider_query = """
            MATCH (e:AnalyticsEvent)
            WHERE e.occurred_at >= $start_iso
              AND e.provider IS NOT NULL
            WITH
                date(datetime(e.occurred_at)) as event_date,
                coalesce(e.provider, "unknown_provider") as provider,
                coalesce(e.model, "unknown_model") as model,
                count(e) as event_count,
                coalesce(sum(toFloat(e.cost_estimate_usd)), 0.0) as total_cost_estimate_usd,
                coalesce(sum(CASE WHEN e.latency_ms IS NOT NULL THEN toFloat(e.latency_ms) ELSE 0.0 END), 0.0) as latency_sum_ms,
                sum(CASE WHEN e.latency_ms IS NOT NULL THEN 1 ELSE 0 END) as latency_count,
                sum(CASE WHEN e.source_event_type_raw = "report_provider_fallback_triggered" THEN 1 ELSE 0 END) as fallback_triggered,
                sum(CASE WHEN e.source_event_type_raw = "report_provider_fallback_succeeded" THEN 1 ELSE 0 END) as fallback_succeeded,
                sum(CASE WHEN e.source_event_type_raw = "report_provider_fallback_failed" THEN 1 ELSE 0 END) as fallback_failed
            MERGE (r:AnalyticsDailyProviderRollup {
                event_date: toString(event_date),
                provider: provider,
                model: model
            })
            SET
                r.event_count = event_count,
                r.total_cost_estimate_usd = total_cost_estimate_usd,
                r.latency_sum_ms = latency_sum_ms,
                r.latency_count = latency_count,
                r.fallback_triggered = fallback_triggered,
                r.fallback_succeeded = fallback_succeeded,
                r.fallback_failed = fallback_failed,
                r.avg_latency_ms = CASE WHEN latency_count = 0 THEN 0.0 ELSE latency_sum_ms / toFloat(latency_count) END,
                r.updated_at = datetime()
            RETURN count(r) as count_rows
            """
            start_iso = (datetime.now(timezone.utc) - timedelta(days=max(1, int(days_back)))).isoformat()
            campaign_rows = int((await (await session.run(campaign_query, start_iso=start_iso)).single() or {}).get("count_rows") or 0)
            user_rows = int((await (await session.run(user_query, start_iso=start_iso)).single() or {}).get("count_rows") or 0)
            system_rows = int((await (await session.run(system_query, start_iso=start_iso)).single() or {}).get("count_rows") or 0)
            provider_rows = int((await (await session.run(provider_query, start_iso=start_iso)).single() or {}).get("count_rows") or 0)
            return {
                "campaign_rollups": campaign_rows,
                "user_rollups": user_rows,
                "system_rollups": system_rows,
                "provider_rollups": provider_rows,
            }

    async def get_client_structure(self, client_id: str) -> Dict[str, Any]:
        """Retrieve the structure of a client including campaigns and assets.

        This allows Riley to "look up" exactly what campaigns belong to a client ID,
        solving the semantic mapping problem.

        Args:
            client_id: The unique identifier for the client.

        Returns:
            Dictionary containing:
                - ClientName: Name of the client
                - Campaigns: List of campaign names
                - AssetCount: Total number of assets across all campaigns

        Raises:
            Exception: If the query fails or client is not found.
        """
        cypher_query = """
        MATCH (c:Client {id: $client_id})-[:RUNS_CAMPAIGN]->(cmp:Campaign)
        OPTIONAL MATCH (cmp)-[:HAS_ASSET]->(a:Asset)
        RETURN c.name as ClientName, collect(cmp.name) as Campaigns, count(a) as AssetCount
        """

        async with self._driver.session() as session:
            result = await session.run(cypher_query, client_id=client_id)
            record = await result.single()

            if not record:
                return {
                    "ClientName": None,
                    "Campaigns": [],
                    "AssetCount": 0,
                }

            return {
                "ClientName": record["ClientName"],
                "Campaigns": record["Campaigns"] or [],
                "AssetCount": record["AssetCount"] or 0,
            }

    async def save_message(
        self, session_id: str, role: str, content: str, tenant_id: str, user_id: str
    ) -> None:
        """Save a chat message to Neo4j.
        
        Creates or merges a ChatSession node with tenant and user scoping, and links a Message node to it.
        Schema: (Session:ChatSession {id: "...", tenant_id: "...", user_id: "..."}) <-[:BELONGS_TO]- (Message:Message {role: "...", content: "...", timestamp: "..."})
        
        SECURITY: The MERGE includes tenant_id and user_id to prevent cross-tenant/user memory mixing.
        
        Args:
            session_id: Unique identifier for the chat session
            role: Message role ("user" or "model")
            content: Message content text
            tenant_id: Tenant/client identifier for scope isolation
            user_id: User identifier for scope isolation
        """
        async with self._driver.session() as session:
            query = """
            MERGE (s:ChatSession {
                id: $session_id,
                tenant_id: $tenant_id,
                user_id: $user_id
            })
            ON CREATE SET s.created_at = datetime()
            CREATE (m:Message {
                role: $role,
                content: $content,
                timestamp: datetime()
            })
            CREATE (m)-[:BELONGS_TO]->(s)
            """
            
            await session.run(
                query,
                session_id=session_id,
                tenant_id=tenant_id,
                user_id=user_id,
                role=role,
                content=content,
            )
        await self.append_analytics_event(
            event_id=f"assistant_message:{session_id}:{role}:{now_iso_utc()}",
            source_event_type_raw="assistant_message_saved",
            source_entity="Message",
            campaign_id=tenant_id,
            user_id=user_id,
            actor_user_id=user_id,
            object_id=session_id,
            status=role,
            metadata={"chat_type": "assistant_chat"},
        )

    async def get_chat_history(
        self, session_id: str, tenant_id: str, user_id: str, limit: int = 10
    ) -> List[Dict[str, str]]:
        """Retrieve chat history for a session.
        
        Gets the last N messages ordered by timestamp DESC, then reverses
        to return in chronological order (oldest -> newest).
        
        SECURITY: MATCH includes tenant_id and user_id to prevent cross-scope memory access.
        Never returns messages across different tenants or users.
        
        Args:
            session_id: Unique identifier for the chat session
            tenant_id: Tenant/client identifier for scope isolation
            user_id: User identifier for scope isolation
            limit: Maximum number of messages to retrieve (default: 10)
            
        Returns:
            List of message dicts with 'role' and 'content' keys, ordered chronologically
        """
        async with self._driver.session() as session:
            query = """
            MATCH (m:Message)-[:BELONGS_TO]->(s:ChatSession {
                id: $session_id,
                tenant_id: $tenant_id,
                user_id: $user_id
            })
            RETURN m.role as role, m.content as content, toString(m.timestamp) as timestamp
            ORDER BY m.timestamp DESC
            LIMIT $limit
            """
            
            result = await session.run(
                query,
                session_id=session_id,
                tenant_id=tenant_id,
                user_id=user_id,
                limit=limit
            )
            
            messages = []
            async for record in result:
                messages.append({
                    "role": record["role"],
                    "content": record["content"],
                })
            
            # Reverse to get chronological order (oldest -> newest)
            messages.reverse()
            return messages

    async def list_riley_conversations(
        self, tenant_id: str, user_id: str, limit: int = 50
    ) -> List[Dict[str, Any]]:
        """List persisted Riley conversations for a user and tenant."""
        async with self._driver.session() as session:
            query = """
            MATCH (s:ChatSession {tenant_id: $tenant_id, user_id: $user_id})
            OPTIONAL MATCH (m:Message)-[:BELONGS_TO]->(s)
            WITH s, m
            ORDER BY m.timestamp DESC
            WITH s, collect(m)[0] as latest
            RETURN
                s.id as id,
                coalesce(s.title, "New Conversation") as title,
                s.project_id as project_id,
                latest.content as last_message,
                toString(latest.timestamp) as last_message_at,
                toString(s.created_at) as created_at
            ORDER BY coalesce(latest.timestamp, s.created_at) DESC
            LIMIT $limit
            """
            result = await session.run(
                query,
                tenant_id=tenant_id,
                user_id=user_id,
                limit=limit,
            )

            conversations: List[Dict[str, Any]] = []
            async for record in result:
                conversations.append(
                    {
                        "id": record["id"],
                        "title": record.get("title") or "New Conversation",
                        "project_id": record.get("project_id"),
                        "last_message": record.get("last_message") or "",
                        "last_message_at": record.get("last_message_at"),
                        "created_at": record.get("created_at"),
                    }
                )
            return conversations

    async def list_riley_projects(
        self, tenant_id: str, user_id: str, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """List Riley projects for a user and tenant scope."""
        async with self._driver.session() as session:
            query = """
            MATCH (p:RileyProject {tenant_id: $tenant_id, user_id: $user_id})
            RETURN
                p.id as id,
                p.name as name,
                toString(p.created_at) as created_at,
                toString(p.updated_at) as updated_at
            ORDER BY coalesce(p.updated_at, p.created_at) DESC
            LIMIT $limit
            """
            result = await session.run(
                query,
                tenant_id=tenant_id,
                user_id=user_id,
                limit=limit,
            )

            projects: List[Dict[str, Any]] = []
            async for record in result:
                projects.append(
                    {
                        "id": record["id"],
                        "name": record.get("name") or "Untitled Project",
                        "created_at": record.get("created_at"),
                        "updated_at": record.get("updated_at"),
                    }
                )
            return projects

    async def create_riley_project(
        self, tenant_id: str, user_id: str, name: str, project_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a Riley project for a user and tenant scope."""
        normalized_name = name.strip()
        if normalized_name == "":
            normalized_name = "Untitled Project"
        next_project_id = project_id or f"riley_project_{uuid.uuid4()}"

        async with self._driver.session() as session:
            query = """
            CREATE (p:RileyProject {
                id: $project_id,
                tenant_id: $tenant_id,
                user_id: $user_id,
                name: $name,
                created_at: datetime(),
                updated_at: datetime()
            })
            RETURN
                p.id as id,
                p.name as name,
                toString(p.created_at) as created_at,
                toString(p.updated_at) as updated_at
            """
            result = await session.run(
                query,
                project_id=next_project_id,
                tenant_id=tenant_id,
                user_id=user_id,
                name=normalized_name,
            )
            record = await result.single()
            if not record:
                raise Exception("Failed to create Riley project")
            return {
                "id": record["id"],
                "name": record.get("name") or "Untitled Project",
                "created_at": record.get("created_at"),
                "updated_at": record.get("updated_at"),
            }

    async def update_riley_project(
        self, project_id: str, tenant_id: str, user_id: str, name: str
    ) -> Dict[str, Any]:
        """Rename a Riley project in scope."""
        normalized_name = name.strip()
        if normalized_name == "":
            raise Exception("Project name cannot be empty")

        async with self._driver.session() as session:
            query = """
            MATCH (p:RileyProject {id: $project_id, tenant_id: $tenant_id, user_id: $user_id})
            SET p.name = $name, p.updated_at = datetime()
            RETURN
                p.id as id,
                p.name as name,
                toString(p.created_at) as created_at,
                toString(p.updated_at) as updated_at
            """
            result = await session.run(
                query,
                project_id=project_id,
                tenant_id=tenant_id,
                user_id=user_id,
                name=normalized_name,
            )
            record = await result.single()
            if not record:
                raise Exception("Riley project not found")
            return {
                "id": record["id"],
                "name": record.get("name") or "Untitled Project",
                "created_at": record.get("created_at"),
                "updated_at": record.get("updated_at"),
            }

    async def delete_riley_project(self, project_id: str, tenant_id: str, user_id: str) -> None:
        """Delete a Riley project and unassign conversations in scope."""
        async with self._driver.session() as session:
            query = """
            OPTIONAL MATCH (s:ChatSession {tenant_id: $tenant_id, user_id: $user_id, project_id: $project_id})
            SET s.project_id = null
            WITH count(s) as _
            MATCH (p:RileyProject {id: $project_id, tenant_id: $tenant_id, user_id: $user_id})
            DETACH DELETE p
            """
            await session.run(
                query,
                project_id=project_id,
                tenant_id=tenant_id,
                user_id=user_id,
            )

    async def assign_riley_conversation_project(
        self,
        session_id: str,
        tenant_id: str,
        user_id: str,
        project_id: Optional[str],
    ) -> Dict[str, Any]:
        """Assign or clear a Riley conversation's project."""
        async with self._driver.session() as session:
            if project_id is None:
                clear_query = """
                MATCH (s:ChatSession {id: $session_id, tenant_id: $tenant_id, user_id: $user_id})
                SET s.project_id = null
                RETURN s.id as id, s.project_id as project_id
                """
                result = await session.run(
                    clear_query,
                    session_id=session_id,
                    tenant_id=tenant_id,
                    user_id=user_id,
                )
                record = await result.single()
                if not record:
                    raise Exception("Conversation not found")
                return {
                    "id": record["id"],
                    "project_id": record.get("project_id"),
                }

            assign_query = """
            MATCH (p:RileyProject {id: $project_id, tenant_id: $tenant_id, user_id: $user_id})
            WITH p
            MATCH (s:ChatSession {id: $session_id, tenant_id: $tenant_id, user_id: $user_id})
            SET s.project_id = $project_id
            RETURN s.id as id, s.project_id as project_id
            """
            result = await session.run(
                assign_query,
                project_id=project_id,
                session_id=session_id,
                tenant_id=tenant_id,
                user_id=user_id,
            )
            record = await result.single()
            if not record:
                raise Exception("Conversation or project not found in scope")
            return {
                "id": record["id"],
                "project_id": record.get("project_id"),
            }

    async def delete_riley_conversation(self, session_id: str, tenant_id: str, user_id: str) -> None:
        """Delete a Riley conversation and all messages in scope."""
        async with self._driver.session() as session:
            query = """
            MATCH (s:ChatSession {id: $session_id, tenant_id: $tenant_id, user_id: $user_id})
            OPTIONAL MATCH (m:Message)-[:BELONGS_TO]->(s)
            WITH s, collect(m) as messages
            FOREACH (msg IN messages | DETACH DELETE msg)
            DETACH DELETE s
            """
            await session.run(
                query,
                session_id=session_id,
                tenant_id=tenant_id,
                user_id=user_id,
            )

    async def create_riley_conversation(
        self, tenant_id: str, user_id: str, session_id: Optional[str] = None, title: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a Riley conversation shell (ChatSession node)."""
        normalized_title = title.strip() if isinstance(title, str) else None
        if normalized_title == "":
            normalized_title = None
        conversation_id = session_id or f"session_{tenant_id}_{user_id}_{uuid.uuid4()}"

        async with self._driver.session() as session:
            query = """
            MERGE (s:ChatSession {id: $session_id, tenant_id: $tenant_id, user_id: $user_id})
            ON CREATE SET s.created_at = datetime()
            SET s.title = coalesce($title, s.title, "New Conversation")
            RETURN
                s.id as id,
                s.title as title,
                toString(s.created_at) as created_at
            """
            result = await session.run(
                query,
                session_id=conversation_id,
                tenant_id=tenant_id,
                user_id=user_id,
                title=normalized_title,
            )
            record = await result.single()
            if not record:
                raise Exception("Failed to create Riley conversation")
            created = {
                "id": record["id"],
                "title": record.get("title") or "New Conversation",
                "created_at": record.get("created_at"),
            }
            await self.append_analytics_event(
                event_id=f"assistant_session:{created.get('id')}:created",
                source_event_type_raw="assistant_session_created",
                source_entity="ChatSession",
                campaign_id=tenant_id,
                user_id=user_id,
                actor_user_id=user_id,
                occurred_at=created.get("created_at"),
                object_id=created.get("id"),
                status="created",
                metadata={"chat_type": "assistant_chat"},
            )
            return created

    async def get_riley_conversation_messages(
        self, session_id: str, tenant_id: str, user_id: str, limit: int = 100
    ) -> List[Dict[str, str]]:
        """Get messages for a Riley conversation in chronological order."""
        async with self._driver.session() as session:
            query = """
            MATCH (m:Message)-[:BELONGS_TO]->(s:ChatSession {
                id: $session_id,
                tenant_id: $tenant_id,
                user_id: $user_id
            })
            RETURN m.role as role, m.content as content, toString(m.timestamp) as timestamp
            ORDER BY m.timestamp ASC
            LIMIT $limit
            """
            result = await session.run(
                query,
                session_id=session_id,
                tenant_id=tenant_id,
                user_id=user_id,
                limit=limit,
            )

            messages: List[Dict[str, str]] = []
            async for record in result:
                messages.append(
                    {
                        "role": record["role"],
                        "content": record["content"],
                    }
                )
            return messages

    async def append_riley_conversation_message(
        self, session_id: str, tenant_id: str, user_id: str, role: str, content: str
    ) -> None:
        """Append a message to an existing Riley conversation."""
        async with self._driver.session() as session:
            query = """
            MATCH (s:ChatSession {
                id: $session_id,
                tenant_id: $tenant_id,
                user_id: $user_id
            })
            CREATE (m:Message {
                role: $role,
                content: $content,
                timestamp: datetime()
            })
            CREATE (m)-[:BELONGS_TO]->(s)
            """
            await session.run(
                query,
                session_id=session_id,
                tenant_id=tenant_id,
                user_id=user_id,
                role=role,
                content=content,
            )
        await self.append_analytics_event(
            event_id=f"assistant_message:{session_id}:{role}:{now_iso_utc()}",
            source_event_type_raw="assistant_conversation_message_appended",
            source_entity="Message",
            campaign_id=tenant_id,
            user_id=user_id,
            actor_user_id=user_id,
            object_id=session_id,
            status=role,
            metadata={"chat_type": "assistant_chat"},
        )

    async def create_riley_report_job(
        self,
        *,
        report_job_id: str,
        tenant_id: str,
        user_id: str,
        conversation_id: Optional[str],
        report_type: str,
        title: str,
        query_text: str,
        mode: str,
    ) -> Dict[str, Any]:
        """Create a durable Riley report job node."""
        async with self._driver.session() as session:
            query = """
            CREATE (r:RileyReportJob {
                id: $report_job_id,
                tenant_id: $tenant_id,
                user_id: $user_id,
                conversation_id: $conversation_id,
                report_type: $report_type,
                title: $title,
                status: "queued",
                created_at: datetime(),
                started_at: null,
                completed_at: null,
                cancel_requested_at: null,
                cancelled_at: null,
                deleted_at: null,
                cancellation_reason: null,
                error_message: null,
                output_file_id: null,
                output_url: null,
                summary_text: null,
                report_fidelity_level: null,
                report_context_reduction_applied: false,
                report_context_strategy: null,
                retrieval_doc_count: null,
                retrieval_chunk_count: null,
                context_chars_included: null,
                generation_model: null,
                generation_attempts_used: null,
                failure_stage: null,
                failure_code: null,
                failure_detail: null,
                query_text: $query_text,
                mode: $mode,
                report_body: null
            })
            RETURN
                r.id as report_job_id,
                r.tenant_id as tenant_id,
                r.user_id as user_id,
                r.conversation_id as conversation_id,
                r.report_type as report_type,
                r.title as title,
                r.status as status,
                toString(r.created_at) as created_at,
                toString(r.started_at) as started_at,
                toString(r.completed_at) as completed_at,
                toString(r.cancel_requested_at) as cancel_requested_at,
                toString(r.cancelled_at) as cancelled_at,
                toString(r.deleted_at) as deleted_at,
                r.cancellation_reason as cancellation_reason,
                r.error_message as error_message,
                r.output_file_id as output_file_id,
                r.output_url as output_url,
                r.summary_text as summary_text,
                r.report_fidelity_level as report_fidelity_level,
                r.report_context_reduction_applied as report_context_reduction_applied,
                r.report_context_strategy as report_context_strategy,
                r.retrieval_doc_count as retrieval_doc_count,
                r.retrieval_chunk_count as retrieval_chunk_count,
                r.context_chars_included as context_chars_included,
                r.generation_model as generation_model,
                r.generation_attempts_used as generation_attempts_used,
                r.failure_stage as failure_stage,
                r.failure_code as failure_code,
                r.failure_detail as failure_detail,
                r.query_text as query,
                r.mode as mode
            """
            result = await session.run(
                query,
                report_job_id=report_job_id,
                tenant_id=tenant_id,
                user_id=user_id,
                conversation_id=conversation_id,
                report_type=report_type,
                title=title,
                query_text=query_text,
                mode=mode,
            )
            record = await result.single()
            if not record:
                raise Exception("Failed to create Riley report job")
            created_job = dict(record)
            await self.append_analytics_event(
                event_id=f"riley_report_job:{report_job_id}:created",
                source_event_type_raw="report_job_created",
                source_entity="RileyReportJob",
                campaign_id=tenant_id,
                user_id=user_id,
                actor_user_id=user_id,
                occurred_at=created_job.get("created_at"),
                object_id=report_job_id,
                status=created_job.get("status"),
                metadata={
                    "mode": mode,
                    "report_type": report_type,
                    "conversation_id": conversation_id,
                },
            )
            return created_job

    async def list_riley_report_jobs(
        self,
        *,
        tenant_id: str,
        user_id: str,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """List report jobs for tenant/user scope."""
        if not await self._label_exists("RileyReportJob"):
            return []

        async with self._driver.session() as session:
            query = """
            MATCH (r:RileyReportJob {tenant_id: $tenant_id, user_id: $user_id})
            WHERE r.deleted_at IS NULL AND coalesce(r.status, "") <> "deleted"
            RETURN properties(r) as props
            LIMIT $limit
            """
            result = await session.run(
                query,
                tenant_id=tenant_id,
                user_id=user_id,
                limit=limit,
            )
            jobs: List[Dict[str, Any]] = []
            async for record in result:
                props = dict(record.get("props") or {})
                jobs.append(
                    {
                        "report_job_id": props.get("id"),
                        "tenant_id": props.get("tenant_id"),
                        "user_id": props.get("user_id"),
                        "conversation_id": props.get("conversation_id"),
                        "report_type": props.get("report_type"),
                        "title": props.get("title"),
                        "status": props.get("status"),
                        "created_at": str(props.get("created_at")) if props.get("created_at") else None,
                        "started_at": str(props.get("started_at")) if props.get("started_at") else None,
                        "completed_at": str(props.get("completed_at")) if props.get("completed_at") else None,
                        "cancel_requested_at": str(props.get("cancel_requested_at")) if props.get("cancel_requested_at") else None,
                        "cancelled_at": str(props.get("cancelled_at")) if props.get("cancelled_at") else None,
                        "deleted_at": str(props.get("deleted_at")) if props.get("deleted_at") else None,
                        "cancellation_reason": props.get("cancellation_reason"),
                        "error_message": props.get("error_message"),
                        "output_file_id": props.get("output_file_id"),
                        "output_url": props.get("output_url"),
                        "summary_text": props.get("summary_text"),
                        "report_fidelity_level": props.get("report_fidelity_level"),
                        "report_context_reduction_applied": props.get("report_context_reduction_applied"),
                        "report_context_strategy": props.get("report_context_strategy"),
                        "retrieval_doc_count": props.get("retrieval_doc_count"),
                        "retrieval_chunk_count": props.get("retrieval_chunk_count"),
                        "context_chars_included": props.get("context_chars_included"),
                        "generation_model": props.get("generation_model"),
                        "generation_attempts_used": props.get("generation_attempts_used"),
                        "failure_stage": props.get("failure_stage"),
                        "failure_code": props.get("failure_code"),
                        "failure_detail": props.get("failure_detail"),
                        "query": props.get("query_text"),
                        "mode": props.get("mode"),
                    }
                )
            jobs.sort(
                key=lambda item: (item.get("started_at") or item.get("created_at") or ""),
                reverse=True,
            )
            return jobs

    async def count_active_riley_report_jobs_for_tenant(self, *, tenant_id: str) -> int:
        """Count active report jobs for a tenant across all users."""
        if not await self._label_exists("RileyReportJob"):
            return 0
        async with self._driver.session() as session:
            query = """
            MATCH (r:RileyReportJob {tenant_id: $tenant_id})
            WHERE r.deleted_at IS NULL
              AND coalesce(r.status, "") IN ["queued", "processing", "cancelling"]
            RETURN count(r) as active_count
            """
            result = await session.run(query, tenant_id=tenant_id)
            record = await result.single()
            if not record:
                return 0
            return int(record.get("active_count") or 0)

    async def get_riley_report_job(
        self,
        *,
        report_job_id: str,
        tenant_id: str,
        user_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Get one report job in tenant/user scope, including report body."""
        if not await self._label_exists("RileyReportJob"):
            return None

        async with self._driver.session() as session:
            query = """
            MATCH (r:RileyReportJob {
                id: $report_job_id,
                tenant_id: $tenant_id,
                user_id: $user_id
            })
            RETURN properties(r) as props
            """
            result = await session.run(
                query,
                report_job_id=report_job_id,
                tenant_id=tenant_id,
                user_id=user_id,
            )
            record = await result.single()
            if not record:
                return None
            props = dict(record.get("props") or {})
            return {
                "report_job_id": props.get("id"),
                "tenant_id": props.get("tenant_id"),
                "user_id": props.get("user_id"),
                "conversation_id": props.get("conversation_id"),
                "report_type": props.get("report_type"),
                "title": props.get("title"),
                "status": props.get("status"),
                "created_at": str(props.get("created_at")) if props.get("created_at") else None,
                "started_at": str(props.get("started_at")) if props.get("started_at") else None,
                "completed_at": str(props.get("completed_at")) if props.get("completed_at") else None,
                "cancel_requested_at": str(props.get("cancel_requested_at")) if props.get("cancel_requested_at") else None,
                "cancelled_at": str(props.get("cancelled_at")) if props.get("cancelled_at") else None,
                "deleted_at": str(props.get("deleted_at")) if props.get("deleted_at") else None,
                "cancellation_reason": props.get("cancellation_reason"),
                "error_message": props.get("error_message"),
                "output_file_id": props.get("output_file_id"),
                "output_url": props.get("output_url"),
                "summary_text": props.get("summary_text"),
                "report_fidelity_level": props.get("report_fidelity_level"),
                "report_context_reduction_applied": props.get("report_context_reduction_applied"),
                "report_context_strategy": props.get("report_context_strategy"),
                "retrieval_doc_count": props.get("retrieval_doc_count"),
                "retrieval_chunk_count": props.get("retrieval_chunk_count"),
                "context_chars_included": props.get("context_chars_included"),
                "generation_model": props.get("generation_model"),
                "generation_attempts_used": props.get("generation_attempts_used"),
                "failure_stage": props.get("failure_stage"),
                "failure_code": props.get("failure_code"),
                "failure_detail": props.get("failure_detail"),
                "query": props.get("query_text"),
                "mode": props.get("mode"),
                "report_body": props.get("report_body"),
            }

    async def get_riley_report_job_for_worker(
        self,
        *,
        report_job_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Get one report job by ID for trusted internal worker execution."""
        if not await self._label_exists("RileyReportJob"):
            return None

        async with self._driver.session() as session:
            query = """
            MATCH (r:RileyReportJob {id: $report_job_id})
            RETURN properties(r) as props
            """
            result = await session.run(query, report_job_id=report_job_id)
            record = await result.single()
            if not record:
                return None
            props = dict(record.get("props") or {})
            return {
                "report_job_id": props.get("id"),
                "tenant_id": props.get("tenant_id"),
                "user_id": props.get("user_id"),
                "conversation_id": props.get("conversation_id"),
                "report_type": props.get("report_type"),
                "title": props.get("title"),
                "status": props.get("status"),
                "created_at": str(props.get("created_at")) if props.get("created_at") else None,
                "started_at": str(props.get("started_at")) if props.get("started_at") else None,
                "completed_at": str(props.get("completed_at")) if props.get("completed_at") else None,
                "cancel_requested_at": str(props.get("cancel_requested_at")) if props.get("cancel_requested_at") else None,
                "cancelled_at": str(props.get("cancelled_at")) if props.get("cancelled_at") else None,
                "deleted_at": str(props.get("deleted_at")) if props.get("deleted_at") else None,
                "cancellation_reason": props.get("cancellation_reason"),
                "error_message": props.get("error_message"),
                "output_file_id": props.get("output_file_id"),
                "output_url": props.get("output_url"),
                "summary_text": props.get("summary_text"),
                "report_fidelity_level": props.get("report_fidelity_level"),
                "report_context_reduction_applied": props.get("report_context_reduction_applied"),
                "report_context_strategy": props.get("report_context_strategy"),
                "retrieval_doc_count": props.get("retrieval_doc_count"),
                "retrieval_chunk_count": props.get("retrieval_chunk_count"),
                "context_chars_included": props.get("context_chars_included"),
                "generation_model": props.get("generation_model"),
                "generation_attempts_used": props.get("generation_attempts_used"),
                "failure_stage": props.get("failure_stage"),
                "failure_code": props.get("failure_code"),
                "failure_detail": props.get("failure_detail"),
                "query": props.get("query_text"),
                "mode": props.get("mode"),
                "report_body": props.get("report_body"),
            }

    async def update_riley_report_job(
        self,
        *,
        report_job_id: str,
        tenant_id: str,
        user_id: str,
        status: str,
        started_at: Optional[str] = None,
        completed_at: Optional[str] = None,
        error_message: Optional[str] = None,
        output_file_id: Optional[str] = None,
        output_url: Optional[str] = None,
        summary_text: Optional[str] = None,
        report_body: Optional[str] = None,
        cancel_requested_at: Optional[str] = None,
        cancelled_at: Optional[str] = None,
        deleted_at: Optional[str] = None,
        cancellation_reason: Optional[str] = None,
        report_fidelity_level: Optional[str] = None,
        report_context_reduction_applied: Optional[bool] = None,
        report_context_strategy: Optional[str] = None,
        retrieval_doc_count: Optional[int] = None,
        retrieval_chunk_count: Optional[int] = None,
        context_chars_included: Optional[int] = None,
        generation_model: Optional[str] = None,
        generation_attempts_used: Optional[int] = None,
        failure_stage: Optional[str] = None,
        failure_code: Optional[str] = None,
        failure_detail: Optional[str] = None,
    ) -> None:
        """Update report job status and optional output fields."""
        async with self._driver.session() as session:
            query = """
            MATCH (r:RileyReportJob {
                id: $report_job_id,
                tenant_id: $tenant_id,
                user_id: $user_id
            })
            SET
                r.status = $status,
                r.started_at = CASE
                    WHEN $started_at IS NULL THEN r.started_at
                    ELSE datetime($started_at)
                END,
                r.completed_at = CASE
                    WHEN $completed_at IS NULL THEN r.completed_at
                    ELSE datetime($completed_at)
                END,
                r.cancel_requested_at = CASE
                    WHEN $cancel_requested_at IS NULL THEN r.cancel_requested_at
                    ELSE datetime($cancel_requested_at)
                END,
                r.cancelled_at = CASE
                    WHEN $cancelled_at IS NULL THEN r.cancelled_at
                    ELSE datetime($cancelled_at)
                END,
                r.deleted_at = CASE
                    WHEN $deleted_at IS NULL THEN r.deleted_at
                    ELSE datetime($deleted_at)
                END,
                r.cancellation_reason = CASE
                    WHEN $cancellation_reason IS NULL THEN r.cancellation_reason
                    ELSE $cancellation_reason
                END,
                r.error_message = $error_message,
                r.output_file_id = $output_file_id,
                r.output_url = $output_url,
                r.summary_text = $summary_text,
                r.report_fidelity_level = $report_fidelity_level,
                r.report_context_reduction_applied = CASE
                    WHEN $report_context_reduction_applied IS NULL THEN r.report_context_reduction_applied
                    ELSE $report_context_reduction_applied
                END,
                r.report_context_strategy = $report_context_strategy,
                r.retrieval_doc_count = CASE
                    WHEN $retrieval_doc_count IS NULL THEN r.retrieval_doc_count
                    ELSE $retrieval_doc_count
                END,
                r.retrieval_chunk_count = CASE
                    WHEN $retrieval_chunk_count IS NULL THEN r.retrieval_chunk_count
                    ELSE $retrieval_chunk_count
                END,
                r.context_chars_included = CASE
                    WHEN $context_chars_included IS NULL THEN r.context_chars_included
                    ELSE $context_chars_included
                END,
                r.generation_model = $generation_model,
                r.generation_attempts_used = CASE
                    WHEN $generation_attempts_used IS NULL THEN r.generation_attempts_used
                    ELSE $generation_attempts_used
                END,
                r.failure_stage = $failure_stage,
                r.failure_code = $failure_code,
                r.failure_detail = $failure_detail,
                r.report_body = CASE
                    WHEN $report_body IS NULL THEN r.report_body
                    ELSE $report_body
                END
            """
            await session.run(
                query,
                report_job_id=report_job_id,
                tenant_id=tenant_id,
                user_id=user_id,
                status=status,
                started_at=started_at,
                completed_at=completed_at,
                error_message=error_message,
                output_file_id=output_file_id,
                output_url=output_url,
                summary_text=summary_text,
                report_body=report_body,
                cancel_requested_at=cancel_requested_at,
                cancelled_at=cancelled_at,
                deleted_at=deleted_at,
                cancellation_reason=cancellation_reason,
                report_fidelity_level=report_fidelity_level,
                report_context_reduction_applied=report_context_reduction_applied,
                report_context_strategy=report_context_strategy,
                retrieval_doc_count=retrieval_doc_count,
                retrieval_chunk_count=retrieval_chunk_count,
                context_chars_included=context_chars_included,
                generation_model=generation_model,
                generation_attempts_used=generation_attempts_used,
                failure_stage=failure_stage,
                failure_code=failure_code,
                failure_detail=failure_detail,
            )
        await self.append_analytics_event(
            event_id=f"riley_report_job:{report_job_id}:status:{status}:{now_iso_utc()}",
            source_event_type_raw="report_job_status_changed",
            source_entity="RileyReportJob",
            campaign_id=tenant_id,
            user_id=user_id,
            actor_user_id=user_id,
            object_id=report_job_id,
            status=status,
            model=generation_model,
            metadata={
                "failure_stage": failure_stage,
                "failure_code": failure_code,
                "retrieval_doc_count": retrieval_doc_count,
                "retrieval_chunk_count": retrieval_chunk_count,
                "context_chars_included": context_chars_included,
                "report_fidelity_level": report_fidelity_level,
                "report_context_strategy": report_context_strategy,
            },
        )

    async def upsert_riley_document_intelligence(
        self,
        *,
        file_id: str,
        tenant_id: str,
        is_global: bool,
        status: str,
        analysis_started_at: Optional[str] = None,
        analysis_completed_at: Optional[str] = None,
        analysis_error: Optional[str] = None,
        doc_summary_short: Optional[str] = None,
        doc_summary_long: Optional[str] = None,
        key_themes: Optional[List[str]] = None,
        key_entities: Optional[List[str]] = None,
        sentiment_overall: Optional[str] = None,
        tone_labels: Optional[List[str]] = None,
        framing_labels: Optional[List[str]] = None,
        audience_implications: Optional[List[str]] = None,
        persuasion_risks: Optional[List[str]] = None,
        strategic_opportunities: Optional[List[str]] = None,
        tone_profile: Optional[str] = None,
        framing_profile: Optional[str] = None,
        strategic_notes: Optional[str] = None,
        major_claims_or_evidence: Optional[List[str]] = None,
        source_chunk_count: Optional[int] = None,
        source_char_count: Optional[int] = None,
        analysis_fidelity_level: Optional[str] = None,
        analysis_retry_count_used: Optional[int] = None,
        analysis_selection_strategy: Optional[str] = None,
        analysis_context_reduction_applied: Optional[bool] = None,
        chunks_coverage_ratio: Optional[float] = None,
        chars_coverage_ratio: Optional[float] = None,
        ocr_content_included: Optional[bool] = None,
        vision_content_included: Optional[bool] = None,
        analysis_execution_mode: Optional[str] = None,
        total_bands: Optional[int] = None,
        analyzed_bands: Optional[int] = None,
        band_coverage_ratio: Optional[float] = None,
        contradiction_count: Optional[int] = None,
        validation_status: Optional[str] = None,
        validation_note: Optional[str] = None,
        band_artifacts_json: Optional[str] = None,
        intra_document_tensions_json: Optional[str] = None,
    ) -> None:
        """Persist per-document intelligence artifact in Neo4j."""
        artifact_id = f"{tenant_id}:{file_id}"
        async with self._driver.session() as session:
            query = """
            MERGE (d:RileyDocumentIntelligence {id: $artifact_id})
            ON CREATE SET
                d.created_at = datetime(),
                d.file_id = $file_id,
                d.tenant_id = $tenant_id,
                d.is_global = $is_global
            SET
                d.updated_at = datetime(),
                d.status = $status,
                d.analysis_started_at = CASE
                    WHEN $analysis_started_at IS NULL THEN d.analysis_started_at
                    ELSE datetime($analysis_started_at)
                END,
                d.analysis_completed_at = CASE
                    WHEN $analysis_completed_at IS NULL THEN d.analysis_completed_at
                    ELSE datetime($analysis_completed_at)
                END,
                d.analysis_error = $analysis_error,
                d.doc_summary_short = $doc_summary_short,
                d.doc_summary_long = $doc_summary_long,
                d.key_themes = CASE
                    WHEN $key_themes IS NULL THEN d.key_themes
                    ELSE $key_themes
                END,
                d.key_entities = CASE
                    WHEN $key_entities IS NULL THEN d.key_entities
                    ELSE $key_entities
                END,
                d.sentiment_overall = $sentiment_overall,
                d.tone_labels = CASE
                    WHEN $tone_labels IS NULL THEN d.tone_labels
                    ELSE $tone_labels
                END,
                d.framing_labels = CASE
                    WHEN $framing_labels IS NULL THEN d.framing_labels
                    ELSE $framing_labels
                END,
                d.audience_implications = CASE
                    WHEN $audience_implications IS NULL THEN d.audience_implications
                    ELSE $audience_implications
                END,
                d.persuasion_risks = CASE
                    WHEN $persuasion_risks IS NULL THEN d.persuasion_risks
                    ELSE $persuasion_risks
                END,
                d.strategic_opportunities = CASE
                    WHEN $strategic_opportunities IS NULL THEN d.strategic_opportunities
                    ELSE $strategic_opportunities
                END,
                d.tone_profile = $tone_profile,
                d.framing_profile = $framing_profile,
                d.strategic_notes = $strategic_notes,
                d.major_claims_or_evidence = CASE
                    WHEN $major_claims_or_evidence IS NULL THEN d.major_claims_or_evidence
                    ELSE $major_claims_or_evidence
                END,
                d.source_chunk_count = $source_chunk_count,
                d.source_char_count = $source_char_count,
                d.analysis_fidelity_level = $analysis_fidelity_level,
                d.analysis_retry_count_used = $analysis_retry_count_used,
                d.analysis_selection_strategy = $analysis_selection_strategy,
                d.analysis_context_reduction_applied = $analysis_context_reduction_applied,
                d.chunks_coverage_ratio = $chunks_coverage_ratio,
                d.chars_coverage_ratio = $chars_coverage_ratio,
                d.ocr_content_included = $ocr_content_included,
                d.vision_content_included = $vision_content_included,
                d.analysis_execution_mode = $analysis_execution_mode,
                d.total_bands = $total_bands,
                d.analyzed_bands = $analyzed_bands,
                d.band_coverage_ratio = $band_coverage_ratio,
                d.contradiction_count = $contradiction_count,
                d.validation_status = $validation_status,
                d.validation_note = $validation_note,
                d.band_artifacts_json = CASE
                    WHEN $band_artifacts_json IS NULL THEN d.band_artifacts_json
                    ELSE $band_artifacts_json
                END,
                d.intra_document_tensions_json = CASE
                    WHEN $intra_document_tensions_json IS NULL THEN d.intra_document_tensions_json
                    ELSE $intra_document_tensions_json
                END
            """
            await session.run(
                query,
                artifact_id=artifact_id,
                file_id=file_id,
                tenant_id=tenant_id,
                is_global=is_global,
                status=status,
                analysis_started_at=analysis_started_at,
                analysis_completed_at=analysis_completed_at,
                analysis_error=analysis_error,
                doc_summary_short=doc_summary_short,
                doc_summary_long=doc_summary_long,
                key_themes=key_themes,
                key_entities=key_entities,
                sentiment_overall=sentiment_overall,
                tone_labels=tone_labels,
                framing_labels=framing_labels,
                audience_implications=audience_implications,
                persuasion_risks=persuasion_risks,
                strategic_opportunities=strategic_opportunities,
                tone_profile=tone_profile,
                framing_profile=framing_profile,
                strategic_notes=strategic_notes,
                major_claims_or_evidence=major_claims_or_evidence,
                source_chunk_count=source_chunk_count,
                source_char_count=source_char_count,
                analysis_fidelity_level=analysis_fidelity_level,
                analysis_retry_count_used=analysis_retry_count_used,
                analysis_selection_strategy=analysis_selection_strategy,
                analysis_context_reduction_applied=analysis_context_reduction_applied,
                chunks_coverage_ratio=chunks_coverage_ratio,
                chars_coverage_ratio=chars_coverage_ratio,
                ocr_content_included=ocr_content_included,
                vision_content_included=vision_content_included,
                analysis_execution_mode=analysis_execution_mode,
                total_bands=total_bands,
                analyzed_bands=analyzed_bands,
                band_coverage_ratio=band_coverage_ratio,
                contradiction_count=contradiction_count,
                validation_status=validation_status,
                validation_note=validation_note,
                band_artifacts_json=band_artifacts_json,
                intra_document_tensions_json=intra_document_tensions_json,
            )

    async def create_riley_campaign_intelligence_job(
        self,
        *,
        job_id: str,
        tenant_id: str,
        requested_by_user_id: Optional[str],
        trigger_source: str,
    ) -> Dict[str, Any]:
        """Create a campaign-level intelligence aggregation job record."""
        async with self._driver.session() as session:
            query = """
            CREATE (j:RileyCampaignIntelligenceJob {
                id: $job_id,
                tenant_id: $tenant_id,
                requested_by_user_id: $requested_by_user_id,
                trigger_source: $trigger_source,
                status: "queued",
                created_at: datetime(),
                started_at: null,
                completed_at: null,
                error_message: null
            })
            RETURN
                j.id as job_id,
                j.tenant_id as tenant_id,
                j.requested_by_user_id as requested_by_user_id,
                j.trigger_source as trigger_source,
                j.status as status,
                toString(j.created_at) as created_at,
                toString(j.started_at) as started_at,
                toString(j.completed_at) as completed_at,
                j.error_message as error_message
            """
            result = await session.run(
                query,
                job_id=job_id,
                tenant_id=tenant_id,
                requested_by_user_id=requested_by_user_id,
                trigger_source=trigger_source,
            )
            record = await result.single()
            if not record:
                raise Exception("Failed to create campaign intelligence job")
            return dict(record)

    async def get_riley_campaign_intelligence_job_for_worker(
        self,
        *,
        job_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Get one campaign intelligence job by ID for trusted worker execution."""
        async with self._driver.session() as session:
            query = """
            MATCH (j:RileyCampaignIntelligenceJob {id: $job_id})
            RETURN
                j.id as job_id,
                j.tenant_id as tenant_id,
                j.requested_by_user_id as requested_by_user_id,
                j.trigger_source as trigger_source,
                j.status as status,
                toString(j.created_at) as created_at,
                toString(j.started_at) as started_at,
                toString(j.completed_at) as completed_at,
                j.error_message as error_message
            """
            result = await session.run(query, job_id=job_id)
            record = await result.single()
            return dict(record) if record else None

    async def update_riley_campaign_intelligence_job(
        self,
        *,
        job_id: str,
        status: str,
        started_at: Optional[str] = None,
        completed_at: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """Update campaign intelligence job status."""
        async with self._driver.session() as session:
            query = """
            MATCH (j:RileyCampaignIntelligenceJob {id: $job_id})
            SET
                j.status = $status,
                j.started_at = CASE
                    WHEN $started_at IS NULL THEN j.started_at
                    ELSE datetime($started_at)
                END,
                j.completed_at = CASE
                    WHEN $completed_at IS NULL THEN j.completed_at
                    ELSE datetime($completed_at)
                END,
                j.error_message = $error_message
            """
            await session.run(
                query,
                job_id=job_id,
                status=status,
                started_at=started_at,
                completed_at=completed_at,
                error_message=error_message,
            )

    async def create_riley_campaign_intelligence_snapshot(
        self,
        *,
        tenant_id: str,
        job_id: str,
        campaign_theme_clusters_json: str,
        dominant_narratives: List[str],
        key_actors_entities: List[str],
        sentiment_distribution_json: str,
        tone_distribution_json: str,
        framing_distribution_json: str,
        campaign_contradictions: List[str],
        contradiction_tensions_json: str,
        strategic_opportunities: List[str],
        strategic_risks: List[str],
        evidence_snippets: List[str],
        docs_total: int,
        docs_analyzed: int,
        docs_failed: int,
        partial_recompute: bool,
        doc_intel_coverage_ratio: float,
        input_completeness_status: str,
        input_completeness_note: str,
        doc_intel_full_fidelity_docs: int,
        doc_intel_degraded_docs: int,
        doc_intel_degraded_ratio: float,
        input_quality_status: str,
        input_quality_note: str,
    ) -> int:
        """Create a versioned campaign intelligence snapshot and return the version."""
        async with self._driver.session() as session:
            query = """
            OPTIONAL MATCH (prev:RileyCampaignIntelligenceSnapshot {tenant_id: $tenant_id})
            WITH coalesce(max(prev.version), 0) + 1 as next_version
            CREATE (s:RileyCampaignIntelligenceSnapshot {
                id: $snapshot_id,
                tenant_id: $tenant_id,
                job_id: $job_id,
                version: next_version,
                created_at: datetime(),
                campaign_theme_clusters_json: $campaign_theme_clusters_json,
                dominant_narratives: $dominant_narratives,
                key_actors_entities: $key_actors_entities,
                sentiment_distribution_json: $sentiment_distribution_json,
                tone_distribution_json: $tone_distribution_json,
                framing_distribution_json: $framing_distribution_json,
                campaign_contradictions: $campaign_contradictions,
                contradiction_tensions_json: $contradiction_tensions_json,
                strategic_opportunities: $strategic_opportunities,
                strategic_risks: $strategic_risks,
                evidence_snippets: $evidence_snippets,
                docs_total: $docs_total,
                docs_analyzed: $docs_analyzed,
                docs_failed: $docs_failed,
                partial_recompute: $partial_recompute,
                doc_intel_coverage_ratio: $doc_intel_coverage_ratio,
                input_completeness_status: $input_completeness_status,
                input_completeness_note: $input_completeness_note,
                doc_intel_full_fidelity_docs: $doc_intel_full_fidelity_docs,
                doc_intel_degraded_docs: $doc_intel_degraded_docs,
                doc_intel_degraded_ratio: $doc_intel_degraded_ratio,
                input_quality_status: $input_quality_status,
                input_quality_note: $input_quality_note
            })
            RETURN next_version as version
            """
            result = await session.run(
                query,
                snapshot_id=f"{tenant_id}:{job_id}",
                tenant_id=tenant_id,
                job_id=job_id,
                campaign_theme_clusters_json=campaign_theme_clusters_json,
                dominant_narratives=dominant_narratives,
                key_actors_entities=key_actors_entities,
                sentiment_distribution_json=sentiment_distribution_json,
                tone_distribution_json=tone_distribution_json,
                framing_distribution_json=framing_distribution_json,
                campaign_contradictions=campaign_contradictions,
                contradiction_tensions_json=contradiction_tensions_json,
                strategic_opportunities=strategic_opportunities,
                strategic_risks=strategic_risks,
                evidence_snippets=evidence_snippets,
                docs_total=docs_total,
                docs_analyzed=docs_analyzed,
                docs_failed=docs_failed,
                partial_recompute=partial_recompute,
                doc_intel_coverage_ratio=doc_intel_coverage_ratio,
                input_completeness_status=input_completeness_status,
                input_completeness_note=input_completeness_note,
                doc_intel_full_fidelity_docs=doc_intel_full_fidelity_docs,
                doc_intel_degraded_docs=doc_intel_degraded_docs,
                doc_intel_degraded_ratio=doc_intel_degraded_ratio,
                input_quality_status=input_quality_status,
                input_quality_note=input_quality_note,
            )
            record = await result.single()
            return int(record["version"]) if record else 1

    async def get_latest_riley_campaign_intelligence_snapshot(
        self,
        *,
        tenant_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Return latest campaign intelligence snapshot for a tenant."""
        async with self._driver.session() as session:
            query = """
            MATCH (s:RileyCampaignIntelligenceSnapshot {tenant_id: $tenant_id})
            RETURN
                s.id as snapshot_id,
                s.tenant_id as tenant_id,
                s.job_id as job_id,
                s.version as version,
                toString(s.created_at) as created_at,
                s.campaign_theme_clusters_json as campaign_theme_clusters_json,
                s.dominant_narratives as dominant_narratives,
                s.key_actors_entities as key_actors_entities,
                s.sentiment_distribution_json as sentiment_distribution_json,
                s.tone_distribution_json as tone_distribution_json,
                s.framing_distribution_json as framing_distribution_json,
                s.campaign_contradictions as campaign_contradictions,
                s.contradiction_tensions_json as contradiction_tensions_json,
                s.strategic_opportunities as strategic_opportunities,
                s.strategic_risks as strategic_risks,
                s.evidence_snippets as evidence_snippets,
                s.docs_total as docs_total,
                s.docs_analyzed as docs_analyzed,
                s.docs_failed as docs_failed,
                s.partial_recompute as partial_recompute,
                s.doc_intel_coverage_ratio as doc_intel_coverage_ratio,
                s.input_completeness_status as input_completeness_status,
                s.input_completeness_note as input_completeness_note,
                s.doc_intel_full_fidelity_docs as doc_intel_full_fidelity_docs,
                s.doc_intel_degraded_docs as doc_intel_degraded_docs,
                s.doc_intel_degraded_ratio as doc_intel_degraded_ratio,
                s.input_quality_status as input_quality_status,
                s.input_quality_note as input_quality_note
            ORDER BY s.version DESC
            LIMIT 1
            """
            result = await session.run(query, tenant_id=tenant_id)
            record = await result.single()
            return dict(record) if record else None

    async def search_campaigns_fuzzy(self, query: str) -> str:
        """Search campaigns in the graph using fuzzy matching on name and description.
        
        Removes common stop words and searches for campaigns that contain the query terms.
        This is the "Golden Set" - structured campaign data from the graph.
        
        Args:
            query: The search query string
            
        Returns:
            Formatted string summary of matching campaigns, or empty string if none found
        """
        # Remove common stop words
        stop_words = {"have", "we", "done", "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for", "of", "with", "by", "is", "are", "was", "were", "be", "been", "being"}
        query_terms = [word.lower() for word in query.split() if word.lower() not in stop_words and len(word) > 2]
        
        if not query_terms:
            return ""
        
        # Use the first meaningful term for search
        search_term = query_terms[0]
        
        try:
            async with self._driver.session() as session:
                cypher_query = """
                MATCH (c:Campaign)
                WHERE toLower(c.name) CONTAINS $term OR toLower(c.description) CONTAINS $term
                RETURN c.name as name, c.description as description
                LIMIT 3
                """
                
                result = await session.run(cypher_query, term=search_term)
                
                campaigns = []
                async for record in result:
                    name = record.get("name", "Unknown Campaign")
                    description = record.get("description", "No description")
                    campaigns.append(f"Campaign: '{name}' ({description})")
                
                if campaigns:
                    return "Graph found: " + " | ".join(campaigns)
                return ""
        except Exception as e:
            # Log error but return empty string to not break the flow
            print(f"Error searching campaigns in graph: {e}")
            return ""

    async def get_campaign_names_by_ids(self, campaign_ids: List[str]) -> Dict[str, str]:
        """Resolve campaign ids to human-readable campaign names."""
        normalized_ids = [
            str(campaign_id or "").strip()
            for campaign_id in campaign_ids
            if str(campaign_id or "").strip()
        ]
        if not normalized_ids:
            return {}
        async with self._driver.session() as session:
            query = """
            MATCH (c:Campaign)
            WHERE c.id IN $campaign_ids
            RETURN c.id as id, c.name as name
            """
            result = await session.run(query, campaign_ids=normalized_ids)
            mapping: Dict[str, str] = {}
            async for record in result:
                campaign_id = str(record.get("id") or "").strip()
                campaign_name = str(record.get("name") or "").strip()
                if campaign_id:
                    mapping[campaign_id] = campaign_name or campaign_id
            return mapping

    async def clear_chat_history(self, session_id: str, tenant_id: str, user_id: str) -> None:
        """Clear all messages for a chat session.
        
        Deletes the ChatSession node and all associated Message nodes using DETACH DELETE.
        This removes the session and all its relationships (links to messages).
        If the session doesn't exist, the query will simply match nothing (no error).
        
        SECURITY: MATCH includes tenant_id and user_id to prevent cross-scope deletion.
        Only deletes sessions that match all three criteria.
        
        Args:
            session_id: Unique identifier for the chat session to clear
            tenant_id: Tenant/client identifier for scope isolation
            user_id: User identifier for scope isolation
            
        Raises:
            Exception: If the deletion fails (wrapped in try/except by caller)
        """
        try:
            async with self._driver.session() as session:
                # Delete session and all its messages
                # DETACH DELETE removes the node and all its relationships
                # SECURITY: Only delete if all three match (prevents cross-scope deletion)
                query = """
                MATCH (s:ChatSession {
                    id: $session_id,
                    tenant_id: $tenant_id,
                    user_id: $user_id
                })-[r:BELONGS_TO]-(m:Message)
                DETACH DELETE s, m
                """
                
                result = await session.run(
                    query,
                    session_id=session_id,
                    tenant_id=tenant_id,
                    user_id=user_id
                )
                await result.consume()  # Ensure query executes
        except Exception as e:
            # Log error but don't crash - let the router handle it
            print(f"Error clearing chat history for session {session_id}: {e}")
            raise

    async def update_session_title(
        self, session_id: str, tenant_id: str, user_id: str, title: str
    ) -> str:
        """Update the title of a chat session.
        
        SECURITY: MATCH includes tenant_id and user_id to prevent cross-scope updates.
        Only updates sessions that match all three criteria.
        
        Args:
            session_id: Unique identifier for the chat session
            tenant_id: Tenant/client identifier for scope isolation
            user_id: User identifier for scope isolation
            title: New title for the session
            
        Returns:
            The updated title string
            
        Raises:
            ValueError: If the session is not found (doesn't match all three criteria)
            Exception: If the update fails
        """
        async with self._driver.session() as session:
            query = """
            MATCH (s:ChatSession {
                id: $id,
                tenant_id: $tenant_id,
                user_id: $user_id
            })
            SET s.title = $title
            RETURN s.title as title
            """
            
            result = await session.run(
                query,
                id=session_id,
                tenant_id=tenant_id,
                user_id=user_id,
                title=title
            )
            record = await result.single()
            
            if not record:
                raise ValueError(
                    f"Session {session_id} not found for tenant {tenant_id} and user {user_id}"
                )
            
            return record["title"]

    async def create_campaign(
        self, name: str, description: Optional[str], user_id: str
    ) -> Dict[str, Any]:
        """Create a new campaign and add the creator as Lead member.
        
        Generates a server-side UUID for the campaign_id and creates:
        - User node (if not exists)
        - Campaign node with metadata
        - MEMBER_OF relationship with role="Lead"
        
        Args:
            name: Campaign name
            description: Optional campaign description
            user_id: ID of the user creating the campaign
            
        Returns:
            Dictionary with campaign details: {id, name, description, role: "Lead"}
        """
        campaign_id = str(uuid.uuid4())
        
        async with self._driver.session() as session:
            query = """
            MERGE (u:User {id: $user_id})
            CREATE (c:Campaign {
                id: $campaign_id,
                name: $name,
                description: $description,
                status: "active",
                created_at: datetime()
            })
            CREATE (u)-[:MEMBER_OF {
                role: "Lead",
                added_at: datetime()
            }]->(c)
            RETURN
                c.id as id,
                c.name as name,
                c.description as description,
                c.status as status,
                toString(c.archived_at) as archived_at
            """
            
            result = await session.run(
                query,
                user_id=user_id,
                campaign_id=campaign_id,
                name=name,
                description=description
            )
            record = await result.single()
            
            if not record:
                raise Exception("Failed to create campaign")
            
            return {
                "id": record["id"],
                "name": record["name"],
                "description": record.get("description"),
                "role": "Lead",
                "status": record.get("status") or "active",
                "archived_at": record.get("archived_at"),
            }

    async def get_user_campaigns(self, user_id: str, status_filter: str = "active") -> List[Dict[str, Any]]:
        """Get all campaigns that a user is a member of.
        
        Args:
            user_id: ID of the user
            
        Returns:
            List of campaign dictionaries: [{id, name, description, role}, ...]
        """
        async with self._driver.session() as session:
            query = """
            MATCH (u:User {id: $user_id})-[r:MEMBER_OF]->(c:Campaign)
            WHERE (
                ($status_filter = "active" AND coalesce(c.status, "active") = "active")
                OR ($status_filter = "archived" AND c.status = "archived")
            )
            RETURN
                c.id as id,
                c.name as name,
                c.description as description,
                r.role as role,
                coalesce(c.status, "active") as status,
                toString(c.archived_at) as archived_at
            ORDER BY c.created_at DESC
            """
            
            result = await session.run(query, user_id=user_id, status_filter=status_filter)
            
            campaigns = []
            async for record in result:
                campaigns.append({
                    "id": record["id"],
                    "name": record["name"],
                    "description": record.get("description"),
                    "role": record["role"],
                    "access": "member",
                    "status": record.get("status") or "active",
                    "archived_at": record.get("archived_at"),
                })
            
            return campaigns

    async def get_all_campaigns_with_access(self, user_id: str, status_filter: str = "active") -> List[Dict[str, Any]]:
        """Return metadata-only campaign list across org with user access status.

        access:
        - "member" when the user has MEMBER_OF relationship to campaign
        - "requestable" otherwise
        """
        async with self._driver.session() as session:
            query = """
            MATCH (c:Campaign)
            WHERE (
                ($status_filter = "active" AND coalesce(c.status, "active") = "active")
                OR ($status_filter = "archived" AND c.status = "archived")
            )
            OPTIONAL MATCH (owner:User)-[owner_rel:MEMBER_OF]->(c)
            WHERE owner_rel.role = "Lead"
            WITH c, owner
            OPTIONAL MATCH (me:User {id: $user_id})-[r:MEMBER_OF]->(c)
            RETURN
                c.id as id,
                c.name as name,
                c.description as description,
                toString(c.created_at) as created_at,
                coalesce(c.status, "active") as status,
                toString(c.archived_at) as archived_at,
                owner.id as owner_id,
                coalesce(owner.email, owner.id) as owner_name,
                CASE WHEN r IS NULL THEN "requestable" ELSE "member" END as access,
                r.role as role
            ORDER BY c.created_at DESC
            """

            result = await session.run(query, user_id=user_id, status_filter=status_filter)
            campaigns: List[Dict[str, Any]] = []
            async for record in result:
                campaigns.append(
                    {
                        "id": record["id"],
                        "name": record["name"],
                        "description": record.get("description"),
                        "role": record.get("role"),
                        "access": record["access"],
                        "created_at": record.get("created_at"),
                        "status": record.get("status") or "active",
                        "archived_at": record.get("archived_at"),
                        "owner_id": record.get("owner_id"),
                        "owner_name": record.get("owner_name"),
                    }
                )
            return campaigns

    async def archive_campaign(self, campaign_id: str) -> Dict[str, Any]:
        """Archive a campaign by setting status and archived timestamp."""
        async with self._driver.session() as session:
            query = """
            MATCH (c:Campaign {id: $campaign_id})
            SET
                c.status = "archived",
                c.archived_at = datetime()
            RETURN
                c.id as id,
                coalesce(c.status, "active") as status,
                toString(c.archived_at) as archived_at
            """
            result = await session.run(query, campaign_id=campaign_id)
            record = await result.single()
            if not record:
                raise ValueError(f"Campaign {campaign_id} not found")
            return {
                "id": record.get("id"),
                "status": record.get("status") or "archived",
                "archived_at": record.get("archived_at"),
            }

    async def create_access_request(
        self, tenant_id: str, requester_user_id: str, message: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create or update a pending access request for a campaign."""
        request_id = str(uuid.uuid4())
        normalized_message = message.strip() if isinstance(message, str) else None
        if normalized_message == "":
            normalized_message = None

        async with self._driver.session() as session:
            query = """
            MATCH (c:Campaign {id: $tenant_id})
            MERGE (u:User {id: $requester_user_id})
            MERGE (ar:CampaignAccessRequest {
                campaign_id: $tenant_id,
                user_id: $requester_user_id,
                status: "pending"
            })
            ON CREATE SET
                ar.id = $request_id,
                ar.message = $message,
                ar.created_at = datetime()
            ON MATCH SET
                ar.id = coalesce(ar.id, $request_id),
                ar.message = coalesce($message, ar.message)
            MERGE (u)-[:REQUESTED_ACCESS]->(ar)
            MERGE (ar)-[:FOR_CAMPAIGN]->(c)
            CREATE (evt:CampaignEvent {
                id: $event_id,
                campaign_id: $tenant_id,
                type: "access_request_created",
                message: "Access request submitted",
                user_id: $requester_user_id,
                actor_user_id: $requester_user_id,
                request_id: ar.id,
                created_at: datetime()
            })
            RETURN ar.id as id, toString(ar.created_at) as created_at, ar.status as status
            """
            result = await session.run(
                query,
                tenant_id=tenant_id,
                requester_user_id=requester_user_id,
                request_id=request_id,
                message=normalized_message,
                event_id=str(uuid.uuid4()),
            )
            record = await result.single()
            if not record:
                raise ValueError(f"Campaign {tenant_id} not found")
            created = {
                "id": record.get("id"),
                "status": record.get("status") or "pending",
                "created_at": record.get("created_at"),
            }
            await self.append_analytics_event(
                event_id=f"campaign_access_request:{created.get('id')}",
                source_event_type_raw="access_request_created",
                source_entity="CampaignAccessRequest",
                campaign_id=tenant_id,
                user_id=requester_user_id,
                actor_user_id=requester_user_id,
                occurred_at=created.get("created_at"),
                object_id=created.get("id"),
                status=created.get("status"),
                metadata={"message_present": bool(normalized_message)},
            )
            return created

    async def list_campaign_access_requests(
        self, campaign_id: str, status: str = "pending", limit: int = 50
    ) -> List[Dict[str, Any]]:
        """List campaign access requests by status."""
        async with self._driver.session() as session:
            query = """
            MATCH (ar:CampaignAccessRequest {campaign_id: $campaign_id})
            OPTIONAL MATCH (u:User {id: ar.user_id})
            WITH
                ar,
                u,
                CASE
                    WHEN u.email IS NULL OR trim(u.email) = "" THEN NULL
                    ELSE split(u.email, "@")[0]
                END as email_prefix
            WHERE ($status = "" OR ar.status = $status)
            RETURN
                ar.id as id,
                ar.campaign_id as campaign_id,
                ar.user_id as user_id,
                coalesce(u.email, "") as user_email,
                CASE
                    WHEN u.display_name IS NOT NULL AND trim(u.display_name) <> "" THEN u.display_name
                    WHEN u.username IS NOT NULL AND trim(u.username) <> "" THEN u.username
                    WHEN email_prefix IS NOT NULL AND trim(email_prefix) <> "" THEN email_prefix
                    WHEN u.email IS NOT NULL AND trim(u.email) <> "" THEN u.email
                    WHEN ar.user_id IS NOT NULL AND trim(ar.user_id) <> "" THEN ar.user_id
                    ELSE ar.user_id
                END as user_name,
                ar.message as message,
                ar.status as status,
                toString(ar.created_at) as created_at,
                toString(ar.decided_at) as decided_at,
                ar.decided_by as decided_by
            ORDER BY ar.created_at DESC
            LIMIT $limit
            """
            result = await session.run(
                query,
                campaign_id=campaign_id,
                status=(status or "").strip(),
                limit=limit,
            )
            requests: List[Dict[str, Any]] = []
            async for record in result:
                requests.append(
                    {
                        "id": record.get("id"),
                        "campaign_id": record.get("campaign_id"),
                        "user_id": record.get("user_id"),
                        "user_email": record.get("user_email") or "",
                        "user_name": record.get("user_name") or "",
                        "message": record.get("message"),
                        "status": record.get("status") or "pending",
                        "created_at": record.get("created_at"),
                        "decided_at": record.get("decided_at"),
                        "decided_by": record.get("decided_by"),
                    }
                )
            return requests

    async def decide_campaign_access_request(
        self, campaign_id: str, request_id: str, actor_user_id: str, decision: str
    ) -> Dict[str, Any]:
        """Approve or deny a pending campaign access request."""
        normalized_decision = (decision or "").strip().lower()
        if normalized_decision not in {"approved", "denied"}:
            raise ValueError("decision must be 'approved' or 'denied'")

        async with self._driver.session() as session:
            existing_result = await session.run(
                """
                MATCH (ar:CampaignAccessRequest {id: $request_id, campaign_id: $campaign_id})
                RETURN
                    ar.id as id,
                    ar.user_id as user_id,
                    ar.status as status,
                    toString(ar.created_at) as created_at,
                    toString(ar.decided_at) as decided_at,
                    ar.decided_by as decided_by
                """,
                request_id=request_id,
                campaign_id=campaign_id,
            )
            existing_record = await existing_result.single()
            if not existing_record:
                raise ValueError("Access request not found")

            existing_status = (existing_record.get("status") or "").strip().lower()
            if existing_status in {"approved", "denied"}:
                if existing_status != normalized_decision:
                    raise RuntimeError(
                        f"Access request already decided as '{existing_status}'"
                    )
                return {
                    "id": existing_record.get("id"),
                    "user_id": existing_record.get("user_id"),
                    "status": existing_record.get("status"),
                    "created_at": existing_record.get("created_at"),
                    "decided_at": existing_record.get("decided_at"),
                    "decided_by": existing_record.get("decided_by"),
                }

            query = """
            MATCH (ar:CampaignAccessRequest {id: $request_id, campaign_id: $campaign_id})
            WHERE ar.status = "pending"
            MATCH (c:Campaign {id: $campaign_id})
            MERGE (u:User {id: ar.user_id})
            SET
                ar.status = $decision,
                ar.decided_at = datetime(),
                ar.decided_by = $actor_user_id
            FOREACH (_ IN CASE WHEN $decision = "approved" THEN [1] ELSE [] END |
                MERGE (u)-[m:MEMBER_OF]->(c)
                SET m.role = coalesce(m.role, "Member"), m.added_at = datetime()
            )
            CREATE (evt:CampaignEvent {
                id: $event_id,
                campaign_id: $campaign_id,
                type: CASE WHEN $decision = "approved" THEN "access_request_approved" ELSE "access_request_denied" END,
                message: CASE WHEN $decision = "approved" THEN "Access request approved" ELSE "Access request denied" END,
                user_id: ar.user_id,
                actor_user_id: $actor_user_id,
                request_id: ar.id,
                created_at: datetime()
            })
            FOREACH (_ IN CASE WHEN $decision = "approved" THEN [1] ELSE [] END |
                CREATE (:CampaignEvent {
                    id: $user_event_id,
                    campaign_id: $campaign_id,
                    type: "campaign_member_added_notification",
                    message: "You were added to Campaign " + coalesce(c.name, $campaign_id),
                    user_id: ar.user_id,
                    actor_user_id: $actor_user_id,
                    request_id: ar.id,
                    created_at: datetime()
                })
            )
            RETURN
                ar.id as id,
                ar.user_id as user_id,
                ar.status as status,
                toString(ar.created_at) as created_at,
                toString(ar.decided_at) as decided_at,
                ar.decided_by as decided_by
            """
            result = await session.run(
                query,
                request_id=request_id,
                campaign_id=campaign_id,
                actor_user_id=actor_user_id,
                decision=normalized_decision,
                event_id=str(uuid.uuid4()),
                user_event_id=str(uuid.uuid4()),
            )
            record = await result.single()
            if not record:
                raise ValueError("Pending access request not found")
            decided_payload = {
                "id": record.get("id"),
                "user_id": record.get("user_id"),
                "status": record.get("status"),
                "created_at": record.get("created_at"),
                "decided_at": record.get("decided_at"),
                "decided_by": record.get("decided_by"),
            }
            decided_type = (
                "access_request_approved"
                if str(decided_payload.get("status") or "").lower() == "approved"
                else "access_request_denied"
            )
            await self.append_analytics_event(
                event_id=f"campaign_access_request_decision:{decided_payload.get('id')}:{decided_type}",
                source_event_type_raw=decided_type,
                source_entity="CampaignAccessRequest",
                campaign_id=campaign_id,
                user_id=decided_payload.get("user_id"),
                actor_user_id=actor_user_id,
                occurred_at=decided_payload.get("decided_at") or decided_payload.get("created_at"),
                object_id=decided_payload.get("id"),
                status=decided_payload.get("status"),
                metadata={"decision_actor_user_id": actor_user_id},
            )
            return decided_payload

    async def list_campaign_events(
        self,
        campaign_id: str,
        limit: int = 50,
        viewer_user_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """List recent campaign events for activity/feed surfaces."""
        async with self._driver.session() as session:
            query = """
            MATCH (e:CampaignEvent {campaign_id: $campaign_id})
            OPTIONAL MATCH (requester:User {id: e.user_id})
            WITH
                e,
                requester,
                CASE
                    WHEN requester.email IS NULL OR trim(requester.email) = "" THEN NULL
                    ELSE split(requester.email, "@")[0]
                END as requester_email_prefix
            WHERE
                (
                    $viewer_user_id IS NULL
                    OR NOT EXISTS {
                        MATCH (:User {id: $viewer_user_id})-[:DISMISSED_FEED_EVENT]->(e)
                    }
                )
                AND (
                    NOT (e.type IN [
                        "document_assigned_to_user",
                        "document_tagged_for_review",
                        "document_mentioned_user",
                        "campaign_message_mentioned_user",
                        "deadline_reminder_10m",
                        "deadline_happening_now"
                    ])
                    OR (
                        $viewer_user_id IS NOT NULL
                        AND (
                            e.user_id IS NULL
                            OR e.user_id = $viewer_user_id
                            OR e.actor_user_id = $viewer_user_id
                        )
                    )
                )
                AND (
                    e.type <> "access_request_created"
                    OR (
                        EXISTS {
                            MATCH (ar:CampaignAccessRequest {
                                id: e.request_id,
                                campaign_id: $campaign_id
                            })
                            WHERE ar.status = "pending"
                        }
                        OR (
                            e.request_id IS NULL
                            AND EXISTS {
                                MATCH (ar:CampaignAccessRequest {
                                    campaign_id: $campaign_id,
                                    user_id: e.user_id,
                                    status: "pending"
                                })
                            }
                        )
                    )
                )
            RETURN
                e.id as id,
                e.type as type,
                e.message as message,
                e.user_id as user_id,
                e.actor_user_id as actor_user_id,
                e.request_id as request_id,
                CASE
                    WHEN requester.display_name IS NOT NULL AND trim(requester.display_name) <> "" THEN requester.display_name
                    WHEN requester.username IS NOT NULL AND trim(requester.username) <> "" THEN requester.username
                    WHEN requester_email_prefix IS NOT NULL AND trim(requester_email_prefix) <> "" THEN requester_email_prefix
                    WHEN requester.email IS NOT NULL AND trim(requester.email) <> "" THEN requester.email
                    WHEN e.user_id IS NOT NULL AND trim(e.user_id) <> "" THEN e.user_id
                    ELSE e.user_id
                END as requester_display_name,
                toString(e.created_at) as created_at
            ORDER BY e.created_at DESC
            LIMIT $limit
            """
            result = await session.run(
                query,
                campaign_id=campaign_id,
                limit=limit,
                viewer_user_id=viewer_user_id,
            )
            items: List[Dict[str, Any]] = []
            async for record in result:
                items.append(
                    {
                        "id": record.get("id"),
                        "type": record.get("type") or "",
                        "message": record.get("message") or "",
                        "user_id": record.get("user_id"),
                        "actor_user_id": record.get("actor_user_id"),
                        "request_id": record.get("request_id"),
                        "requester_display_name": record.get("requester_display_name"),
                        "created_at": record.get("created_at"),
                    }
                )
            return items

    async def list_user_campaign_feed_events(
        self,
        *,
        user_id: str,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """List cross-campaign feed events relevant to a user."""
        async with self._driver.session() as session:
            query = """
            MATCH (c:Campaign)
            WHERE EXISTS {
                MATCH (:User {id: $user_id})-[:MEMBER_OF]->(c)
            }
            MATCH (e:CampaignEvent {campaign_id: c.id})
            OPTIONAL MATCH (requester:User {id: e.user_id})
            WITH
                c,
                e,
                requester,
                CASE
                    WHEN requester.email IS NULL OR trim(requester.email) = "" THEN NULL
                    ELSE split(requester.email, "@")[0]
                END as requester_email_prefix
            WHERE
                NOT (e.type IN [
                    "document_assigned_to_user",
                    "document_tagged_for_review",
                    "document_mentioned_user",
                    "campaign_message_mentioned_user"
                ])
                AND
                NOT EXISTS {
                    MATCH (:User {id: $user_id})-[:DISMISSED_FEED_EVENT]->(e)
                }
                AND
                (
                    e.type <> "access_request_created"
                    OR (
                        EXISTS {
                            MATCH (ar:CampaignAccessRequest {
                                id: e.request_id,
                                campaign_id: c.id
                            })
                            WHERE ar.status = "pending"
                        }
                        OR (
                            e.request_id IS NULL
                            AND EXISTS {
                                MATCH (ar:CampaignAccessRequest {
                                    campaign_id: c.id,
                                    user_id: e.user_id,
                                    status: "pending"
                                })
                            }
                        )
                    )
                )
                AND
                (
                e.user_id = $user_id
                OR e.actor_user_id = $user_id
                OR e.type IN [
                    "mention",
                    "document_assigned",
                    "document_moved_needs_review",
                    "document_moved_in_review",
                    "deadline_created",
                    "deadline_upcoming"
                ]
                OR (
                    e.type IN [
                        "deadline_reminder_10m",
                        "deadline_happening_now"
                    ]
                    AND (
                        e.user_id IS NULL
                        OR e.user_id = $user_id
                    )
                )
                OR (
                    EXISTS {
                        MATCH (:User {id: $user_id})-[m:MEMBER_OF]->(c)
                        WHERE m.role = "Lead"
                    }
                    AND e.type IN [
                        "access_request_created",
                        "access_request_approved",
                        "access_request_denied"
                    ]
                )
                )
            RETURN
                e.id as id,
                e.type as type,
                e.message as message,
                e.user_id as user_id,
                e.actor_user_id as actor_user_id,
                e.request_id as request_id,
                e.campaign_id as campaign_id,
                c.name as campaign_name,
                CASE
                    WHEN requester.display_name IS NOT NULL AND trim(requester.display_name) <> "" THEN requester.display_name
                    WHEN requester.username IS NOT NULL AND trim(requester.username) <> "" THEN requester.username
                    WHEN requester_email_prefix IS NOT NULL AND trim(requester_email_prefix) <> "" THEN requester_email_prefix
                    WHEN requester.email IS NOT NULL AND trim(requester.email) <> "" THEN requester.email
                    WHEN e.user_id IS NOT NULL AND trim(e.user_id) <> "" THEN e.user_id
                    ELSE e.user_id
                END as requester_display_name,
                CASE
                    WHEN EXISTS {
                        MATCH (:User {id: $user_id})-[m:MEMBER_OF]->(c)
                        WHERE m.role = "Lead"
                    } THEN "Lead"
                    ELSE "Member"
                END as member_role,
                toString(e.created_at) as created_at
            ORDER BY e.created_at DESC
            LIMIT $limit
            """
            result = await session.run(query, user_id=user_id, limit=limit)
            items: List[Dict[str, Any]] = []
            async for record in result:
                items.append(
                    {
                        "id": record.get("id"),
                        "type": record.get("type") or "",
                        "message": record.get("message") or "",
                        "user_id": record.get("user_id"),
                        "actor_user_id": record.get("actor_user_id"),
                        "request_id": record.get("request_id"),
                        "campaign_id": record.get("campaign_id"),
                        "campaign_name": record.get("campaign_name"),
                        "requester_display_name": record.get("requester_display_name"),
                        "member_role": record.get("member_role"),
                        "created_at": record.get("created_at"),
                    }
                )
            return items

    async def dismiss_user_feed_event(
        self,
        *,
        user_id: str,
        event_id: str,
        campaign_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Mark a campaign event dismissed for a specific user feed."""
        async with self._driver.session() as session:
            query = """
            MATCH (me:User {id: $user_id})-[:MEMBER_OF]->(c:Campaign)
            MATCH (e:CampaignEvent {id: $event_id, campaign_id: c.id})
            WHERE ($campaign_id IS NULL OR e.campaign_id = $campaign_id)
            MERGE (me)-[d:DISMISSED_FEED_EVENT]->(e)
            ON CREATE SET d.dismissed_at = datetime()
            ON MATCH SET d.dismissed_at = datetime()
            RETURN e.id as event_id, toString(d.dismissed_at) as dismissed_at
            """
            result = await session.run(
                query,
                user_id=user_id,
                event_id=event_id,
                campaign_id=campaign_id,
            )
            record = await result.single()
            if not record:
                raise ValueError("Feed event not found or not visible to current user")
            return {
                "event_id": record.get("event_id"),
                "dismissed_at": record.get("dismissed_at"),
            }

    async def create_campaign_event(
        self,
        *,
        campaign_id: str,
        event_type: str,
        message: str,
        user_id: Optional[str] = None,
        actor_user_id: Optional[str] = None,
        request_id: Optional[str] = None,
        object_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Create a campaign-scoped activity event."""
        event_id = str(uuid.uuid4())
        async with self._driver.session() as session:
            query = """
            CREATE (e:CampaignEvent {
                id: $event_id,
                campaign_id: $campaign_id,
                type: $event_type,
                message: $message,
                user_id: $user_id,
                actor_user_id: $actor_user_id,
                request_id: $request_id,
                created_at: datetime()
            })
            """
            await session.run(
                query,
                event_id=event_id,
                campaign_id=campaign_id,
                event_type=event_type,
                message=message,
                user_id=user_id,
                actor_user_id=actor_user_id,
                request_id=request_id,
            )
        await self.append_analytics_event(
            event_id=event_id,
            source_event_type_raw=event_type,
            source_entity="CampaignEvent",
            campaign_id=campaign_id,
            user_id=user_id,
            actor_user_id=actor_user_id,
            object_id=object_id or request_id,
            metadata={"request_id": request_id, **(metadata or {})},
        )

    async def create_campaign_deadline(
        self,
        *,
        campaign_id: str,
        created_by: str,
        title: str,
        description: Optional[str],
        due_at: str,
        visibility: str,
        assigned_user_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a campaign deadline with team/personal visibility."""
        normalized_title = (title or "").strip()
        if not normalized_title:
            raise ValueError("title is required")
        normalized_visibility = (visibility or "").strip().lower()
        if normalized_visibility not in {"team", "personal"}:
            raise ValueError("visibility must be 'team' or 'personal'")
        normalized_description = description.strip() if isinstance(description, str) else None
        if normalized_description == "":
            normalized_description = None

        effective_assigned_user_id: Optional[str] = None
        if normalized_visibility == "personal":
            effective_assigned_user_id = (assigned_user_id or "").strip() or created_by
            is_member = await self.check_membership(effective_assigned_user_id, campaign_id)
            if not is_member:
                raise ValueError("assigned_user_id must be a campaign member")

        deadline_id = str(uuid.uuid4())
        async with self._driver.session() as session:
            query = """
            MATCH (c:Campaign {id: $campaign_id})
            CREATE (d:CampaignDeadline {
                id: $deadline_id,
                campaign_id: $campaign_id,
                created_by: $created_by,
                title: $title,
                description: $description,
                due_at: datetime($due_at),
                visibility: $visibility,
                assigned_user_id: $assigned_user_id,
                created_at: datetime(),
                completed_at: null,
                reminder_10m_sent_at: null,
                reminder_10m_event_id: null,
                reminder_now_sent_at: null,
                reminder_now_event_id: null
            })
            RETURN
                d.id as id,
                d.campaign_id as campaign_id,
                d.created_by as created_by,
                d.title as title,
                d.description as description,
                toString(d.due_at) as due_at,
                d.visibility as visibility,
                d.assigned_user_id as assigned_user_id,
                toString(d.created_at) as created_at,
                toString(d.completed_at) as completed_at
            """
            result = await session.run(
                query,
                deadline_id=deadline_id,
                campaign_id=campaign_id,
                created_by=created_by,
                title=normalized_title,
                description=normalized_description,
                due_at=due_at,
                visibility=normalized_visibility,
                assigned_user_id=effective_assigned_user_id,
            )
            record = await result.single()
            if not record:
                raise ValueError(f"Campaign {campaign_id} not found")
            created = {
                "id": record.get("id"),
                "campaign_id": record.get("campaign_id"),
                "created_by": record.get("created_by"),
                "title": record.get("title"),
                "description": record.get("description"),
                "due_at": record.get("due_at"),
                "visibility": record.get("visibility"),
                "assigned_user_id": record.get("assigned_user_id"),
                "created_at": record.get("created_at"),
                "completed_at": record.get("completed_at"),
            }
            await self.append_analytics_event(
                event_id=f"campaign_deadline:{created.get('id')}:created",
                source_event_type_raw="campaign_deadline_created",
                source_entity="CampaignDeadline",
                campaign_id=campaign_id,
                user_id=created.get("assigned_user_id") or created_by,
                actor_user_id=created_by,
                occurred_at=created.get("created_at"),
                object_id=created.get("id"),
                status="created",
                metadata={
                    "visibility": created.get("visibility"),
                    "assigned_user_id": created.get("assigned_user_id"),
                },
            )
            return created

    async def generate_deadline_reminder_events(self) -> Dict[str, Any]:
        """Generate deduplicated deadline reminder campaign events.

        Emits at most one reminder per deadline per type:
        - deadline_reminder_10m
        - deadline_happening_now
        """
        async with self._driver.session() as session:
            ten_min_query = """
            MATCH (d:CampaignDeadline)
            WHERE d.completed_at IS NULL
                AND d.due_at IS NOT NULL
                AND d.reminder_10m_sent_at IS NULL
                AND d.due_at > datetime()
                AND d.due_at <= datetime() + duration({minutes: 10})
            WITH d
            CREATE (e:CampaignEvent {
                id: randomUUID(),
                campaign_id: d.campaign_id,
                type: "deadline_reminder_10m",
                message: "Deadline in 10 minutes: " + coalesce(d.title, "Untitled deadline"),
                user_id: CASE
                    WHEN d.visibility = "personal" THEN coalesce(d.assigned_user_id, d.created_by)
                    ELSE null
                END,
                actor_user_id: "system:deadline-reminder",
                request_id: null,
                created_at: datetime()
            })
            SET
                d.reminder_10m_sent_at = datetime(),
                d.reminder_10m_event_id = e.id
            RETURN
                count(e) as emitted_count,
                collect({
                    event_id: e.id,
                    campaign_id: d.campaign_id,
                    user_id: CASE
                        WHEN d.visibility = "personal" THEN coalesce(d.assigned_user_id, d.created_by)
                        ELSE null
                    END,
                    deadline_id: d.id
                }) as emitted_events
            """
            ten_min_result = await session.run(ten_min_query)
            ten_min_record = await ten_min_result.single()
            ten_min_count = int((ten_min_record or {}).get("emitted_count") or 0)
            ten_min_events = list((ten_min_record or {}).get("emitted_events") or [])

            now_query = """
            MATCH (d:CampaignDeadline)
            WHERE d.completed_at IS NULL
                AND d.due_at IS NOT NULL
                AND d.reminder_now_sent_at IS NULL
                AND d.due_at <= datetime()
                AND d.due_at >= datetime() - duration({minutes: 10})
            WITH d
            CREATE (e:CampaignEvent {
                id: randomUUID(),
                campaign_id: d.campaign_id,
                type: "deadline_happening_now",
                message: "Deadline happening now: " + coalesce(d.title, "Untitled deadline"),
                user_id: CASE
                    WHEN d.visibility = "personal" THEN coalesce(d.assigned_user_id, d.created_by)
                    ELSE null
                END,
                actor_user_id: "system:deadline-reminder",
                request_id: null,
                created_at: datetime()
            })
            SET
                d.reminder_now_sent_at = datetime(),
                d.reminder_now_event_id = e.id
            RETURN
                count(e) as emitted_count,
                collect({
                    event_id: e.id,
                    campaign_id: d.campaign_id,
                    user_id: CASE
                        WHEN d.visibility = "personal" THEN coalesce(d.assigned_user_id, d.created_by)
                        ELSE null
                    END,
                    deadline_id: d.id
                }) as emitted_events
            """
            now_result = await session.run(now_query)
            now_record = await now_result.single()
            now_count = int((now_record or {}).get("emitted_count") or 0)
            now_events = list((now_record or {}).get("emitted_events") or [])

            for emitted in ten_min_events:
                event_id = str(emitted.get("event_id") or "").strip()
                if not event_id:
                    continue
                await self.append_analytics_event(
                    event_id=event_id,
                    source_event_type_raw="deadline_reminder_10m",
                    source_entity="CampaignEvent",
                    campaign_id=str(emitted.get("campaign_id") or "").strip() or None,
                    user_id=str(emitted.get("user_id") or "").strip() or None,
                    actor_user_id="system:deadline-reminder",
                    object_id=str(emitted.get("deadline_id") or "").strip() or None,
                    status="emitted",
                    metadata={"reminder_type": "deadline_reminder_10m"},
                )
            for emitted in now_events:
                event_id = str(emitted.get("event_id") or "").strip()
                if not event_id:
                    continue
                await self.append_analytics_event(
                    event_id=event_id,
                    source_event_type_raw="deadline_happening_now",
                    source_entity="CampaignEvent",
                    campaign_id=str(emitted.get("campaign_id") or "").strip() or None,
                    user_id=str(emitted.get("user_id") or "").strip() or None,
                    actor_user_id="system:deadline-reminder",
                    object_id=str(emitted.get("deadline_id") or "").strip() or None,
                    status="emitted",
                    metadata={"reminder_type": "deadline_happening_now"},
                )

            return {
                "deadline_reminder_10m": ten_min_count,
                "deadline_happening_now": now_count,
                "total_emitted": ten_min_count + now_count,
            }

    async def list_campaign_deadlines(
        self,
        *,
        campaign_id: str,
        viewer_user_id: str,
        limit: int = 100,
        include_past: bool = False,
    ) -> List[Dict[str, Any]]:
        """List campaign deadlines visible to a specific user."""
        async with self._driver.session() as session:
            query = """
            MATCH (d:CampaignDeadline {campaign_id: $campaign_id})
            WHERE
                (
                    d.visibility = "team"
                    OR (
                        d.visibility = "personal"
                        AND coalesce(d.assigned_user_id, d.created_by) = $viewer_user_id
                    )
                )
                AND d.completed_at IS NULL
            RETURN
                d.id as id,
                d.campaign_id as campaign_id,
                d.created_by as created_by,
                d.title as title,
                d.description as description,
                toString(d.due_at) as due_at,
                d.visibility as visibility,
                d.assigned_user_id as assigned_user_id,
                toString(d.created_at) as created_at,
                toString(d.completed_at) as completed_at
            ORDER BY d.due_at ASC
            LIMIT $limit
            """
            result = await session.run(
                query,
                campaign_id=campaign_id,
                viewer_user_id=viewer_user_id,
                include_past=include_past,
                limit=limit,
            )
            items: List[Dict[str, Any]] = []
            async for record in result:
                items.append(
                    {
                        "id": record.get("id"),
                        "campaign_id": record.get("campaign_id"),
                        "created_by": record.get("created_by"),
                        "title": record.get("title"),
                        "description": record.get("description"),
                        "due_at": record.get("due_at"),
                        "visibility": record.get("visibility"),
                        "assigned_user_id": record.get("assigned_user_id"),
                        "created_at": record.get("created_at"),
                        "completed_at": record.get("completed_at"),
                    }
                )
            return items

    async def complete_campaign_deadline(
        self,
        *,
        campaign_id: str,
        deadline_id: str,
        viewer_user_id: str,
    ) -> Dict[str, Any]:
        """Mark a visible deadline complete (idempotent)."""
        async with self._driver.session() as session:
            query = """
            MATCH (d:CampaignDeadline {campaign_id: $campaign_id, id: $deadline_id})
            WHERE
                (
                    d.visibility = "team"
                    OR (
                        d.visibility = "personal"
                        AND coalesce(d.assigned_user_id, d.created_by) = $viewer_user_id
                    )
                )
            SET d.completed_at = coalesce(d.completed_at, datetime())
            RETURN
                d.id as id,
                d.campaign_id as campaign_id,
                d.created_by as created_by,
                d.title as title,
                d.description as description,
                toString(d.due_at) as due_at,
                d.visibility as visibility,
                d.assigned_user_id as assigned_user_id,
                toString(d.created_at) as created_at,
                toString(d.completed_at) as completed_at
            """
            result = await session.run(
                query,
                campaign_id=campaign_id,
                deadline_id=deadline_id,
                viewer_user_id=viewer_user_id,
            )
            record = await result.single()
            if not record:
                raise ValueError("Deadline not found or not visible to current user")
            completed = {
                "id": record.get("id"),
                "campaign_id": record.get("campaign_id"),
                "created_by": record.get("created_by"),
                "title": record.get("title"),
                "description": record.get("description"),
                "due_at": record.get("due_at"),
                "visibility": record.get("visibility"),
                "assigned_user_id": record.get("assigned_user_id"),
                "created_at": record.get("created_at"),
                "completed_at": record.get("completed_at"),
            }
            await self.append_analytics_event(
                event_id=f"campaign_deadline:{completed.get('id')}:completed",
                source_event_type_raw="campaign_deadline_completed",
                source_entity="CampaignDeadline",
                campaign_id=campaign_id,
                user_id=completed.get("assigned_user_id") or viewer_user_id,
                actor_user_id=viewer_user_id,
                occurred_at=completed.get("completed_at"),
                object_id=completed.get("id"),
                status="completed",
                metadata={"visibility": completed.get("visibility")},
            )
            return completed

    async def check_membership(self, user_id: str, tenant_id: str) -> bool:
        """Check if a user is a member of a campaign (tenant).
        
        Special case: "global" tenant_id always returns True (launch behavior).
        
        Args:
            user_id: ID of the user
            tenant_id: Campaign/tenant ID to check membership for
            
        Returns:
            True if user is a member, False otherwise
        """
        # Special case: "global" tenant always allows access
        if tenant_id == "global":
            return True
        
        async with self._driver.session() as session:
            query = """
            MATCH (u:User {id: $user_id})-[:MEMBER_OF]->(c:Campaign {id: $tenant_id})
            RETURN count(u) > 0 as is_member
            """
            
            result = await session.run(query, user_id=user_id, tenant_id=tenant_id)
            record = await result.single()
            
            return record["is_member"] if record else False

    async def add_member(
        self,
        tenant_id: str,
        target_user_id: str,
        role: str,
        actor_user_id: str
    ) -> Dict[str, Any]:
        """Add a user as a member to a campaign.
        
        Note: Role enforcement (e.g., only Lead can add members) should be
        implemented at the router/endpoint level.
        
        Args:
            tenant_id: Campaign ID to add member to
            target_user_id: ID of the user to add
            role: Role to assign (e.g., "Member", "Lead")
            actor_user_id: ID of the user performing the action (for audit)
            
        Returns:
            Dictionary with membership details: {tenant_id, user_id, role}
            
        Raises:
            ValueError: If campaign doesn't exist
        """
        async with self._driver.session() as session:
            # First verify campaign exists
            check_query = """
            MATCH (c:Campaign {id: $tenant_id})
            RETURN c.id as id
            """
            check_result = await session.run(check_query, tenant_id=tenant_id)
            if not await check_result.single():
                raise ValueError(f"Campaign {tenant_id} not found")
            
            # Add member
            query = """
            MERGE (u:User {id: $target_user_id})
            MATCH (c:Campaign {id: $tenant_id})
            MERGE (u)-[r:MEMBER_OF]->(c)
            SET r.role = $role, r.added_at = datetime()
            RETURN c.id as tenant_id, u.id as user_id, r.role as role
            """
            
            result = await session.run(
                query,
                tenant_id=tenant_id,
                target_user_id=target_user_id,
                role=role
            )
            record = await result.single()
            
            if not record:
                raise Exception("Failed to add member")
            
            return {
                "tenant_id": record["tenant_id"],
                "user_id": record["user_id"],
                "role": record["role"]
            }

    async def post_team_message(
        self,
        campaign_id: str,
        user: Dict[str, Any],
        content: str,
        mention_user_ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Post a team message to a campaign.
        
        Validates content (non-empty, max 2000 chars) and creates:
        - User node (if not exists)
        - TeamMessage node with content and timestamp
        - Relationships: (User)-[:SENT]->(Message)-[:POSTED_IN]->(Campaign)
        
        Args:
            campaign_id: Campaign ID to post message to
            user: User dictionary with "id" key (from auth)
            content: Message content (validated)
            
        Returns:
            Dictionary with message details: {id, content, timestamp (ISO string), author_id}
            
        Raises:
            ValueError: If content is empty or exceeds max length
        """
        # Validate content
        content = content.strip()
        if not content:
            raise ValueError("Message content cannot be empty")
        if len(content) > 2000:
            raise ValueError("Message content cannot exceed 2000 characters")
        
        user_id = self._require_team_chat_author_id(
            user.get("id"),
            campaign_id=campaign_id,
            thread_id=None,
        )
        
        message_id = str(uuid.uuid4())
        normalized_mentions = sorted(
            {
                str(mentioned_user_id).strip()
                for mentioned_user_id in (mention_user_ids or [])
                if str(mentioned_user_id).strip() and str(mentioned_user_id).strip() != user_id
            }
        )
        
        async with self._driver.session() as session:
            query = """
            MATCH (c:Campaign {id: $campaign_id})
            MERGE (u:User {id: $user_id})
            WITH
                c,
                u,
                CASE
                    WHEN u.email IS NULL OR trim(u.email) = "" THEN NULL
                    ELSE split(u.email, "@")[0]
                END as email_prefix
            CREATE (m:TeamMessage {
                id: $message_id,
                content: $content,
                timestamp: datetime()
            })
            CREATE (u)-[:SENT]->(m)-[:POSTED_IN]->(c)
            RETURN
                m.id as id,
                m.content as content,
                toString(m.timestamp) as timestamp,
                u.id as author_id,
                CASE
                    WHEN u.display_name IS NOT NULL AND trim(u.display_name) <> "" THEN u.display_name
                    WHEN u.username IS NOT NULL AND trim(u.username) <> "" THEN u.username
                    WHEN email_prefix IS NOT NULL AND trim(email_prefix) <> "" THEN email_prefix
                    WHEN u.email IS NOT NULL AND trim(u.email) <> "" THEN u.email
                    ELSE u.id
                END as author_display_name,
                CASE
                    WHEN u.display_name IS NOT NULL AND trim(u.display_name) <> "" THEN 0
                    WHEN u.username IS NOT NULL AND trim(u.username) <> "" THEN 1
                    WHEN email_prefix IS NOT NULL AND trim(email_prefix) <> "" THEN 2
                    WHEN u.email IS NOT NULL AND trim(u.email) <> "" THEN 3
                    ELSE 4
                END as author_fallback_level,
                u.avatar_url as author_avatar_url
            """
            
            result = await session.run(
                query,
                campaign_id=campaign_id,
                user_id=user_id,
                message_id=message_id,
                content=content
            )
            record = await result.single()
            
            if not record:
                raise Exception("Failed to create team message")
            
            identity = self._coerce_team_chat_read_identity(
                author_id=record.get("author_id"),
                author_display_name=record.get("author_display_name"),
                author_fallback_level=record.get("author_fallback_level"),
                campaign_id=campaign_id,
                message_id=record.get("id"),
                thread_id=None,
            )
            created_message = {
                "id": record["id"],
                "content": record["content"],
                "timestamp": record["timestamp"],
                "author_id": identity["author_id"],
                "author_display_name": identity["author_display_name"],
                "author_avatar_url": record.get("author_avatar_url"),
                "edited_at": None,
                "deleted_at": None,
            }
            if normalized_mentions:
                actor_display_name = await self.resolve_user_display_name(
                    user_id=user_id,
                    email_fallback=user.get("email"),
                )
                for mentioned_user_id in normalized_mentions:
                    await session.run(
                        """
                        MATCH (c:Campaign {id: $campaign_id})
                        MATCH (target:User {id: $mentioned_user_id})-[:MEMBER_OF]->(c)
                        CREATE (:CampaignEvent {
                            id: $event_id,
                            campaign_id: $campaign_id,
                            type: "campaign_message_mentioned_user",
                            message: $message,
                            user_id: $mentioned_user_id,
                            actor_user_id: null,
                            created_at: datetime()
                        })
                        """,
                        campaign_id=campaign_id,
                        mentioned_user_id=mentioned_user_id,
                        event_id=str(uuid.uuid4()),
                        message=f"{actor_display_name} mentioned you in campaign chat",
                    )
            await self.append_analytics_event(
                event_id=f"team_message:{created_message.get('id')}",
                source_event_type_raw="team_message_posted",
                source_entity="TeamMessage",
                campaign_id=campaign_id,
                user_id=user_id,
                actor_user_id=user_id,
                occurred_at=created_message.get("timestamp"),
                object_id=created_message.get("id"),
                status="created",
                metadata={
                    "chat_type": "campaign_team_chat",
                    "mention_count": len(normalized_mentions),
                    "content_chars": len(content),
                },
            )
            return created_message

    async def create_private_chat_thread(
        self,
        campaign_id: str,
        created_by_user_id: str,
        member_user_ids: List[str],
        name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a campaign-scoped private chat thread."""
        thread_id = str(uuid.uuid4())
        normalized_name = name.strip() if isinstance(name, str) else None
        if normalized_name == "":
            normalized_name = None

        # Thread members must be campaign members. Always include creator.
        valid_member_ids = await self.get_campaign_member_ids(campaign_id)
        selected_members = set(member_user_ids or [])
        selected_members.add(created_by_user_id)

        invalid_members = [member_id for member_id in selected_members if member_id not in valid_member_ids]
        if invalid_members:
            raise ValueError("All thread members must be campaign members")

        async with self._driver.session() as session:
            query = """
            MATCH (c:Campaign {id: $campaign_id})
            MERGE (creator:User {id: $created_by_user_id})
            CREATE (t:ChatThread {
                id: $thread_id,
                tenant_id: $campaign_id,
                name: $name,
                is_private: true,
                created_by_user_id: $created_by_user_id,
                created_at: datetime()
            })
            CREATE (t)-[:THREAD_IN]->(c)
            WITH t
            UNWIND $member_user_ids as member_id
            MATCH (u:User {id: member_id})-[:MEMBER_OF]->(:Campaign {id: $campaign_id})
            MERGE (u)-[:THREAD_MEMBER {added_at: datetime()}]->(t)
            RETURN
                t.id as thread_id,
                t.tenant_id as tenant_id,
                t.name as name,
                t.is_private as is_private,
                t.created_by_user_id as created_by_user_id,
                toString(t.created_at) as created_at
            """
            result = await session.run(
                query,
                campaign_id=campaign_id,
                created_by_user_id=created_by_user_id,
                thread_id=thread_id,
                name=normalized_name,
                member_user_ids=list(selected_members),
            )
            record = await result.single()
            if not record:
                raise ValueError(f"Campaign {campaign_id} not found")

            return {
                "thread_id": record["thread_id"],
                "tenant_id": record["tenant_id"],
                "name": record.get("name"),
                "is_private": bool(record["is_private"]),
                "created_by_user_id": record["created_by_user_id"],
                "created_at": record["created_at"],
            }

    async def list_private_chat_threads(
        self, campaign_id: str, user_id: str
    ) -> List[Dict[str, Any]]:
        """List private chat threads in campaign that the user belongs to."""
        async with self._driver.session() as session:
            query = """
            MATCH (u:User {id: $user_id})-[:THREAD_MEMBER]->(t:ChatThread {tenant_id: $campaign_id, is_private: true})-[:THREAD_IN]->(:Campaign {id: $campaign_id})
            OPTIONAL MATCH (u)-[read:THREAD_READ]->(t)
            WITH t, coalesce(read.last_read_at, datetime({epochMillis: 0})) as last_read_at
            OPTIONAL MATCH (t)<-[:IN_THREAD]-(m:ThreadMessage)<-[:SENT_THREAD_MESSAGE]-(author:User)
            WHERE m.timestamp > last_read_at AND author.id <> $user_id
            RETURN
                t.id as thread_id,
                t.tenant_id as tenant_id,
                t.name as name,
                t.is_private as is_private,
                t.created_by_user_id as created_by_user_id,
                toString(t.created_at) as created_at,
                count(m) as unread_count
            ORDER BY t.created_at DESC
            """
            result = await session.run(
                query,
                campaign_id=campaign_id,
                user_id=user_id,
            )
            threads: List[Dict[str, Any]] = []
            async for record in result:
                threads.append(
                    {
                        "thread_id": record["thread_id"],
                        "tenant_id": record["tenant_id"],
                        "name": record.get("name"),
                        "is_private": bool(record["is_private"]),
                        "created_by_user_id": record["created_by_user_id"],
                        "created_at": record["created_at"],
                        "has_unread": (record.get("unread_count") or 0) > 0,
                        "unread_count": int(record.get("unread_count") or 0),
                    }
                )
            return threads

    async def get_team_comms_unread_status(self, campaign_id: str, user_id: str) -> Dict[str, Any]:
        """Return unread status for the main Team Comms thread for a user."""
        async with self._driver.session() as session:
            query = """
            MATCH (u:User {id: $user_id})-[:MEMBER_OF]->(c:Campaign {id: $campaign_id})
            OPTIONAL MATCH (u)-[read:TEAM_CHAT_READ]->(c)
            WITH c, coalesce(read.last_read_at, datetime({epochMillis: 0})) as last_read_at
            OPTIONAL MATCH (c)<-[:POSTED_IN]-(m:TeamMessage)<-[:SENT]-(author:User)
            WHERE m.timestamp > last_read_at AND author.id <> $user_id
            RETURN count(m) as unread_count
            """
            result = await session.run(query, campaign_id=campaign_id, user_id=user_id)
            record = await result.single()
            unread_count = int(record.get("unread_count") or 0) if record else 0
            return {
                "has_unread": unread_count > 0,
                "unread_count": unread_count,
            }

    async def get_private_thread_messages(
        self, campaign_id: str, thread_id: str, user_id: str, limit: int = 50, since: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get messages from a private campaign thread for an authorized thread member."""
        async with self._driver.session() as session:
            membership_query = """
            MATCH (u:User {id: $user_id})-[:THREAD_MEMBER]->(t:ChatThread {id: $thread_id, tenant_id: $campaign_id, is_private: true})
            RETURN t.id as thread_id
            """
            membership_result = await session.run(
                membership_query,
                campaign_id=campaign_id,
                thread_id=thread_id,
                user_id=user_id,
            )
            if not await membership_result.single():
                raise PermissionError("Access denied to this private chat thread")

            # Opening a thread marks it read for this user.
            await session.run(
                """
                MATCH (u:User {id: $user_id})-[:THREAD_MEMBER]->(t:ChatThread {id: $thread_id, tenant_id: $campaign_id, is_private: true})
                MERGE (u)-[read:THREAD_READ]->(t)
                SET read.last_read_at = datetime()
                """,
                campaign_id=campaign_id,
                thread_id=thread_id,
                user_id=user_id,
            )

            if since:
                query = """
                MATCH (t:ChatThread {id: $thread_id, tenant_id: $campaign_id, is_private: true})<-[:IN_THREAD]-(m:ThreadMessage)<-[:SENT_THREAD_MESSAGE]-(u:User)
                WHERE m.timestamp > datetime($since)
                WITH
                    m,
                    u,
                    CASE
                        WHEN u.email IS NULL OR trim(u.email) = "" THEN NULL
                        ELSE split(u.email, "@")[0]
                    END as email_prefix
                RETURN
                    m.id as id,
                    m.content as content,
                    toString(m.timestamp) as timestamp,
                    u.id as author_id,
                    CASE
                        WHEN u.display_name IS NOT NULL AND trim(u.display_name) <> "" THEN u.display_name
                        WHEN u.username IS NOT NULL AND trim(u.username) <> "" THEN u.username
                        WHEN email_prefix IS NOT NULL AND trim(email_prefix) <> "" THEN email_prefix
                        WHEN u.email IS NOT NULL AND trim(u.email) <> "" THEN u.email
                        ELSE u.id
                    END as author_display_name,
                    CASE
                        WHEN u.display_name IS NOT NULL AND trim(u.display_name) <> "" THEN 0
                        WHEN u.username IS NOT NULL AND trim(u.username) <> "" THEN 1
                        WHEN email_prefix IS NOT NULL AND trim(email_prefix) <> "" THEN 2
                        WHEN u.email IS NOT NULL AND trim(u.email) <> "" THEN 3
                        ELSE 4
                    END as author_fallback_level,
                    u.avatar_url as author_avatar_url
                ORDER BY m.timestamp DESC
                LIMIT $limit
                """
                result = await session.run(
                    query,
                    campaign_id=campaign_id,
                    thread_id=thread_id,
                    since=since,
                    limit=limit,
                )
            else:
                query = """
                MATCH (t:ChatThread {id: $thread_id, tenant_id: $campaign_id, is_private: true})<-[:IN_THREAD]-(m:ThreadMessage)<-[:SENT_THREAD_MESSAGE]-(u:User)
                WITH
                    m,
                    u,
                    CASE
                        WHEN u.email IS NULL OR trim(u.email) = "" THEN NULL
                        ELSE split(u.email, "@")[0]
                    END as email_prefix
                RETURN
                    m.id as id,
                    m.content as content,
                    toString(m.timestamp) as timestamp,
                    u.id as author_id,
                    CASE
                        WHEN u.display_name IS NOT NULL AND trim(u.display_name) <> "" THEN u.display_name
                        WHEN u.username IS NOT NULL AND trim(u.username) <> "" THEN u.username
                        WHEN email_prefix IS NOT NULL AND trim(email_prefix) <> "" THEN email_prefix
                        WHEN u.email IS NOT NULL AND trim(u.email) <> "" THEN u.email
                        ELSE u.id
                    END as author_display_name,
                    CASE
                        WHEN u.display_name IS NOT NULL AND trim(u.display_name) <> "" THEN 0
                        WHEN u.username IS NOT NULL AND trim(u.username) <> "" THEN 1
                        WHEN email_prefix IS NOT NULL AND trim(email_prefix) <> "" THEN 2
                        WHEN u.email IS NOT NULL AND trim(u.email) <> "" THEN 3
                        ELSE 4
                    END as author_fallback_level,
                    u.avatar_url as author_avatar_url
                ORDER BY m.timestamp DESC
                LIMIT $limit
                """
                result = await session.run(
                    query,
                    campaign_id=campaign_id,
                    thread_id=thread_id,
                    limit=limit,
                )

            messages: List[Dict[str, Any]] = []
            async for record in result:
                identity = self._coerce_team_chat_read_identity(
                    author_id=record.get("author_id"),
                    author_display_name=record.get("author_display_name"),
                    author_fallback_level=record.get("author_fallback_level"),
                    campaign_id=campaign_id,
                    message_id=record.get("id"),
                    thread_id=thread_id,
                )
                messages.append(
                    {
                        "id": record["id"],
                        "content": record["content"],
                        "timestamp": record["timestamp"],
                        "author_id": identity["author_id"],
                        "author_display_name": identity["author_display_name"],
                        "author_avatar_url": record.get("author_avatar_url"),
                        "edited_at": None,
                        "deleted_at": None,
                    }
                )
            messages.reverse()
            return messages

    async def post_private_thread_message(
        self,
        campaign_id: str,
        thread_id: str,
        user_id: str,
        content: str,
        mention_user_ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Post message to a private campaign thread for an authorized thread member."""
        user_id = self._require_team_chat_author_id(
            user_id,
            campaign_id=campaign_id,
            thread_id=thread_id,
        )
        content = content.strip()
        if not content:
            raise ValueError("Message content cannot be empty")
        if len(content) > 2000:
            raise ValueError("Message content cannot exceed 2000 characters")

        normalized_mentions = sorted(
            {
                str(mentioned_user_id).strip()
                for mentioned_user_id in (mention_user_ids or [])
                if str(mentioned_user_id).strip() and str(mentioned_user_id).strip() != user_id
            }
        )

        async with self._driver.session() as session:
            membership_query = """
            MATCH (u:User {id: $user_id})-[:THREAD_MEMBER]->(t:ChatThread {id: $thread_id, tenant_id: $campaign_id, is_private: true})
            RETURN t.id as thread_id
            """
            membership_result = await session.run(
                membership_query,
                campaign_id=campaign_id,
                thread_id=thread_id,
                user_id=user_id,
            )
            if not await membership_result.single():
                raise PermissionError("Access denied to this private chat thread")

            message_id = str(uuid.uuid4())
            query = """
            MATCH (u:User {id: $user_id})-[:THREAD_MEMBER]->(t:ChatThread {id: $thread_id, tenant_id: $campaign_id, is_private: true})
            CREATE (m:ThreadMessage {
                id: $message_id,
                content: $content,
                timestamp: datetime()
            })
            CREATE (u)-[:SENT_THREAD_MESSAGE]->(m)-[:IN_THREAD]->(t)
            MERGE (u)-[read:THREAD_READ]->(t)
            SET read.last_read_at = datetime()
            WITH
                m,
                u,
                CASE
                    WHEN u.email IS NULL OR trim(u.email) = "" THEN NULL
                    ELSE split(u.email, "@")[0]
                END as email_prefix
            RETURN
                m.id as id,
                m.content as content,
                toString(m.timestamp) as timestamp,
                u.id as author_id,
                CASE
                    WHEN u.display_name IS NOT NULL AND trim(u.display_name) <> "" THEN u.display_name
                    WHEN u.username IS NOT NULL AND trim(u.username) <> "" THEN u.username
                    WHEN email_prefix IS NOT NULL AND trim(email_prefix) <> "" THEN email_prefix
                    WHEN u.email IS NOT NULL AND trim(u.email) <> "" THEN u.email
                    ELSE u.id
                END as author_display_name,
                CASE
                    WHEN u.display_name IS NOT NULL AND trim(u.display_name) <> "" THEN 0
                    WHEN u.username IS NOT NULL AND trim(u.username) <> "" THEN 1
                    WHEN email_prefix IS NOT NULL AND trim(email_prefix) <> "" THEN 2
                    WHEN u.email IS NOT NULL AND trim(u.email) <> "" THEN 3
                    ELSE 4
                END as author_fallback_level,
                u.avatar_url as author_avatar_url
            """
            result = await session.run(
                query,
                campaign_id=campaign_id,
                thread_id=thread_id,
                user_id=user_id,
                message_id=message_id,
                content=content,
            )
            record = await result.single()
            if not record:
                raise Exception("Failed to post thread message")

            identity = self._coerce_team_chat_read_identity(
                author_id=record.get("author_id"),
                author_display_name=record.get("author_display_name"),
                author_fallback_level=record.get("author_fallback_level"),
                campaign_id=campaign_id,
                message_id=record.get("id"),
                thread_id=thread_id,
            )
            created_message = {
                "id": record["id"],
                "content": record["content"],
                "timestamp": record["timestamp"],
                "author_id": identity["author_id"],
                "author_display_name": identity["author_display_name"],
                "author_avatar_url": record.get("author_avatar_url"),
                "edited_at": None,
                "deleted_at": None,
            }
            if normalized_mentions:
                actor_display_name = await self.resolve_user_display_name(user_id=user_id)
                for mentioned_user_id in normalized_mentions:
                    await session.run(
                        """
                        MATCH (t:ChatThread {id: $thread_id, tenant_id: $campaign_id, is_private: true})
                        MATCH (target:User {id: $mentioned_user_id})-[:THREAD_MEMBER]->(t)
                        CREATE (:CampaignEvent {
                            id: $event_id,
                            campaign_id: $campaign_id,
                            type: "campaign_message_mentioned_user",
                            message: $message,
                            user_id: $mentioned_user_id,
                            actor_user_id: null,
                            created_at: datetime()
                        })
                        """,
                        thread_id=thread_id,
                        campaign_id=campaign_id,
                        mentioned_user_id=mentioned_user_id,
                        event_id=str(uuid.uuid4()),
                        message=f"{actor_display_name} mentioned you in campaign chat",
                    )
            await self.append_analytics_event(
                event_id=f"thread_message:{created_message.get('id')}",
                source_event_type_raw="private_thread_message_posted",
                source_entity="ThreadMessage",
                campaign_id=campaign_id,
                user_id=user_id,
                actor_user_id=user_id,
                occurred_at=created_message.get("timestamp"),
                object_id=created_message.get("id"),
                status="created",
                metadata={
                    "chat_type": "private_thread_chat",
                    "thread_id": thread_id,
                    "is_private_thread": True,
                    "mention_count": len(normalized_mentions),
                    "content_chars": len(content),
                },
            )
            return created_message

    async def update_team_message(
        self, campaign_id: str, message_id: str, user_id: str, content: str
    ) -> Dict[str, Any]:
        """Update an existing team message (author only)."""
        content = content.strip()
        if not content:
            raise ValueError("Message content cannot be empty")
        if len(content) > 2000:
            raise ValueError("Message content cannot exceed 2000 characters")

        async with self._driver.session() as session:
            # Fetch message + author for explicit author check.
            lookup_query = """
            MATCH (c:Campaign {id: $campaign_id})<-[:POSTED_IN]-(m:TeamMessage)<-[:SENT]-(u:User)
            WHERE m.id = $message_id
            RETURN m.id as id, u.id as author_id, m.deleted_at as deleted_at
            """
            lookup_result = await session.run(
                lookup_query,
                campaign_id=campaign_id,
                message_id=message_id,
            )
            lookup_record = await lookup_result.single()

            if not lookup_record:
                raise ValueError(f"Message {message_id} not found")

            if lookup_record["author_id"] != user_id:
                raise PermissionError("Only the message author can edit this message")

            if lookup_record.get("deleted_at") is not None:
                raise ValueError("Deleted messages cannot be edited")

            update_query = """
            MATCH (c:Campaign {id: $campaign_id})<-[:POSTED_IN]-(m:TeamMessage)<-[:SENT]-(u:User {id: $user_id})
            WHERE m.id = $message_id
            SET m.content = $content, m.edited_at = datetime()
            WITH
                m,
                u,
                CASE
                    WHEN u.email IS NULL OR trim(u.email) = "" THEN NULL
                    ELSE split(u.email, "@")[0]
                END as email_prefix
            RETURN
                m.id as id,
                m.content as content,
                toString(m.timestamp) as timestamp,
                u.id as author_id,
                CASE
                    WHEN u.display_name IS NOT NULL AND trim(u.display_name) <> "" THEN u.display_name
                    WHEN u.username IS NOT NULL AND trim(u.username) <> "" THEN u.username
                    WHEN email_prefix IS NOT NULL AND trim(email_prefix) <> "" THEN email_prefix
                    WHEN u.email IS NOT NULL AND trim(u.email) <> "" THEN u.email
                    ELSE u.id
                END as author_display_name,
                CASE
                    WHEN u.display_name IS NOT NULL AND trim(u.display_name) <> "" THEN 0
                    WHEN u.username IS NOT NULL AND trim(u.username) <> "" THEN 1
                    WHEN email_prefix IS NOT NULL AND trim(email_prefix) <> "" THEN 2
                    WHEN u.email IS NOT NULL AND trim(u.email) <> "" THEN 3
                    ELSE 4
                END as author_fallback_level,
                u.avatar_url as author_avatar_url,
                toString(m.edited_at) as edited_at,
                toString(m.deleted_at) as deleted_at
            """
            update_result = await session.run(
                update_query,
                campaign_id=campaign_id,
                message_id=message_id,
                user_id=user_id,
                content=content,
            )
            update_record = await update_result.single()

            if not update_record:
                raise Exception("Failed to update team message")

            identity = self._coerce_team_chat_read_identity(
                author_id=update_record.get("author_id"),
                author_display_name=update_record.get("author_display_name"),
                author_fallback_level=update_record.get("author_fallback_level"),
                campaign_id=campaign_id,
                message_id=update_record.get("id"),
                thread_id=None,
            )
            return {
                "id": update_record["id"],
                "content": update_record["content"],
                "timestamp": update_record["timestamp"],
                "author_id": identity["author_id"],
                "author_display_name": identity["author_display_name"],
                "author_avatar_url": update_record.get("author_avatar_url"),
                "edited_at": update_record.get("edited_at"),
                "deleted_at": update_record.get("deleted_at"),
            }

    async def soft_delete_team_message(
        self, campaign_id: str, message_id: str, user_id: str
    ) -> Dict[str, Any]:
        """Soft-delete a team message (author only)."""
        async with self._driver.session() as session:
            lookup_query = """
            MATCH (c:Campaign {id: $campaign_id})<-[:POSTED_IN]-(m:TeamMessage)<-[:SENT]-(u:User)
            WHERE m.id = $message_id
            RETURN m.id as id, u.id as author_id
            """
            lookup_result = await session.run(
                lookup_query,
                campaign_id=campaign_id,
                message_id=message_id,
            )
            lookup_record = await lookup_result.single()

            if not lookup_record:
                raise ValueError(f"Message {message_id} not found")

            if lookup_record["author_id"] != user_id:
                raise PermissionError("Only the message author can delete this message")

            delete_query = """
            MATCH (c:Campaign {id: $campaign_id})<-[:POSTED_IN]-(m:TeamMessage)<-[:SENT]-(u:User {id: $user_id})
            WHERE m.id = $message_id
            SET m.deleted_at = coalesce(m.deleted_at, datetime()),
                m.content = ""
            WITH
                m,
                u,
                CASE
                    WHEN u.email IS NULL OR trim(u.email) = "" THEN NULL
                    ELSE split(u.email, "@")[0]
                END as email_prefix
            RETURN
                m.id as id,
                m.content as content,
                toString(m.timestamp) as timestamp,
                u.id as author_id,
                CASE
                    WHEN u.display_name IS NOT NULL AND trim(u.display_name) <> "" THEN u.display_name
                    WHEN u.username IS NOT NULL AND trim(u.username) <> "" THEN u.username
                    WHEN email_prefix IS NOT NULL AND trim(email_prefix) <> "" THEN email_prefix
                    WHEN u.email IS NOT NULL AND trim(u.email) <> "" THEN u.email
                    ELSE u.id
                END as author_display_name,
                CASE
                    WHEN u.display_name IS NOT NULL AND trim(u.display_name) <> "" THEN 0
                    WHEN u.username IS NOT NULL AND trim(u.username) <> "" THEN 1
                    WHEN email_prefix IS NOT NULL AND trim(email_prefix) <> "" THEN 2
                    WHEN u.email IS NOT NULL AND trim(u.email) <> "" THEN 3
                    ELSE 4
                END as author_fallback_level,
                u.avatar_url as author_avatar_url,
                toString(m.edited_at) as edited_at,
                toString(m.deleted_at) as deleted_at
            """
            delete_result = await session.run(
                delete_query,
                campaign_id=campaign_id,
                message_id=message_id,
                user_id=user_id,
            )
            delete_record = await delete_result.single()

            if not delete_record:
                raise Exception("Failed to delete team message")

            identity = self._coerce_team_chat_read_identity(
                author_id=delete_record.get("author_id"),
                author_display_name=delete_record.get("author_display_name"),
                author_fallback_level=delete_record.get("author_fallback_level"),
                campaign_id=campaign_id,
                message_id=delete_record.get("id"),
                thread_id=None,
            )
            return {
                "id": delete_record["id"],
                "content": delete_record["content"],
                "timestamp": delete_record["timestamp"],
                "author_id": identity["author_id"],
                "author_display_name": identity["author_display_name"],
                "author_avatar_url": delete_record.get("author_avatar_url"),
                "edited_at": delete_record.get("edited_at"),
                "deleted_at": delete_record.get("deleted_at"),
            }

    async def get_team_messages(
        self, campaign_id: str, limit: int = 50, since: Optional[str] = None, user_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get team messages for a campaign.
        
        Returns messages ordered by timestamp (newest first), then reversed
        to return in chronological order (oldest -> newest).
        
        Args:
            campaign_id: Campaign ID to get messages for
            limit: Maximum number of messages to return (default: 50)
            since: Optional ISO timestamp string - only return messages after this time
            
        Returns:
            List of message dictionaries: [{id, content, timestamp (ISO string), author_id}, ...]
            Ordered chronologically (oldest -> newest)
        """
        async with self._driver.session() as session:
            if since:
                # Parse ISO timestamp and use it in query
                # Neo4j datetime() can parse ISO 8601 strings directly
                query = """
                MATCH (c:Campaign {id: $campaign_id})<-[:POSTED_IN]-(m:TeamMessage)<-[:SENT]-(u:User)
                WHERE m.timestamp > datetime($since)
                WITH
                    m,
                    u,
                    CASE
                        WHEN u.email IS NULL OR trim(u.email) = "" THEN NULL
                        ELSE split(u.email, "@")[0]
                    END as email_prefix
                RETURN
                    m.id as id,
                    m.content as content,
                    toString(m.timestamp) as timestamp,
                    u.id as author_id,
                    CASE
                        WHEN u.display_name IS NOT NULL AND trim(u.display_name) <> "" THEN u.display_name
                        WHEN u.username IS NOT NULL AND trim(u.username) <> "" THEN u.username
                        WHEN email_prefix IS NOT NULL AND trim(email_prefix) <> "" THEN email_prefix
                        WHEN u.email IS NOT NULL AND trim(u.email) <> "" THEN u.email
                        ELSE u.id
                    END as author_display_name,
                    CASE
                        WHEN u.display_name IS NOT NULL AND trim(u.display_name) <> "" THEN 0
                        WHEN u.username IS NOT NULL AND trim(u.username) <> "" THEN 1
                        WHEN email_prefix IS NOT NULL AND trim(email_prefix) <> "" THEN 2
                        WHEN u.email IS NOT NULL AND trim(u.email) <> "" THEN 3
                        ELSE 4
                    END as author_fallback_level,
                    u.avatar_url as author_avatar_url,
                    toString(m.edited_at) as edited_at,
                    toString(m.deleted_at) as deleted_at
                ORDER BY m.timestamp DESC
                LIMIT $limit
                """
                result = await session.run(
                    query,
                    campaign_id=campaign_id,
                    since=since,  # ISO 8601 string, e.g., "2024-01-01T12:00:00Z"
                    limit=limit
                )
            else:
                query = """
                MATCH (c:Campaign {id: $campaign_id})<-[:POSTED_IN]-(m:TeamMessage)<-[:SENT]-(u:User)
                WITH
                    m,
                    u,
                    CASE
                        WHEN u.email IS NULL OR trim(u.email) = "" THEN NULL
                        ELSE split(u.email, "@")[0]
                    END as email_prefix
                RETURN
                    m.id as id,
                    m.content as content,
                    toString(m.timestamp) as timestamp,
                    u.id as author_id,
                    CASE
                        WHEN u.display_name IS NOT NULL AND trim(u.display_name) <> "" THEN u.display_name
                        WHEN u.username IS NOT NULL AND trim(u.username) <> "" THEN u.username
                        WHEN email_prefix IS NOT NULL AND trim(email_prefix) <> "" THEN email_prefix
                        WHEN u.email IS NOT NULL AND trim(u.email) <> "" THEN u.email
                        ELSE u.id
                    END as author_display_name,
                    CASE
                        WHEN u.display_name IS NOT NULL AND trim(u.display_name) <> "" THEN 0
                        WHEN u.username IS NOT NULL AND trim(u.username) <> "" THEN 1
                        WHEN email_prefix IS NOT NULL AND trim(email_prefix) <> "" THEN 2
                        WHEN u.email IS NOT NULL AND trim(u.email) <> "" THEN 3
                        ELSE 4
                    END as author_fallback_level,
                    u.avatar_url as author_avatar_url,
                    toString(m.edited_at) as edited_at,
                    toString(m.deleted_at) as deleted_at
                ORDER BY m.timestamp DESC
                LIMIT $limit
                """
                result = await session.run(
                    query,
                    campaign_id=campaign_id,
                    limit=limit
                )
            
            messages = []
            async for record in result:
                identity = self._coerce_team_chat_read_identity(
                    author_id=record.get("author_id"),
                    author_display_name=record.get("author_display_name"),
                    author_fallback_level=record.get("author_fallback_level"),
                    campaign_id=campaign_id,
                    message_id=record.get("id"),
                    thread_id=None,
                )
                messages.append({
                    "id": record["id"],
                    "content": record["content"],
                    "timestamp": record["timestamp"],
                    "author_id": identity["author_id"],
                    "author_display_name": identity["author_display_name"],
                    "author_avatar_url": record.get("author_avatar_url"),
                    "edited_at": record.get("edited_at"),
                    "deleted_at": record.get("deleted_at"),
                })
            
            # Reverse to get chronological order (oldest -> newest)
            messages.reverse()

            # Opening Team Comms marks it read for this user.
            if user_id:
                await session.run(
                    """
                    MATCH (u:User {id: $user_id})-[:MEMBER_OF]->(c:Campaign {id: $campaign_id})
                    MERGE (u)-[read:TEAM_CHAT_READ]->(c)
                    SET read.last_read_at = datetime()
                    """,
                    campaign_id=campaign_id,
                    user_id=user_id,
                )
            return messages

    async def get_member_role(self, user_id: str, campaign_id: str) -> Optional[str]:
        """Get the role of a user in a campaign.
        
        Args:
            user_id: ID of the user
            campaign_id: ID of the campaign
            
        Returns:
            Role string ("Lead" or "Member") if user is a member, None otherwise
        """
        async with self._driver.session() as session:
            query = """
            MATCH (u:User {id: $user_id})-[r:MEMBER_OF]->(c:Campaign {id: $campaign_id})
            RETURN r.role as role
            """
            
            result = await session.run(query, user_id=user_id, campaign_id=campaign_id)
            record = await result.single()
            
            return record["role"] if record else None

    async def list_campaign_members(self, campaign_id: str) -> List[Dict[str, Any]]:
        """List all members of a campaign.
        
        Args:
            campaign_id: ID of the campaign
            
        Returns:
            List of member dictionaries: [{id, email, role}, ...]
        """
        async with self._driver.session() as session:
            query = """
            MATCH (u:User)-[r:MEMBER_OF]->(c:Campaign {id: $campaign_id})
            RETURN u.id as id, u.email as email, r.role as role
            ORDER BY r.role DESC, u.email ASC
            """
            
            result = await session.run(query, campaign_id=campaign_id)
            
            members = []
            async for record in result:
                members.append({
                    "id": record["id"],
                    "email": record.get("email", ""),
                    "role": record["role"]
                })
            
            return members

    async def list_campaign_members_for_mentions(
        self, campaign_id: str
    ) -> List[Dict[str, Any]]:
        """List campaign members in a minimal, mention-safe shape.

        Returns only users who are members of this campaign.
        """
        async with self._driver.session() as session:
            query = """
            MATCH (u:User)-[r:MEMBER_OF]->(c:Campaign {id: $campaign_id})
            WITH
                u,
                r,
                coalesce(u.email, "") as email_value,
                CASE
                    WHEN u.email IS NULL OR trim(u.email) = "" THEN NULL
                    ELSE split(u.email, "@")[0]
                END as email_prefix
            RETURN
                u.id as user_id,
                CASE
                    WHEN u.display_name IS NOT NULL AND trim(u.display_name) <> "" THEN u.display_name
                    WHEN u.username IS NOT NULL AND trim(u.username) <> "" THEN u.username
                    WHEN email_prefix IS NOT NULL AND trim(email_prefix) <> "" THEN email_prefix
                    WHEN email_value <> "" THEN email_value
                    WHEN u.id IS NOT NULL AND trim(u.id) <> "" THEN u.id
                    ELSE u.id
                END as display_name,
                u.avatar_url as avatar_url,
                r.role as role,
                coalesce(u.status, "active") as status,
                toString(u.status_updated_at) as status_updated_at
            ORDER BY toLower(
                CASE
                    WHEN u.display_name IS NOT NULL AND trim(u.display_name) <> "" THEN u.display_name
                    WHEN u.username IS NOT NULL AND trim(u.username) <> "" THEN u.username
                    WHEN email_prefix IS NOT NULL AND trim(email_prefix) <> "" THEN email_prefix
                    WHEN email_value <> "" THEN email_value
                    WHEN u.id IS NOT NULL AND trim(u.id) <> "" THEN u.id
                    ELSE u.id
                END
            ) ASC
            """

            result = await session.run(query, campaign_id=campaign_id)
            members: List[Dict[str, Any]] = []
            async for record in result:
                members.append(
                    {
                        "user_id": record["user_id"],
                        "display_name": record["display_name"],
                        "avatar_url": record.get("avatar_url"),
                        "role": record.get("role"),
                        "status": record.get("status") or "active",
                        "status_updated_at": record.get("status_updated_at"),
                    }
                )
            # Defensive dedupe by user_id. Legacy duplicate User nodes can exist.
            # Prefer richer identity rows over user_id fallback rows.
            by_user_id: Dict[str, Dict[str, Any]] = {}
            for member in members:
                user_id = str(member.get("user_id") or "")
                if not user_id:
                    continue
                existing = by_user_id.get(user_id)
                if existing is None:
                    by_user_id[user_id] = member
                    continue
                existing_name = str(existing.get("display_name") or "").strip()
                candidate_name = str(member.get("display_name") or "").strip()
                existing_score = 0
                candidate_score = 0
                if existing_name and existing_name != user_id:
                    existing_score += 3
                if candidate_name and candidate_name != user_id:
                    candidate_score += 3
                if existing.get("avatar_url"):
                    existing_score += 1
                if member.get("avatar_url"):
                    candidate_score += 1
                if existing.get("role") == "Lead":
                    existing_score += 1
                if member.get("role") == "Lead":
                    candidate_score += 1
                if candidate_score > existing_score:
                    by_user_id[user_id] = member

            deduped = list(by_user_id.values())
            deduped.sort(
                key=lambda item: str(
                    item.get("display_name") or item.get("user_id") or ""
                ).lower()
            )
            return deduped

    async def get_user_status(self, user_id: str) -> Dict[str, Any]:
        """Get global manual status for a user."""
        async with self._driver.session() as session:
            query = """
            MATCH (u:User {id: $user_id})
            RETURN
                coalesce(u.status, "active") as status,
                toString(u.status_updated_at) as updated_at
            """
            result = await session.run(query, user_id=user_id)
            record = await result.single()
            if not record:
                return {"status": "active", "updated_at": None}
            return {
                "status": record.get("status") or "active",
                "updated_at": record.get("updated_at"),
            }

    async def update_user_status(
        self,
        *,
        user_id: str,
        status_value: str,
        email: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Set global manual status for a user."""
        normalized_status = (status_value or "").strip().lower()
        if normalized_status not in {"active", "away", "in_meeting"}:
            raise ValueError("status must be active, away, or in_meeting")

        async with self._driver.session() as session:
            query = """
            MERGE (u:User {id: $user_id})
            SET
                u.email = coalesce($email, u.email),
                u.status = $status_value,
                u.status_updated_at = datetime()
            RETURN
                u.status as status,
                toString(u.status_updated_at) as updated_at
            """
            result = await session.run(
                query,
                user_id=user_id,
                email=email,
                status_value=normalized_status,
            )
            record = await result.single()
            if not record:
                raise Exception("Failed to update user status")
            return {
                "status": record.get("status") or "active",
                "updated_at": record.get("updated_at"),
            }

    async def get_user_profile(
        self,
        *,
        user_id: str,
        email_fallback: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get Riley-native user profile fields."""
        async with self._driver.session() as session:
            query = """
            MATCH (u:User {id: $user_id})
            RETURN
                coalesce(u.email, $email_fallback) as email,
                u.username as username,
                u.display_name as display_name,
                toString(u.updated_at) as updated_at
            """
            result = await session.run(
                query,
                user_id=user_id,
                email_fallback=email_fallback,
            )
            record = await result.single()
            if not record:
                return {
                    "email": email_fallback or "",
                    "username": None,
                    "display_name": None,
                    "updated_at": None,
                }
            return {
                "email": record.get("email") or (email_fallback or ""),
                "username": record.get("username"),
                "display_name": record.get("display_name"),
                "updated_at": record.get("updated_at"),
            }

    async def resolve_user_display_name(
        self,
        *,
        user_id: str,
        email_fallback: Optional[str] = None,
    ) -> str:
        """Resolve display_name -> username -> email_prefix -> email -> user_id chain."""
        profile = await self.get_user_profile(user_id=user_id, email_fallback=email_fallback)
        display_name = str(profile.get("display_name") or "").strip()
        if display_name:
            return display_name
        username = str(profile.get("username") or "").strip()
        if username:
            return username
        email = str(profile.get("email") or "").strip()
        if email:
            email_prefix = email.split("@")[0].strip()
            if email_prefix:
                return email_prefix
            return email
        normalized_user_id = (user_id or "").strip()
        return normalized_user_id or "Unknown User"

    async def update_user_profile(
        self,
        *,
        user_id: str,
        email: Optional[str],
        display_name: Optional[str],
    ) -> Dict[str, Any]:
        """Update Riley-native user profile fields."""
        normalized_display_name = None
        if isinstance(display_name, str):
            normalized_display_name = display_name.strip() or None

        async with self._driver.session() as session:
            query = """
            MERGE (u:User {id: $user_id})
            SET
                u.email = coalesce($email, u.email),
                u.display_name = $display_name,
                u.updated_at = datetime()
            RETURN
                coalesce(u.email, "") as email,
                u.display_name as display_name,
                toString(u.updated_at) as updated_at
            """
            result = await session.run(
                query,
                user_id=user_id,
                email=email,
                display_name=normalized_display_name,
            )
            record = await result.single()
            if not record:
                raise Exception("Failed to update user profile")
            return {
                "email": record.get("email") or "",
                "display_name": record.get("display_name"),
                "updated_at": record.get("updated_at"),
            }

    async def upsert_user_identity(
        self,
        *,
        user_id: str,
        email: Optional[str] = None,
        username: Optional[str] = None,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
        display_name: Optional[str] = None,
        avatar_url: Optional[str] = None,
    ) -> None:
        """Best-effort identity enrichment for User nodes."""
        async with self._driver.session() as session:
            query = """
            MERGE (u:User {id: $user_id})
            SET
                u.email = coalesce($email, u.email),
                u.username = coalesce($username, u.username),
                u.first_name = coalesce($first_name, u.first_name),
                u.last_name = coalesce($last_name, u.last_name),
                u.display_name = coalesce($display_name, u.display_name),
                u.avatar_url = coalesce($avatar_url, u.avatar_url)
            """
            await session.run(
                query,
                user_id=user_id,
                email=email,
                username=username,
                first_name=first_name,
                last_name=last_name,
                display_name=display_name,
                avatar_url=avatar_url,
            )

    async def search_users_for_campaign_add(
        self,
        *,
        query: str,
        limit: int = 8,
    ) -> List[Dict[str, str]]:
        """Search known Riley users by display_name/email/user_id with Riley fallback display names."""
        normalized_query = (query or "").strip().lower()
        if not normalized_query:
            return []
        capped_limit = max(1, min(int(limit), 20))

        async with self._driver.session() as session:
            result = await session.run(
                """
                MATCH (u:User)
                WITH
                    u,
                    CASE
                        WHEN u.email IS NULL OR trim(u.email) = "" THEN NULL
                        ELSE split(u.email, "@")[0]
                    END as email_prefix
                WHERE
                    (u.id IS NOT NULL AND toLower(u.id) CONTAINS $query)
                    OR (u.email IS NOT NULL AND toLower(u.email) CONTAINS $query)
                    OR (email_prefix IS NOT NULL AND toLower(email_prefix) CONTAINS $query)
                    OR (u.username IS NOT NULL AND toLower(u.username) CONTAINS $query)
                    OR (u.display_name IS NOT NULL AND toLower(u.display_name) CONTAINS $query)
                RETURN
                    u.id as id,
                    coalesce(u.email, "") as email,
                    CASE
                        WHEN u.display_name IS NOT NULL AND trim(u.display_name) <> "" THEN u.display_name
                        WHEN u.username IS NOT NULL AND trim(u.username) <> "" THEN u.username
                        WHEN email_prefix IS NOT NULL AND trim(email_prefix) <> "" THEN email_prefix
                        WHEN u.email IS NOT NULL AND trim(u.email) <> "" THEN u.email
                        WHEN u.id IS NOT NULL AND trim(u.id) <> "" THEN u.id
                        ELSE u.id
                    END as display_name
                ORDER BY coalesce(u.updated_at, u.status_updated_at, datetime({epochMillis: 0})) DESC
                LIMIT $limit
                """,
                query=normalized_query,
                limit=capped_limit,
            )
            users: List[Dict[str, str]] = []
            async for record in result:
                user_id = str(record.get("id") or "").strip()
                email = str(record.get("email") or "").strip()
                if not user_id or not email:
                    continue
                users.append(
                    {
                        "id": user_id,
                        "email": email,
                        "display_name": str(record.get("display_name") or user_id).strip() or user_id,
                    }
                )
            return users

    async def get_campaign_member_ids(self, campaign_id: str) -> List[str]:
        """Return all user IDs that are members of a campaign."""
        async with self._driver.session() as session:
            query = """
            MATCH (u:User)-[:MEMBER_OF]->(c:Campaign {id: $campaign_id})
            RETURN u.id as user_id
            """
            result = await session.run(query, campaign_id=campaign_id)
            ids: List[str] = []
            async for record in result:
                user_id = record.get("user_id")
                if user_id:
                    ids.append(user_id)
            return ids

    async def add_campaign_member(
        self,
        campaign_id: str,
        target_user_id: str,
        target_email: str,
        role: str,
        target_first_name: Optional[str] = None,
        target_last_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Add a user as a member to a campaign.
        
        Creates or updates the User node and creates/updates the MEMBER_OF relationship.
        
        Args:
            campaign_id: ID of the campaign
            target_user_id: ID of the user to add
            target_email: Email of the user to add
            role: Role to assign ("Lead" or "Member")
            
        Returns:
            Dictionary with membership state: {campaign_id, user_id, role, already_member, campaign_name}

        Raises:
            ValueError: If campaign doesn't exist
        """
        async with self._driver.session() as session:
            # First verify campaign exists
            check_query = """
            MATCH (c:Campaign {id: $campaign_id})
            RETURN c.id as id
            """
            check_result = await session.run(check_query, campaign_id=campaign_id)
            if not await check_result.single():
                raise ValueError(f"Campaign {campaign_id} not found")
            
            # Add or update member (idempotent MERGE, no duplicate relationships)
            query = """
            MERGE (u:User {id: $target_user_id})
            SET u.email = $target_email
            SET u.first_name = coalesce($target_first_name, u.first_name)
            SET u.last_name = coalesce($target_last_name, u.last_name)
            MATCH (c:Campaign {id: $campaign_id})
            OPTIONAL MATCH (u)-[existing:MEMBER_OF]->(c)
            WITH u, c, existing
            MERGE (u)-[r:MEMBER_OF]->(c)
            ON CREATE SET r.role = $role, r.added_at = datetime()
            ON MATCH SET r.role = coalesce(r.role, $role)
            RETURN
                c.id as campaign_id,
                c.name as campaign_name,
                u.id as user_id,
                r.role as role,
                (existing IS NOT NULL) as already_member
            """

            result = await session.run(
                query,
                campaign_id=campaign_id,
                target_user_id=target_user_id,
                target_email=target_email,
                role=role,
                target_first_name=target_first_name,
                target_last_name=target_last_name,
            )
            record = await result.single()
            if not record:
                raise Exception("Failed to add or load campaign membership")
            return {
                "campaign_id": record.get("campaign_id"),
                "campaign_name": record.get("campaign_name"),
                "user_id": record.get("user_id"),
                "role": record.get("role"),
                "already_member": bool(record.get("already_member")),
            }

    async def remove_campaign_member(self, campaign_id: str, target_user_id: str) -> None:
        """Remove a user from a campaign.
        
        Deletes the MEMBER_OF relationship between the user and campaign.
        
        Args:
            campaign_id: ID of the campaign
            target_user_id: ID of the user to remove
            
        Raises:
            ValueError: If the membership relationship doesn't exist
        """
        async with self._driver.session() as session:
            query = """
            MATCH (u:User {id: $target_user_id})-[r:MEMBER_OF]->(c:Campaign {id: $campaign_id})
            DELETE r
            RETURN count(r) as deleted_count
            """
            
            result = await session.run(query, campaign_id=campaign_id, target_user_id=target_user_id)
            record = await result.single()
            
            if not record or record["deleted_count"] == 0:
                raise ValueError(f"User {target_user_id} is not a member of campaign {campaign_id}")


# Global service instance that can be imported by routers.
# Note: This will be initialized in main.py startup event.
graph_service: Optional[GraphService] = None

