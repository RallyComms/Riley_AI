import copy
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Sequence, Optional
import logging

from qdrant_client import AsyncQdrantClient
from qdrant_client.http import models
from qdrant_client.http.models import Filter, FieldCondition, MatchValue, PointStruct

from app.core.config import get_settings

logger = logging.getLogger(__name__)


class VectorService:
    """Service responsible for all Qdrant vector operations.

    All access is strictly tenant-scoped via Qdrant filters to preserve
    data sovereignty between clients.
    """

    _USAGE_METRICS_CACHE_TTL_SECONDS = 10 * 60

    def __init__(self, client: AsyncQdrantClient | None = None) -> None:
        settings = get_settings()

        if client:
            self._client = client
        elif settings.QDRANT_URL:
            # Qdrant Cloud mode: use URL and API key
            self._client = AsyncQdrantClient(
                url=settings.QDRANT_URL,
                api_key=settings.QDRANT_API_KEY,
            )
        else:
            # Local dev mode: use host and port
            self._client = AsyncQdrantClient(
                host=settings.QDRANT_HOST,
                port=settings.QDRANT_PORT,
            )
        self._bm25_support_cache: Dict[str, bool] = {}
        self._bm25_warned_collections: set[str] = set()
        self._chunk_type_index_unavailable_collections: set[str] = set()
        self._usage_metrics_cache: Optional[Dict[str, Any]] = None
        self._usage_metrics_cache_expires_at: Optional[datetime] = None
        self._usage_metrics_cache_key: Optional[str] = None

    @property
    def client(self) -> AsyncQdrantClient:
        return self._client

    async def ensure_collections(self) -> None:
        """Ensure Tier 1 and Tier 2 collections exist with the configured vector size.

        This does NOT delete or recreate collections. It only creates missing collections.
        Use `scripts/reset_qdrant_collections.py` for one-time destructive reset.
        """
        settings = get_settings()
        await self._ensure_collection(settings.QDRANT_COLLECTION_TIER_1)
        await self._ensure_collection(settings.QDRANT_COLLECTION_TIER_2)
        await self._ensure_payload_indexes(settings.QDRANT_COLLECTION_TIER_1)
        await self._ensure_payload_indexes(settings.QDRANT_COLLECTION_TIER_2)
        logger.info(
            "bm25_collection_status collection=%s enabled=%s",
            settings.QDRANT_COLLECTION_TIER_1,
            await self.bm25_enabled_for_collection(settings.QDRANT_COLLECTION_TIER_1),
        )
        logger.info(
            "bm25_collection_status collection=%s enabled=%s",
            settings.QDRANT_COLLECTION_TIER_2,
            await self.bm25_enabled_for_collection(settings.QDRANT_COLLECTION_TIER_2),
        )

    @staticmethod
    def _extract_sparse_vectors_map(collection_info: Any) -> Dict[str, Any]:
        """Best-effort extractor for sparse vector config across Qdrant versions."""
        dumped = collection_info.model_dump() if hasattr(collection_info, "model_dump") else {}
        params = ((dumped.get("config") or {}).get("params") or {})
        sparse_vectors = (
            params.get("sparse_vectors")
            or params.get("sparse_vectors_config")
            or dumped.get("sparse_vectors")
            or dumped.get("sparse_vectors_config")
            or {}
        )
        return sparse_vectors if isinstance(sparse_vectors, dict) else {}

    @staticmethod
    def _is_missing_vector_name_error(exc: Exception, vector_name: str) -> bool:
        message = str(exc).lower()
        return (
            "not existing vector name" in message
            or "unknown vector name" in message
        ) and vector_name.lower() in message

    @staticmethod
    def is_missing_bm25_vector_error(exc: Exception) -> bool:
        return VectorService._is_missing_vector_name_error(exc, "bm25")

    async def bm25_enabled_for_collection(self, collection_name: str) -> bool:
        """Return whether BM25 sparse vector is currently available for a collection."""
        settings = get_settings()
        if not settings.BM25_ENABLED:
            return False
        if collection_name in self._bm25_support_cache:
            return self._bm25_support_cache[collection_name]
        return await self._refresh_bm25_support(collection_name, attempt_enable=True)

    async def refresh_bm25_support(self, collection_name: str) -> bool:
        """Force refresh BM25 support state (best-effort migration + verify)."""
        return await self._refresh_bm25_support(collection_name, attempt_enable=True)

    async def _refresh_bm25_support(
        self,
        collection_name: str,
        *,
        attempt_enable: bool,
    ) -> bool:
        try:
            info = await self._client.get_collection(collection_name=collection_name)
            has_bm25 = "bm25" in self._extract_sparse_vectors_map(info)
            if has_bm25:
                self._bm25_support_cache[collection_name] = True
                return True
        except Exception:
            # If collection introspection fails, prefer dense-only behavior.
            self._bm25_support_cache[collection_name] = False
            return False

        if not attempt_enable:
            self._bm25_support_cache[collection_name] = False
            return False

        enabled = await self._ensure_sparse_vectors_config(collection_name)
        self._bm25_support_cache[collection_name] = enabled
        return enabled

    def mark_bm25_unavailable(self, collection_name: str, reason: str) -> None:
        """Mark BM25 as unavailable and warn once per collection."""
        self._bm25_support_cache[collection_name] = False
        if collection_name in self._bm25_warned_collections:
            return
        self._bm25_warned_collections.add(collection_name)
        logger.warning(
            "bm25_disabled collection=%s reason=%s; using dense-only retrieval/indexing",
            collection_name,
            reason,
        )

    @staticmethod
    def _is_missing_payload_index_error(exc: Exception, field_name: str) -> bool:
        message = str(exc).lower()
        return (
            "index required but not found" in message
            and field_name.lower() in message
        )

    async def _ensure_payload_indexes(self, collection_name: str) -> None:
        """Ensure required payload indexes exist (idempotent)."""
        try:
            await self._client.create_payload_index(
                collection_name=collection_name,
                field_name="record_type",
                field_schema=models.PayloadSchemaType.KEYWORD,
            )
        except Exception:
            # If index already exists or backend version differs, don't fail startup.
            pass
        try:
            await self._client.create_payload_index(
                collection_name=collection_name,
                field_name="parent_file_id",
                field_schema=models.PayloadSchemaType.KEYWORD,
            )
        except Exception:
            pass
        try:
            await self._client.create_payload_index(
                collection_name=collection_name,
                field_name="chunk_type",
                field_schema=models.PayloadSchemaType.KEYWORD,
            )
        except Exception:
            pass
        # Campaign ownership/source fields used by hard-delete and campaign-scoped filters.
        for field_name in ("client_id", "tenant_id", "source_campaign_id"):
            try:
                await self._client.create_payload_index(
                    collection_name=collection_name,
                    field_name=field_name,
                    field_schema=models.PayloadSchemaType.KEYWORD,
                )
            except Exception:
                pass

    async def _ensure_collection(self, collection_name: str) -> None:
        """Create a collection if it doesn't exist, using the configured vector params."""
        settings = get_settings()

        # Map string setting to Qdrant Distance enum (default Cosine).
        distance_str = (settings.QDRANT_DISTANCE or "Cosine").strip().lower()
        distance = models.Distance.COSINE
        if distance_str == "dot":
            distance = models.Distance.DOT
        elif distance_str == "euclid":
            distance = models.Distance.EUCLID

        try:
            await self._client.get_collection(collection_name=collection_name)
            bm25_enabled = await self._ensure_sparse_vectors_config(collection_name)
            self._bm25_support_cache[collection_name] = bm25_enabled
            return  # exists
        except Exception:
            # If get_collection fails (not found / connection), attempt create. Any real errors
            # will still bubble up from create_collection.
            pass

        try:
            await self._client.create_collection(
                collection_name=collection_name,
                vectors_config=models.VectorParams(
                    size=settings.EMBEDDING_DIM,
                    distance=distance,
                ),
                sparse_vectors_config={
                    "bm25": models.SparseVectorParams(
                        modifier=models.Modifier.IDF,
                    )
                },
            )
        except Exception as exc:
            logger.warning(
                "bm25_sparse_vector_create_unavailable collection=%s error=%s; creating dense-only collection",
                collection_name,
                exc,
            )
            await self._client.create_collection(
                collection_name=collection_name,
                vectors_config=models.VectorParams(
                    size=settings.EMBEDDING_DIM,
                    distance=distance,
                ),
            )
        bm25_enabled = await self._ensure_sparse_vectors_config(collection_name)
        self._bm25_support_cache[collection_name] = bm25_enabled

    async def _ensure_sparse_vectors_config(self, collection_name: str) -> bool:
        """Best-effort idempotent ensure for BM25 sparse vector config."""
        settings = get_settings()
        if not settings.BM25_ENABLED:
            return False

        try:
            info = await self._client.get_collection(collection_name=collection_name)
            if "bm25" in self._extract_sparse_vectors_map(info):
                return True
        except Exception:
            # If introspection fails, still attempt update in a best-effort way.
            pass

        try:
            await self._client.update_collection(
                collection_name=collection_name,
                sparse_vectors_config={
                    "bm25": models.SparseVectorParams(
                        modifier=models.Modifier.IDF,
                    )
                },
            )
            # Verify update took effect.
            info = await self._client.get_collection(collection_name=collection_name)
            if "bm25" in self._extract_sparse_vectors_map(info):
                return True
        except Exception as exc:
            # Do not fail startup for clusters/plans that don't support sparse vectors yet.
            logger.warning(
                "bm25_sparse_vector_config_unavailable collection=%s error=%s",
                collection_name,
                exc,
            )
            return False
        return False

    @staticmethod
    def _point_to_dict(point: Any) -> Dict[str, Any]:
        if isinstance(point, dict):
            return point
        if hasattr(point, "dict"):
            return point.dict()
        return {
            "id": str(getattr(point, "id", "")),
            "payload": getattr(point, "payload", {}) or {},
            "score": getattr(point, "score", None),
        }

    @staticmethod
    def _candidate_key(point: Dict[str, Any]) -> str:
        payload = point.get("payload", {}) or {}
        chunk_id = payload.get("chunk_id")
        if chunk_id:
            return str(chunk_id)
        parent_file_id = payload.get("parent_file_id")
        chunk_index = payload.get("chunk_index")
        if parent_file_id is not None and chunk_index is not None:
            return f"{parent_file_id}::chunk::{chunk_index}"
        return str(point.get("id", ""))

    @staticmethod
    def _drop_record_type_from_must_not(filter_obj: Filter) -> Filter:
        must_not = []
        for condition in (filter_obj.must_not or []):
            key = condition.get("key") if isinstance(condition, dict) else getattr(condition, "key", None)
            if key == "record_type":
                continue
            must_not.append(condition)
        return Filter(
            must=filter_obj.must or [],
            should=filter_obj.should or [],
            must_not=must_not,
        )

    @staticmethod
    def _has_filter_field_in_must(filter_obj: Filter, field_name: str) -> bool:
        for condition in (filter_obj.must or []):
            key = condition.get("key") if isinstance(condition, dict) else getattr(condition, "key", None)
            if key == field_name:
                return True
        return False

    @staticmethod
    def _drop_field_from_must(filter_obj: Filter, field_name: str) -> Filter:
        must = []
        for condition in (filter_obj.must or []):
            key = condition.get("key") if isinstance(condition, dict) else getattr(condition, "key", None)
            if key == field_name:
                continue
            must.append(condition)
        return Filter(
            must=must,
            should=filter_obj.should or [],
            must_not=filter_obj.must_not or [],
        )

    def _mark_chunk_type_index_unavailable(self, collection_name: str, *, error: Exception) -> None:
        if collection_name in self._chunk_type_index_unavailable_collections:
            return
        self._chunk_type_index_unavailable_collections.add(collection_name)
        logger.warning(
            "hybrid_chunk_type_filter_disabled collection=%s reason=%s fallback_without_chunk_type_filter=true",
            collection_name,
            str(error),
        )

    @staticmethod
    def _extract_points_from_query_points(raw_result: Any) -> List[Any]:
        if raw_result is None:
            return []
        if isinstance(raw_result, list):
            return raw_result
        if hasattr(raw_result, "points"):
            return list(raw_result.points or [])
        if isinstance(raw_result, dict):
            points = raw_result.get("points")
            if isinstance(points, list):
                return points
        return []

    @staticmethod
    def _filter_chunk_only(points: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        filtered: List[Dict[str, Any]] = []
        for point in points:
            payload = point.get("payload", {}) or {}
            if payload.get("record_type") == "file":
                continue
            if payload.get("record_type") == "chunk" or payload.get("parent_file_id"):
                filtered.append(point)
        return filtered

    @staticmethod
    def _rrf_fuse(
        dense_results: List[Dict[str, Any]],
        bm25_results: List[Dict[str, Any]],
        limit: int,
        k: int = 60,
    ) -> List[Dict[str, Any]]:
        scored: Dict[str, Dict[str, Any]] = {}
        for rank, point in enumerate(dense_results, start=1):
            key = VectorService._candidate_key(point)
            if key not in scored:
                scored[key] = {"point": point, "rrf_score": 0.0}
            scored[key]["rrf_score"] += 1.0 / (k + rank)
        for rank, point in enumerate(bm25_results, start=1):
            key = VectorService._candidate_key(point)
            if key not in scored:
                scored[key] = {"point": point, "rrf_score": 0.0}
            scored[key]["rrf_score"] += 1.0 / (k + rank)

        fused = sorted(scored.values(), key=lambda item: item["rrf_score"], reverse=True)
        output: List[Dict[str, Any]] = []
        for item in fused[:limit]:
            point = item["point"]
            point["rrf_score"] = item["rrf_score"]
            output.append(point)
        return output

    @staticmethod
    def _with_chunk_type_filter(filter_obj: Filter, chunk_type: str) -> Filter:
        return Filter(
            must=[
                *(filter_obj.must or []),
                FieldCondition(
                    key="chunk_type",
                    match=MatchValue(value=chunk_type),
                ),
            ],
            should=filter_obj.should or [],
            must_not=filter_obj.must_not or [],
        )

    @staticmethod
    def _detect_research_intent(query_text: str) -> str:
        query = (query_text or "").lower()
        quote_terms = ["quote", "exact", "verbatim", "citation", "evidence", "source text"]
        synthesis_terms = ["summarize", "summary", "theme", "compare", "strategy", "synthesis", "cross-document"]
        tone_terms = ["sentiment", "tone", "framing", "narrative", "messaging tone"]
        if any(term in query for term in quote_terms):
            return "quote"
        if any(term in query for term in synthesis_terms):
            return "synthesis"
        if any(term in query for term in tone_terms):
            return "tone"
        return "balanced"

    @staticmethod
    def _intent_profile(intent: str) -> Dict[str, int]:
        # Values are percentages for total limit and per-document cap.
        if intent == "quote":
            return {"micro_pct": 75, "macro_pct": 25, "per_doc_cap": 3}
        if intent == "synthesis":
            return {"micro_pct": 30, "macro_pct": 70, "per_doc_cap": 2}
        if intent == "tone":
            return {"micro_pct": 40, "macro_pct": 60, "per_doc_cap": 2}
        return {"micro_pct": 50, "macro_pct": 50, "per_doc_cap": 2}

    @staticmethod
    def _merge_with_diversity(
        *,
        micro_results: List[Dict[str, Any]],
        macro_results: List[Dict[str, Any]],
        micro_quota: int,
        macro_quota: int,
        per_doc_cap: int,
        limit: int,
    ) -> List[Dict[str, Any]]:
        merged: List[Dict[str, Any]] = []
        seen: set[str] = set()
        per_doc_counts: Dict[str, int] = {}

        def _try_add(point: Dict[str, Any]) -> bool:
            key = VectorService._candidate_key(point)
            if not key or key in seen:
                return False
            payload = point.get("payload", {}) or {}
            doc_key = str(payload.get("parent_file_id") or payload.get("filename") or "__unknown__")
            if per_doc_counts.get(doc_key, 0) >= per_doc_cap:
                return False
            seen.add(key)
            per_doc_counts[doc_key] = per_doc_counts.get(doc_key, 0) + 1
            merged.append(point)
            return True

        micro_added = 0
        for point in micro_results:
            if len(merged) >= limit or micro_added >= micro_quota:
                break
            if _try_add(point):
                micro_added += 1

        macro_added = 0
        for point in macro_results:
            if len(merged) >= limit or macro_added >= macro_quota:
                break
            if _try_add(point):
                macro_added += 1

        # Fill remaining slots using both pools while still honoring diversity cap.
        for point in [*micro_results, *macro_results]:
            if len(merged) >= limit:
                break
            _try_add(point)
        return merged

    async def hybrid_search(
        self,
        *,
        collection_name: str,
        query_text: str,
        query_embedding: Sequence[float],
        tenant_filter: Filter,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """Hybrid dense + BM25 search with RRF fusion and graceful degradation."""
        dense_results: Optional[List[Dict[str, Any]]] = None
        bm25_results: Optional[List[Dict[str, Any]]] = None
        effective_filter = tenant_filter
        if (
            collection_name in self._chunk_type_index_unavailable_collections
            and self._has_filter_field_in_must(effective_filter, "chunk_type")
        ):
            effective_filter = self._drop_field_from_must(effective_filter, "chunk_type")

        # Dense branch (existing retrieval behavior baseline)
        try:
            dense_raw = await self._client.search(
                collection_name=collection_name,
                query_vector=query_embedding,
                query_filter=effective_filter,
                limit=limit,
                with_payload=True,
            )
            dense_results = self._filter_chunk_only([self._point_to_dict(point) for point in dense_raw])
        except Exception as exc:
            if self._is_missing_payload_index_error(exc, "record_type"):
                fallback_filter = self._drop_record_type_from_must_not(effective_filter)
                try:
                    dense_raw = await self._client.search(
                        collection_name=collection_name,
                        query_vector=query_embedding,
                        query_filter=fallback_filter,
                        limit=limit,
                        with_payload=True,
                    )
                    dense_results = self._filter_chunk_only([self._point_to_dict(point) for point in dense_raw])
                except Exception as record_type_fallback_exc:
                    if self._is_missing_payload_index_error(record_type_fallback_exc, "chunk_type"):
                        self._mark_chunk_type_index_unavailable(collection_name, error=record_type_fallback_exc)
                        fallback_filter = self._drop_field_from_must(fallback_filter, "chunk_type")
                        dense_raw = await self._client.search(
                            collection_name=collection_name,
                            query_vector=query_embedding,
                            query_filter=fallback_filter,
                            limit=limit,
                            with_payload=True,
                        )
                        dense_results = self._filter_chunk_only([self._point_to_dict(point) for point in dense_raw])
                        effective_filter = fallback_filter
                    else:
                        raise
            elif self._is_missing_payload_index_error(exc, "chunk_type"):
                self._mark_chunk_type_index_unavailable(collection_name, error=exc)
                fallback_filter = self._drop_field_from_must(effective_filter, "chunk_type")
                dense_raw = await self._client.search(
                    collection_name=collection_name,
                    query_vector=query_embedding,
                    query_filter=fallback_filter,
                    limit=limit,
                    with_payload=True,
                )
                dense_results = self._filter_chunk_only([self._point_to_dict(point) for point in dense_raw])
                effective_filter = fallback_filter
            else:
                logger.warning(
                    "hybrid_dense_search_failed collection=%s error=%s",
                    collection_name,
                    exc,
                )
                dense_results = []

        # BM25 branch (best-effort)
        bm25_available = await self.bm25_enabled_for_collection(collection_name)
        if bm25_available:
            try:
                bm25_raw = await self._client.query_points(
                    collection_name=collection_name,
                    query=models.Document(text=query_text, model="qdrant/bm25"),
                    using="bm25",
                    query_filter=effective_filter,
                    limit=limit,
                    with_payload=True,
                )
                bm25_points = self._extract_points_from_query_points(bm25_raw)
                bm25_results = self._filter_chunk_only([self._point_to_dict(point) for point in bm25_points])
            except Exception as exc:
                if self._is_missing_payload_index_error(exc, "record_type"):
                    try:
                        fallback_filter = self._drop_record_type_from_must_not(effective_filter)
                        bm25_raw = await self._client.query_points(
                            collection_name=collection_name,
                            query=models.Document(text=query_text, model="qdrant/bm25"),
                            using="bm25",
                            query_filter=fallback_filter,
                            limit=limit,
                            with_payload=True,
                        )
                        bm25_points = self._extract_points_from_query_points(bm25_raw)
                        bm25_results = self._filter_chunk_only([self._point_to_dict(point) for point in bm25_points])
                    except Exception as bm25_fallback_exc:
                        if self._is_missing_payload_index_error(bm25_fallback_exc, "chunk_type"):
                            self._mark_chunk_type_index_unavailable(collection_name, error=bm25_fallback_exc)
                            try:
                                fallback_filter = self._drop_field_from_must(fallback_filter, "chunk_type")
                                bm25_raw = await self._client.query_points(
                                    collection_name=collection_name,
                                    query=models.Document(text=query_text, model="qdrant/bm25"),
                                    using="bm25",
                                    query_filter=fallback_filter,
                                    limit=limit,
                                    with_payload=True,
                                )
                                bm25_points = self._extract_points_from_query_points(bm25_raw)
                                bm25_results = self._filter_chunk_only([self._point_to_dict(point) for point in bm25_points])
                                effective_filter = fallback_filter
                            except Exception as bm25_chunktype_fallback_exc:
                                if self._is_missing_vector_name_error(bm25_chunktype_fallback_exc, "bm25"):
                                    self.mark_bm25_unavailable(collection_name, str(bm25_chunktype_fallback_exc))
                                else:
                                    logger.warning(
                                        "hybrid_bm25_search_unavailable collection=%s error=%s",
                                        collection_name,
                                        bm25_chunktype_fallback_exc,
                                    )
                                bm25_results = []
                        elif self._is_missing_vector_name_error(bm25_fallback_exc, "bm25"):
                            self.mark_bm25_unavailable(collection_name, str(bm25_fallback_exc))
                        else:
                            logger.warning(
                                "hybrid_bm25_search_unavailable collection=%s error=%s",
                                collection_name,
                                bm25_fallback_exc,
                            )
                        bm25_results = []
                elif self._is_missing_payload_index_error(exc, "chunk_type"):
                    self._mark_chunk_type_index_unavailable(collection_name, error=exc)
                    try:
                        fallback_filter = self._drop_field_from_must(effective_filter, "chunk_type")
                        bm25_raw = await self._client.query_points(
                            collection_name=collection_name,
                            query=models.Document(text=query_text, model="qdrant/bm25"),
                            using="bm25",
                            query_filter=fallback_filter,
                            limit=limit,
                            with_payload=True,
                        )
                        bm25_points = self._extract_points_from_query_points(bm25_raw)
                        bm25_results = self._filter_chunk_only([self._point_to_dict(point) for point in bm25_points])
                        effective_filter = fallback_filter
                    except Exception as bm25_fallback_exc:
                        if self._is_missing_vector_name_error(bm25_fallback_exc, "bm25"):
                            self.mark_bm25_unavailable(collection_name, str(bm25_fallback_exc))
                        else:
                            logger.warning(
                                "hybrid_bm25_search_unavailable collection=%s error=%s",
                                collection_name,
                                bm25_fallback_exc,
                            )
                        bm25_results = []
                else:
                    if self._is_missing_vector_name_error(exc, "bm25"):
                        self.mark_bm25_unavailable(collection_name, str(exc))
                    else:
                        logger.warning(
                            "hybrid_bm25_search_unavailable collection=%s error=%s",
                            collection_name,
                            exc,
                        )
                    bm25_results = []
        else:
            bm25_results = []

        dense_results = dense_results or []
        bm25_results = bm25_results or []
        if dense_results and bm25_results:
            return self._rrf_fuse(dense_results, bm25_results, limit=limit)
        if dense_results:
            return dense_results[:limit]
        if bm25_results:
            return bm25_results[:limit]
        return []

    async def hybrid_search_research(
        self,
        *,
        collection_name: str,
        query_text: str,
        query_embedding: Sequence[float],
        tenant_filter: Filter,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """Chunk-type-aware hybrid retrieval orchestration for Riley research workflows."""
        intent = self._detect_research_intent(query_text)
        profile = self._intent_profile(intent)
        micro_quota = max(1, int(round(limit * (profile["micro_pct"] / 100.0))))
        macro_quota = max(1, limit - micro_quota)
        per_doc_cap = max(1, profile["per_doc_cap"])
        branch_fetch_limit = max(4, limit * 3)

        micro_results: List[Dict[str, Any]] = []
        macro_results: List[Dict[str, Any]] = []

        # Retrieve branches independently; any branch failure degrades gracefully.
        try:
            micro_filter = self._with_chunk_type_filter(tenant_filter, "micro")
            micro_results = await self.hybrid_search(
                collection_name=collection_name,
                query_text=query_text,
                query_embedding=query_embedding,
                tenant_filter=micro_filter,
                limit=branch_fetch_limit,
            )
        except Exception as exc:
            logger.warning(
                "hybrid_research_micro_branch_failed collection=%s error=%s",
                collection_name,
                exc,
            )
            micro_results = []

        try:
            macro_filter = self._with_chunk_type_filter(tenant_filter, "macro")
            macro_results = await self.hybrid_search(
                collection_name=collection_name,
                query_text=query_text,
                query_embedding=query_embedding,
                tenant_filter=macro_filter,
                limit=branch_fetch_limit,
            )
        except Exception as exc:
            logger.warning(
                "hybrid_research_macro_branch_failed collection=%s error=%s",
                collection_name,
                exc,
            )
            macro_results = []

        if micro_results or macro_results:
            merged = self._merge_with_diversity(
                micro_results=micro_results,
                macro_results=macro_results,
                micro_quota=micro_quota,
                macro_quota=macro_quota,
                per_doc_cap=per_doc_cap,
                limit=limit,
            )
            if merged:
                return merged

        # Full fallback: single-pool hybrid search preserves legacy behavior.
        return await self.hybrid_search(
            collection_name=collection_name,
            query_text=query_text,
            query_embedding=query_embedding,
            tenant_filter=tenant_filter,
            limit=limit,
        )

    async def search_silo(
        self,
        collection_name: str,
        query_vector: Sequence[float],
        tenant_id: str,
        limit: int = 10,
        require_ai_enabled: bool = False,
    ) -> List[Any]:
        """Search tenant-scoped collection with strict isolation.
        
        SECURITY: This method MUST enforce tenant isolation. If tenant_id is missing
        or empty, a ValueError is raised to prevent cross-tenant data leaks.
        
        Args:
            collection_name: Name of the Qdrant collection to search
            query_vector: The query embedding vector
            tenant_id: Tenant/client ID to filter by (required)
            limit: Maximum number of results to return
            require_ai_enabled: If True, only return files where ai_enabled == true
        
        Returns:
            List of search results (points) as dictionaries
        """
        # Enforce tenant isolation - raise error if tenant_id is missing
        if not tenant_id or not tenant_id.strip():
            raise ValueError(
                "tenant_id is required for search_silo. Cannot perform unfiltered search."
            )
        
        # Construct tenant filter for strict isolation
        filter_conditions = [
                FieldCondition(
                    key="client_id",
                    match=MatchValue(value=tenant_id),
                )
            ]
        
        # Add ai_enabled filter if required
        if require_ai_enabled:
            filter_conditions.append(
                FieldCondition(
                    key="ai_enabled",
                    match=MatchValue(value=True),
                )
            )
        
        tenant_filter = Filter(
            must=filter_conditions,
            must_not=[
                FieldCondition(
                    key="record_type",
                    match=MatchValue(value="file"),
                )
            ],
        )

        try:
            search_result = await self._client.search(
                collection_name=collection_name,
                query_vector=query_vector,
                query_filter=tenant_filter,  # SECURITY: Always use tenant filter
                limit=limit,
                with_payload=True,  # Ensure payload (including filename) is returned
            )
        except Exception as exc:
            if not self._is_missing_payload_index_error(exc, "record_type"):
                raise
            # Compatibility fallback for collections created before payload indexing.
            legacy_filter = Filter(must=filter_conditions)
            search_result = await self._client.search(
                collection_name=collection_name,
                query_vector=query_vector,
                query_filter=legacy_filter,
                limit=limit,
                with_payload=True,
            )
            search_result = [
                point for point in search_result
                if (point.payload or {}).get("record_type") != "file"
            ]

        return [point.dict() for point in search_result]

    async def search_global(
        self,
        collection_name: str,
        query_vector: Sequence[float],
        limit: int = 5,
        filter: Filter | None = None,
    ) -> List[Any]:
        """Search a collection with an explicit filter (required for security).

        This method is used for searching global firm knowledge collections.
        
        SECURITY: This method NEVER allows unfiltered searches. A filter must be provided
        to prevent accidental data leaks. For global searches, use is_global=True filter.
        
        Args:
            collection_name: Name of the Qdrant collection to search
            query_vector: The query embedding vector
            limit: Maximum number of results to return
            filter: REQUIRED Filter object to apply (e.g., is_global=True for Tier 1)
        
        Returns:
            List of search results (points) as dictionaries
        
        Raises:
            ValueError: If filter is None (unfiltered searches are not allowed)
        """
        # SECURITY: Never allow unfiltered searches
        if filter is None:
            raise ValueError(
                "search_global requires an explicit filter. Unfiltered searches are not allowed. "
                "For global files, use Filter with is_global=True condition."
            )
        
        query_filter = Filter(
            must=filter.must or [],
            should=filter.should or [],
            must_not=[
                *(filter.must_not or []),
                FieldCondition(
                    key="record_type",
                    match=MatchValue(value="file"),
                ),
            ],
        )

        try:
            search_result = await self._client.search(
                collection_name=collection_name,
                query_vector=query_vector,
                query_filter=query_filter,  # SECURITY: Always use explicit filter
                limit=limit,
                with_payload=True,  # Ensure payload (including filename) is returned
            )
        except Exception as exc:
            if not self._is_missing_payload_index_error(exc, "record_type"):
                raise
            search_result = await self._client.search(
                collection_name=collection_name,
                query_vector=query_vector,
                query_filter=filter,
                limit=limit,
                with_payload=True,
            )
            search_result = [
                point for point in search_result
                if (point.payload or {}).get("record_type") != "file"
            ]

        return [point.dict() for point in search_result]

    async def list_tenant_files(
        self,
        collection_name: str,
        tenant_id: str,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """List all files for a specific tenant from the collection.

        Uses Qdrant's scroll API with a filter to retrieve all points
        matching the tenant_id, then extracts file metadata.

        Special handling: If tenant_id == "global", filters by is_global=True
        instead of client_id to retrieve files from the global archive.

        Args:
            collection_name: Name of the Qdrant collection to query.
            tenant_id: The tenant/client ID to filter by, or "global" for global archive.
            limit: Maximum number of files to return (default: 20).

        Returns:
            List of dictionaries containing:
                - id: Point ID
                - filename: File name from payload
                - type: File type/extension from payload
                - year: Year from payload (if available)
        """
        # Special handling for global archive
        if tenant_id == "global":
            # Filter by is_global flag (do NOT filter by client_id)
            tenant_filter = Filter(
                must=[
                    FieldCondition(
                        key="is_global",
                        match=MatchValue(value=True),
                    )
                ],
                must_not=[
                    FieldCondition(
                        key="record_type",
                        match=MatchValue(value="chunk"),
                    )
                ],
            )
        else:
            # Standard tenant-scoped filter
            tenant_filter = Filter(
                must=[
                    FieldCondition(
                        key="client_id",
                        match=MatchValue(value=tenant_id),
                    )
                ],
                must_not=[
                    FieldCondition(
                        key="record_type",
                        match=MatchValue(value="chunk"),
                    )
                ],
            )

        # Use scroll to get all matching points
        try:
            scroll_result = await self._client.scroll(
                collection_name=collection_name,
                scroll_filter=tenant_filter,
                limit=limit,
                with_payload=True,
            )
            points = scroll_result[0]
        except Exception as exc:
            if not self._is_missing_payload_index_error(exc, "record_type"):
                raise
            legacy_filter = Filter(
                must=tenant_filter.must or [],
                should=tenant_filter.should or [],
            )
            scroll_result = await self._client.scroll(
                collection_name=collection_name,
                scroll_filter=legacy_filter,
                limit=limit,
                with_payload=True,
            )
            points = [
                point for point in scroll_result[0]
                if (point.payload or {}).get("record_type") != "chunk"
            ]

        files = []
        for point in points:
            payload = point.payload or {}
            filename = payload.get("filename") or payload.get("name", "Unknown")
            file_type = payload.get("type") or payload.get("file_type", "")
            year = payload.get("year")

            files.append({
                "id": str(point.id),
                "filename": filename,
                "type": file_type,
                "year": year,
                "url": payload.get("url", ""),
                "tags": payload.get("tags", []),
                "status": payload.get("status"),
                "assignee": payload.get("assignee"),
                "assigned_to": payload.get("assigned_to", []),
                "messaging_visible_user_ids": payload.get("messaging_visible_user_ids", []),
                "messaging_created_by_user_id": payload.get("messaging_created_by_user_id"),
                "size": payload.get("size", "Unknown"),
                "upload_date": payload.get("upload_date", datetime.now().isoformat()),
                "ai_enabled": payload.get("ai_enabled"),
                "ocr_enabled": payload.get("ocr_enabled"),
                "ocr_status": payload.get("ocr_status"),
                "ocr_confidence": payload.get("ocr_confidence"),
                "ocr_extracted_at": payload.get("ocr_extracted_at"),
                "preview_url": payload.get("preview_url"),
                "preview_type": payload.get("preview_type"),
                "preview_status": payload.get("preview_status"),
                "preview_error": payload.get("preview_error"),
                "ingestion_status": payload.get("ingestion_status"),
                "extracted_char_count": payload.get("extracted_char_count"),
                "chunk_count": payload.get("chunk_count"),
                "analysis_status": payload.get("analysis_status"),
                "doc_summary_short": payload.get("doc_summary_short"),
                "key_themes": payload.get("key_themes"),
                "key_entities": payload.get("key_entities"),
                "sentiment_overall": payload.get("sentiment_overall"),
                "tone_labels": payload.get("tone_labels"),
                "framing_labels": payload.get("framing_labels"),
                "audience_implications": payload.get("audience_implications"),
                "persuasion_risks": payload.get("persuasion_risks"),
                "strategic_opportunities": payload.get("strategic_opportunities"),
                "major_claims_or_evidence": payload.get("major_claims_or_evidence"),
                "analysis_fidelity_level": payload.get("analysis_fidelity_level"),
                "analysis_chunks_coverage_ratio": payload.get("analysis_chunks_coverage_ratio"),
                "analysis_chars_coverage_ratio": payload.get("analysis_chars_coverage_ratio"),
                "analysis_context_reduction_applied": payload.get("analysis_context_reduction_applied"),
                "analysis_execution_mode": payload.get("analysis_execution_mode"),
                "analysis_total_bands": payload.get("analysis_total_bands"),
                "analysis_analyzed_bands": payload.get("analysis_analyzed_bands"),
                "analysis_band_coverage_ratio": payload.get("analysis_band_coverage_ratio"),
                "analysis_final_fidelity_level": payload.get("analysis_final_fidelity_level"),
                "analysis_validation_status": payload.get("analysis_validation_status"),
                "analysis_validation_note": payload.get("analysis_validation_note"),
                "analysis_contradiction_count": payload.get("analysis_contradiction_count"),
                "analysis_failed_bands_count": payload.get("analysis_failed_bands_count"),
                "analysis_validation_reasons_json": payload.get("analysis_validation_reasons_json"),
                "analysis_high_signal_band_coverage_ratio": payload.get("analysis_high_signal_band_coverage_ratio"),
                "analysis_appendix_required": payload.get("analysis_appendix_required"),
                "analysis_appendix_covered": payload.get("analysis_appendix_covered"),
            })

        return files

    async def list_global_files(
        self,
        collection_name: str,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """List all global files from the collection (Tier 1 - Global Firm Archive).
        
        Returns files that have been promoted to the global archive (have is_golden
        or promoted_at fields, or are in the global collection without client_id).

        Args:
            collection_name: Name of the Qdrant collection to query (should be Tier 1).
            limit: Maximum number of files to return (default: 1000).

        Returns:
            List of dictionaries containing:
                - id: Point ID
                - filename: File name from payload
                - type: File type/extension from payload
                - url: File URL
                - is_golden: Whether marked as golden standard
                - promoted_at: Promotion timestamp
                - client_id: Original campaign ID (if preserved)
        """
        # Filter for files marked as global (is_global=True)
        # This ensures we only get files that were explicitly promoted
        global_filter = Filter(
            must=[
                FieldCondition(
                    key="is_global",
                    match=MatchValue(value=True),
                )
            ],
            must_not=[
                FieldCondition(
                    key="record_type",
                    match=MatchValue(value="chunk"),
                )
            ],
        )

        points: List[Any] = []
        offset = None
        page_size = 500
        while True:
            try:
                scroll_result = await self._client.scroll(
                    collection_name=collection_name,
                    scroll_filter=global_filter,
                    limit=page_size,
                    offset=offset,
                    with_payload=True,
                    with_vectors=False,  # Don't need vectors for listing
                )
                batch = scroll_result[0]
            except Exception as exc:
                if not self._is_missing_payload_index_error(exc, "record_type"):
                    raise
                legacy_filter = Filter(
                    must=global_filter.must or [],
                    should=global_filter.should or [],
                )
                scroll_result = await self._client.scroll(
                    collection_name=collection_name,
                    scroll_filter=legacy_filter,
                    limit=page_size,
                    offset=offset,
                    with_payload=True,
                    with_vectors=False,
                )
                batch = [
                    point for point in scroll_result[0]
                    if (point.payload or {}).get("record_type") != "chunk"
                ]

            if not batch:
                break
            points.extend(batch)
            offset = scroll_result[1]
            if offset is None:
                break

        files = []
        for point in points:
            payload = point.payload or {}
            filename = payload.get("filename") or payload.get("name")
            file_type = payload.get("type") or payload.get("file_type", "")
            source_campaign_id = (
                payload.get("source_campaign_id")
                or payload.get("client_id")
                or payload.get("tenant_id")
            )
            
            files.append({
                "id": str(point.id),
                "filename": filename,
                "type": file_type,
                "url": payload.get("url", ""),
                "is_golden": payload.get("is_golden", False),
                "promoted_at": payload.get("promoted_at"),
                "client_id": source_campaign_id,
                "source_campaign_id": source_campaign_id,
                "ai_enabled": payload.get("ai_enabled"),
                "ocr_enabled": payload.get("ocr_enabled"),
                "ocr_status": payload.get("ocr_status"),
                "ocr_confidence": payload.get("ocr_confidence"),
                "ocr_extracted_at": payload.get("ocr_extracted_at"),
                "preview_url": payload.get("preview_url"),
                "preview_type": payload.get("preview_type"),
                "preview_status": payload.get("preview_status"),
                "preview_error": payload.get("preview_error"),
                "ingestion_status": payload.get("ingestion_status"),
                "extracted_char_count": payload.get("extracted_char_count"),
                "chunk_count": payload.get("chunk_count"),
                "analysis_status": payload.get("analysis_status"),
                "doc_summary_short": payload.get("doc_summary_short"),
                "key_themes": payload.get("key_themes"),
                "key_entities": payload.get("key_entities"),
                "sentiment_overall": payload.get("sentiment_overall"),
                "tone_labels": payload.get("tone_labels"),
                "framing_labels": payload.get("framing_labels"),
                "audience_implications": payload.get("audience_implications"),
                "persuasion_risks": payload.get("persuasion_risks"),
                "strategic_opportunities": payload.get("strategic_opportunities"),
                "major_claims_or_evidence": payload.get("major_claims_or_evidence"),
                "analysis_fidelity_level": payload.get("analysis_fidelity_level"),
                "analysis_chunks_coverage_ratio": payload.get("analysis_chunks_coverage_ratio"),
                "analysis_chars_coverage_ratio": payload.get("analysis_chars_coverage_ratio"),
                "analysis_context_reduction_applied": payload.get("analysis_context_reduction_applied"),
                "analysis_execution_mode": payload.get("analysis_execution_mode"),
                "analysis_total_bands": payload.get("analysis_total_bands"),
                "analysis_analyzed_bands": payload.get("analysis_analyzed_bands"),
                "analysis_band_coverage_ratio": payload.get("analysis_band_coverage_ratio"),
                "analysis_final_fidelity_level": payload.get("analysis_final_fidelity_level"),
                "analysis_validation_status": payload.get("analysis_validation_status"),
                "analysis_validation_note": payload.get("analysis_validation_note"),
                "analysis_contradiction_count": payload.get("analysis_contradiction_count"),
                "analysis_failed_bands_count": payload.get("analysis_failed_bands_count"),
                "analysis_validation_reasons_json": payload.get("analysis_validation_reasons_json"),
                "analysis_high_signal_band_coverage_ratio": payload.get("analysis_high_signal_band_coverage_ratio"),
                "analysis_appendix_required": payload.get("analysis_appendix_required"),
                "analysis_appendix_covered": payload.get("analysis_appendix_covered"),
            })

        files.sort(key=lambda item: str(item.get("promoted_at") or ""), reverse=True)
        return files[:limit]

    async def get_index_summary(
        self,
        collection_name: str,
        tenant_id: str,
    ) -> Dict[str, Any]:
        """Return campaign/global document ingestion summary for researcher visibility."""
        if tenant_id == "global":
            summary_filter = Filter(
                must=[
                    FieldCondition(
                        key="is_global",
                        match=MatchValue(value=True),
                    )
                ],
                must_not=[
                    FieldCondition(
                        key="record_type",
                        match=MatchValue(value="chunk"),
                    )
                ],
            )
        else:
            summary_filter = Filter(
                must=[
                    FieldCondition(
                        key="client_id",
                        match=MatchValue(value=tenant_id),
                    )
                ],
                must_not=[
                    FieldCondition(
                        key="record_type",
                        match=MatchValue(value="chunk"),
                    )
                ],
            )

        points: List[Any] = []
        offset = None
        while True:
            try:
                scroll_result = await self._client.scroll(
                    collection_name=collection_name,
                    scroll_filter=summary_filter,
                    limit=500,
                    offset=offset,
                    with_payload=True,
                    with_vectors=False,
                )
                batch = scroll_result[0]
            except Exception as exc:
                if not self._is_missing_payload_index_error(exc, "record_type"):
                    raise
                legacy_filter = Filter(
                    must=summary_filter.must or [],
                    should=summary_filter.should or [],
                )
                scroll_result = await self._client.scroll(
                    collection_name=collection_name,
                    scroll_filter=legacy_filter,
                    limit=500,
                    offset=offset,
                    with_payload=True,
                    with_vectors=False,
                )
                batch = [
                    point for point in scroll_result[0]
                    if (point.payload or {}).get("record_type") != "chunk"
                ]

            if not batch:
                break

            points.extend(batch)
            offset = scroll_result[1]
            if offset is None:
                break

        counts_by_file_type: Dict[str, int] = {}
        recent_uploads: List[Dict[str, str]] = []
        indexed_count = 0
        processing_count = 0
        failed_count = 0
        low_text_count = 0
        ocr_needed_count = 0
        ocr_processed_count = 0
        vision_processed_count = 0
        partial_count = 0

        for point in points:
            payload = point.payload or {}
            file_type = str(payload.get("type") or payload.get("file_type") or "unknown").lower()
            counts_by_file_type[file_type] = counts_by_file_type.get(file_type, 0) + 1

            ingestion_status = str(payload.get("ingestion_status") or "uploaded").lower()
            if ingestion_status == "indexed":
                indexed_count += 1
            elif ingestion_status in {"queued", "processing", "uploaded"}:
                processing_count += 1
            elif ingestion_status == "failed":
                failed_count += 1
            elif ingestion_status == "low_text":
                low_text_count += 1
            elif ingestion_status == "ocr_needed":
                ocr_needed_count += 1

            ocr_status = str(payload.get("ocr_status") or "").lower()
            vision_status = str(payload.get("vision_status") or "").lower()
            multimodal_status = str(payload.get("multimodal_status") or "").lower()
            if ocr_status == "complete":
                ocr_processed_count += 1
            if vision_status == "complete":
                vision_processed_count += 1
            is_partial = (
                ingestion_status == "partial"
                or ocr_status == "failed"
                or vision_status == "failed"
                or multimodal_status in {"ocr_failed", "ocr_unavailable", "partial"}
            )
            if is_partial:
                partial_count += 1

            recent_uploads.append(
                {
                    "filename": str(payload.get("filename") or "Unknown"),
                    "file_type": file_type,
                    "ingestion_status": ingestion_status,
                    "upload_date": str(payload.get("upload_date") or payload.get("uploaded_at") or ""),
                }
            )

        recent_uploads.sort(key=lambda item: item.get("upload_date", ""), reverse=True)

        return {
            "total_documents": len(points),
            "indexed_count": indexed_count,
            "processing_count": processing_count,
            "failed_count": failed_count,
            "low_text_count": low_text_count,
            "ocr_needed_count": ocr_needed_count,
            "ocr_processed_count": ocr_processed_count,
            "vision_processed_count": vision_processed_count,
            "partial_count": partial_count,
            "counts_by_file_type": counts_by_file_type,
            "recent_uploads": recent_uploads[:5],
        }

    async def promote_to_global(
        self,
        file_id: str,
        is_golden: bool,
        source_campaign_id: Optional[str] = None,
    ) -> None:
        """Promote a file from Tier 2 (Private Client Silo) to Tier 1 (Global Firm Archive).
        
        This copies a high-value document from the private client collection to the global
        collection, allowing "Global Riley" to learn from it without violating tenant security.
        
        Args:
            file_id: The UUID of the file point in Tier 2 collection.
            is_golden: Whether to mark this as a "Golden Standard" (high priority learning).
        
        Raises:
            ValueError: If the file is not found in Tier 2.
        """
        settings = get_settings()
        
        # Step 1: Fetch the point from Tier 2
        points = await self._client.retrieve(
            collection_name=settings.QDRANT_COLLECTION_TIER_2,
            ids=[file_id],
            with_payload=True,
            with_vectors=True,
        )
        
        if not points:
            raise ValueError(f"File {file_id} not found in Tier 2 collection")
        
        point = points[0]
        original_payload = point.payload or {}
        
        # Step 2: Prepare the new payload for Tier 1
        # Copy existing payload and modify for global archive
        new_payload = original_payload.copy()
        
        # Add/Update golden standard flag
        new_payload["is_golden"] = is_golden
        
        # Add promotion timestamp
        new_payload["promoted_at"] = datetime.now().isoformat()
        
        # Mark as global file (CRITICAL for retrieval)
        new_payload["is_global"] = True

        # Preserve original campaign/source scope for Firm Documents origin metadata.
        resolved_source_campaign_id = (
            str(source_campaign_id or "").strip()
            or original_payload.get("source_campaign_id")
            or original_payload.get("client_id")
            or original_payload.get("tenant_id")
        )
        if resolved_source_campaign_id:
            new_payload["source_campaign_id"] = resolved_source_campaign_id
        
        # Remove tenant-specific fields or set to "global"
        # Remove client_id to ensure it's truly global (or set to "global" if needed for tracking)
        if "client_id" in new_payload:
            # Option 1: Remove it completely (truly global)
            del new_payload["client_id"]
            # Option 2: Set to "global" for tracking (uncomment if preferred)
            # new_payload["client_id"] = "global"
        
        # Step 3: Upsert into Tier 1 with the same file_id (for tracking)
        global_point = PointStruct(
            id=file_id,  # Use same ID for tracking
            vector=point.vector,  # Use the same vector
            payload=new_payload
        )
        
        await self._client.upsert(
            collection_name=settings.QDRANT_COLLECTION_TIER_1,
            points=[global_point]
        )

        # Also promote chunk records linked to this file so global retrieval
        # can use chunked vectors instead of only the parent document point.
        chunk_filter = Filter(
            must=[
                FieldCondition(
                    key="parent_file_id",
                    match=MatchValue(value=file_id),
                ),
                FieldCondition(
                    key="record_type",
                    match=MatchValue(value="chunk"),
                ),
            ]
        )
        try:
            chunk_points, _ = await self._client.scroll(
                collection_name=settings.QDRANT_COLLECTION_TIER_2,
                scroll_filter=chunk_filter,
                limit=5000,
                with_payload=True,
                with_vectors=True,
            )
        except Exception as exc:
            if not self._is_missing_payload_index_error(exc, "record_type"):
                raise
            fallback_filter = Filter(
                must=[FieldCondition(key="parent_file_id", match=MatchValue(value=file_id))]
            )
            chunk_points, _ = await self._client.scroll(
                collection_name=settings.QDRANT_COLLECTION_TIER_2,
                scroll_filter=fallback_filter,
                limit=5000,
                with_payload=True,
                with_vectors=True,
            )
            chunk_points = [
                point for point in chunk_points
                if (point.payload or {}).get("record_type") == "chunk"
            ]
        if chunk_points:
            promoted_chunks: List[PointStruct] = []
            for chunk in chunk_points:
                chunk_payload = (chunk.payload or {}).copy()
                chunk_payload["is_global"] = True
                chunk_payload["promoted_at"] = datetime.now().isoformat()
                if "client_id" in chunk_payload:
                    del chunk_payload["client_id"]
                promoted_chunks.append(
                    PointStruct(
                        id=str(chunk.id),
                        vector=chunk.vector,
                        payload=chunk_payload,
                    )
                )
            await self._client.upsert(
                collection_name=settings.QDRANT_COLLECTION_TIER_1,
                points=promoted_chunks,
            )

    async def _delete_chunk_points_for_parent(
        self,
        *,
        collection_name: str,
        parent_file_id: str,
    ) -> int:
        """Delete chunk points associated with a parent file id."""
        chunk_filter = Filter(
            must=[
                FieldCondition(
                    key="parent_file_id",
                    match=MatchValue(value=parent_file_id),
                ),
                FieldCondition(
                    key="record_type",
                    match=MatchValue(value="chunk"),
                ),
            ]
        )
        try:
            points, _ = await self._client.scroll(
                collection_name=collection_name,
                scroll_filter=chunk_filter,
                limit=5000,
                with_payload=False,
                with_vectors=False,
            )
        except Exception as exc:
            if not self._is_missing_payload_index_error(exc, "record_type"):
                raise
            fallback_filter = Filter(
                must=[
                    FieldCondition(
                        key="parent_file_id",
                        match=MatchValue(value=parent_file_id),
                    )
                ]
            )
            points, _ = await self._client.scroll(
                collection_name=collection_name,
                scroll_filter=fallback_filter,
                limit=5000,
                with_payload=True,
                with_vectors=False,
            )
            points = [
                point for point in points
                if (point.payload or {}).get("record_type") == "chunk"
            ]
        if not points:
            return 0
        point_ids = [str(point.id) for point in points]
        await self._client.delete(
            collection_name=collection_name,
            points_selector=models.PointIdsList(points=point_ids),
        )
        return len(point_ids)

    async def remove_from_global_archive(self, file_id: str) -> bool:
        """Remove a promoted file (and global chunk copies) from Tier 1 only."""
        settings = get_settings()
        points = await self._client.retrieve(
            collection_name=settings.QDRANT_COLLECTION_TIER_1,
            ids=[file_id],
            with_payload=True,
            with_vectors=False,
        )
        if not points:
            return False
        payload = points[0].payload or {}
        if not bool(payload.get("is_global")):
            return False
        await self._client.delete(
            collection_name=settings.QDRANT_COLLECTION_TIER_1,
            points_selector=models.PointIdsList(points=[file_id]),
        )
        await self._delete_chunk_points_for_parent(
            collection_name=settings.QDRANT_COLLECTION_TIER_1,
            parent_file_id=file_id,
        )
        return True

    async def hard_delete_campaign_vectors(self, campaign_id: str) -> Dict[str, Any]:
        """Hard delete vectors for a campaign across shared collections."""
        normalized_campaign_id = str(campaign_id or "").strip()
        if normalized_campaign_id.lower() == "global" or not normalized_campaign_id:
            raise ValueError(f"Cannot delete vectors for campaign_id '{campaign_id}'. Protected campaign.")

        settings = get_settings()
        # Best-effort ensure for legacy collections that predate payload indexes.
        await self._ensure_payload_indexes(settings.QDRANT_COLLECTION_TIER_1)
        await self._ensure_payload_indexes(settings.QDRANT_COLLECTION_TIER_2)

        vector_count_deleted = 0
        file_urls: List[str] = []

        async def _collect_points_unfiltered(
            collection_name: str,
            *,
            payload_matcher: Callable[[Dict[str, Any]], bool],
        ) -> List[Any]:
            points: List[Any] = []
            offset = None
            while True:
                scroll_result = await self._client.scroll(
                    collection_name=collection_name,
                    limit=500,
                    offset=offset,
                    with_payload=True,
                    with_vectors=False,
                )
                batch = scroll_result[0]
                if not batch:
                    break
                points.extend(
                    point for point in batch if payload_matcher(point.payload or {})
                )
                offset = scroll_result[1]
                if offset is None:
                    break
            return points

        async def _collect_points(
            collection_name: str,
            qdrant_filter: Filter,
            *,
            fallback_on_missing_index_for: Optional[str] = None,
            fallback_payload_matcher: Optional[Callable[[Dict[str, Any]], bool]] = None,
        ) -> List[Any]:
            points: List[Any] = []
            offset = None
            try:
                while True:
                    scroll_result = await self._client.scroll(
                        collection_name=collection_name,
                        scroll_filter=qdrant_filter,
                        limit=500,
                        offset=offset,
                        with_payload=True,
                        with_vectors=False,
                    )
                    batch = scroll_result[0]
                    if not batch:
                        break
                    points.extend(batch)
                    offset = scroll_result[1]
                    if offset is None:
                        break
            except Exception as exc:
                if (
                    fallback_on_missing_index_for
                    and fallback_payload_matcher is not None
                    and self._is_missing_payload_index_error(exc, fallback_on_missing_index_for)
                ):
                    logger.warning(
                        "qdrant_missing_payload_index_fallback collection=%s field=%s",
                        collection_name,
                        fallback_on_missing_index_for,
                    )
                    return await _collect_points_unfiltered(
                        collection_name,
                        payload_matcher=fallback_payload_matcher,
                    )
                raise
            return points

        async def _delete_points(collection_name: str, point_ids: List[str]) -> None:
            nonlocal vector_count_deleted
            if not point_ids:
                return
            await self._client.delete(
                collection_name=collection_name,
                points_selector=models.PointIdsList(points=point_ids),
            )
            vector_count_deleted += len(point_ids)

        # Tier 2 (shared private collection): delete all points where client_id == campaign_id.
        tier2_filter = Filter(
            must=[FieldCondition(key="client_id", match=MatchValue(value=normalized_campaign_id))]
        )
        tier2_points = await _collect_points(
            settings.QDRANT_COLLECTION_TIER_2,
            tier2_filter,
            fallback_on_missing_index_for="client_id",
            fallback_payload_matcher=(
                lambda payload: str(payload.get("client_id") or "").strip()
                == normalized_campaign_id
            ),
        )
        tier2_ids = [str(point.id) for point in tier2_points]
        for point in tier2_points:
            url = (point.payload or {}).get("url")
            if url:
                file_urls.append(url)
        await _delete_points(settings.QDRANT_COLLECTION_TIER_2, tier2_ids)

        # Tier 1 (global archive copies): delete file points sourced from this campaign.
        tier1_filter = Filter(
            should=[
                FieldCondition(key="source_campaign_id", match=MatchValue(value=normalized_campaign_id)),
                FieldCondition(key="client_id", match=MatchValue(value=normalized_campaign_id)),
                FieldCondition(key="tenant_id", match=MatchValue(value=normalized_campaign_id)),
            ]
        )
        tier1_points = await _collect_points(
            settings.QDRANT_COLLECTION_TIER_1,
            tier1_filter,
            fallback_on_missing_index_for="source_campaign_id",
            fallback_payload_matcher=(
                lambda payload: str(payload.get("source_campaign_id") or "").strip()
                == normalized_campaign_id
                or str(payload.get("client_id") or "").strip() == normalized_campaign_id
                or str(payload.get("tenant_id") or "").strip() == normalized_campaign_id
            ),
        )
        tier1_ids = [str(point.id) for point in tier1_points]
        parent_file_ids: List[str] = []
        for point in tier1_points:
            payload = point.payload or {}
            if payload.get("record_type") != "chunk":
                parent_file_ids.append(str(point.id))
            url = payload.get("url")
            if url:
                file_urls.append(url)
        await _delete_points(settings.QDRANT_COLLECTION_TIER_1, tier1_ids)

        # Remove chunk vectors linked to promoted parent files.
        for parent_file_id in set(parent_file_ids):
            deleted_chunks = await self._delete_chunk_points_for_parent(
                collection_name=settings.QDRANT_COLLECTION_TIER_1,
                parent_file_id=parent_file_id,
            )
            vector_count_deleted += int(deleted_chunks or 0)

        return {
            "vector_count_deleted": vector_count_deleted,
            "file_urls": list(dict.fromkeys(file_urls)),
        }

    @staticmethod
    def _resolve_campaign_id_from_payload(payload: Dict[str, Any]) -> str:
        """Resolve campaign identity from Qdrant payload fields."""
        for key in ("client_id", "source_campaign_id", "tenant_id"):
            value = str(payload.get(key) or "").strip()
            if value and value.lower() != "global":
                return value
        return ""

    @staticmethod
    def _extract_collection_points_count(collection_info: Any) -> int:
        """Best-effort count extraction across Qdrant response versions."""
        for key in ("points_count", "vectors_count"):
            value = getattr(collection_info, key, None)
            if value is not None:
                try:
                    return int(value)
                except Exception:
                    pass
        if hasattr(collection_info, "model_dump"):
            dumped = collection_info.model_dump() or {}
            for key in ("points_count", "vectors_count"):
                value = dumped.get(key)
                if value is not None:
                    try:
                        return int(value)
                    except Exception:
                        pass
        return 0

    async def get_qdrant_usage_metrics(self) -> Dict[str, Any]:
        """Aggregate Qdrant usage for Mission Control reporting."""
        settings = get_settings()
        collections = [settings.QDRANT_COLLECTION_TIER_1, settings.QDRANT_COLLECTION_TIER_2]
        vector_bytes = max(1, int(settings.EMBEDDING_DIM)) * 4

        qdrant_cost_per_gb_month_usd = float(settings.QDRANT_COST_PER_GB_MONTH_USD or 0.25)
        cache_key = (
            f"{collections[0]}|{collections[1]}|{vector_bytes}|"
            f"{qdrant_cost_per_gb_month_usd}"
        )
        now = datetime.now(timezone.utc)
        if (
            self._usage_metrics_cache_key == cache_key
            and self._usage_metrics_cache is not None
            and self._usage_metrics_cache_expires_at is not None
            and self._usage_metrics_cache_expires_at > now
        ):
            return copy.deepcopy(self._usage_metrics_cache)

        campaign_vector_counts: Dict[str, int] = {}
        total_vectors = 0

        for collection_name in collections:
            try:
                info = await self._client.get_collection(collection_name=collection_name)
                total_vectors += self._extract_collection_points_count(info)
            except Exception:
                logger.exception("qdrant_usage_collection_info_failed collection=%s", collection_name)

            offset = None
            while True:
                try:
                    points, next_offset = await self._client.scroll(
                        collection_name=collection_name,
                        limit=500,
                        offset=offset,
                        with_payload=True,
                        with_vectors=False,
                    )
                except Exception:
                    logger.exception("qdrant_usage_collection_scan_failed collection=%s", collection_name)
                    break

                if not points:
                    break

                for point in points:
                    payload = point.payload or {}
                    campaign_id = self._resolve_campaign_id_from_payload(payload)
                    if not campaign_id:
                        continue
                    campaign_vector_counts[campaign_id] = campaign_vector_counts.get(campaign_id, 0) + 1

                offset = next_offset
                if offset is None:
                    break

        if total_vectors <= 0:
            total_vectors = int(sum(campaign_vector_counts.values()))

        campaigns = []
        for campaign_id, vectors in campaign_vector_counts.items():
            estimated_size_mb = (float(vectors) * float(vector_bytes)) / float(1024 * 1024)
            campaigns.append(
                {
                    "campaign_id": campaign_id,
                    "vectors": int(vectors),
                    "estimated_size_mb": round(estimated_size_mb, 2),
                }
            )
        campaigns.sort(key=lambda row: int(row.get("vectors") or 0), reverse=True)

        total_estimated_size_mb = (float(total_vectors) * float(vector_bytes)) / float(1024 * 1024)
        estimated_monthly_cost = (total_estimated_size_mb / 1024.0) * qdrant_cost_per_gb_month_usd

        result = {
            "total_vectors": int(total_vectors),
            "campaigns": campaigns,
            "total_estimated_size_mb": round(float(total_estimated_size_mb), 2),
            "estimated_monthly_cost": round(float(estimated_monthly_cost), 2),
        }
        self._usage_metrics_cache_key = cache_key
        self._usage_metrics_cache_expires_at = datetime.now(timezone.utc) + timedelta(
            seconds=self._USAGE_METRICS_CACHE_TTL_SECONDS
        )
        self._usage_metrics_cache = copy.deepcopy(result)
        return result

    async def delete_tenant_data(self, tenant_id: str) -> List[str]:
        """Delete all data for a tenant from Qdrant and return list of file URLs.
        
        This is used for campaign termination - permanently removes all vectors
        associated with a tenant from Tier 2 collection.
        
        Args:
            tenant_id: The tenant/client ID to delete data for.
            
        Returns:
            List of file URLs from the deleted points' payloads.
            
        Raises:
            ValueError: If tenant_id is "global" (protected) or empty.
        """
        # Security: Prevent deleting global tenant
        if tenant_id == "global" or not tenant_id or not tenant_id.strip():
            raise ValueError(f"Cannot delete data for tenant_id '{tenant_id}'. Protected tenant.")
        
        settings = get_settings()
        
        # Create filter for tenant_id
        tenant_filter = Filter(
            must=[
                FieldCondition(
                    key="client_id",
                    match=MatchValue(value=tenant_id),
                )
            ]
        )
        
        # Collect all file URLs before deletion
        file_urls: List[str] = []
        offset = None
        
        # Scroll through all points for this tenant
        while True:
            scroll_result = await self._client.scroll(
                collection_name=settings.QDRANT_COLLECTION_TIER_2,
                scroll_filter=tenant_filter,
                limit=100,  # Process in batches
                offset=offset,
                with_payload=True,
                with_vectors=False,
            )
            
            points = scroll_result[0]
            if not points:
                break
            
            # Extract file URLs from payloads
            for point in points:
                payload = point.payload or {}
                url = payload.get("url")
                if url:
                    file_urls.append(url)
            
            # Get next offset
            offset = scroll_result[1]
            if offset is None:
                break
        
        # Delete all points for this tenant
        if file_urls:
            # Get all point IDs to delete
            point_ids: List[str] = []
            offset = None
            
            while True:
                scroll_result = await self._client.scroll(
                    collection_name=settings.QDRANT_COLLECTION_TIER_2,
                    scroll_filter=tenant_filter,
                    limit=100,
                    offset=offset,
                    with_payload=False,
                    with_vectors=False,
                )
                
                points = scroll_result[0]
                if not points:
                    break
                
                point_ids.extend([str(point.id) for point in points])
                
                offset = scroll_result[1]
                if offset is None:
                    break
            
            # Delete all points
            if point_ids:
                await self._client.delete(
                    collection_name=settings.QDRANT_COLLECTION_TIER_2,
                    points_selector=models.PointIdsList(
                        points=point_ids
                    )
                )
        
        return file_urls


# Global service instance that can be imported by routers.
vector_service = VectorService()



