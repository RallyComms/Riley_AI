from datetime import datetime
from typing import Any, Dict, List, Sequence

from qdrant_client import AsyncQdrantClient
from qdrant_client.http import models
from qdrant_client.http.models import Filter, FieldCondition, MatchValue, PointStruct

from app.core.config import get_settings


class VectorService:
    """Service responsible for all Qdrant vector operations.

    All access is strictly tenant-scoped via Qdrant filters to preserve
    data sovereignty between clients.
    """

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
            return  # exists
        except Exception:
            # If get_collection fails (not found / connection), attempt create. Any real errors
            # will still bubble up from create_collection.
            pass

        await self._client.create_collection(
            collection_name=collection_name,
            vectors_config=models.VectorParams(
                size=settings.EMBEDDING_DIM,
                distance=distance,
            ),
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
        
        tenant_filter = Filter(must=filter_conditions)

        search_result = await self._client.search(
            collection_name=collection_name,
            query_vector=query_vector,
            query_filter=tenant_filter,  # SECURITY: Always use tenant filter
            limit=limit,
            with_payload=True,  # Ensure payload (including filename) is returned
        )

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
        
        search_result = await self._client.search(
            collection_name=collection_name,
            query_vector=query_vector,
            query_filter=filter,  # SECURITY: Always use explicit filter
            limit=limit,
            with_payload=True,  # Ensure payload (including filename) is returned
        )

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
                ]
            )
        else:
            # Standard tenant-scoped filter
            tenant_filter = Filter(
                must=[
                    FieldCondition(
                        key="client_id",
                        match=MatchValue(value=tenant_id),
                    )
                ]
            )

        # Use scroll to get all matching points
        scroll_result = await self._client.scroll(
            collection_name=collection_name,
            scroll_filter=tenant_filter,
            limit=limit,
            with_payload=True,
        )

        files = []
        for point in scroll_result[0]:  # scroll_result is a tuple: (points, next_page_offset)
            payload = point.payload or {}
            filename = payload.get("filename") or payload.get("name", "Unknown")
            file_type = payload.get("type") or payload.get("file_type", "")
            year = payload.get("year")

            files.append({
                "id": str(point.id),
                "filename": filename,
                "type": file_type,
                "year": year,
                "ai_enabled": payload.get("ai_enabled"),
                "ocr_enabled": payload.get("ocr_enabled"),
                "ocr_status": payload.get("ocr_status"),
                "ocr_confidence": payload.get("ocr_confidence"),
                "ocr_extracted_at": payload.get("ocr_extracted_at"),
                "preview_url": payload.get("preview_url"),
                "preview_type": payload.get("preview_type"),
                "preview_status": payload.get("preview_status"),
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
            ]
        )

        # Use scroll to get all matching points
        scroll_result = await self._client.scroll(
            collection_name=collection_name,
            scroll_filter=global_filter,
            limit=limit,
            with_payload=True,
            with_vectors=False,  # Don't need vectors for listing
        )

        files = []
        for point in scroll_result[0]:  # scroll_result is a tuple: (points, next_page_offset)
            payload = point.payload or {}
            filename = payload.get("filename") or payload.get("name", "Unknown")
            file_type = payload.get("type") or payload.get("file_type", "")
            
            files.append({
                "id": str(point.id),
                "filename": filename,
                "type": file_type,
                "url": payload.get("url", ""),
                "is_golden": payload.get("is_golden", False),
                "promoted_at": payload.get("promoted_at"),
                "client_id": payload.get("client_id"),  # Original campaign ID
                "ai_enabled": payload.get("ai_enabled"),
                "ocr_enabled": payload.get("ocr_enabled"),
                "ocr_status": payload.get("ocr_status"),
                "ocr_confidence": payload.get("ocr_confidence"),
                "ocr_extracted_at": payload.get("ocr_extracted_at"),
                "preview_url": payload.get("preview_url"),
                "preview_type": payload.get("preview_type"),
                "preview_status": payload.get("preview_status"),
            })

        return files

    async def promote_to_global(self, file_id: str, is_golden: bool) -> None:
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



