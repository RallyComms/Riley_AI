import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from neo4j import AsyncGraphDatabase

from app.core.config import get_settings


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

    @property
    def driver(self) -> Any:
        """Return the Neo4j driver instance."""
        return self._driver

    async def close(self) -> None:
        """Close the Neo4j driver connection."""
        if self._driver:
            await self._driver.close()

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
                created_at: datetime()
            })
            CREATE (u)-[:MEMBER_OF {
                role: "Lead",
                added_at: datetime()
            }]->(c)
            RETURN c.id as id, c.name as name, c.description as description
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
                "role": "Lead"
            }

    async def get_user_campaigns(self, user_id: str) -> List[Dict[str, Any]]:
        """Get all campaigns that a user is a member of.
        
        Args:
            user_id: ID of the user
            
        Returns:
            List of campaign dictionaries: [{id, name, description, role}, ...]
        """
        async with self._driver.session() as session:
            query = """
            MATCH (u:User {id: $user_id})-[r:MEMBER_OF]->(c:Campaign)
            RETURN c.id as id, c.name as name, c.description as description, r.role as role
            ORDER BY c.created_at DESC
            """
            
            result = await session.run(query, user_id=user_id)
            
            campaigns = []
            async for record in result:
                campaigns.append({
                    "id": record["id"],
                    "name": record["name"],
                    "description": record.get("description"),
                    "role": record["role"]
                })
            
            return campaigns

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
        self, campaign_id: str, user: Dict[str, Any], content: str
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
        
        user_id = user.get("id")
        if not user_id:
            raise ValueError("User ID is required")
        
        message_id = str(uuid.uuid4())
        
        async with self._driver.session() as session:
            query = """
            MATCH (c:Campaign {id: $campaign_id})
            MERGE (u:User {id: $user_id})
            CREATE (m:TeamMessage {
                id: $message_id,
                content: $content,
                timestamp: datetime()
            })
            CREATE (u)-[:SENT]->(m)-[:POSTED_IN]->(c)
            RETURN m.id as id, m.content as content, toString(m.timestamp) as timestamp, u.id as author_id
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
            
            return {
                "id": record["id"],
                "content": record["content"],
                "timestamp": record["timestamp"],
                "author_id": record["author_id"]
            }

    async def get_team_messages(
        self, campaign_id: str, limit: int = 50, since: Optional[str] = None
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
                RETURN m.id as id, m.content as content, toString(m.timestamp) as timestamp, 
                       u.id as author_id
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
                RETURN m.id as id, m.content as content, toString(m.timestamp) as timestamp, 
                       u.id as author_id
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
                messages.append({
                    "id": record["id"],
                    "content": record["content"],
                    "timestamp": record["timestamp"],
                    "author_id": record.get("author_id", "unknown")
                })
            
            # Reverse to get chronological order (oldest -> newest)
            messages.reverse()
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

    async def add_campaign_member(
        self, campaign_id: str, target_user_id: str, target_email: str, role: str
    ) -> None:
        """Add a user as a member to a campaign.
        
        Creates or updates the User node and creates/updates the MEMBER_OF relationship.
        
        Args:
            campaign_id: ID of the campaign
            target_user_id: ID of the user to add
            target_email: Email of the user to add
            role: Role to assign ("Lead" or "Member")
            
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
            
            # Add or update member
            query = """
            MERGE (u:User {id: $target_user_id})
            SET u.email = $target_email
            MATCH (c:Campaign {id: $campaign_id})
            MERGE (u)-[r:MEMBER_OF]->(c)
            SET r.role = $role, r.added_at = datetime()
            """
            
            await session.run(
                query,
                campaign_id=campaign_id,
                target_user_id=target_user_id,
                target_email=target_email,
                role=role
            )

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

