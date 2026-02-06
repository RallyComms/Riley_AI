"""Purge Script: Remove ALL Persona labels from Neo4j.

This script removes the :Persona label from all nodes in the database.
Use this to reset the data state before running the Intelligent Tagger.
"""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path to allow imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from neo4j import AsyncGraphDatabase

from app.core.config import get_settings


async def purge_persona_tags():
    """Remove all Persona labels from Neo4j nodes."""
    settings = get_settings()
    
    # Connect to Neo4j
    driver = AsyncGraphDatabase.driver(
        settings.NEO4J_URI,
        auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD),
    )
    
    try:
        print(f"Connected to Neo4j at {settings.NEO4J_URI}")
        print("Purging all Persona labels...\n")
        
        async with driver.session() as session:
            # Remove Persona label from all nodes
            purge_query = """
            MATCH (n:Persona)
            REMOVE n:Persona
            RETURN count(n) as RemovedCount
            """
            
            result = await session.run(purge_query)
            record = await result.single()
            removed_count = record["RemovedCount"] if record else 0
            
            print(f"✅ PURGE COMPLETE. Removed labels from {removed_count} nodes.")
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        raise
    finally:
        await driver.close()
        print("\nConnection closed.")


if __name__ == "__main__":
    asyncio.run(purge_persona_tags())
