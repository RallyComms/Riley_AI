"""Verification Script: Audit the quality of AI-powered Persona tagging.

This script verifies that the intelligent tagger correctly kept "Signal" (Strategic Personas)
and rejected "Noise" (Files, Addresses, Tools).
"""

import asyncio
import re
import sys
from pathlib import Path

# Add parent directory to path to allow imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from neo4j import AsyncGraphDatabase

from app.core.config import get_settings


async def verify_tags():
    """Verify the quality of Persona tags in Neo4j."""
    settings = get_settings()
    
    # Connect to Neo4j
    driver = AsyncGraphDatabase.driver(
        settings.NEO4J_URI,
        auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD),
    )
    
    try:
        print("=" * 60)
        print("PERSONA TAG VERIFICATION REPORT")
        print("=" * 60)
        print(f"Connected to Neo4j at {settings.NEO4J_URI}\n")
        
        async with driver.session() as session:
            # Check 1: The Media Test
            print("CHECK 1: The Media Test")
            print("-" * 60)
            print("Verifying Media personas are abstract concepts, not addresses/files...\n")
            
            media_query = """
            MATCH (n:Persona)
            WHERE n.name CONTAINS 'Media'
            RETURN n.name
            ORDER BY n.name
            """
            
            result = await session.run(media_query)
            media_nodes = []
            async for record in result:
                name = record.get("n.name")
                if name:
                    media_nodes.append(name)
            
            if media_nodes:
                print(f"Found {len(media_nodes)} Media-related Persona node(s):")
                for idx, name in enumerate(media_nodes, 1):
                    # Flag potential issues
                    is_file = name.endswith(('.pdf', '.docx', '.xlsx', '.txt', '.doc', '.xls'))
                    is_address = bool(re.match(r'^\d+', name)) if name else False
                    status = ""
                    if is_file:
                        status = " ⚠️  FILE"
                    elif is_address:
                        status = " ⚠️  ADDRESS"
                    else:
                        status = " ✓"
                    print(f"  {idx:2d}. {name}{status}")
            else:
                print("  No Media-related Persona nodes found.")
            
            print()
            
            # Check 2: The File Test
            print("CHECK 2: The File Test")
            print("-" * 60)
            print("Checking for Persona nodes with file extensions...\n")
            
            file_query = """
            MATCH (n:Persona)
            WHERE n.name ENDS WITH '.pdf' OR 
                  n.name ENDS WITH '.docx' OR 
                  n.name ENDS WITH '.xlsx' OR
                  n.name ENDS WITH '.pptx' OR
                  n.name ENDS WITH '.txt' OR
                  n.name ENDS WITH '.doc' OR
                  n.name ENDS WITH '.xls' OR
                  n.name ENDS WITH '.csv'
            RETURN count(n) as count
            """
            
            result = await session.run(file_query)
            record = await result.single()
            file_count = record["count"] if record else 0
            
            if file_count == 0:
                print(f"✓ PASS: {file_count} file nodes incorrectly tagged as Persona.")
            else:
                print(f"✗ FAIL: {file_count} file node(s) incorrectly tagged as Persona.")
                # Get sample of file nodes
                file_sample_query = """
                MATCH (n:Persona)
                WHERE n.name ENDS WITH '.pdf' OR 
                      n.name ENDS WITH '.docx' OR 
                      n.name ENDS WITH '.xlsx' OR
                      n.name ENDS WITH '.pptx' OR
                      n.name ENDS WITH '.txt' OR
                      n.name ENDS WITH '.doc' OR
                      n.name ENDS WITH '.xls' OR
                      n.name ENDS WITH '.csv'
                RETURN n.name
                LIMIT 10
                """
                result = await session.run(file_sample_query)
                print("\n  Sample of incorrectly tagged files:")
                async for record in result:
                    name = record.get("n.name")
                    if name:
                        print(f"    - {name}")
            
            print()
            
            # Check 3: The Address Test
            print("CHECK 3: The Address Test")
            print("-" * 60)
            print("Checking for Persona nodes starting with numbers (addresses/dates)...\n")
            
            address_query = """
            MATCH (n:Persona)
            WHERE n.name =~ '^[0-9].*'
            RETURN n.name
            ORDER BY n.name
            """
            
            result = await session.run(address_query)
            address_nodes = []
            async for record in result:
                name = record.get("n.name")
                if name:
                    address_nodes.append(name)
            
            address_count = len(address_nodes)
            if address_count == 0:
                print(f"✓ PASS: {address_count} address/date nodes incorrectly tagged as Persona.")
            else:
                print(f"⚠ WARNING: {address_count} address/date node(s) incorrectly tagged as Persona.")
                print("\n  Incorrectly tagged addresses/dates:")
                for idx, name in enumerate(address_nodes[:10], 1):  # Show first 10
                    print(f"    {idx}. {name}")
                if address_count > 10:
                    print(f"    ... and {address_count - 10} more")
            
            print()
            
            # Check 4: The Sample
            print("CHECK 4: Random Sample (Quality Spot-Check)")
            print("-" * 60)
            print("20 random Persona nodes for manual review:\n")
            
            sample_query = """
            MATCH (n:Persona)
            WITH n, rand() as r
            ORDER BY r
            LIMIT 20
            RETURN n.name as name
            ORDER BY name
            """
            
            result = await session.run(sample_query)
            sample_nodes = []
            async for record in result:
                name = record.get("name")
                if name:
                    sample_nodes.append(name)
            
            if sample_nodes:
                for idx, name in enumerate(sample_nodes, 1):
                    # Quick quality indicators
                    is_file = name.endswith(('.pdf', '.docx', '.xlsx', '.txt', '.doc', '.xls', '.csv', '.pptx'))
                    is_address = bool(re.match(r'^\d+', name)) if name else False
                    is_tool = any(term in name.lower() for term in ['toolkit', 'plan', 'tracker', 'brief', 'training'])
                    
                    status = ""
                    if is_file:
                        status = " ⚠️  FILE"
                    elif is_address:
                        status = " ⚠️  ADDRESS"
                    elif is_tool:
                        status = " ⚠️  TOOL"
                    else:
                        status = " ✓"
                    
                    print(f"  {idx:2d}. {name}{status}")
            else:
                print("  No Persona nodes found.")
            
            print()
            
            # Summary Statistics
            print("SUMMARY STATISTICS")
            print("-" * 60)
            
            total_query = """
            MATCH (n:Persona)
            RETURN count(n) as total
            """
            
            result = await session.run(total_query)
            record = await result.single()
            total_personas = record["total"] if record else 0
            
            print(f"Total Persona nodes: {total_personas}")
            print(f"File pollution: {file_count} ({'✓ PASS' if file_count == 0 else '✗ FAIL'})")
            print(f"Address pollution: {address_count} ({'✓ PASS' if address_count == 0 else '⚠ WARNING'})")
            print(f"Media nodes: {len(media_nodes)}")
            
            if total_personas > 0:
                pollution_rate = ((file_count + address_count) / total_personas) * 100
                print(f"\nOverall pollution rate: {pollution_rate:.1f}%")
                if pollution_rate == 0:
                    print("✓ EXCELLENT: No pollution detected!")
                elif pollution_rate < 1:
                    print("✓ GOOD: Minimal pollution detected.")
                elif pollution_rate < 5:
                    print("⚠ ACCEPTABLE: Some pollution detected.")
                else:
                    print("✗ POOR: Significant pollution detected.")
            
            print()
            print("=" * 60)
            print("VERIFICATION COMPLETE")
            print("=" * 60)
        
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        raise
    finally:
        await driver.close()
        print("\nConnection closed.")


if __name__ == "__main__":
    asyncio.run(verify_tags())
