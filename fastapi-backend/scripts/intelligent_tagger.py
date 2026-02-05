"""Intelligent Tagger: Semantic graph tagging for Persona nodes.

This script uses Gemini AI to intelligently tag nodes as :Persona only if they
represent Strategic Audiences, filtering out noise like files, addresses, and tools.
"""

import asyncio
import json
import os
import re
import sys
from pathlib import Path
from typing import List, Set

import google.generativeai as genai
from neo4j import AsyncGraphDatabase
from tqdm import tqdm

# Add parent directory to path to allow imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.core.config import get_settings

# Try to import RALLY_PERSONAS, fallback to hardcoded list if import fails
try:
    from app.core.personas import RALLY_PERSONAS
except ImportError:
    print("Warning: Could not import RALLY_PERSONAS from app.core.personas. Using fallback list.")
    RALLY_PERSONAS = [
        "Unbothered Ursula & Ulysses",
        "Empathetic Eric",
        "Active Ava",
        "Skeptical Sam",
        "Passive Patty",
        "Supporter Spencer",
        "Advocate Angie",
        "Legislator Brian (State Level)",
        "Secretary of Education Michelle (State Level)",
        "Mayor Michael (Local Level)",
        "Superintendent James (District Level)",
        "District Administrator Jennifer (District Level)",
        "Educator Christina (Educator)",
        "Foundation CEO Sarah (Philanthropoid)",
        "Program Director Emily (Philanthropoid)",
        "Philanthropist Chuck (Philantropist)",
        "National Housing Advocate Monica",
        "National Education Advocate Chris",
        "Federal Policymaker David",
        "Governor Charlie",
        "Education Personnel",
        "Decision Makers and Policy Makers",
        "Media",
        "Parents",
        "Gen Z",
        "Thoughtleaders and Advocates",
        "Policymaker Jeff (Federal Level)",
        "Legislator Margaret (State Level)",
        "District Administrator Daniel (District Level)",
        "Philanthropist Chuck Harvey",
        "Equity Forward Salesperson Benjamin Pattton",
        "Concerned Californians",
        "Mental Health Specialist",
        "The Field",
        "The Media",
        "Unbothered",
        "Empathetic",
        "Advocates",
        "Unaware + Unbothered",
    ]


def extract_json_from_response(text: str) -> List[str]:
    """Extract JSON array from Gemini response, handling markdown code blocks."""
    # Try to find JSON in markdown code blocks
    json_match = re.search(r'```(?:json)?\s*(\[.*?\])', text, re.DOTALL)
    if json_match:
        json_str = json_match.group(1)
    else:
        # Try to find JSON array directly
        json_match = re.search(r'(\[.*?\])', text, re.DOTALL)
        if json_match:
            json_str = json_match.group(1)
        else:
            # Fallback: try to parse the whole text
            json_str = text.strip()
    
    try:
        parsed = json.loads(json_str)
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
        return []
    except json.JSONDecodeError:
        # If JSON parsing fails, try to extract quoted strings
        quoted_strings = re.findall(r'"([^"]+)"', text)
        return quoted_strings if quoted_strings else []


async def collect_candidates(driver, personas: List[str]) -> Set[str]:
    """Step A: Collect candidate node names using fuzzy matching."""
    print("Step A: Collecting candidate nodes...")
    candidates: Set[str] = set()
    
    async with driver.session() as session:
        for persona_name in tqdm(personas, desc="Searching personas"):
            query = """
            MATCH (n)
            WHERE toLower(n.name) CONTAINS toLower($persona_name)
            RETURN DISTINCT n.name as Name
            """
            
            result = await session.run(query, persona_name=persona_name)
            async for record in result:
                name = record.get("Name")
                if name:
                    candidates.add(name)
    
    print(f"Found {len(candidates)} unique candidate nodes.\n")
    return candidates


async def validate_with_gemini(candidates: List[str], api_key: str) -> List[str]:
    """Step B: Use Gemini to validate which candidates are actual personas."""
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel("gemini-2.5-flash-lite")
    
    prompt = """You are a Data Cleanliness Expert for a Political Campaign graph.
Review this list of node names. Identify which ones are **Strategic Personas / Target Audiences** (groups of people) and which are **Noise** (Files, Addresses, Events, Tools).

Rules:
- KEEP: 'Passive Patty', 'The Media', 'Gen Z', 'Parents'.
- DISCARD: 'Policy.pdf', '10 La Media Dr', 'Social Media Training', 'Meeting Notes'.

Return ONLY a JSON list of strings for the names to KEEP.

Node names to review:
""" + "\n".join(f"- {name}" for name in candidates)
    
    try:
        response = model.generate_content(prompt)
        response_text = response.text if hasattr(response, "text") else str(response)
        validated = extract_json_from_response(response_text)
        return validated
    except Exception as e:
        print(f"Error calling Gemini: {e}")
        return []


async def tag_validated_nodes(driver, validated_names: List[str]) -> int:
    """Step C: Tag validated nodes with :Persona label."""
    print("\nStep C: Tagging validated nodes...")
    total_tagged = 0
    
    async with driver.session() as session:
        for name in tqdm(validated_names, desc="Tagging nodes"):
            query = """
            MATCH (n)
            WHERE n.name = $approved_name
            SET n:Persona
            RETURN count(n) as count
            """
            
            result = await session.run(query, approved_name=name)
            record = await result.single()
            count = record["count"] if record else 0
            total_tagged += count
    
    return total_tagged


async def intelligent_tagger():
    """Main function: Run the intelligent tagging pipeline."""
    settings = get_settings()
    
    # Validate API key
    api_key = settings.GOOGLE_API_KEY or os.getenv("GOOGLE_API_KEY")
    if not api_key:
        raise RuntimeError("GOOGLE_API_KEY is not configured. Set it in .env or environment variables.")
    
    # Connect to Neo4j
    driver = AsyncGraphDatabase.driver(
        settings.NEO4J_URI,
        auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD),
    )
    
    try:
        print("=" * 60)
        print("INTELLIGENT PERSONA TAGGER")
        print("=" * 60)
        print(f"Connected to Neo4j at {settings.NEO4J_URI}")
        print(f"Using Gemini API for validation\n")
        
        # Step A: Collect candidates
        candidates = await collect_candidates(driver, RALLY_PERSONAS)
        
        if not candidates:
            print("No candidates found. Exiting.")
            return
        
        # Step B: Batch validation with Gemini
        print("Step B: Validating candidates with Gemini AI...")
        candidates_list = list(candidates)
        batch_size = 20
        all_validated: List[str] = []
        
        # Process in batches
        for i in tqdm(range(0, len(candidates_list), batch_size), desc="Processing batches"):
            batch = candidates_list[i:i + batch_size]
            validated_batch = await validate_with_gemini(batch, api_key)
            all_validated.extend(validated_batch)
        
        print(f"\nGemini validated {len(all_validated)} out of {len(candidates)} candidates.")
        
        # Step C: Tag validated nodes
        total_tagged = await tag_validated_nodes(driver, all_validated)
        
        # Summary
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"Analyzed {len(candidates)} candidates.")
        print(f"Tagged {total_tagged} validated personas.")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        raise
    finally:
        await driver.close()
        print("\nConnection closed.")


if __name__ == "__main__":
    asyncio.run(intelligent_tagger())
