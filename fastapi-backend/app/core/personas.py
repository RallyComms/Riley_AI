# Strategic Personas for Rally Campaigns
# These are Archetypes, not real people.

RALLY_PERSONAS = [
    "Conservative Catherine",
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
    "Policymaker Jeff (Federal Level)",
    "Legislator Margaret (State Level)",
    "District Administrator Daniel (District Level)",
    "Philanthropist Chuck Harvey", 
    "Equity Forward Salesperson Benjamin Pattton",
    "Concerned Californians",
    "Unbothered Ulysses",
    "Unbothered Ursula",
    "Empathetic Eric",
    "Active Ava"
]

def get_persona_context() -> str:
    """Format personas for System Prompt Injection."""
    return ", ".join(RALLY_PERSONAS)
