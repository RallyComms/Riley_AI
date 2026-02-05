/** Persona Codex: Strategic archetypes for target audience recognition. */

export const RALLY_PERSONAS = [
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
];

/**
 * Detects if a message contains any persona names.
 * Returns the first matching persona found, or null if none.
 */
export function detectPersona(content: string): string | null {
  // Sort personas by length (longest first) to match more specific names first
  const sortedPersonas = [...RALLY_PERSONAS].sort((a, b) => b.length - a.length);
  
  for (const persona of sortedPersonas) {
    // Case-insensitive search for the persona name
    const regex = new RegExp(persona.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
    if (regex.test(content)) {
      return persona;
    }
  }
  
  return null;
}
