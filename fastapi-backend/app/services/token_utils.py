import re


def estimate_tokens(text: str) -> int:
    """Robust token estimate using word/punctuation groups."""
    if not text:
        return 0
    pieces = re.findall(r"\w+|[^\w\s]", str(text))
    # Typical English ratio for this split to model tokens.
    return max(1, int(len(pieces) * 0.75))


def truncate_text_to_token_budget(text: str, max_tokens: int) -> str:
    """Deterministically truncate text to fit estimated token budget."""
    if max_tokens <= 0:
        return ""
    normalized = (text or "").strip()
    if not normalized:
        return ""

    if estimate_tokens(normalized) <= max_tokens:
        return normalized

    words = normalized.split()
    kept: list[str] = []
    for word in words:
        candidate = " ".join([*kept, word]) if kept else word
        if estimate_tokens(candidate) > max_tokens:
            break
        kept.append(word)

    if not kept:
        return ""
    clipped = " ".join(kept).rstrip()
    return f"{clipped}…"
