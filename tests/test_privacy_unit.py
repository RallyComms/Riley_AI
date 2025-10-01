from __future__ import annotations

from pipeline.privacy import detect_pii, redact_text, decide_index_mode


def test_detect_pii_basic():
    text = "Contact me at abc.def@org.com or +1 (202) 555-0123, 123 Main Street"
    pii = detect_pii(text)
    assert pii["emails"] >= 1
    assert pii["phones"] >= 1
    assert pii["addresses"] >= 1


def test_redact_order_and_tokens():
    text = "Write to a@b.com call 202-555-0187 visit 77 Broadway Avenue"
    red = redact_text(text)
    assert "[EMAIL]" in red
    assert "a@b.com" not in red
    assert "[PHONE]" in red
    assert "202-555-0187" not in red
    assert "[ADDR]" in red
    assert "Broadway" not in red


def test_decide_index_mode_rules():
    # summary-only for spreadsheets
    mode, flags = decide_index_mode("story_bank", "xlsx_schema", "any")
    assert mode == "summary_only" and flags == "Contacts"

    # redaction when contacts present
    mode, flags = decide_index_mode(
        "messaging_one_pager", "text", "Media contact: joe@org.org"
    )
    assert mode == "redacted" and flags in {"MediaContacts", "Contacts"}

    # full for normal
    mode, flags = decide_index_mode("messaging_one_pager", "text", "General memo")
    assert mode == "full"
