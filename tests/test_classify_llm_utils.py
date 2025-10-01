from __future__ import annotations

import pipeline.classify_llm as clf


def test_clean_model_json_variants():
    # plain json
    d = clf.clean_model_json('{"doc_type":"press_release","confidence":0.9}')
    assert d.get("doc_type") == "press_release"

    # fenced code block
    s = '```json\n{"doc_type":"pitch_email","confidence":0.7}\n```'
    d2 = clf.clean_model_json(s)
    assert d2.get("doc_type") == "pitch_email"

    # noisy text with JSON inside
    s2 = 'some pre\n{ "doc_type": "bio_profile", "confidence": 0.5 }\npost'
    d3 = clf.clean_model_json(s2)
    assert d3.get("doc_type") == "bio_profile"


def test_build_sample_text_slides_xlsx():
    txt = {"type": "text", "text": "A" * 3000 + "END"}
    s = clf.build_sample("text", txt, max_chars=1000)
    assert "END" in s and len(s) <= 1000

    slides = {"type": "slides", "slides": {1: {"title": "T", "body": "B"}}}
    ss = clf.build_sample("slides", slides, max_chars=200)
    assert "[Slide 1]" in ss

    schema = {"type": "xlsx_schema", "schema": {"cols": ["email", "reporter"]}}
    sx = clf.build_sample("xlsx_schema", schema, max_chars=50)
    assert "email" in sx


def test_path_context_tokens():
    s = clf.path_context_tokens("/a/b/c/d/e/f.txt")
    assert s.endswith("c / d / e / f.txt")


def test_build_label_hints():
    labels = ["press_release", "media_list"]
    synonyms = {"press_release": ["news release", "press announcement"]}
    patterns = {"media_list": {"must_any": ["reporter", "outlet", "beat"]}}
    s = clf.build_label_hints(labels, synonyms, patterns)
    assert "press_release:" in s or "news release" in s
    assert "media_list:" in s or "reporter" in s


def test_coerce_to_allowed():
    allowed = ["press_release", "media_list"]
    data = {"doc_type": "unknown", "confidence": 0.9, "evidence": ["x"]}
    out = clf.coerce_to_allowed(data, allowed, unknown_label="unmapped_other")
    assert out["doc_type"] == "unmapped_other"
    assert out["confidence"] <= 0.6  # clamped


def test_soft_override_press_release_and_pitch_email():
    allowed = [
        "press_release",
        "pitch_email",
        "media_response",
        "interview_guide",
        "interview_question_bank",
        "editorial_calendar",
        "speaking_tracker",
        "competitive_analysis",
        "bio_profile",
        "workback_plan",
        "timeline",
        "deck|training_materials",
        "deck|analysis_report",
        "coverage_tracker",
        "story_bank",
        "press_statement",
        "work_plan|strategy_memo",
        "messaging_one_pager",
    ]
    synonyms = {}
    patterns = {}

    # Start with a generic label, ensure press release markers override + lock
    sample = "For immediate release\n###\nMedia contact: a@b.com"
    result = {"doc_type": "messaging_one_pager", "confidence": 0.4, "evidence": []}
    out = clf.soft_override(
        result, "foo/release_final.pdf", "text", sample, synonyms, patterns, allowed
    )
    assert out["doc_type"] == "press_release"
    assert out.get("confidence", 0) >= 0.9

    # Pitch override when filename mentions 'pitch' + boilerplate placeholders
    sample2 = "For immediate release\ncontact name\n###\ncontact email"
    result2 = {"doc_type": "press_release", "confidence": 0.95, "evidence": []}
    out2 = clf.soft_override(
        result2,
        "bar/important_pitch.docx",
        "text",
        sample2,
        synonyms,
        patterns,
        allowed,
    )
    assert out2["doc_type"] == "pitch_email"
    assert out2["confidence"] <= 0.6  # lowered for triage


def test_enforce_extract_constraints_xlsx_slides():
    hay = "outlet reporter email beat"
    data = {"doc_type": "press_release", "confidence": 0.5}
    out = clf.enforce_extract_constraints(
        data, "xlsx_schema", hay, "media list template.xlsx"
    )
    assert out["doc_type"] in {
        "media_list",
        "editorial_calendar",
        "coverage_tracker",
        "speaking_tracker",
        "competitive_analysis",
    }
    assert out["confidence"] <= 0.85  # ceiling applied for constraint correction

    hay2 = "analysis findings insights KPI"
    data2 = {"doc_type": "press_release", "confidence": 0.5}
    out2 = clf.enforce_extract_constraints(data2, "slides", hay2, "deck.pptx")
    assert out2["doc_type"] in {"deck|analysis_report", "deck|training_materials"}
