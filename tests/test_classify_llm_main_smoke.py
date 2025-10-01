from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
import pytest

import pipeline.classify_llm as clf


def _write_yaml(p: Path, obj: dict) -> None:
    import yaml

    p.write_text(yaml.safe_dump(obj), encoding="utf-8")


def test_classify_llm_main_no_network(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    camp = "CAMP"
    proc_root = tmp_path / "data" / "processed"
    proc_root.mkdir(parents=True, exist_ok=True)
    cdir = proc_root / camp
    cdir.mkdir(parents=True, exist_ok=True)

    # Minimal extracted.jsonl with one text and one xlsx_schema record
    extracted = cdir / "extracted.jsonl"
    rec1 = {
        "source_path": "/foo/bar/press_release_final.pdf",
        "sha256": "x1",
        "extract": {"type": "text", "text": "For immediate release..."},
    }  # noqa: E501
    rec2 = {
        "source_path": "/foo/baz/media.xlsx",
        "sha256": "x2",
        "extract": {
            "type": "xlsx_schema",
            "schema": {"cols": ["outlet", "email", "reporter"]},
        },
    }  # noqa: E501
    with extracted.open("w", encoding="utf-8") as f:
        f.write(json.dumps(rec1) + "\n")
        f.write(json.dumps(rec2) + "\n")

    # Minimal taxonomy + patterns
    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir(exist_ok=True)
    _write_yaml(
        cfg_dir / "taxonomy.yaml",
        {
            "labels": [
                "press_release",
                "media_list",
                "messaging_one_pager",
                "deck|training_materials",
            ],
            "synonyms": {"press_release": ["news release"]},
        },
    )
    _write_yaml(
        cfg_dir / "patterns.yaml",
        {
            "media_list": {"must_any": ["outlet", "reporter", "email"]},
        },
    )

    # Make classify_llm find our configs
    (tmp_path / "configs").mkdir(exist_ok=True)
    (tmp_path / "configs" / "taxonomy.yaml").write_text(
        (cfg_dir / "taxonomy.yaml").read_text(), encoding="utf-8"
    )
    (tmp_path / "configs" / "patterns.yaml").write_text(
        (cfg_dir / "patterns.yaml").read_text(), encoding="utf-8"
    )

    # Monkeypatch the working dir & env
    cwd = os.getcwd()
    os.chdir(tmp_path)
    monkeypatch.setenv("CLASSIFY_PROCESSED_ROOT", str(proc_root))
    monkeypatch.setenv("UNKNOWN_LABEL", "unmapped_other")

    # Monkeypatch classify_sample to avoid OpenAI
    def fake_classify_sample(
        client, model, labels, filename, source_path, extract_type, sample, label_hints
    ):
        if "release" in filename.lower():
            return {
                "doc_type": "press_release",
                "doc_subtype": "statement",
                "confidence": 0.92,
                "evidence": ["release"],
            }
        if extract_type == "xlsx_schema":
            return {
                "doc_type": "media_list",
                "doc_subtype": "xlsx",
                "confidence": 0.88,
                "evidence": ["outlet"],
            }
        return {
            "doc_type": "messaging_one_pager",
            "doc_subtype": "",
            "confidence": 0.5,
            "evidence": [],
        }

    monkeypatch.setattr(clf, "classify_sample", fake_classify_sample, raising=True)

    try:
        clf.main(str(tmp_path / camp), model="fake")
        out_csv = cdir / "classified.csv"
        triage_csv = cdir / "triage_needs_review.csv"
        assert out_csv.exists()
        df = pd.read_csv(out_csv, dtype=str)
        assert set(
            [
                "source_path",
                "sha256",
                "doc_type",
                "doc_subtype",
                "clf_confidence",
                "extract_type",
            ]
        ).issubset(
            df.columns
        )  # noqa: E501
        assert (df["doc_type"] == "press_release").any()
        assert (df["doc_type"] == "media_list").any()

        # triage may or may not exist depending on thresholds; just ensure the pipeline completes
        # If present, it should have expected columns
        if triage_csv.exists():
            df_t = pd.read_csv(triage_csv, dtype=str)
            assert "triage_reasons" in df_t.columns
    finally:
        os.chdir(cwd)
