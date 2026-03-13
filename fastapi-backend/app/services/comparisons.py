import json
import uuid
from collections import Counter
from datetime import datetime
from typing import Any, Dict, List, Optional

from qdrant_client.http.models import FieldCondition, Filter, MatchValue

from app.core.config import get_settings
from app.services.graph import GraphService
from app.services.qdrant import vector_service


def _safe_json_loads(raw: Any, default: Any) -> Any:
    if not isinstance(raw, str) or not raw.strip():
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def _normalize_list(value: Any, *, max_items: int = 20, max_len: int = 220) -> List[str]:
    if not isinstance(value, list):
        return []
    cleaned: List[str] = []
    for item in value:
        text = str(item or "").strip()
        if not text:
            continue
        cleaned.append(text[:max_len])
        if len(cleaned) >= max_items:
            break
    return cleaned


async def _load_campaign_docs(tenant_id: str) -> List[Dict[str, Any]]:
    settings = get_settings()
    collection_name = (
        settings.QDRANT_COLLECTION_TIER_1
        if tenant_id == "global"
        else settings.QDRANT_COLLECTION_TIER_2
    )
    if tenant_id == "global":
        root_filter = Filter(
            must=[FieldCondition(key="is_global", match=MatchValue(value=True))],
            must_not=[FieldCondition(key="record_type", match=MatchValue(value="chunk"))],
        )
    else:
        root_filter = Filter(
            must=[FieldCondition(key="client_id", match=MatchValue(value=tenant_id))],
            must_not=[FieldCondition(key="record_type", match=MatchValue(value="chunk"))],
        )

    points: List[Any] = []
    offset = None
    while True:
        try:
            scroll_result = await vector_service.client.scroll(
                collection_name=collection_name,
                scroll_filter=root_filter,
                limit=500,
                offset=offset,
                with_payload=True,
                with_vectors=False,
            )
            batch = scroll_result[0]
        except Exception:
            legacy_filter = Filter(must=root_filter.must or [], should=root_filter.should or [])
            scroll_result = await vector_service.client.scroll(
                collection_name=collection_name,
                scroll_filter=legacy_filter,
                limit=500,
                offset=offset,
                with_payload=True,
                with_vectors=False,
            )
            batch = [
                point for point in scroll_result[0]
                if (point.payload or {}).get("record_type") != "chunk"
            ]
        if not batch:
            break
        points.extend(batch)
        offset = scroll_result[1]
        if offset is None:
            break

    docs: List[Dict[str, Any]] = []
    for point in points:
        payload = (point.payload or {}).copy()
        payload["id"] = str(point.id)
        docs.append(payload)
    return docs


def _filter_docs(docs: List[Dict[str, Any]], filters: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not filters:
        return docs
    themes_any = {str(v).strip().lower() for v in filters.get("themes_any", []) if str(v).strip()}
    tones_any = {str(v).strip().lower().replace(" ", "_") for v in filters.get("tone_labels_any", []) if str(v).strip()}
    framings_any = {str(v).strip().lower().replace(" ", "_") for v in filters.get("framing_labels_any", []) if str(v).strip()}
    sentiments_any = {str(v).strip().lower().replace(" ", "_") for v in filters.get("sentiment_any", []) if str(v).strip()}
    name_contains = str(filters.get("filename_contains") or "").strip().lower()

    def _matches(doc: Dict[str, Any]) -> bool:
        if name_contains and name_contains not in str(doc.get("filename") or "").lower():
            return False
        if themes_any:
            doc_themes = {str(v).strip().lower() for v in _normalize_list(doc.get("key_themes"), max_items=40, max_len=120)}
            if not doc_themes.intersection(themes_any):
                return False
        if tones_any:
            doc_tones = {str(v).strip().lower().replace(" ", "_") for v in _normalize_list(doc.get("tone_labels"), max_items=30, max_len=120)}
            if not doc_tones.intersection(tones_any):
                return False
        if framings_any:
            doc_framings = {str(v).strip().lower().replace(" ", "_") for v in _normalize_list(doc.get("framing_labels"), max_items=30, max_len=120)}
            if not doc_framings.intersection(framings_any):
                return False
        if sentiments_any:
            sentiment = str(doc.get("sentiment_overall") or "").strip().lower().replace(" ", "_")
            if sentiment not in sentiments_any:
                return False
        return True

    return [doc for doc in docs if _matches(doc)]


def _pick_docs_by_ids_and_names(
    docs: List[Dict[str, Any]],
    *,
    document_ids: Optional[List[str]] = None,
    document_filenames: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    ids = {str(v).strip() for v in (document_ids or []) if str(v).strip()}
    names = {str(v).strip().lower() for v in (document_filenames or []) if str(v).strip()}
    if not ids and not names:
        return []
    selected: List[Dict[str, Any]] = []
    for doc in docs:
        doc_id = str(doc.get("id") or "")
        filename = str(doc.get("filename") or "").strip().lower()
        if (doc_id and doc_id in ids) or (filename and filename in names):
            selected.append(doc)
    return selected


def _dedupe_docs(docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: set[str] = set()
    out: List[Dict[str, Any]] = []
    for doc in docs:
        key = str(doc.get("id") or "")
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(doc)
    return out


def _top_labels(docs: List[Dict[str, Any]], field: str, *, max_items: int = 5) -> str:
    counter: Counter[str] = Counter()
    for doc in docs:
        for label in _normalize_list(doc.get(field), max_items=24, max_len=120):
            counter[label.lower()] += 1
    if not counter:
        return "No strong signal."
    return ", ".join(f"{label} ({count})" for label, count in counter.most_common(max_items))


def _top_items(docs: List[Dict[str, Any]], field: str, *, max_items: int = 4) -> str:
    counter: Counter[str] = Counter()
    for doc in docs:
        for value in _normalize_list(doc.get(field), max_items=16, max_len=220):
            counter[value] += 1
    if not counter:
        return "No strong signal."
    return " | ".join(item for item, _ in counter.most_common(max_items))


def _evidence_refs(docs: List[Dict[str, Any]], *, max_items: int = 6) -> List[str]:
    refs: List[str] = []
    for doc in docs:
        filename = str(doc.get("filename") or "unknown")
        items = _normalize_list(doc.get("major_claims_or_evidence"), max_items=4, max_len=240)
        if not items:
            summary = str(doc.get("doc_summary_short") or "").strip()
            if summary:
                refs.append(f"{filename}: {summary[:220]}")
        for item in items:
            refs.append(f"{filename}: {item}")
        if len(refs) >= max_items:
            break
    return refs[:max_items]


def _recommended_action(docs: List[Dict[str, Any]]) -> str:
    opportunities = _top_items(docs, "strategic_opportunities", max_items=3)
    risks = _top_items(docs, "persuasion_risks", max_items=3)
    if opportunities == "No strong signal." and risks == "No strong signal.":
        return "Request additional grounded evidence before committing to strategy."
    if risks == "No strong signal.":
        return f"Lean into these opportunities: {opportunities}"
    return f"Pursue opportunities while mitigating top risks. Opportunities: {opportunities}. Risks: {risks}."


def _group_contradictions(
    *,
    group_doc_ids: set[str],
    campaign_contradictions: List[Dict[str, Any]],
) -> str:
    matched: List[str] = []
    for item in campaign_contradictions:
        involved = {str(v) for v in item.get("involved_documents", [])}
        if len(group_doc_ids.intersection(involved)) >= 2:
            summary = str(item.get("contradiction_summary") or "").strip()
            if summary:
                matched.append(summary)
    if not matched:
        return "No high-confidence contradiction artifact in this selection."
    return " | ".join(matched[:3])


def _build_rows(
    groups: List[Dict[str, Any]],
    *,
    campaign_contradictions: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    row_specs = [
        ("core_thesis", "Core Thesis"),
        ("dominant_tone", "Dominant Tone"),
        ("framing", "Framing"),
        ("audience_implications", "Audience Implications"),
        ("message_risks", "Message Risks"),
        ("strategic_opportunities", "Strategic Opportunities"),
        ("contradictions_tensions", "Contradictions/Tensions"),
        ("recommended_action", "Recommended Action"),
    ]
    rows: List[Dict[str, Any]] = []
    for row_key, label in row_specs:
        cells: List[Dict[str, Any]] = []
        for group in groups:
            docs = group["docs"]
            doc_ids = {str(doc.get("id") or "") for doc in docs if str(doc.get("id") or "")}
            if row_key == "core_thesis":
                value = _top_items(docs, "doc_summary_short", max_items=2)
            elif row_key == "dominant_tone":
                value = _top_labels(docs, "tone_labels", max_items=5)
            elif row_key == "framing":
                value = _top_labels(docs, "framing_labels", max_items=5)
            elif row_key == "audience_implications":
                value = _top_items(docs, "audience_implications", max_items=4)
            elif row_key == "message_risks":
                value = _top_items(docs, "persuasion_risks", max_items=4)
            elif row_key == "strategic_opportunities":
                value = _top_items(docs, "strategic_opportunities", max_items=4)
            elif row_key == "contradictions_tensions":
                value = _group_contradictions(
                    group_doc_ids=doc_ids,
                    campaign_contradictions=campaign_contradictions,
                )
            else:
                value = _recommended_action(docs)
            cells.append(
                {
                    "column_key": group["key"],
                    "value": value,
                    "supporting_document_ids": sorted(doc_ids),
                    "supporting_evidence_refs": _evidence_refs(docs, max_items=6),
                }
            )
        rows.append({"row_key": row_key, "label": label, "cells": cells})
    return rows


async def generate_comparison_table(
    *,
    tenant_id: str,
    mode: str,
    groups_request: List[Dict[str, Any]],
    graph: GraphService,
) -> Dict[str, Any]:
    docs = await _load_campaign_docs(tenant_id)
    analyzed_docs = [
        doc for doc in docs
        if str(doc.get("ingestion_status") or "").lower() == "indexed"
        and str(doc.get("analysis_status") or "").lower() == "complete"
    ]
    if not analyzed_docs:
        raise RuntimeError("No analyzed indexed documents available for comparison")

    resolved_groups: List[Dict[str, Any]] = []
    for idx, group in enumerate(groups_request):
        label = str(group.get("label") or f"Group {idx + 1}")
        key = str(group.get("key") or f"group_{idx + 1}")
        picked = _pick_docs_by_ids_and_names(
            analyzed_docs,
            document_ids=group.get("document_ids") or [],
            document_filenames=group.get("document_filenames") or [],
        )
        filtered = _filter_docs(analyzed_docs, group.get("filters"))
        combined = _dedupe_docs([*picked, *filtered]) if (picked or group.get("filters")) else []
        if not combined and mode == "campaign_filtered_sets":
            combined = filtered
        if not combined and mode == "two_documents" and picked:
            combined = picked
        if not combined:
            continue
        resolved_groups.append({"key": key, "label": label, "docs": combined, "filters": group.get("filters") or {}})

    if mode == "two_documents" and len(resolved_groups) != 2:
        raise RuntimeError("two_documents mode requires exactly two resolvable groups")
    if mode in {"selected_sets", "campaign_filtered_sets"} and len(resolved_groups) < 1:
        raise RuntimeError("No document groups resolved for comparison")

    latest_snapshot = await graph.get_latest_riley_campaign_intelligence_snapshot(tenant_id=tenant_id)
    contradiction_tensions = _safe_json_loads(
        (latest_snapshot or {}).get("contradiction_tensions_json"),
        [],
    )

    columns: List[Dict[str, Any]] = []
    for group in resolved_groups:
        docs_for_column = group["docs"]
        columns.append(
            {
                "key": group["key"],
                "label": group["label"],
                "document_ids": [str(doc.get("id")) for doc in docs_for_column],
                "document_names": [str(doc.get("filename") or "unknown") for doc in docs_for_column],
                "doc_count": len(docs_for_column),
                "filters": group.get("filters") or {},
            }
        )

    rows = _build_rows(
        resolved_groups,
        campaign_contradictions=contradiction_tensions if isinstance(contradiction_tensions, list) else [],
    )
    return {
        "comparison_id": str(uuid.uuid4()),
        "tenant_id": tenant_id,
        "generated_at": datetime.now().isoformat(),
        "mode": mode,
        "columns": columns,
        "rows": rows,
        "grounding": {
            "documents_considered": len(analyzed_docs),
            "campaign_snapshot_version": int((latest_snapshot or {}).get("version") or 0),
            "campaign_snapshot_id": (latest_snapshot or {}).get("snapshot_id"),
        },
    }
