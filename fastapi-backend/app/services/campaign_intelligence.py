import asyncio
import json
import logging
import uuid
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from fastapi.concurrency import run_in_threadpool
from google.api_core.exceptions import AlreadyExists
from google.cloud import tasks_v2

from app.core.config import get_settings
from app.services.graph import GraphService
from app.services.qdrant import vector_service

logger = logging.getLogger(__name__)


def _normalize_list(value: Any, *, max_items: int = 20, max_len: int = 180) -> List[str]:
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


def _normalize_dist_label(value: Any) -> str:
    return str(value or "").strip().lower().replace(" ", "_")


async def _collect_document_artifacts(tenant_id: str) -> Tuple[List[Dict[str, Any]], int]:
    settings = get_settings()
    collection_name = settings.QDRANT_COLLECTION_TIER_2
    files = await vector_service.list_tenant_files(
        collection_name=collection_name,
        tenant_id=tenant_id,
        limit=5000,
    )
    indexed_total = 0
    analyzed: List[Dict[str, Any]] = []
    for payload in files:
        ingestion_status = str(payload.get("ingestion_status") or "").lower()
        if ingestion_status != "indexed":
            continue
        indexed_total += 1
        if str(payload.get("analysis_status") or "").lower() != "complete":
            continue
        analyzed.append(payload)
    return analyzed, indexed_total


def _build_theme_clusters(
    docs: List[Dict[str, Any]],
    *,
    top_n: int = 12,
) -> List[Dict[str, Any]]:
    counts: Counter[str] = Counter()
    docs_by_theme: defaultdict[str, List[str]] = defaultdict(list)
    for doc in docs:
        filename = str(doc.get("filename") or "unknown")
        for theme in _normalize_list(doc.get("key_themes"), max_items=24, max_len=120):
            key = theme.lower()
            counts[key] += 1
            if filename not in docs_by_theme[key]:
                docs_by_theme[key].append(filename)
    if not counts:
        return []
    total = max(1, len(docs))
    clusters: List[Dict[str, Any]] = []
    for theme, count in counts.most_common(top_n):
        clusters.append(
            {
                "theme": theme,
                "doc_count": count,
                "share_of_docs": round(count / total, 4),
                "sample_documents": docs_by_theme[theme][:5],
            }
        )
    return clusters


def _detect_contradictions(
    sentiment_counter: Counter[str],
    tone_counter: Counter[str],
    framing_counter: Counter[str],
) -> List[str]:
    contradictions: List[str] = []
    if sentiment_counter.get("positive", 0) > 0 and sentiment_counter.get("negative", 0) > 0:
        contradictions.append("Corpus contains both strongly positive and strongly negative sentiment signals.")
    if tone_counter.get("optimism", 0) > 0 and tone_counter.get("pessimism", 0) > 0:
        contradictions.append("Both optimistic and pessimistic rhetoric are present across documents.")
    if tone_counter.get("institutional", 0) > 0 and tone_counter.get("populist", 0) > 0:
        contradictions.append("Institutional and populist tones coexist, suggesting message-style tension.")
    if tone_counter.get("fear", 0) > 0 and tone_counter.get("trust", 0) > 0:
        contradictions.append("Fear-based and trust-building emotional frames appear simultaneously.")
    if tone_counter.get("coalition", 0) > 0 and tone_counter.get("opposition", 0) > 0:
        contradictions.append("Coalition-building and opposition-driven cues both appear in the corpus.")
    if framing_counter.get("stability", 0) > 0 and framing_counter.get("change", 0) > 0:
        contradictions.append("Competing stability vs change framing appears across source documents.")
    return contradictions[:10]


def _top_counter(counter: Counter[str], *, max_items: int = 12) -> Dict[str, int]:
    return {key: int(value) for key, value in counter.most_common(max_items)}


def _safe_slug(value: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in value.lower()).strip("_")[:80] or "contradiction"


def _doc_themes(doc: Dict[str, Any]) -> set[str]:
    return {str(theme).strip().lower() for theme in _normalize_list(doc.get("key_themes"), max_items=32, max_len=140)}


def _doc_tones(doc: Dict[str, Any]) -> set[str]:
    return {_normalize_dist_label(tone) for tone in _normalize_list(doc.get("tone_labels"), max_items=24, max_len=90)}


def _doc_framings(doc: Dict[str, Any]) -> set[str]:
    return {_normalize_dist_label(frame) for frame in _normalize_list(doc.get("framing_labels"), max_items=24, max_len=90)}


def _doc_evidence_refs(doc: Dict[str, Any], *, max_items: int = 3) -> List[str]:
    filename = str(doc.get("filename") or "unknown")
    refs: List[str] = []
    for evidence in _normalize_list(doc.get("major_claims_or_evidence"), max_items=max_items, max_len=220):
        refs.append(f"{filename}: {evidence}")
    if not refs:
        summary = str(doc.get("doc_summary_short") or "").strip()
        if summary:
            refs.append(f"{filename}: {summary[:220]}")
    return refs


def _doc_priority_bucket(doc: Dict[str, Any]) -> Optional[str]:
    text = " ".join(_normalize_list(doc.get("strategic_opportunities"), max_items=20, max_len=220)).lower()
    if not text:
        return None
    if any(token in text for token in ["persuad", "swing", "independent", "moderate"]):
        return "persuasion"
    if any(token in text for token in ["turnout", "base", "mobiliz", "activate"]):
        return "turnout"
    if any(token in text for token in ["fundrais", "money", "donor"]):
        return "fundraising"
    if any(token in text for token in ["governance", "policy", "delivery", "implementation"]):
        return "governance"
    return None


def _doc_audience_bucket(doc: Dict[str, Any]) -> Optional[str]:
    text = " ".join(_normalize_list(doc.get("audience_implications"), max_items=20, max_len=220)).lower()
    if not text:
        return None
    if any(token in text for token in ["base", "activist", "partisan", "core voter"]):
        return "base"
    if any(token in text for token in ["independent", "moderate", "swing", "persuadable"]):
        return "persuadable"
    if any(token in text for token in ["suburban", "urban", "rural", "youth", "senior", "latino", "black"]):
        return "segment_specific"
    return None


def _new_contradiction(
    *,
    kind: str,
    documents: List[str],
    topic: str,
    summary: str,
    why_it_matters: str,
    strategic_implication: str,
    evidence_refs: List[str],
) -> Dict[str, Any]:
    base = f"{kind}|{topic}|{'|'.join(sorted(set(documents)))}"
    contradiction_id = f"ct_{_safe_slug(kind)}_{uuid.uuid5(uuid.NAMESPACE_DNS, base).hex[:12]}"
    return {
        "contradiction_id": contradiction_id,
        "involved_documents": sorted(set(documents)),
        "topic/theme": topic,
        "contradiction_summary": summary,
        "why_it_matters": why_it_matters,
        "strategic_implication": strategic_implication,
        "supporting_evidence_refs": evidence_refs[:8],
    }


def _build_contradiction_tensions(docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if len(docs) < 2:
        return []

    docs_by_filename: Dict[str, Dict[str, Any]] = {
        str(doc.get("filename") or f"doc_{idx}"): doc
        for idx, doc in enumerate(docs)
    }
    filenames = list(docs_by_filename.keys())
    contradictions: List[Dict[str, Any]] = []

    # 1) Framing misalignment: institutional vs populist.
    institutional_docs = [f for f in filenames if "institutional" in _doc_tones(docs_by_filename[f])]
    populist_docs = [f for f in filenames if "populist" in _doc_tones(docs_by_filename[f])]
    if institutional_docs and populist_docs:
        involved = institutional_docs[:2] + populist_docs[:2]
        evidence = []
        for name in involved:
            evidence.extend(_doc_evidence_refs(docs_by_filename[name], max_items=2))
        contradictions.append(
            _new_contradiction(
                kind="framing_misalignment",
                documents=involved,
                topic="institutional_vs_populist_frame",
                summary="Documents split between institutional governance framing and populist anti-establishment framing.",
                why_it_matters="Mixed governing posture can dilute message discipline and confuse persuadable audiences.",
                strategic_implication="Choose a primary frame per channel and define explicit bridge language between governance and populist critique.",
                evidence_refs=evidence,
            )
        )

    # 2) Priority mismatch: persuasion vs turnout priorities.
    bucket_to_docs: defaultdict[str, List[str]] = defaultdict(list)
    for name in filenames:
        bucket = _doc_priority_bucket(docs_by_filename[name])
        if bucket:
            bucket_to_docs[bucket].append(name)
    if bucket_to_docs.get("persuasion") and bucket_to_docs.get("turnout"):
        involved = bucket_to_docs["persuasion"][:2] + bucket_to_docs["turnout"][:2]
        evidence = []
        for name in involved:
            evidence.extend(_doc_evidence_refs(docs_by_filename[name], max_items=2))
        contradictions.append(
            _new_contradiction(
                kind="priority_mismatch",
                documents=involved,
                topic="persuasion_vs_turnout_priority",
                summary="Corpus reflects competing strategic priorities between persuasion expansion and base turnout mobilization.",
                why_it_matters="Resource allocation and creative strategy may fragment if priority hierarchy is unclear.",
                strategic_implication="Set explicit campaign phase priorities and tie document guidance to phase-specific objectives.",
                evidence_refs=evidence,
            )
        )

    # 3) Audience mismatch: base-focused vs persuadable-focused guidance.
    aud_to_docs: defaultdict[str, List[str]] = defaultdict(list)
    for name in filenames:
        bucket = _doc_audience_bucket(docs_by_filename[name])
        if bucket:
            aud_to_docs[bucket].append(name)
    if aud_to_docs.get("base") and aud_to_docs.get("persuadable"):
        involved = aud_to_docs["base"][:2] + aud_to_docs["persuadable"][:2]
        evidence = []
        for name in involved:
            evidence.extend(_doc_evidence_refs(docs_by_filename[name], max_items=2))
        contradictions.append(
            _new_contradiction(
                kind="audience_mismatch",
                documents=involved,
                topic="base_vs_persuadable_targeting",
                summary="Some documents optimize for base activation while others assume persuadable-moderate targeting.",
                why_it_matters="Audience incoherence increases risk of mixed signals and weak persuasion performance.",
                strategic_implication="Split messaging tracks by audience and define non-negotiable shared narrative spine.",
                evidence_refs=evidence,
            )
        )

    # 4) Message inconsistency: optimism vs pessimism / attack vs trust.
    optimism_docs = [f for f in filenames if "optimism" in _doc_tones(docs_by_filename[f])]
    pessimism_docs = [f for f in filenames if "pessimism" in _doc_tones(docs_by_filename[f])]
    if optimism_docs and pessimism_docs:
        involved = optimism_docs[:2] + pessimism_docs[:2]
        evidence = []
        for name in involved:
            evidence.extend(_doc_evidence_refs(docs_by_filename[name], max_items=2))
        contradictions.append(
            _new_contradiction(
                kind="message_inconsistency",
                documents=involved,
                topic="optimism_vs_pessimism_tone",
                summary="Documents alternate between hopeful-forward and decline-threat rhetoric.",
                why_it_matters="Emotional whiplash can reduce credibility and weaken repeatable message architecture.",
                strategic_implication="Define a primary emotional posture and reserve alternative tone for explicit contexts.",
                evidence_refs=evidence,
            )
        )

    # 5) Evidence tension: shared theme with opposing sentiment.
    theme_to_sentiments: defaultdict[str, set[str]] = defaultdict(set)
    theme_to_docs: defaultdict[str, List[str]] = defaultdict(list)
    for name in filenames:
        doc = docs_by_filename[name]
        sentiment = _normalize_dist_label(doc.get("sentiment_overall"))
        if not sentiment:
            continue
        for theme in _doc_themes(doc):
            theme_to_sentiments[theme].add(sentiment)
            if name not in theme_to_docs[theme]:
                theme_to_docs[theme].append(name)
    for theme, sentiments in theme_to_sentiments.items():
        if len(theme_to_docs[theme]) < 2:
            continue
        if not ({"positive", "negative"} <= sentiments or {"optimistic", "pessimistic"} <= sentiments):
            continue
        involved = theme_to_docs[theme][:4]
        evidence = []
        for name in involved:
            evidence.extend(_doc_evidence_refs(docs_by_filename[name], max_items=2))
        contradictions.append(
            _new_contradiction(
                kind="evidence_tension",
                documents=involved,
                topic=theme,
                summary=f"Evidence on theme '{theme}' points in conflicting directions across documents.",
                why_it_matters="Conflicting evidence can produce unstable recommendations if treated as single-direction signal.",
                strategic_implication="Separate claims by evidence quality and test competing interpretations before message lock.",
                evidence_refs=evidence,
            )
        )
        if len(contradictions) >= 12:
            break

    # Conservative filter: keep only contradictions with >=2 docs and >=2 evidence refs.
    filtered = [
        item
        for item in contradictions
        if len(item.get("involved_documents", [])) >= 2
        and len(item.get("supporting_evidence_refs", [])) >= 2
    ]
    return filtered[:12]


def _aggregate_campaign_intelligence(
    docs: List[Dict[str, Any]],
    *,
    indexed_total: int,
) -> Dict[str, Any]:
    theme_clusters = _build_theme_clusters(docs)
    entity_counter: Counter[str] = Counter()
    sentiment_counter: Counter[str] = Counter()
    tone_counter: Counter[str] = Counter()
    framing_counter: Counter[str] = Counter()
    narratives_counter: Counter[str] = Counter()
    opportunities_counter: Counter[str] = Counter()
    risks_counter: Counter[str] = Counter()
    evidence_snippets: List[str] = []

    for doc in docs:
        filename = str(doc.get("filename") or "unknown")
        for entity in _normalize_list(doc.get("key_entities"), max_items=40, max_len=120):
            entity_counter[entity.lower()] += 1
        sentiment = _normalize_dist_label(doc.get("sentiment_overall"))
        if sentiment:
            sentiment_counter[sentiment] += 1
        tone_labels = _normalize_list(doc.get("tone_labels"), max_items=24, max_len=90)
        framing_labels = _normalize_list(doc.get("framing_labels"), max_items=24, max_len=90)
        for tone in tone_labels:
            tone_counter[_normalize_dist_label(tone)] += 1
        for frame in framing_labels:
            key = _normalize_dist_label(frame)
            framing_counter[key] += 1
            narratives_counter[key] += 1

        for opportunity in _normalize_list(doc.get("strategic_opportunities"), max_items=20, max_len=240):
            opportunities_counter[opportunity.lower()] += 1
        for risk in _normalize_list(doc.get("persuasion_risks"), max_items=20, max_len=240):
            risks_counter[risk.lower()] += 1

        for evidence in _normalize_list(doc.get("major_claims_or_evidence"), max_items=6, max_len=260):
            evidence_snippets.append(f"{filename}: {evidence}")
            if len(evidence_snippets) >= 24:
                break
        if len(evidence_snippets) >= 24:
            continue

    contradiction_tensions = _build_contradiction_tensions(docs)
    contradictions = [
        str(item.get("contradiction_summary") or "").strip()
        for item in contradiction_tensions
        if str(item.get("contradiction_summary") or "").strip()
    ]
    if not contradictions:
        contradictions = _detect_contradictions(sentiment_counter, tone_counter, framing_counter)
    dominant_narratives = [label for label, _ in narratives_counter.most_common(12)]
    key_actors_entities = [label for label, _ in entity_counter.most_common(20)]
    strategic_opportunities = [label for label, _ in opportunities_counter.most_common(18)]
    strategic_risks = [label for label, _ in risks_counter.most_common(18)]

    docs_analyzed = len(docs)
    docs_failed = max(0, indexed_total - docs_analyzed)
    partial_recompute = docs_failed > 0
    coverage_ratio = round((docs_analyzed / indexed_total), 4) if indexed_total > 0 else 0.0
    degraded_docs = 0
    full_fidelity_docs = 0
    for doc in docs:
        fidelity = str(doc.get("analysis_fidelity_level") or "").strip().lower()
        if not fidelity:
            fidelity = "unknown"
        reduced = bool(doc.get("analysis_context_reduction_applied"))
        if fidelity in {"full"} and not reduced:
            full_fidelity_docs += 1
        else:
            degraded_docs += 1
    degraded_ratio = round((degraded_docs / docs_analyzed), 4) if docs_analyzed > 0 else 0.0

    if docs_analyzed <= 0:
        input_completeness_status = "none"
        input_completeness_note = (
            "Campaign intelligence is generated without completed document-intelligence inputs "
            "(0 analyzed documents). Results should be treated as incomplete."
        )
    elif partial_recompute:
        input_completeness_status = "partial"
        input_completeness_note = (
            f"Campaign intelligence uses partial inputs: analyzed {docs_analyzed}/{indexed_total} indexed documents. "
            "Some document-level analyses are missing (failed or still processing)."
        )
    else:
        input_completeness_status = "complete"
        input_completeness_note = (
            f"Campaign intelligence covers all indexed documents ({docs_analyzed}/{indexed_total})."
        )
    if docs_analyzed <= 0:
        input_quality_status = "unknown"
        input_quality_note = "No completed document-intelligence analyses are available."
    elif degraded_docs <= 0:
        input_quality_status = "full_fidelity"
        input_quality_note = "All analyzed documents were processed at full fidelity without context reduction."
    elif degraded_ratio >= 0.6:
        input_quality_status = "degraded_fidelity"
        input_quality_note = (
            f"{degraded_docs}/{docs_analyzed} analyzed documents used reduced-context document intelligence. "
            "Campaign synthesis should be treated as materially degraded until fuller document analyses complete."
        )
    else:
        input_quality_status = "mixed_fidelity"
        input_quality_note = (
            f"{degraded_docs}/{docs_analyzed} analyzed documents used reduced-context document intelligence. "
            "Campaign synthesis should account for potential evidence compression."
        )

    return {
        "campaign_theme_clusters": theme_clusters,
        "dominant_narratives": dominant_narratives,
        "key_actors_entities": key_actors_entities,
        "sentiment_distribution": _top_counter(sentiment_counter, max_items=12),
        "tone_distribution": _top_counter(tone_counter, max_items=20),
        "framing_distribution": _top_counter(framing_counter, max_items=20),
        "campaign_contradictions": contradictions,
        "contradiction_tensions": contradiction_tensions,
        "strategic_opportunities": strategic_opportunities,
        "strategic_risks": strategic_risks,
        "evidence_snippets": evidence_snippets[:20],
        "docs_total": indexed_total,
        "docs_analyzed": docs_analyzed,
        "docs_failed": docs_failed,
        "partial_recompute": partial_recompute,
        "doc_intel_coverage_ratio": coverage_ratio,
        "input_completeness_status": input_completeness_status,
        "input_completeness_note": input_completeness_note,
        "doc_intel_full_fidelity_docs": full_fidelity_docs,
        "doc_intel_degraded_docs": degraded_docs,
        "doc_intel_degraded_ratio": degraded_ratio,
        "input_quality_status": input_quality_status,
        "input_quality_note": input_quality_note,
    }


def _build_worker_payload(job_id: str) -> Dict[str, str]:
    return {"job_id": job_id}


async def _enqueue_campaign_intel_cloud_task(job_id: str) -> None:
    settings = get_settings()
    if not settings.RILEY_CAMPAIGN_INTEL_USE_CLOUD_TASKS:
        return
    if not settings.GCP_PROJECT_ID or not settings.RILEY_CAMPAIGN_INTEL_WORKER_URL:
        raise RuntimeError(
            "Campaign intelligence Cloud Tasks is enabled but GCP_PROJECT_ID/RILEY_CAMPAIGN_INTEL_WORKER_URL is missing"
        )
    payload = _build_worker_payload(job_id)

    def _create_task_sync() -> None:
        client = tasks_v2.CloudTasksClient()
        parent = client.queue_path(
            settings.GCP_PROJECT_ID,
            settings.RILEY_CAMPAIGN_INTEL_TASKS_LOCATION,
            settings.RILEY_CAMPAIGN_INTEL_TASKS_QUEUE,
        )
        task_name = f"{parent}/tasks/{job_id}"
        headers = {"Content-Type": "application/json"}
        if settings.RILEY_CAMPAIGN_INTEL_WORKER_TOKEN:
            headers["X-Riley-Campaign-Intel-Worker-Token"] = settings.RILEY_CAMPAIGN_INTEL_WORKER_TOKEN
        http_request: Dict[str, Any] = {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": settings.RILEY_CAMPAIGN_INTEL_WORKER_URL,
            "headers": headers,
            "body": json.dumps(payload).encode("utf-8"),
        }
        if settings.RILEY_CAMPAIGN_INTEL_TASKS_SERVICE_ACCOUNT_EMAIL:
            http_request["oidc_token"] = {
                "service_account_email": settings.RILEY_CAMPAIGN_INTEL_TASKS_SERVICE_ACCOUNT_EMAIL,
                "audience": settings.RILEY_CAMPAIGN_INTEL_WORKER_URL,
            }
        task = {"name": task_name, "http_request": http_request}
        try:
            client.create_task(request={"parent": parent, "task": task})
        except AlreadyExists:
            return

    await run_in_threadpool(_create_task_sync)


async def enqueue_campaign_intelligence_job(
    *,
    graph: GraphService,
    tenant_id: str,
    requested_by_user_id: Optional[str],
    trigger_source: str,
) -> Dict[str, Any]:
    settings = get_settings()
    if not settings.RILEY_CAMPAIGN_INTEL_ENABLED:
        raise RuntimeError("Campaign intelligence is disabled")
    job_id = str(uuid.uuid4())
    job = await graph.create_riley_campaign_intelligence_job(
        job_id=job_id,
        tenant_id=tenant_id,
        requested_by_user_id=requested_by_user_id,
        trigger_source=trigger_source,
    )
    try:
        if settings.RILEY_CAMPAIGN_INTEL_USE_CLOUD_TASKS:
            await _enqueue_campaign_intel_cloud_task(job_id)
        else:
            asyncio.create_task(run_campaign_intelligence_job(job_id=job_id, graph=graph))
    except Exception as exc:
        await graph.update_riley_campaign_intelligence_job(
            job_id=job_id,
            status="failed",
            completed_at=datetime.now().isoformat(),
            error_message=f"Failed to enqueue campaign intelligence job: {type(exc).__name__}",
        )
        raise
    return job


async def run_campaign_intelligence_job(
    *,
    job_id: str,
    graph: GraphService,
) -> None:
    started_at = datetime.now().isoformat()
    await graph.update_riley_campaign_intelligence_job(
        job_id=job_id,
        status="processing",
        started_at=started_at,
        error_message=None,
    )
    try:
        job = await graph.get_riley_campaign_intelligence_job_for_worker(job_id=job_id)
        if not job:
            raise RuntimeError(f"Campaign intelligence job not found: {job_id}")
        tenant_id = str(job.get("tenant_id") or "").strip()
        if not tenant_id:
            raise RuntimeError(f"Campaign intelligence job missing tenant_id: {job_id}")
        analyzed_docs, indexed_total = await _collect_document_artifacts(tenant_id)
        if indexed_total <= 0:
            raise RuntimeError("No indexed documents found for campaign intelligence recompute")

        aggregate = _aggregate_campaign_intelligence(
            analyzed_docs,
            indexed_total=indexed_total,
        )
        version = await graph.create_riley_campaign_intelligence_snapshot(
            tenant_id=tenant_id,
            job_id=job_id,
            campaign_theme_clusters_json=json.dumps(aggregate["campaign_theme_clusters"]),
            dominant_narratives=aggregate["dominant_narratives"],
            key_actors_entities=aggregate["key_actors_entities"],
            sentiment_distribution_json=json.dumps(aggregate["sentiment_distribution"]),
            tone_distribution_json=json.dumps(aggregate["tone_distribution"]),
            framing_distribution_json=json.dumps(aggregate["framing_distribution"]),
            campaign_contradictions=aggregate["campaign_contradictions"],
            contradiction_tensions_json=json.dumps(aggregate["contradiction_tensions"]),
            strategic_opportunities=aggregate["strategic_opportunities"],
            strategic_risks=aggregate["strategic_risks"],
            evidence_snippets=aggregate["evidence_snippets"],
            docs_total=aggregate["docs_total"],
            docs_analyzed=aggregate["docs_analyzed"],
            docs_failed=aggregate["docs_failed"],
            partial_recompute=aggregate["partial_recompute"],
            doc_intel_coverage_ratio=aggregate["doc_intel_coverage_ratio"],
            input_completeness_status=aggregate["input_completeness_status"],
            input_completeness_note=aggregate["input_completeness_note"],
            doc_intel_full_fidelity_docs=aggregate["doc_intel_full_fidelity_docs"],
            doc_intel_degraded_docs=aggregate["doc_intel_degraded_docs"],
            doc_intel_degraded_ratio=aggregate["doc_intel_degraded_ratio"],
            input_quality_status=aggregate["input_quality_status"],
            input_quality_note=aggregate["input_quality_note"],
        )
        await graph.update_riley_campaign_intelligence_job(
            job_id=job_id,
            status="complete",
            completed_at=datetime.now().isoformat(),
            error_message=None,
        )
        logger.info(
            "campaign_intel_complete tenant=%s job_id=%s version=%s docs_total=%s docs_analyzed=%s docs_failed=%s "
            "degraded_docs=%s quality_status=%s",
            tenant_id,
            job_id,
            version,
            aggregate["docs_total"],
            aggregate["docs_analyzed"],
            aggregate["docs_failed"],
            aggregate["doc_intel_degraded_docs"],
            aggregate["input_quality_status"],
        )
    except Exception as exc:
        logger.exception("campaign_intel_failed job_id=%s error=%s", job_id, exc)
        await graph.update_riley_campaign_intelligence_job(
            job_id=job_id,
            status="failed",
            completed_at=datetime.now().isoformat(),
            error_message=f"{type(exc).__name__}: {exc}",
        )
