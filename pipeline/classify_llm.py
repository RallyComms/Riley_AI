import os, sys, json, yaml, time, re
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

CLASSIFY_MODEL = os.getenv("CLASSIFY_MODEL", "gpt-4o-mini")
SAMPLE_CHARS = int(os.getenv("CLASSIFY_SAMPLE_CHARS", "6000"))
MAX_RETRIES = int(os.getenv("CLASSIFY_MAX_RETRIES", "3"))
RETRY_BASE_SEC = float(os.getenv("CLASSIFY_RETRY_BASE_SEC", "1.2"))

def load_yaml(p):
    if not Path(p).exists():
        return {}
    return yaml.safe_load(Path(p).read_text(encoding="utf-8")) or {}

def clean_model_json(txt: str) -> dict:
    s = (txt or "").strip()
    if s.startswith("```"):
        s = s.strip("`").lstrip("json").strip()
    try:
        return json.loads(s)
    except Exception:
        pass
    m = re.search(r"\{.*\}", s, flags=re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except Exception:
            pass
    return {}

def head(text: str, n: int) -> str:
    return (text or "")[:n]

def tail(text: str, n: int) -> str:
    t = (text or "")
    return t[-n:] if t else t

def build_sample(extract_type: str, extract_obj: dict, max_chars: int) -> str:
    if extract_type == "text":
        t = extract_obj.get("text", "")
        a = head(t, int(max_chars * 0.7))
        b = tail(t, int(max_chars * 0.3))
        return (a + "\n...\n" + b)[:max_chars]
    if extract_type == "slides":
        slides = extract_obj.get("slides", {}) or {}
        parts = []
        for i in (1, 2, 3):
            si = slides.get(i, {})
            parts.append(f"[Slide {i}] {si.get('title','')}\n{si.get('body','')}")
        return "\n\n".join(parts)[:max_chars]
    if extract_type == "xlsx_schema":
        schema = extract_obj.get("schema", {}) or {}
        return json.dumps(schema)[:max_chars]
    return ""

def path_context_tokens(source_path: str) -> str:
    p = Path(source_path)
    parts = [seg for seg in p.parts if seg not in ("/", "\\")]
    last = [seg for seg in parts[-4:]]
    return " / ".join(last)

def build_label_hints(labels, synonyms_map, patterns_map) -> str:
    lines = []
    for lab in labels:
        syns = list(dict.fromkeys((synonyms_map.get(lab) or [])))
        pats = list(dict.fromkeys((patterns_map.get(lab, {}).get("must_any") or [])))
        tokens = syns + pats
        if tokens:
            lines.append(f"{lab}: {', '.join(tokens[:14])}")
    return "\n".join(lines)

SYSTEM_PROMPT = (
    "You are a careful document classifier for a communications/PR knowledge base.\n"
    "Return STRICT JSON with fields: doc_type, doc_subtype, confidence, evidence.\n"
    "- doc_type MUST be one of the allowed labels provided.\n"
    "- doc_subtype is a short free-text description (e.g., \"fact sheet\", \"proclamation language\").\n"
    "- confidence is a number 0..1 (be honest; do not always return 1).\n"
    "- evidence is a list (2-5) of short phrases you saw that justify your choice.\n"
    "Use CONTENT, FILENAME, SOURCE_PATH (folder context), EXTRACT_TYPE, and LABEL_HINTS.\n"
    "If uncertain, choose the safest close label from the allowed list."
)

def classify_sample(client, model, labels, filename, source_path, extract_type, sample, label_hints):
    user_content = (
        "ALLOWED_LABELS = [" + ", ".join(labels) + "]\n"
        "LABEL_HINTS =\n" + label_hints + "\n\n"
        "FILENAME = " + str(filename) + "\n"
        "SOURCE_PATH_LAST_PARTS = " + path_context_tokens(source_path) + "\n"
        "EXTRACT_TYPE = " + str(extract_type) + "\n"
        "CONTENT_SAMPLE = <<BEGIN>>" + sample + "<<END>>\n\n"
        "Respond with JSON ONLY:\n"
        "{\"doc_type\": \"...\", \"doc_subtype\": \"...\", \"confidence\": 0.0, \"evidence\": [\"...\",\"...\"]}"
    )
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_content},
                ],
                temperature=0,
                response_format={"type": "json_object"} 
            )
            raw = resp.choices[0].message.content or ""
            data = clean_model_json(raw)
            if not isinstance(data, dict):
                raise ValueError("Unable to parse model JSON")
            return data
        except Exception as e:
            last_err = e
            if attempt == MAX_RETRIES:
                break
            time.sleep(RETRY_BASE_SEC * (2 ** (attempt - 1)))
    print(f"[classify_llm] ERROR: classification failed after {MAX_RETRIES} attempts for '{filename}'")
    raise last_err if last_err else RuntimeError("Unknown classification error")

def coerce_to_allowed(data: dict, allowed: list) -> dict:
    out = dict(data or {})

    # Normalize evidence early
    ev = out.get("evidence", [])
    if not isinstance(ev, list):
        ev = [str(ev)]
    out["evidence"] = [str(x)[:120] for x in ev[:5]]

    # Clamp subtype length
    out["doc_subtype"] = (out.get("doc_subtype") or "")[:160]

    dt = out.get("doc_type")

    if dt not in allowed:
        # Prefer neutral bucket over one-pager if available
        out["doc_type"] = "press_statement" if "press_statement" in allowed else "messaging_one_pager"
        # Parse existing confidence (if any), then cap at 0.6 when coerced
        try:
            c = float(out.get("confidence", 0.4))
        except Exception:
            c = 0.4
        out["confidence"] = min(max(c, 0.0), 0.6)
    else:
        # Valid type: just coerce to float and bound 0..1 (no artificial capping)
        try:
            c = float(out.get("confidence", 0.5))
        except Exception:
            c = 0.5
        out["confidence"] = min(max(c, 0.0), 1.0)

    return out

def soft_override(result: dict, source_path: str, extract_type: str, sample: str,
                synonyms_map: dict, patterns_map: dict, allowed: list) -> dict:
    def norm(s): return (s or "").lower()
    def has(needle, hay): return needle in norm(hay)
    def has_any(terms, hay):
        h = norm(hay)
        return any(t in h for t in terms)

    filename = Path(source_path).name
    hay = " ".join([
        norm(source_path),
        norm(result.get("doc_subtype")),
        norm(sample),
        norm(filename)
    ])

    # Helper: set label, raise confidence floor, and optionally lock it.
    def set_label(lbl, conf_floor=0.90, lock=False):
    
    # Respect an existing lock: once something locked the label, do nothing.
        if result.get("_locked"):
            return

        if lbl in allowed and result.get("doc_type") != lbl:
            result["doc_type"] = lbl
        try:
            cur_conf = float(result.get("confidence", 0.0))
        except Exception:
            cur_conf = 0.0
        result["confidence"] = max(cur_conf, conf_floor)
        if lock:
            result["_locked"] = True

    # ---------- ORDERED, HIGH-PRIORITY DETECTORS ----------
    # (Order matters; specific beats generic.)

    # PRESS RELEASE markers (strong lock)
    release_markers = ["for immediate release", "media contact", "###"]
    if has_any(release_markers, hay):
        set_label("press_release", 0.90, lock=True)
        
    #Filename contains 'release' → treat as press release (lock even if boilerplate is missing)
    if "release" in filename.lower():
        set_label("press_release", 0.90, lock=True)
        
    # --- Surgical override: "Pitch" files that still contain release boilerplate ---
    # If filename says "pitch" but text shows release boilerplate/placeholders,
    # force pitch_email and LOWER confidence so it gets manual review.

    if "pitch" in filename.lower():
            _boilerplate = ["for immediate release", "media contact", "###"]
            _placeholders = [
                "contact name", "contact email", "contact phone", "[contact", "<contact", "xxx",
        # date/placeholders (variants)
                "january xx", "january xx,", "jan xx", "jan xx,",
                " los angeles—xx", " los angeles — xx", " los angeles–xx", " los angeles xx",
                " los angeles, xx", " los angeles,  xx",  # stray spaces/commas
                " xx, 20", " xx 20"  
            ]
            if has_any(_boilerplate, hay) and has_any(_placeholders, hay):
                if "pitch_email" in allowed:
                    # Manual override (don't use set_label which raises confidence)
                    result["doc_type"] = "pitch_email"
                    try:
                        cur = float(result.get("confidence", 0.9))
                    except Exception:
                        cur = 0.9
                    result["confidence"] = min(cur, 0.60)  # flag for manual review
                    result["_locked"] = True               # prevent later overrides

##-------------------------------------------------------------------------

    # MEDIA RESPONSE (data/inquiry/Q&A), before pitch/bio
    if not result.get("_locked") and not has("talking points", hay) and (
        has_any(["data request response", "data request", "media inquiry", "q&a",
                "responses below", "as requested"], hay)
        or ("data request response" in filename.lower())
        or ("fox response" in filename.lower())
        or ("lat data request" in filename.lower())
    ):
        set_label("media_response", 0.91, lock=True)
        
    # Force real guides by filename (beats accidental bios)
    if ("interview guide" in filename.lower()) and not result.get("_locked"):
        if "interview_guide" in allowed:
            set_label("interview_guide", 0.90, lock=True)
        else:
            set_label("interview_question_bank", 0.90, lock=True)
            
    # XLSX: if the filename says "Media List", it's a media_list (lock)
    if extract_type == "xlsx_schema" and "media list" in filename.lower():
        set_label("media_list", 0.90, lock=True)

    # Filename contains "statement" → prefer press_statement when not a press release
    if ("statement" in filename.lower()) and not has_any(release_markers, hay):
        if "press_statement" in allowed:
            set_label("press_statement", 0.90)
            
    # SPEAKING TRACKER by filename (lock, xlsx only)/(fallback block way below)
    if ("speaking opportunities" in filename.lower()) and extract_type == "xlsx_schema":
        if "speaking_tracker" in allowed:
            set_label("speaking_tracker", 0.90, lock=True)

            
    # CONFERENCE PROPOSAL (before bio!)
    if ("conferences" in norm(source_path) or "conference" in norm(source_path)) and \
        has_any(["proposal", "application", "session", "workshop", "learning objectives", "abstract", "cfp"], hay):
        set_label("conference_proposal", 0.90, lock=True)

    # EDITORIAL CALENDAR (xlsx schema)
    if extract_type == "xlsx_schema" and (
        has("editorial calendar", hay) or
        has_any(["\"editorial calendar", "content calendar", "social calendar"], hay) or
        has_any(["\"january\"", "\"february\"", "\"march\"", "\"april\"", "\"may\"", "\"june\"",
                "\"july\"", "\"august\"", "\"september\"", "\"october\"", "\"november\"", "\"december\""], hay)
    ):
        set_label("editorial_calendar", 0.90, lock=True)

    # MEDIA ANALYSIS PLAN → work_plan|strategy_memo
    if ("media analysis plan" in filename.lower()) or has_any(
        ["guiding question", "research question", "methodology", "time frame", "limitations", "scope"],
        hay
    ):
        set_label("work_plan|strategy_memo", 0.90, lock=True)

    # INTERVIEW GUIDES (before bio)
    if has_any(["interview guide", "stakeholder interview", "interview objectives", "interview questions"], hay):
        want = "interview_guide" if "interview_guide" in allowed else "interview_question_bank"
        set_label(want, 0.90, lock=True)

    # WORKBACK PLAN
    if has_any(["work back plan", "workback", "reverse timeline"], hay):
        set_label("workback_plan", 0.90)
        
    # TIMELINE (only when filename/path says so)
    if (("timeline" in filename.lower()) or ("timeline" in norm(source_path))) and \
        not has_any(["stakeholder interview", "interview"], hay):
        if "timeline" in allowed:
            set_label("timeline", 0.90)

            
    # SPEAKING TRACKER
    if has_any(["speaking opportunities", "call for speakers", "cfp"], hay):
        # Ensure this label exists in taxonomy; otherwise map to competitive_analysis or tracker bucket.
        if "speaking_tracker" in allowed:
            set_label("speaking_tracker", 0.90)
        else:
            # fallback to competitive_analysis or media_list (still better than bio_profile)
            if "competitive_analysis" in allowed:
                set_label("competitive_analysis", 0.85)
            elif "media_list" in allowed:
                set_label("media_list", 0.85)

    # GIVEBUTTER / DONATIONS → platform_copy_edits
    if has_any(["givebutter", "donation page", "donor cta", "donate", "your gift", "make a gift"], hay):
        set_label("platform_copy_edits", 0.90)

    # FRONTLINES PITCH / EMBARGO → pitch_email (unless release markers)
    if ("pitch" in filename.lower() or "embargo" in filename.lower()) and not has_any(release_markers, hay):
        set_label("pitch_email", 0.92, lock=True)

    # BIO/PROFILE (after proposals & guides)
    if not result.get("_locked") and has_any([" bio", "biograph", "panelist", "about the speaker", "headshot"], hay):
        set_label("bio_profile", 0.90)

    # TALKING POINTS fix (avoid mis-bucketing into platform_copy_edits)
    if has("talking points", hay) and result.get("doc_type") == "platform_copy_edits":
        set_label("talking_points", 0.90, lock=True)

    # ---------- GENERIC VOTE (only if low confidence and not locked) ----------
    try:
        conf_now = float(result.get("confidence", 0.0))
    except Exception:
        conf_now = 0.0
    current = result.get("doc_type", "")

    # IMPORTANT: do NOT treat 'work_plan|strategy_memo' as generic.
    generic = {"press_statement", "messaging_one_pager"}

    # Only generic-vote when confidence is low and not locked.
    if not result.get("_locked"):
        best_label = current; best_hits = 0
        for lab in allowed:
            tokens = []
            tokens += [t.lower() for t in (synonyms_map.get(lab) or [])]
            tokens += [t.lower() for t in (patterns_map.get(lab, {}).get("must_any") or [])]
            tokens += [lab.replace("_", " ").lower()]
            hits = sum(1 for t in set(tokens) if t and t in hay)
            if hits > best_hits:
                best_hits, best_label = hits, lab

        if best_hits >= 2 and (conf_now < 0.85 or current in generic) and best_label in allowed and best_label != current:
            result["doc_type"] = best_label
            result["confidence"] = max(conf_now, 0.88)

    result.pop("_locked", None)
    return result

# --- this goes just below soft_override and above main()so that main can call it---

ALLOWED_BY_EXTRACT = {
    "xlsx_schema": {"editorial_calendar", "media_list", "coverage_tracker", "story_bank",
                    "speaking_tracker", "competitive_analysis"},
    "slides": {"deck|training_materials", "deck|analysis_report"},
    "text": None  # no restriction
}

def enforce_extract_constraints(data, extract_type, hay, filename: str = ""):
    """
    Enforce format ↔︎ label sanity:
    - xlsx_schema → {editorial_calendar, media_list, coverage_tracker, story_bank, speaking_tracker, competitive_analysis}
    - slides      → {deck|training_materials, deck|analysis_report}
    - text        → no restriction
    If the current doc_type is not allowed for the given extract_type, pick the most
    plausible guess based on filename and schema cues and lower the confidence ceiling.
    """
    allowed = ALLOWED_BY_EXTRACT.get(extract_type)
    if allowed is None:  # text -> anything goes
        return data

    # Already acceptable? leave it alone.
    if data.get("doc_type") in allowed:
        return data

    guess = None
    fname = (filename or "").lower()

    if extract_type == "xlsx_schema":
        # 1) Filename cues (strongest)
        if "media list" in fname:
            guess = "media_list"

        # 2) Editorial calendar by schema/content
        elif "calendar" in hay:
            guess = "editorial_calendar"

        # 3) Media-list schema cues (columns etc.)
        elif any(k in hay for k in ["media list", "reporter", "outlet", "beat", "email"]):
            guess = "media_list"

        # 4) Coverage-tracker schema cues
        elif any(k in hay for k in ["coverage", "headline", "link", "status", "published", "url"]):
            guess = "coverage_tracker"

        # 5) Speaking/CFP trackers
        elif any(k in hay for k in ["speaking", "cfp", "call for speakers"]):
            guess = "speaking_tracker" if "speaking_tracker" in allowed else "media_list"

        # 6) Default for spreadsheets
        else:
            guess = "media_list"

    elif extract_type == "slides":
        # If we ever need to disambiguate, we can peek at keywords; default to training materials
        if any(k in hay for k in ["analysis", "findings", "insights", "kpi"]):
            guess = "deck|analysis_report" if "deck|analysis_report" in allowed else None
        else:
            guess = "deck|training_materials" if "deck|training_materials" in allowed else None

    # Apply the guess if valid for this extract_type, lower confidence ceiling a bit
    if guess and guess in allowed:
        data["doc_type"] = guess
        try:
            cur = float(data.get("confidence", 0.0))
        except Exception:
            cur = 0.0
        data["confidence"] = min(cur, 0.85)

    return data


def main(campaign_root: str, model: str = None):
    if not os.getenv("OPENAI_API_KEY"):
        print("[classify_llm] ERROR: OPENAI_API_KEY not set in environment (.env).")
        sys.exit(1)

    client = OpenAI()
    model = model or CLASSIFY_MODEL

    taxonomy = load_yaml("configs/taxonomy.yaml")
    patterns_map = load_yaml("configs/patterns.yaml")
    labels = taxonomy.get("labels", []) or []
    synonyms_map = taxonomy.get("synonyms", {}) or {}

    camp = Path(campaign_root).name
    extracted = Path("data/processed") / camp / "extracted.jsonl"
    out = Path("data/processed") / camp / "classified.csv"

    label_hints = build_label_hints(labels, synonyms_map, patterns_map)

    rows = []
    total = 0
    with open(extracted, encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            ekind = (r.get("extract", {}) or {}).get("type")
            spath = r.get("source_path", "")
            name = Path(spath).name
            sample = build_sample(ekind, r.get("extract", {}) or {}, SAMPLE_CHARS)
            try:
                data = classify_sample(client, model, labels, name, spath, ekind, sample, label_hints)
                data = coerce_to_allowed(data, labels)
                data = soft_override(data, spath, ekind, sample, synonyms_map, patterns_map, labels)
                
                hay = " ".join([spath or "", str(data.get("doc_subtype","")) or "", sample or ""]).lower()
                data = enforce_extract_constraints(data, ekind, hay, name)  # pass filename too
                
                doc_type = data.get("doc_type", "messaging_one_pager")
                doc_subtype = data.get("doc_subtype", "")
                conf = float(data.get("confidence", 0.5))
            except Exception as e:
                print(f"[classify_llm] ERROR classifying '{spath}' (extract_type={ekind}): {e}")
                doc_type = "messaging_one_pager"
                doc_subtype = "unknown"
                conf = 0.3

            rows.append({
                "source_path": spath,
                "sha256": r.get("sha256", ""),
                "doc_type": doc_type,
                "doc_subtype": doc_subtype,
                "clf_confidence": round(conf, 2),
                "extract_type": ekind
            })
            total += 1
            if total % 25 == 0:
                print(f"[classify_llm] processed {total} files...")

    df = pd.DataFrame(rows)
    df.to_csv(out, index=False)
    print(f"[classify_llm] wrote {out} ({len(rows)} rows)")

if __name__ == "__main__":
    if len(sys.argv) >= 3:
        main(sys.argv[1], sys.argv[2])
    else:
        main(sys.argv[1])


