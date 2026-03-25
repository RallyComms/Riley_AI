from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, List, Optional


PRICING_VERSION = "cost-accounting-v1"


@dataclass(frozen=True)
class PricingRule:
    service: str
    provider: str
    model: str
    unit_type: str
    unit_price_usd: float
    pricing_version: str = PRICING_VERSION
    effective_from: Optional[date] = None
    effective_to: Optional[date] = None


DEFAULT_RULES: List[PricingRule] = [
    PricingRule(
        service="embedding",
        provider="google_gemini",
        model="models/gemini-embedding-001",
        unit_type="input_token_1k",
        unit_price_usd=0.0001,
    ),
    PricingRule(
        service="rerank",
        provider="google_gemini",
        model="gemini-2.5-flash",
        unit_type="input_token_1k",
        unit_price_usd=0.0003,
    ),
    PricingRule(
        service="rerank",
        provider="google_gemini",
        model="gemini-2.5-pro",
        unit_type="input_token_1k",
        unit_price_usd=0.00125,
    ),
    PricingRule(
        service="chat_generation",
        provider="google_gemini",
        model="gemini-2.5-flash",
        unit_type="input_token_1k",
        unit_price_usd=0.0003,
    ),
    PricingRule(
        service="chat_generation",
        provider="google_gemini",
        model="gemini-2.5-flash",
        unit_type="output_token_1k",
        unit_price_usd=0.0025,
    ),
    PricingRule(
        service="chat_generation",
        provider="google_gemini",
        model="gemini-2.5-pro",
        unit_type="input_token_1k",
        unit_price_usd=0.00125,
    ),
    PricingRule(
        service="chat_generation",
        provider="google_gemini",
        model="gemini-2.5-pro",
        unit_type="output_token_1k",
        unit_price_usd=0.01,
    ),
    PricingRule(
        service="chat_generation",
        provider="openai",
        model="gpt-4.1",
        unit_type="input_token_1k",
        unit_price_usd=0.002,
    ),
    PricingRule(
        service="chat_generation",
        provider="openai",
        model="gpt-4.1",
        unit_type="output_token_1k",
        unit_price_usd=0.008,
    ),
    PricingRule(
        service="report_generation",
        provider="google_gemini",
        model="gemini-2.5-pro",
        unit_type="input_token_1k",
        unit_price_usd=0.00125,
    ),
    PricingRule(
        service="report_generation",
        provider="google_gemini",
        model="gemini-2.5-pro",
        unit_type="output_token_1k",
        unit_price_usd=0.01,
    ),
    PricingRule(
        service="report_generation",
        provider="openai",
        model="gpt-4.1",
        unit_type="input_token_1k",
        unit_price_usd=0.002,
    ),
    PricingRule(
        service="report_generation",
        provider="openai",
        model="gpt-4.1",
        unit_type="output_token_1k",
        unit_price_usd=0.008,
    ),
]


def _parse_iso_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).date()
    except Exception:
        return None


def _rule_is_effective(rule: PricingRule, occurred_at: Optional[str]) -> bool:
    if not occurred_at:
        return True
    occurred_date = _parse_iso_date(occurred_at)
    if occurred_date is None:
        return True
    if rule.effective_from and occurred_date < rule.effective_from:
        return False
    if rule.effective_to and occurred_date > rule.effective_to:
        return False
    return True


def _rule_match_score(rule: PricingRule, service: str, provider: str, model: str, unit_type: str) -> int:
    if rule.service != service or rule.unit_type != unit_type:
        return -1
    score = 0
    if rule.provider == provider:
        score += 2
    elif rule.provider != "*":
        return -1
    if rule.model == model:
        score += 2
    elif rule.model != "*":
        return -1
    return score


def resolve_unit_price(
    *,
    service: str,
    provider: str,
    model: str,
    unit_type: str,
    occurred_at: Optional[str] = None,
) -> Optional[PricingRule]:
    normalized_provider = (provider or "").strip().lower()
    normalized_model = (model or "").strip()
    best: Optional[PricingRule] = None
    best_score = -1
    for rule in DEFAULT_RULES:
        if not _rule_is_effective(rule, occurred_at):
            continue
        score = _rule_match_score(
            rule,
            service=service,
            provider=normalized_provider,
            model=normalized_model,
            unit_type=unit_type,
        )
        if score > best_score:
            best = rule
            best_score = score
    return best


def estimate_text_generation_cost(
    *,
    service: str,
    provider: str,
    model: str,
    input_tokens: int,
    output_tokens: int,
    usage_is_exact: bool = False,
    occurred_at: Optional[str] = None,
) -> Dict[str, Optional[float] | str]:
    input_rule = resolve_unit_price(
        service=service,
        provider=provider,
        model=model,
        unit_type="input_token_1k",
        occurred_at=occurred_at,
    )
    output_rule = resolve_unit_price(
        service=service,
        provider=provider,
        model=model,
        unit_type="output_token_1k",
        occurred_at=occurred_at,
    )
    if not input_rule and not output_rule:
        return {
            "cost_estimate_usd": None,
            "pricing_version": PRICING_VERSION,
            "cost_confidence": "proxy_only",
        }
    input_cost = ((max(0, int(input_tokens)) / 1000.0) * (input_rule.unit_price_usd if input_rule else 0.0))
    output_cost = ((max(0, int(output_tokens)) / 1000.0) * (output_rule.unit_price_usd if output_rule else 0.0))
    return {
        "cost_estimate_usd": round(input_cost + output_cost, 6),
        "pricing_version": PRICING_VERSION,
        "cost_confidence": "exact_usage" if usage_is_exact else "estimated_units",
    }


def estimate_single_unit_cost(
    *,
    service: str,
    provider: str,
    model: str,
    unit_type: str,
    quantity: float,
    occurred_at: Optional[str] = None,
) -> Dict[str, Optional[float] | str]:
    rule = resolve_unit_price(
        service=service,
        provider=provider,
        model=model,
        unit_type=unit_type,
        occurred_at=occurred_at,
    )
    if not rule:
        return {
            "cost_estimate_usd": None,
            "pricing_version": PRICING_VERSION,
            "cost_confidence": "proxy_only",
        }
    value = max(0.0, float(quantity))
    if unit_type.endswith("_1k"):
        value = value / 1000.0
    return {
        "cost_estimate_usd": round(value * rule.unit_price_usd, 6),
        "pricing_version": rule.pricing_version,
        "cost_confidence": "estimated_units",
    }
