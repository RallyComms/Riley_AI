import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app.services.rerank import (
    _prepare_candidates_with_budget,
    rerank_candidates,
)


def _candidate(chunk_id: str) -> dict:
    return {
        "id": chunk_id,
        "payload": {
            "chunk_id": chunk_id,
            "filename": f"{chunk_id}.txt",
            "chunk_index": 0,
            "text": f"content for {chunk_id}",
        },
    }


class RerankServiceTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _settings() -> SimpleNamespace:
        return SimpleNamespace(
            RERANK_ENABLED=True,
            RERANK_PROVIDER="openai",
            RERANK_MODEL="gpt-4.1-mini",
            GOOGLE_API_KEY=None,
            OPENAI_API_KEY="test-key",
            RERANK_MAX_SNIPPET_TOKENS=180,
            RERANK_MAX_TOTAL_INPUT_TOKENS=6000,
        )

    async def test_openai_valid_ranked_ids_returns_ordered_ids(self) -> None:
        settings = self._settings()
        with patch("app.services.rerank.get_settings", return_value=settings), patch(
            "app.services.rerank.asyncio.to_thread",
            new=AsyncMock(return_value='{"ranked_ids":["c2","c1"],"notes":"ok"}'),
        ):
            ranked = await rerank_candidates(
                query="query",
                candidates=[_candidate("c1"), _candidate("c2"), _candidate("c3")],
                top_k=2,
            )
        self.assertEqual(ranked, ["c2", "c1"])

    async def test_openai_invalid_ranked_ids_returns_none(self) -> None:
        settings = self._settings()
        with patch("app.services.rerank.get_settings", return_value=settings), patch(
            "app.services.rerank.asyncio.to_thread",
            new=AsyncMock(return_value='{"ranked_ids":["c2","bad-id"]}'),
        ):
            ranked = await rerank_candidates(
                query="query",
                candidates=[_candidate("c1"), _candidate("c2"), _candidate("c3")],
                top_k=3,
            )
        self.assertIsNone(ranked)

    async def test_openai_ranked_shape_with_non_numeric_score_returns_none(self) -> None:
        settings = self._settings()
        with patch("app.services.rerank.get_settings", return_value=settings), patch(
            "app.services.rerank.asyncio.to_thread",
            new=AsyncMock(return_value='{"ranked":[{"id":"c2","score":"high","reason":"bad"}]}'),
        ):
            ranked = await rerank_candidates(
                query="query",
                candidates=[_candidate("c1"), _candidate("c2"), _candidate("c3")],
                top_k=3,
            )
        self.assertIsNone(ranked)

    def test_prepare_candidates_respects_total_token_budget(self) -> None:
        candidates = [_candidate("c1"), _candidate("c2"), _candidate("c3")]
        prepared, estimated_tokens = _prepare_candidates_with_budget(
            query="budget query",
            candidates=candidates,
            max_snippet_tokens=50,
            max_total_input_tokens=30,
        )
        self.assertLessEqual(len(prepared), 1)
        self.assertLessEqual(estimated_tokens, 30)

    def test_prepare_candidates_truncates_oversized_snippet_by_tokens(self) -> None:
        long_text = " ".join(["token"] * 800)
        candidate = {
            "id": "cx",
            "payload": {
                "chunk_id": "cx",
                "filename": "long.txt",
                "chunk_index": 0,
                "text": long_text,
            },
        }
        prepared, _ = _prepare_candidates_with_budget(
            query="query",
            candidates=[candidate],
            max_snippet_tokens=20,
            max_total_input_tokens=200,
        )
        self.assertEqual(len(prepared), 1)
        self.assertLess(len(prepared[0]["snippet"].split()), len(long_text.split()))


if __name__ == "__main__":
    unittest.main()
