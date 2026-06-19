"""LLM inference via llama-cpp-python with GBNF constrained decoding."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# System prompt template
_SYSTEM_PROMPT = """\
You are InfraLens Copilot, an expert in observability and infrastructure analysis.
You translate natural-language questions into IQL (InfraLens Query Language) queries.
IQL is SQL with temporal extensions: time_bucket(), rate(), delta(), histogram_quantile(), now(), interval().

Available tables: logs, metrics, traces.
Always return ONLY the IQL query with no explanation. End the query with a semicolon.

{few_shot_section}
"""

_FEW_SHOT_EXAMPLE = """\
Question: {question}
IQL: {iql}
"""


class LLMService:
    def __init__(self, settings: Any) -> None:
        self.settings = settings
        self._llm: Any = None

    def load(self) -> None:
        try:
            from llama_cpp import Llama  # type: ignore[import]
            self._llm = Llama(
                model_path=self.settings.model_path,
                n_ctx=self.settings.n_ctx,
                n_gpu_layers=self.settings.n_gpu_layers,
                verbose=False,
            )
            logger.info("LLM model loaded from %s", self.settings.model_path)
        except Exception as exc:
            logger.warning("LLM not available (%s); will use stub responses", exc)
            self._llm = None

    def unload(self) -> None:
        self._llm = None

    def generate_iql(
        self,
        question:          str,
        grammar:           str,
        few_shot_examples: list[dict],
        context:           dict,
    ) -> str:
        """Generate a syntactically valid IQL query from a natural language question.

        Uses GBNF constrained decoding so the output is guaranteed to match the grammar.
        Falls back to a stub query when the model is unavailable (dev / CI).
        """
        if self._llm is None:
            return self._stub_iql(question)

        few_shot_section = "\n".join(
            _FEW_SHOT_EXAMPLE.format(
                question=ex["nl_question"],
                iql=ex["iql_query"],
            )
            for ex in few_shot_examples[:10]
        )

        system_prompt = _SYSTEM_PROMPT.format(
            few_shot_section=f"Examples:\n{few_shot_section}" if few_shot_section else "",
        )

        prompt = f"{system_prompt}\nQuestion: {question}\nIQL:"

        from llama_cpp import LlamaGrammar  # type: ignore[import]
        grammar_obj = LlamaGrammar.from_string(grammar)

        result = self._llm(
            prompt,
            max_tokens=self.settings.max_tokens,
            temperature=self.settings.temperature,
            grammar=grammar_obj,
            stop=["\n\n", "Question:"],
        )
        iql = result["choices"][0]["text"].strip()
        if not iql.endswith(";"):
            iql += ";"
        logger.info("Generated IQL: %s", iql)
        return iql

    def generate_narrative(self, context: str) -> str:
        """Generate a free-text RCA narrative (no grammar constraint)."""
        if self._llm is None:
            return "Root cause analysis: model not available."

        prompt = (
            "You are an SRE expert. Given the following anomaly data, "
            "provide a concise root-cause analysis in 2-3 sentences.\n\n"
            f"{context}\n\nRoot cause:"
        )
        result = self._llm(
            prompt,
            max_tokens=256,
            temperature=0.3,
            stop=["\n\n"],
        )
        return result["choices"][0]["text"].strip()

    @staticmethod
    def _stub_iql(question: str) -> str:
        """Return a sensible default query when LLM is unavailable."""
        q = question.lower()
        if "error" in q or "log" in q:
            return (
                "SELECT timestamp_ns, severity_text, body FROM logs "
                "WHERE severity_number >= 13 "
                "AND timestamp_ns >= now() - interval '1h' "
                "ORDER BY timestamp_ns DESC LIMIT 100;"
            )
        if "metric" in q or "rate" in q or "latency" in q:
            return (
                "SELECT time_bucket(interval '1m', timestamp_ns) AS minute, "
                "metric_name, avg(value) AS avg_val FROM metrics "
                "WHERE timestamp_ns >= now() - interval '1h' "
                "GROUP BY minute, metric_name ORDER BY minute ASC;"
            )
        if "trace" in q or "span" in q or "slow" in q:
            return (
                "SELECT service_name, avg(duration_ns) AS avg_ns, "
                "count(*) AS span_count FROM traces "
                "WHERE timestamp_ns >= now() - interval '30m' "
                "GROUP BY service_name ORDER BY avg_ns DESC LIMIT 20;"
            )
        return (
            "SELECT timestamp_ns, body FROM logs "
            "WHERE timestamp_ns >= now() - interval '1h' LIMIT 50;"
        )
