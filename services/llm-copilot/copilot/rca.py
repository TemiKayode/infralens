"""Root-cause analysis pipeline.

Steps:
1. Anomaly detection via z-score on numeric columns.
2. Rate-of-change / inflection point detection.
3. Pearson correlation matrix for related columns.
4. LLM narrative generation.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from copilot.llm import LLMService
    from copilot.models import RcaReport


def run_rca(rows: list[dict[str, Any]], llm: "LLMService", question: str) -> "RcaReport":
    from copilot.models import AnomalyPoint, Correlation, RcaReport

    if not rows:
        return RcaReport(narrative="No data returned by the query.")

    numeric_cols = _extract_numeric_columns(rows)
    anomalies    = _detect_anomalies(rows, numeric_cols)
    correlations = _compute_correlations(rows, numeric_cols)
    narrative    = _generate_narrative(llm, question, anomalies, correlations)

    return RcaReport(
        anomalies=anomalies,
        correlations=correlations,
        narrative=narrative,
    )


def _extract_numeric_columns(rows: list[dict]) -> dict[str, list[float]]:
    """Return {col_name: [float values]} for columns that are entirely numeric."""
    if not rows:
        return {}

    cols: dict[str, list[float]] = {}
    for col in rows[0]:
        values: list[float] = []
        ok = True
        for row in rows:
            v = row.get(col)
            try:
                values.append(float(v))  # type: ignore[arg-type]
            except (TypeError, ValueError):
                ok = False
                break
        if ok and values:
            cols[col] = values
    return cols


def _detect_anomalies(
    rows: list[dict],
    numeric_cols: dict[str, list[float]],
    z_threshold: float = 3.0,
) -> list[Any]:
    """Flag rows whose numeric values are > z_threshold standard deviations from mean."""
    try:
        import numpy as np
    except ImportError:
        logger.warning("numpy not available; skipping anomaly detection")
        return []

    from copilot.models import AnomalyPoint

    anomalies: list[AnomalyPoint] = []
    for col, values in numeric_cols.items():
        arr  = np.array(values, dtype=float)
        mean = arr.mean()
        std  = arr.std()
        if std == 0:
            continue
        for i, v in enumerate(arr):
            z = abs(float(v) - float(mean)) / float(std)
            if z > z_threshold:
                anomalies.append(AnomalyPoint(
                    row_index=i, column=col, value=float(v), z_score=round(z, 2)
                ))
    return anomalies


def _compute_correlations(
    rows: list[dict],
    numeric_cols: dict[str, list[float]],
    min_r: float = 0.8,
) -> list[Any]:
    """Return column pairs with |Pearson r| > min_r."""
    if len(numeric_cols) < 2 or not rows:
        return []

    try:
        import numpy as np
        from scipy.stats import pearsonr  # type: ignore[import]
    except ImportError:
        logger.warning("numpy/scipy not available; skipping correlation analysis")
        return []

    from copilot.models import Correlation

    col_names = list(numeric_cols.keys())
    results: list[Correlation] = []

    for i in range(len(col_names)):
        for j in range(i + 1, len(col_names)):
            a = numeric_cols[col_names[i]]
            b = numeric_cols[col_names[j]]
            if len(a) < 3:
                continue
            r, _ = pearsonr(a, b)
            if abs(r) >= min_r:
                results.append(Correlation(
                    col_a=col_names[i], col_b=col_names[j], pearson_r=round(r, 3)
                ))

    return results


def _generate_narrative(
    llm:          "LLMService",
    question:     str,
    anomalies:    list[Any],
    correlations: list[Any],
) -> str:
    context_parts = [f"User question: {question}"]

    if anomalies:
        anom_strs = [
            f"  - Row {a.row_index}: column '{a.column}' = {a.value} (z={a.z_score})"
            for a in anomalies[:10]
        ]
        context_parts.append("Anomalies detected:\n" + "\n".join(anom_strs))
    else:
        context_parts.append("No statistical anomalies detected.")

    if correlations:
        corr_strs = [
            f"  - '{c.col_a}' and '{c.col_b}': r = {c.pearson_r}"
            for c in correlations[:5]
        ]
        context_parts.append("Strong correlations:\n" + "\n".join(corr_strs))

    context = "\n\n".join(context_parts)
    return llm.generate_narrative(context)
