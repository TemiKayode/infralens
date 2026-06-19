from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class CopilotRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=2000)
    context:  dict[str, Any] | None = None


class AnomalyPoint(BaseModel):
    row_index: int
    column:    str
    value:     float
    z_score:   float


class Correlation(BaseModel):
    col_a:   str
    col_b:   str
    pearson_r: float


class RcaReport(BaseModel):
    anomalies:    list[AnomalyPoint] = []
    correlations: list[Correlation]  = []
    narrative:    str                = ""


class CopilotResponse(BaseModel):
    iql:        str
    rows:       list[dict[str, Any]]
    rca:        RcaReport
    latency_ms: int


class FeedbackRequest(BaseModel):
    nl_question: str
    iql_query:   str
    accepted:    bool
