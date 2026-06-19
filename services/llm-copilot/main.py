"""InfraLens LLM Copilot — natural-language to IQL with constrained decoding."""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from typing import Any

import aiosqlite
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from copilot.config import Settings
from copilot.grammar import build_gbnf_grammar, DEFAULT_TABLES
from copilot.llm import LLMService
from copilot.models import (
    CopilotRequest,
    CopilotResponse,
    FeedbackRequest,
    RcaReport,
)
from copilot.rca import run_rca

settings = Settings()


# ── Lifespan ───────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialise SQLite feedback store
    async with aiosqlite.connect(settings.feedback_db_path) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS feedback (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
                nl_question TEXT    NOT NULL,
                iql_query   TEXT    NOT NULL,
                accepted    BOOLEAN NOT NULL DEFAULT 0,
                rca_text    TEXT,
                latency_ms  INTEGER
            )
        """)
        await db.commit()

    # Warm up the LLM (loads model weights into memory)
    app.state.llm = LLMService(settings)
    app.state.llm.load()

    yield

    # Cleanup
    app.state.llm.unload()


# ── App ────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="InfraLens LLM Copilot",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Helpers ────────────────────────────────────────────────────────────────────

async def load_few_shot_examples(n: int = 20) -> list[dict]:
    """Load the most recent accepted queries as few-shot examples."""
    try:
        async with aiosqlite.connect(settings.feedback_db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT nl_question, iql_query FROM feedback "
                "WHERE accepted = 1 ORDER BY created_at DESC LIMIT ?",
                (n,),
            ) as cur:
                rows = await cur.fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


async def execute_iql(iql: str) -> list[dict[str, Any]]:
    """Forward IQL to the API gateway and return result rows."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            f"{settings.gateway_url}/api/v1/query",
            json={"query": iql},
            headers={"Authorization": f"Bearer {settings.gateway_token}"},
        )
        resp.raise_for_status()

        rows: list[dict] = []
        for line in resp.text.strip().splitlines():
            if line.strip():
                import json
                rows.append(json.loads(line))
        return rows


# ── Endpoints ──────────────────────────────────────────────────────────────────

@app.get("/healthz")
async def health() -> dict:
    return {"status": "ok"}


@app.post("/api/v1/copilot/query", response_model=CopilotResponse)
async def copilot_query(req: CopilotRequest) -> CopilotResponse:
    t0 = time.monotonic()
    llm: LLMService = app.state.llm

    # 1. Fetch few-shot examples from feedback DB
    examples = await load_few_shot_examples()

    # 2. Build GBNF grammar from catalog
    grammar = build_gbnf_grammar(tables=DEFAULT_TABLES)

    # 3. Generate IQL via constrained decoding
    iql = llm.generate_iql(
        question=req.question,
        grammar=grammar,
        few_shot_examples=examples,
        context=req.context or {},
    )

    # 4. Execute IQL via API gateway
    try:
        rows = await execute_iql(iql)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Query execution failed: {exc}")

    # 5. Run RCA pipeline
    rca = run_rca(rows, llm, req.question)

    latency_ms = int((time.monotonic() - t0) * 1000)

    # 6. Persist to feedback store (default accepted=False until user signals)
    async with aiosqlite.connect(settings.feedback_db_path) as db:
        await db.execute(
            "INSERT INTO feedback (nl_question, iql_query, accepted, rca_text, latency_ms) "
            "VALUES (?, ?, 0, ?, ?)",
            (req.question, iql, rca.narrative, latency_ms),
        )
        await db.commit()

    return CopilotResponse(iql=iql, rows=rows, rca=rca, latency_ms=latency_ms)


@app.post("/api/v1/copilot/feedback")
async def copilot_feedback(req: FeedbackRequest) -> dict:
    async with aiosqlite.connect(settings.feedback_db_path) as db:
        await db.execute(
            "UPDATE feedback SET accepted = ? WHERE nl_question = ? AND iql_query = ?",
            (1 if req.accepted else 0, req.nl_question, req.iql_query),
        )
        await db.commit()
    return {"status": "recorded"}


@app.get("/api/v1/copilot/history")
async def copilot_history(limit: int = 50) -> list[dict]:
    async with aiosqlite.connect(settings.feedback_db_path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT id, created_at, nl_question, iql_query, accepted, latency_ms "
            "FROM feedback ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ) as cur:
            rows = await cur.fetchall()
    return [dict(r) for r in rows]


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=settings.port, reload=False)
