"""
routes/violations.py
=====================
REST endpoints for violation metadata.

GET /violations/count   → {"count": 4}
GET /violations         → list of recent violation records
GET /health             → {"status": "ok"}
"""

import logging

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from state_store import state_store

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/health")
def health():
    return {"status": "ok"}


@router.get("/violations/count")
def violation_count():
    """
    Returns the total number of violations detected in the current session.
    Frontend polls this every few seconds to keep the counter in sync.
    """
    return {"count": state_store.get_count()}


@router.get("/violations")
def get_violations(limit: int = Query(default=50, ge=1, le=500)):
    """
    Returns the most recent violations, newest first.
    Each record includes: violation_id, frame_id, track_id, roi_id,
    frame_path, timestamp.
    """
    violations = state_store.get_violations(limit=limit)
    return {
        "count":      state_store.get_count(),
        "violations": violations,
    }