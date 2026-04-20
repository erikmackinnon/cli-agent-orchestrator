"""Codex-first orchestration event-bus starter scenario proof."""

from __future__ import annotations

import time
import uuid
from typing import Any, Dict, List

import pytest
import requests

from cli_agent_orchestrator.constants import API_BASE_URL, TERMINAL_LOG_DIR
from cli_agent_orchestrator.services.orchestration_callbacks import encode_worker_callback_marker


def _post(path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    response = requests.post(f"{API_BASE_URL}{path}", json=payload, timeout=30)
    if response.status_code == 404 and path.startswith("/orchestration/"):
        pytest.skip("Running CAO server does not expose orchestration endpoints")
    assert response.status_code == 200, f"{path} failed: {response.status_code} {response.text}"
    return response.json()


def _wait_for_events(
    *,
    run_id: str,
    cursor: int,
    min_events: int,
    max_events: int,
    wait_timeout_sec: int,
    event_types: List[str] | None = None,
    job_ids: List[str] | None = None,
    overall_timeout_sec: int = 240,
) -> Dict[str, Any]:
    deadline = time.time() + float(overall_timeout_sec)
    request_cursor = cursor
    collected: List[Dict[str, Any]] = []
    last_response: Dict[str, Any] = {}

    while time.time() < deadline:
        response = _post(
            "/orchestration/wait",
            {
                "run_id": run_id,
                "cursor": request_cursor,
                "event_types": event_types,
                "job_ids": job_ids,
                "min_events": 1,
                "max_events": max_events,
                "wait_timeout_sec": wait_timeout_sec,
                "include_snapshot": True,
            },
        )
        last_response = response
        request_cursor = response["cursor"]
        collected.extend(response.get("events", []))

        if len(collected) >= min_events:
            response["events"] = collected
            return response

        if response.get("run_status") in {"finalized", "cancelled", "timed_out", "failed"}:
            break

    raise AssertionError(
        f"Timed out waiting for {min_events} events for run {run_id}; "
        f"collected={len(collected)} last={last_response}"
    )


def _status(run_id: str) -> Dict[str, Any]:
    return _post(
        "/orchestration/status",
        {
            "run_id": run_id,
            "include_jobs": True,
            "include_terminal_refs": True,
        },
    )


def _latest_attempts_by_job(run_id: str) -> Dict[str, Dict[str, Any]]:
    attempts = _status(run_id).get("attempts") or []
    by_job: Dict[str, Dict[str, Any]] = {}
    for attempt in attempts:
        current = by_job.get(attempt["job_id"])
        if current is None or attempt["created_at"] >= current["created_at"]:
            by_job[attempt["job_id"]] = attempt
    return by_job


def _append_marker(
    *,
    run_id: str,
    job_id: str,
    attempt_id: str,
    terminal_id: str,
    status: str,
    nonce: str,
    result: Dict[str, Any],
) -> None:
    marker = encode_worker_callback_marker(
        {
            "version": 1,
            "run_id": run_id,
            "job_id": job_id,
            "attempt_id": attempt_id,
            "type": "job.completed",
            "status": status,
            "result": result,
            "nonce": nonce,
        }
    )
    log_path = TERMINAL_LOG_DIR / f"{terminal_id}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(f"{marker}\n")


def _wait_until_terminal_deleted(terminal_id: str, timeout_sec: int = 90) -> None:
    deadline = time.time() + float(timeout_sec)
    while time.time() < deadline:
        response = requests.get(f"{API_BASE_URL}/terminals/{terminal_id}", timeout=10)
        if response.status_code == 404:
            return
        time.sleep(1)
    raise AssertionError(f"Terminal {terminal_id} was not deleted within {timeout_sec}s")


def _spawn_job(
    *,
    run_id: str,
    role: str,
    message: str,
    parent_job_id: str | None = None,
    chain_id: str | None = None,
    kind: str | None = None,
) -> Dict[str, Any]:
    return _post(
        "/orchestration/spawn",
        {
            "run_id": run_id,
            "agent_profile": role,
            "message": message,
            "role": role,
            "kind": kind,
            "parent_job_id": parent_job_id,
            "chain_id": chain_id,
        },
    )


@pytest.mark.e2e
def test_codex_orchestration_starter_scenario(require_codex):
    run_name = f"e2e-orch-codex-{uuid.uuid4().hex[:8]}"

    start = _post(
        "/orchestration/start",
        {
            "name": run_name,
            "policy": {
                "max_active_workers_per_run": 4,
                "max_active_reviewers_per_run": 2,
                "max_total_terminals": 8,
                "default_job_timeout_sec": 900,
                "run_timeout_sec": 3600,
            },
            "jobs": [
                {
                    "agent_profile": "developer",
                    "message": f"Implement chain-{index}",
                    "role": "developer",
                    "kind": "implementation",
                    "chain_id": f"chain-{index}",
                }
                for index in range(1, 5)
            ],
        },
    )
    run_id = start["run_id"]
    cursor = 0

    started = _wait_for_events(
        run_id=run_id,
        cursor=cursor,
        min_events=4,
        max_events=20,
        wait_timeout_sec=45,
        event_types=["job.started"],
        overall_timeout_sec=300,
    )
    cursor = started["cursor"]
    assert started["snapshot"]["jobs_by_status"]["running"] >= 3

    attempts_by_job = _latest_attempts_by_job(run_id)
    developer_jobs = [
        job["job_id"] for job in (_status(run_id).get("jobs") or []) if job["role"] == "developer"
    ]
    assert len(developer_jobs) == 4

    for job_id in developer_jobs:
        attempt = attempts_by_job[job_id]
        _append_marker(
            run_id=run_id,
            job_id=job_id,
            attempt_id=attempt["attempt_id"],
            terminal_id=attempt["terminal_id"],
            status="succeeded",
            nonce=f"dev-{job_id}",
            result={"summary": f"developer complete {job_id}"},
        )

    developer_done = _wait_for_events(
        run_id=run_id,
        cursor=cursor,
        min_events=4,
        max_events=20,
        wait_timeout_sec=45,
        event_types=["job.succeeded"],
        job_ids=developer_jobs,
        overall_timeout_sec=300,
    )
    cursor = developer_done["cursor"]
    assert (
        len([event for event in developer_done["events"] if event["event_type"] == "job.succeeded"])
        >= 4
    )

    reviewer_jobs: List[str] = []
    for job_id in developer_jobs:
        spawned = _spawn_job(
            run_id=run_id,
            role="reviewer",
            kind="review",
            message=f"Review output from {job_id}",
            parent_job_id=job_id,
            chain_id=f"review-{job_id}",
        )
        reviewer_jobs.append(spawned["job_id"])

    reviewer_started = _wait_for_events(
        run_id=run_id,
        cursor=cursor,
        min_events=4,
        max_events=20,
        wait_timeout_sec=45,
        event_types=["job.started"],
        job_ids=reviewer_jobs,
        overall_timeout_sec=300,
    )
    cursor = reviewer_started["cursor"]

    reviewer_attempts = _latest_attempts_by_job(run_id)
    rejected_job = reviewer_jobs[0]
    for job_id in reviewer_jobs:
        attempt = reviewer_attempts[job_id]
        if job_id == rejected_job:
            _append_marker(
                run_id=run_id,
                job_id=job_id,
                attempt_id=attempt["attempt_id"],
                terminal_id=attempt["terminal_id"],
                status="failed",
                nonce=f"review-fail-{job_id}",
                result={"summary": "rejected", "reason": "needs_fix"},
            )
        else:
            _append_marker(
                run_id=run_id,
                job_id=job_id,
                attempt_id=attempt["attempt_id"],
                terminal_id=attempt["terminal_id"],
                status="succeeded",
                nonce=f"review-ok-{job_id}",
                result={"summary": "approved"},
            )

    reviewer_done = _wait_for_events(
        run_id=run_id,
        cursor=cursor,
        min_events=4,
        max_events=20,
        wait_timeout_sec=45,
        event_types=["job.succeeded", "job.failed"],
        job_ids=reviewer_jobs,
        overall_timeout_sec=300,
    )
    cursor = reviewer_done["cursor"]
    assert any(
        event["event_type"] == "job.failed" and event["job_id"] == rejected_job
        for event in reviewer_done["events"]
    )

    fix_job = _spawn_job(
        run_id=run_id,
        role="developer",
        kind="fix",
        message=f"Address reviewer rejection from {rejected_job}",
        parent_job_id=rejected_job,
        chain_id=f"fix-{rejected_job}",
    )["job_id"]

    fix_started = _wait_for_events(
        run_id=run_id,
        cursor=cursor,
        min_events=1,
        max_events=10,
        wait_timeout_sec=45,
        event_types=["job.started"],
        job_ids=[fix_job],
        overall_timeout_sec=180,
    )
    cursor = fix_started["cursor"]

    fix_attempt = _latest_attempts_by_job(run_id)[fix_job]
    _append_marker(
        run_id=run_id,
        job_id=fix_job,
        attempt_id=fix_attempt["attempt_id"],
        terminal_id=fix_attempt["terminal_id"],
        status="succeeded",
        nonce=f"fix-{fix_job}",
        result={"summary": "fix complete"},
    )

    fix_done = _wait_for_events(
        run_id=run_id,
        cursor=cursor,
        min_events=1,
        max_events=10,
        wait_timeout_sec=45,
        event_types=["job.succeeded"],
        job_ids=[fix_job],
        overall_timeout_sec=180,
    )
    cursor = fix_done["cursor"]

    review_fix_job = _spawn_job(
        run_id=run_id,
        role="reviewer",
        kind="review",
        message=f"Validate fix for {fix_job}",
        parent_job_id=fix_job,
        chain_id=f"fix-review-{fix_job}",
    )["job_id"]

    review_fix_started = _wait_for_events(
        run_id=run_id,
        cursor=cursor,
        min_events=1,
        max_events=10,
        wait_timeout_sec=45,
        event_types=["job.started"],
        job_ids=[review_fix_job],
        overall_timeout_sec=180,
    )
    cursor = review_fix_started["cursor"]

    review_fix_attempt = _latest_attempts_by_job(run_id)[review_fix_job]
    _append_marker(
        run_id=run_id,
        job_id=review_fix_job,
        attempt_id=review_fix_attempt["attempt_id"],
        terminal_id=review_fix_attempt["terminal_id"],
        status="succeeded",
        nonce=f"fix-review-{review_fix_job}",
        result={"summary": "approved"},
    )

    review_fix_done = _wait_for_events(
        run_id=run_id,
        cursor=cursor,
        min_events=1,
        max_events=10,
        wait_timeout_sec=45,
        event_types=["job.succeeded"],
        job_ids=[review_fix_job],
        overall_timeout_sec=180,
    )
    cursor = review_fix_done["cursor"]

    final_status = _status(run_id)
    assert final_status["run"]["status"] == "idle"
    assert final_status["snapshot"]["active_job_ids"] == []

    _post(
        "/orchestration/finalize",
        {
            "run_id": run_id,
            "outcome": "succeeded",
            "summary": "starter scenario complete",
        },
    )

    finalized = _wait_for_events(
        run_id=run_id,
        cursor=cursor,
        min_events=1,
        max_events=10,
        wait_timeout_sec=45,
        event_types=["run.finalized"],
        overall_timeout_sec=180,
    )
    assert any(event["event_type"] == "run.finalized" for event in finalized["events"])

    all_attempts = _status(run_id).get("attempts") or []
    terminal_ids = {
        attempt["terminal_id"] for attempt in all_attempts if attempt.get("terminal_id")
    }
    for terminal_id in terminal_ids:
        _wait_until_terminal_deleted(terminal_id)

    # Cancellation cleanup proof: cancelled runs should not leave worker terminals behind.
    cancel_run = _post(
        "/orchestration/start",
        {
            "name": f"{run_name}-cancel",
            "jobs": [
                {
                    "agent_profile": "developer",
                    "message": "cancel-me",
                    "role": "developer",
                    "kind": "implementation",
                    "chain_id": "cancel-chain",
                }
            ],
        },
    )["run_id"]

    cancel_started = _wait_for_events(
        run_id=cancel_run,
        cursor=0,
        min_events=1,
        max_events=5,
        wait_timeout_sec=45,
        event_types=["job.started"],
        overall_timeout_sec=180,
    )
    cancel_cursor = cancel_started["cursor"]
    cancel_attempts = _latest_attempts_by_job(cancel_run)
    cancel_terminal_ids = [attempt["terminal_id"] for attempt in cancel_attempts.values()]

    _post(
        "/orchestration/cancel",
        {"run_id": cancel_run, "scope": "run", "reason": "e2e cancel cleanup check"},
    )
    _wait_for_events(
        run_id=cancel_run,
        cursor=cancel_cursor,
        min_events=1,
        max_events=10,
        wait_timeout_sec=45,
        event_types=["run.cancelled"],
        overall_timeout_sec=180,
    )

    for terminal_id in cancel_terminal_ids:
        _wait_until_terminal_deleted(terminal_id)
