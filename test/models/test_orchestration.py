"""Tests for orchestration model contracts."""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from cli_agent_orchestrator.models.orchestration import (
    AttemptStatus,
    CancelScope,
    CleanupPolicy,
    EventType,
    FailedTerminalCleanupPolicy,
    JobStatus,
    OrchestrationCancelRequest,
    OrchestrationCancelResponse,
    OrchestrationEvent,
    OrchestrationEventPayload,
    OrchestrationFinalizeRequest,
    OrchestrationFinalizeResponse,
    OrchestrationSpawnRequest,
    OrchestrationSpawnResponse,
    OrchestrationStartRequest,
    OrchestrationStartResponse,
    OrchestrationStatusRequest,
    OrchestrationStatusResponse,
    OrchestrationWaitRequest,
    OrchestrationWaitResponse,
    RunOutcome,
    RunRecord,
    RunSnapshot,
    RunStatus,
    SuccessfulTerminalCleanupPolicy,
)


class TestOrchestrationEnums:
    """Tests for orchestration enum stability."""

    def test_run_status_values(self):
        assert RunStatus.RUNNING.value == "running"
        assert RunStatus.IDLE.value == "idle"
        assert RunStatus.FINALIZED.value == "finalized"

    def test_job_status_values(self):
        assert JobStatus.CREATED.value == "created"
        assert JobStatus.SUCCEEDED.value == "succeeded"
        assert JobStatus.TIMED_OUT.value == "timed_out"

    def test_attempt_status_values(self):
        assert AttemptStatus.CREATED.value == "created"
        assert AttemptStatus.RUNNING.value == "running"
        assert AttemptStatus.FAILED.value == "failed"

    def test_event_type_values(self):
        assert EventType.RUN_CREATED.value == "run.created"
        assert EventType.JOB_STARTED.value == "job.started"
        assert EventType.ATTEMPT_FAILED.value == "attempt.failed"


class TestOrchestrationValidation:
    """Tests for key orchestration model validators."""

    def test_default_enum_fields_dump_as_values(self):
        start_response = OrchestrationStartResponse(run_id="run-1")
        cleanup_policy = CleanupPolicy()

        assert start_response.model_dump()["status"] == "running"
        assert cleanup_policy.model_dump()["successful_terminals"] == "kill"
        assert cleanup_policy.model_dump()["failed_terminals"] == "preserve"

    def test_wait_request_rejects_invalid_window(self):
        with pytest.raises(ValidationError) as exc_info:
            OrchestrationWaitRequest(run_id="run-1", min_events=5, max_events=2)

        assert "max_events" in str(exc_info.value)

    def test_cancel_request_requires_job_id_for_job_scope(self):
        with pytest.raises(ValidationError) as exc_info:
            OrchestrationCancelRequest(run_id="run-1", scope=CancelScope.JOB)

        assert "job_id" in str(exc_info.value)

    def test_cancel_request_rejects_attempt_scope(self):
        with pytest.raises(ValidationError) as exc_info:
            OrchestrationCancelRequest(run_id="run-1", scope="attempt")

        assert "scope" in str(exc_info.value)

    def test_cancel_request_rejects_subtree_scope(self):
        with pytest.raises(ValidationError) as exc_info:
            OrchestrationCancelRequest(run_id="run-1", scope="subtree")

        assert "scope" in str(exc_info.value)

    def test_cleanup_policy_requires_ttl_for_success_preserve_until_ttl(self):
        with pytest.raises(ValidationError) as exc_info:
            CleanupPolicy(successful_terminals=SuccessfulTerminalCleanupPolicy.PRESERVE_UNTIL_TTL)

        assert "ttl_sec" in str(exc_info.value)


class TestOrchestrationSurfaceContracts:
    """Tests for the shared start/spawn/wait/status/cancel/finalize contracts."""

    def test_start_surface_contract(self):
        request = OrchestrationStartRequest(name="fanout-run")
        response = OrchestrationStartResponse(run_id="run-1", job_ids=["job-1"])

        assert request.name == "fanout-run"
        assert response.status == "running"

    def test_spawn_surface_contract(self):
        request = OrchestrationSpawnRequest(
            run_id="run-1",
            agent_profile="developer",
            message="Implement feature",
        )
        response = OrchestrationSpawnResponse(
            run_id="run-1",
            job_id="job-1",
            attempt_id="attempt-1",
            status=JobStatus.QUEUED,
        )

        assert request.agent_profile == "developer"
        assert response.status == "queued"

    def test_wait_surface_contract(self):
        now = datetime.now(timezone.utc)
        event = OrchestrationEvent(
            event_id=1,
            run_id="run-1",
            event_type=EventType.JOB_STARTED,
            created_at=now,
            payload=OrchestrationEventPayload(),
            job_id="job-1",
        )
        response = OrchestrationWaitResponse(
            run_id="run-1",
            cursor=1,
            events=[event],
            timeout=False,
            run_status=RunStatus.RUNNING,
        )

        assert response.events[0].event_type == "job.started"
        assert response.run_status == "running"

    def test_status_surface_contract(self):
        now = datetime.now(timezone.utc)
        run = RunRecord(
            run_id="run-1",
            name="Fanout",
            status=RunStatus.RUNNING,
            created_at=now,
            updated_at=now,
        )
        snapshot = RunSnapshot(run_id="run-1", run_status=RunStatus.RUNNING)
        request = OrchestrationStatusRequest(run_id="run-1", include_jobs=True)
        response = OrchestrationStatusResponse(run=run, snapshot=snapshot)

        assert request.include_jobs is True
        assert response.run.run_id == "run-1"

    def test_snapshot_status_counts_use_typed_contract(self):
        snapshot = RunSnapshot(
            run_id="run-1",
            run_status=RunStatus.RUNNING,
            jobs_by_status={"running": 2, "succeeded": 1},
            attempts_by_status={"running": 2, "failed": 1},
        )

        assert snapshot.jobs_by_status.running == 2
        assert snapshot.jobs_by_status.succeeded == 1
        assert snapshot.attempts_by_status.failed == 1
        assert snapshot.model_dump()["jobs_by_status"]["running"] == 2

    def test_snapshot_status_counts_reject_unknown_status_keys(self):
        with pytest.raises(ValidationError) as exc_info:
            RunSnapshot(
                run_id="run-1",
                run_status=RunStatus.RUNNING,
                jobs_by_status={"unknown": 1},
            )

        assert "jobs_by_status.unknown" in str(exc_info.value)

    def test_cancel_surface_contract(self):
        now = datetime.now(timezone.utc)
        event = OrchestrationEvent(
            event_id=2,
            run_id="run-1",
            event_type=EventType.JOB_CANCELLED,
            created_at=now,
        )
        request = OrchestrationCancelRequest(
            run_id="run-1",
            scope=CancelScope.RUN,
            reason="operator_cancelled",
        )
        response = OrchestrationCancelResponse(cancelled_jobs=["job-1"], events=[event])

        assert request.scope == "run"
        assert response.cancelled_jobs == ["job-1"]

    def test_finalize_surface_contract(self):
        now = datetime.now(timezone.utc)
        cleanup_policy = CleanupPolicy(
            successful_terminals=SuccessfulTerminalCleanupPolicy.PRESERVE_UNTIL_TTL,
            failed_terminals=FailedTerminalCleanupPolicy.PRESERVE,
            ttl_sec=300,
        )
        request = OrchestrationFinalizeRequest(
            run_id="run-1",
            outcome=RunOutcome.SUCCEEDED,
            cleanup_policy=cleanup_policy,
        )
        response = OrchestrationFinalizeResponse(
            run_id="run-1",
            status=RunStatus.FINALIZED,
            outcome=RunOutcome.SUCCEEDED,
            finalized_at=now,
            cleanup_policy=cleanup_policy,
        )

        assert request.outcome == "succeeded"
        assert response.cleanup_policy is not None
        assert response.cleanup_policy.successful_terminals == "preserve_until_ttl"
