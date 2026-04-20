"""Typed orchestration contracts for run/job/attempt/event surfaces."""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator


class RunStatus(str, Enum):
    """Run lifecycle state."""

    RUNNING = "running"
    IDLE = "idle"
    FINALIZED = "finalized"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"
    FAILED = "failed"


class JobStatus(str, Enum):
    """Job lifecycle state."""

    CREATED = "created"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"


class AttemptStatus(str, Enum):
    """Attempt lifecycle state."""

    CREATED = "created"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"


class EventType(str, Enum):
    """Durable orchestration event kinds."""

    RUN_CREATED = "run.created"
    RUN_STATUS_CHANGED = "run.status_changed"
    RUN_IDLE = "run.idle"
    RUN_FINALIZED = "run.finalized"
    RUN_CANCELLED = "run.cancelled"
    RUN_TIMED_OUT = "run.timed_out"
    JOB_CREATED = "job.created"
    JOB_QUEUED = "job.queued"
    JOB_STARTED = "job.started"
    JOB_SUCCEEDED = "job.succeeded"
    JOB_FAILED = "job.failed"
    JOB_CANCELLED = "job.cancelled"
    JOB_TIMED_OUT = "job.timed_out"
    ATTEMPT_STARTED = "attempt.started"
    ATTEMPT_SUCCEEDED = "attempt.succeeded"
    ATTEMPT_FAILED = "attempt.failed"
    ATTEMPT_CANCELLED = "attempt.cancelled"
    ATTEMPT_TIMED_OUT = "attempt.timed_out"


class CancelScope(str, Enum):
    """Cancellation scope for orchestration operations."""

    RUN = "run"
    JOB = "job"


class RunOutcome(str, Enum):
    """Final run outcome."""

    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class SuccessfulTerminalCleanupPolicy(str, Enum):
    """Cleanup policy for successful terminals."""

    KILL = "kill"
    PRESERVE_UNTIL_TTL = "preserve_until_ttl"


class FailedTerminalCleanupPolicy(str, Enum):
    """Cleanup policy for failed terminals."""

    PRESERVE = "preserve"
    KILL = "kill"


class OrchestrationModel(BaseModel):
    """Shared model config for orchestration contracts."""

    model_config = ConfigDict(use_enum_values=True, validate_default=True, extra="forbid")


class CleanupPolicy(OrchestrationModel):
    """Run finalization cleanup policy."""

    successful_terminals: SuccessfulTerminalCleanupPolicy = Field(
        default=SuccessfulTerminalCleanupPolicy.KILL,
        description="Cleanup behavior for successful worker terminals.",
    )
    failed_terminals: FailedTerminalCleanupPolicy = Field(
        default=FailedTerminalCleanupPolicy.PRESERVE,
        description="Cleanup behavior for failed worker terminals.",
    )
    ttl_sec: Optional[int] = Field(
        default=None,
        ge=1,
        description="Optional retention window (seconds) for preserve-until-ttl cleanup.",
    )

    @model_validator(mode="after")
    def validate_success_ttl_policy(self) -> "CleanupPolicy":
        """Require a TTL when successful terminals are preserved until TTL."""
        if (
            self.successful_terminals == SuccessfulTerminalCleanupPolicy.PRESERVE_UNTIL_TTL
            and self.ttl_sec is None
        ):
            raise ValueError("ttl_sec is required when successful_terminals=preserve_until_ttl")
        return self


class RunStatePayload(OrchestrationModel):
    """Typed run-state event payload."""

    status: RunStatus
    previous_status: Optional[RunStatus] = None
    reason: Optional[str] = None


class JobStatePayload(OrchestrationModel):
    """Typed job-state event payload."""

    status: JobStatus
    role: Optional[str] = None
    kind: Optional[str] = None
    parent_job_id: Optional[str] = None
    chain_id: Optional[str] = None
    terminal_id: Optional[str] = None
    reason: Optional[str] = None


class AttemptStatePayload(OrchestrationModel):
    """Typed attempt-state event payload."""

    status: AttemptStatus
    terminal_id: Optional[str] = None
    nonce: Optional[str] = None
    result_summary: Optional[str] = None
    result_data: Optional[Dict[str, Any]] = None


class CancellationPayload(OrchestrationModel):
    """Typed cancellation payload."""

    scope: CancelScope
    reason: Optional[str] = None
    cancelled_jobs: List[str] = Field(default_factory=list)
    cancelled_attempts: List[str] = Field(default_factory=list)


class FinalizationPayload(OrchestrationModel):
    """Typed finalization payload."""

    outcome: RunOutcome
    summary: Optional[str] = None
    cleanup_policy: Optional[CleanupPolicy] = None


class OrchestrationEventPayload(OrchestrationModel):
    """Envelope for typed event payload slices."""

    run: Optional[RunStatePayload] = None
    job: Optional[JobStatePayload] = None
    attempt: Optional[AttemptStatePayload] = None
    cancellation: Optional[CancellationPayload] = None
    finalization: Optional[FinalizationPayload] = None
    metadata: Optional[Dict[str, Any]] = None


class RunRecord(OrchestrationModel):
    """Durable orchestration run record."""

    run_id: str
    name: Optional[str] = None
    status: RunStatus
    metadata: Optional[Dict[str, Any]] = None
    created_at: datetime
    updated_at: datetime
    finalized_at: Optional[datetime] = None


class JobRecord(OrchestrationModel):
    """Durable orchestration job record."""

    job_id: str
    run_id: str
    agent_profile: str
    message: str
    status: JobStatus
    role: Optional[str] = None
    kind: Optional[str] = None
    parent_job_id: Optional[str] = None
    chain_id: Optional[str] = None
    timeout_sec: Optional[int] = Field(default=None, ge=1)
    metadata: Optional[Dict[str, Any]] = None
    created_at: datetime
    updated_at: datetime


class AttemptRecord(OrchestrationModel):
    """Durable orchestration attempt record."""

    attempt_id: str
    run_id: str
    job_id: str
    status: AttemptStatus
    terminal_id: Optional[str] = None
    nonce: Optional[str] = None
    result_summary: Optional[str] = None
    result_data: Optional[Dict[str, Any]] = None
    created_at: datetime
    updated_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class OrchestrationEvent(OrchestrationModel):
    """Durable append-only orchestration event."""

    event_id: int
    run_id: str
    event_type: EventType
    created_at: datetime
    payload: OrchestrationEventPayload = Field(default_factory=OrchestrationEventPayload)
    job_id: Optional[str] = None
    attempt_id: Optional[str] = None


class JobStatusCounts(OrchestrationModel):
    """Typed counts for job statuses in a run snapshot."""

    created: int = Field(default=0, ge=0)
    queued: int = Field(default=0, ge=0)
    running: int = Field(default=0, ge=0)
    succeeded: int = Field(default=0, ge=0)
    failed: int = Field(default=0, ge=0)
    cancelled: int = Field(default=0, ge=0)
    timed_out: int = Field(default=0, ge=0)


class AttemptStatusCounts(OrchestrationModel):
    """Typed counts for attempt statuses in a run snapshot."""

    created: int = Field(default=0, ge=0)
    running: int = Field(default=0, ge=0)
    succeeded: int = Field(default=0, ge=0)
    failed: int = Field(default=0, ge=0)
    cancelled: int = Field(default=0, ge=0)
    timed_out: int = Field(default=0, ge=0)


class RunSnapshot(OrchestrationModel):
    """Aggregated run state for status/wait responses."""

    run_id: str
    run_status: RunStatus
    jobs_total: int = 0
    attempts_total: int = 0
    jobs_by_status: JobStatusCounts = Field(default_factory=JobStatusCounts)
    attempts_by_status: AttemptStatusCounts = Field(default_factory=AttemptStatusCounts)
    active_job_ids: List[str] = Field(default_factory=list)
    active_attempt_ids: List[str] = Field(default_factory=list)
    latest_event_cursor: Optional[int] = None


class OrchestrationStartPolicy(OrchestrationModel):
    """Execution policy defaults for a run."""

    max_active_workers_per_run: Optional[int] = Field(default=None, ge=1)
    max_active_reviewers_per_run: Optional[int] = Field(default=None, ge=1)
    max_total_terminals: Optional[int] = Field(default=None, ge=1)
    default_job_timeout_sec: Optional[int] = Field(default=None, ge=1)
    run_timeout_sec: Optional[int] = Field(default=None, ge=1)
    fail_fast: Optional[bool] = None
    debug_retention: Optional[bool] = None


class OrchestrationJobSpec(OrchestrationModel):
    """Shared job spec used by start/spawn surfaces."""

    agent_profile: str
    message: str
    parent_job_id: Optional[str] = None
    chain_id: Optional[str] = None
    role: Optional[str] = None
    kind: Optional[str] = None
    timeout_sec: Optional[int] = Field(default=None, ge=1)
    idempotency_key: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class OrchestrationStartRequest(OrchestrationModel):
    """Request contract for orchestration_start."""

    name: Optional[str] = None
    jobs: List[OrchestrationJobSpec] = Field(default_factory=list)
    policy: Optional[OrchestrationStartPolicy] = None
    metadata: Optional[Dict[str, Any]] = None


class OrchestrationStartResponse(OrchestrationModel):
    """Response contract for orchestration_start."""

    run_id: str
    job_ids: List[str] = Field(default_factory=list)
    status: RunStatus = RunStatus.RUNNING


class OrchestrationSpawnRequest(OrchestrationModel):
    """Request contract for orchestration_spawn."""

    run_id: str
    agent_profile: str
    message: str
    parent_job_id: Optional[str] = None
    chain_id: Optional[str] = None
    role: Optional[str] = None
    kind: Optional[str] = None
    timeout_sec: Optional[int] = Field(default=None, ge=1)
    idempotency_key: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class OrchestrationSpawnResponse(OrchestrationModel):
    """Response contract for orchestration_spawn."""

    run_id: str
    job_id: str
    attempt_id: str
    status: JobStatus
    terminal_id: Optional[str] = None


class OrchestrationWaitRequest(OrchestrationModel):
    """Request contract for orchestration_wait."""

    run_id: str
    cursor: Optional[int] = None
    event_types: Optional[List[EventType]] = None
    job_ids: Optional[List[str]] = None
    min_events: int = Field(default=1, ge=1)
    max_events: int = Field(default=10, ge=1)
    wait_timeout_sec: int = Field(default=45, ge=1)
    include_snapshot: bool = True

    @model_validator(mode="after")
    def validate_event_window(self) -> "OrchestrationWaitRequest":
        """Ensure max_events is at least min_events."""
        if self.max_events < self.min_events:
            raise ValueError("max_events must be greater than or equal to min_events")
        return self


class OrchestrationWaitResponse(OrchestrationModel):
    """Response contract for orchestration_wait."""

    run_id: str
    cursor: int
    events: List[OrchestrationEvent] = Field(default_factory=list)
    timeout: bool = False
    run_status: RunStatus
    snapshot: Optional[RunSnapshot] = None


class OrchestrationStatusRequest(OrchestrationModel):
    """Request contract for orchestration_status."""

    run_id: str
    include_jobs: bool = False
    include_events_since: Optional[int] = None
    include_terminal_refs: bool = False


class OrchestrationStatusResponse(OrchestrationModel):
    """Response contract for orchestration_status."""

    run: RunRecord
    snapshot: RunSnapshot
    jobs: Optional[List[JobRecord]] = None
    attempts: Optional[List[AttemptRecord]] = None
    events: Optional[List[OrchestrationEvent]] = None


class OrchestrationCancelRequest(OrchestrationModel):
    """Request contract for orchestration_cancel."""

    run_id: str
    scope: CancelScope
    job_id: Optional[str] = None
    reason: Optional[str] = None

    @model_validator(mode="after")
    def validate_scope_requirements(self) -> "OrchestrationCancelRequest":
        """Enforce scope-specific required identifiers."""
        if self.scope == CancelScope.JOB and not self.job_id:
            raise ValueError("job_id is required when scope=job")
        return self


class OrchestrationCancelResponse(OrchestrationModel):
    """Response contract for orchestration_cancel."""

    cancelled_jobs: List[str] = Field(default_factory=list)
    cancelled_attempts: List[str] = Field(default_factory=list)
    events: List[OrchestrationEvent] = Field(default_factory=list)


class OrchestrationFinalizeRequest(OrchestrationModel):
    """Request contract for orchestration_finalize."""

    run_id: str
    outcome: RunOutcome
    summary: Optional[str] = None
    cleanup_policy: Optional[CleanupPolicy] = None


class OrchestrationFinalizeResponse(OrchestrationModel):
    """Response contract for orchestration_finalize."""

    run_id: str
    status: RunStatus
    outcome: RunOutcome
    finalized_at: datetime
    summary: Optional[str] = None
    cleanup_policy: Optional[CleanupPolicy] = None
