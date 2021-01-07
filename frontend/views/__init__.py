from .jobs import JobsView, JobStdOutView, JobStdErrView, JobRemoveFromQueue, JobSnapshot
from .workflows import WorkflowsView, WorkflowStdOutView, WorkflowStdErrView
from .health import HealthView
from .usage import UsageView
from .data import DataView
from .web import index, terms_of_use, privacy_policy, storage, storage_add, storage_update, storage_delete, jobs, workflows, create_token, revoke_token, compute, compute_add, compute_update, compute_delete, job_create, job_delete, job_describe, job_json, job_usage, user_usage, job_logs, workflow, workflow_delete, refresh_authorise, refresh_callback
