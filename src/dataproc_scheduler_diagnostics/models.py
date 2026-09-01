# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Dict, List, Optional
from pydantic import BaseModel, Field


class JobScheduleParams(BaseModel):
    """Parameters required to render and schedule an Airflow DAG notebook job."""
    name: str = Field(..., description="Unique job identifier")
    input_filename: str = Field(default="Basic Spark.ipynb", description="Input notebook filename or GCS URI")
    cluster_name: str = Field(default="pyspark-cluster-dev-multitenant", description="Target Dataproc cluster")
    composer_bucket: str = Field(default="", description="Target Composer GCS bucket name")
    mode_selected: str = Field(default="cluster", description="Execution mode: 'cluster' or 'serverless'")
    local_kernel: bool = Field(default=False, description="Whether job runs locally in Airflow worker")
    schedule_value: str = Field(default="@once", description="Cron schedule expression or @once")
    retry_count: int = Field(default=2, description="Number of retries on failure")
    retry_delay: int = Field(default=5, description="Retry delay in minutes")
    email_failure: bool = Field(default=False, description="Send email alert on failure")
    email_delay: bool = Field(default=False, description="Send email alert on retry")
    email_success: bool = Field(default=False, description="Send email alert on success")
    email: List[str] = Field(default_factory=list, description="Notification email recipients")
    parameters: List[str] = Field(default_factory=list, description="Notebook papermill parameters")
    time_zone: str = Field(default="", description="Schedule time zone")


class DiagnosticResult(BaseModel):
    """Diagnostic audit result containing details of the impersonation resolution chain."""
    cluster_name: str
    user_email: str
    cluster_accessible: bool
    dynamic_multi_tenancy_raw: Optional[str] = None
    dynamic_multi_tenancy_enabled: bool = False
    user_service_account_mapping: Dict[str, str] = Field(default_factory=dict)
    resolved_target_sa: str = ""
    token_generation_success: bool = False
    skip_reason: Optional[str] = None


class RenderResult(BaseModel):
    """Result of DAG template rendering."""
    template_used: str
    multi_tenant_service_account: str
    has_impersonation_chain: bool
    diagnostics: DiagnosticResult
    dag_content: str
