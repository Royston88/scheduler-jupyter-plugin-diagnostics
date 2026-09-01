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
    dag_id: Optional[str] = Field(default=None, description="Airflow DAG ID (defaults to dag_<name>)")
    composer_environment_name: str = Field(default="my-airflow-composer", description="Target Composer environment name")
    input_filename: str = Field(default="Basic Spark.ipynb", description="Input notebook filename")
    cluster_name: str = Field(default="pyspark-cluster-dev-multitenant", description="Target Dataproc cluster")
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

    def to_plugin_payload(self) -> Dict:
        """Converts to payload dictionary expected by scheduler_jupyter_plugin."""
        dag_id = self.dag_id or f"dag_{self.name}"
        return {
            "name": self.name,
            "dag_id": dag_id,
            "composer_environment_name": self.composer_environment_name,
            "input_filename": self.input_filename,
            "cluster_name": self.cluster_name,
            "mode_selected": self.mode_selected,
            "selected_mode": self.mode_selected,
            "local_kernel": self.local_kernel,
            "schedule_value": self.schedule_value,
            "retry_count": self.retry_count,
            "retry_delay": self.retry_delay,
            "email_failure": self.email_failure,
            "email_delay": self.email_delay,
            "email_success": self.email_success,
            "email": self.email,
            "parameters": self.parameters,
            "time_zone": self.time_zone,
            "output_formats": ["html"],
            "stop_cluster": False,
            "packages_to_install": [],
            "serverless_name": {},
        }


class DiagnosticResult(BaseModel):
    """Result of native pre-flight diagnostic check."""
    plugin_version: str
    cluster_name: str
    active_account: str
    project_id: str
    region_id: str
    cluster_accessible: bool
    dynamic_multi_tenancy_enabled: bool = False
    raw_properties: Dict[str, str] = Field(default_factory=dict)
    user_service_account_mapping: Dict[str, str] = Field(default_factory=dict)
    resolved_target_sa: str = ""
    impersonation_chain_injected: bool = False
    token_creator_verified: bool = False
    skip_reason: Optional[str] = None
