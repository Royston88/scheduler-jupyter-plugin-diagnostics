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

import json
import os
import subprocess
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple

import jinja2

from .models import DiagnosticResult, JobScheduleParams, RenderResult

TEMPLATES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")


class DiagnosticEngine:
    """Core engine for verifying Dataproc dynamic multi-tenancy impersonation chains and rendering DAGs."""

    def __init__(
        self,
        project_id: Optional[str] = None,
        region_id: Optional[str] = None,
        user_email_override: Optional[str] = None,
        verbose: bool = False,
    ):
        self.project_id = project_id or self._get_gcloud_config("project") or ""
        self.region_id = (
            region_id
            or self._get_gcloud_config("dataproc/region")
            or self._get_gcloud_config("compute/region")
            or "us-central1"
        )
        self.user_email_override = user_email_override
        self.verbose = verbose

        self.jinja_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(TEMPLATES_DIR),
            autoescape=jinja2.select_autoescape(["py", "txt"]),
        )

    def _get_gcloud_config(self, key: str) -> Optional[str]:
        """Reads configuration properties from local gcloud environment."""
        try:
            res = subprocess.run(
                f"gcloud config get {key}",
                shell=True,
                capture_output=True,
                text=True,
            )
            val = res.stdout.strip()
            return val if val and val != "(unset)" else None
        except Exception:
            return None

    def get_active_user(self) -> str:
        """Resolves the active user identity."""
        if self.user_email_override:
            return self.user_email_override
        acc = self._get_gcloud_config("account")
        return acc or "unknown-user@example.com"

    def get_cluster_details(
        self, cluster_name: str, force_cluster_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Fetches Dataproc cluster configuration metadata."""
        if force_cluster_data is not None:
            return force_cluster_data
        try:
            cmd = f"gcloud dataproc clusters describe {cluster_name} --region={self.region_id} --project={self.project_id} --format=json"
            res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            if res.returncode == 0:
                return json.loads(res.stdout)
            else:
                return {"error": f"Failed to describe cluster: {res.stderr.strip()}"}
        except Exception as e:
            return {"error": str(e)}

    def resolve_multi_tenant_sa(
        self,
        cluster_name: str,
        force_cluster_data: Optional[Dict[str, Any]] = None,
        force_user_email: Optional[str] = None,
    ) -> Tuple[str, DiagnosticResult]:
        """
        Replicates the exact decision tree used by scheduler-jupyter-plugin
        to resolve service account impersonation.
        """
        user_email = force_user_email or self.get_active_user()
        cluster_data = self.get_cluster_details(cluster_name, force_cluster_data=force_cluster_data)

        diag = DiagnosticResult(
            cluster_name=cluster_name,
            user_email=user_email,
            cluster_accessible="error" not in cluster_data,
        )

        if "error" in cluster_data:
            diag.skip_reason = f"Dataproc API error: {cluster_data['error']}"
            return "", diag

        properties = cluster_data.get("config", {}).get("softwareConfig", {}).get("properties", {})
        mt_val = properties.get("dataproc:dataproc.dynamic.multi.tenancy.enabled", "false")
        diag.dynamic_multi_tenancy_raw = mt_val

        # Exact plugin rule: case-sensitive string check == "true"
        if mt_val == "true":
            diag.dynamic_multi_tenancy_enabled = True
            mapping = (
                cluster_data.get("config", {})
                .get("securityConfig", {})
                .get("identityConfig", {})
                .get("userServiceAccountMapping", {})
            )
            diag.user_service_account_mapping = mapping
            target_sa = mapping.get(user_email, "")
            diag.resolved_target_sa = target_sa

            if target_sa:
                return target_sa, diag
            else:
                diag.skip_reason = (
                    f"User '{user_email}' not found in cluster userServiceAccountMapping "
                    f"(Available mapped accounts: {list(mapping.keys())})"
                )
                return "", diag
        else:
            diag.skip_reason = (
                f"Dataproc property 'dataproc:dataproc.dynamic.multi.tenancy.enabled' is '{mt_val}' (expected 'true')"
            )
            return "", diag

    def probe_token_creator_permission(self, target_sa: str) -> bool:
        """Validates if caller has Token Creator permission on the target service account."""
        if not target_sa:
            return False
        try:
            cmd = f"gcloud auth print-access-token --impersonate-service-account={target_sa}"
            res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            return "ya29" in res.stdout
        except Exception:
            return False

    def render_dag(
        self,
        params: JobScheduleParams,
        force_cluster_data: Optional[Dict[str, Any]] = None,
        force_user_email: Optional[str] = None,
    ) -> RenderResult:
        """Renders the Airflow DAG file using the plugin Jinja template."""
        user = force_user_email or self.get_active_user()
        owner = user.split("@")[0]
        schedule_interval = params.schedule_value or "@once"
        start_date = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

        multi_tenant_sa, diag = self.resolve_multi_tenant_sa(
            cluster_name=params.cluster_name,
            force_cluster_data=force_cluster_data,
            force_user_email=user,
        )

        template_name = "pysparkJobTemplate-v1.txt"
        if params.local_kernel:
            template_name = "localPythonTemplate-v1.txt"
        elif params.mode_selected == "serverless":
            template_name = "pysparkBatchTemplate-v1.txt"

        template = self.jinja_env.get_template(template_name)
        bucket = params.composer_bucket or "composer-bucket-placeholder"
        input_notebook = f"gs://{bucket}/dataproc-notebooks/{params.name}/input_notebooks/{params.input_filename}"

        content = template.render(
            params.model_dump(),
            name=params.name,
            inputFilePath=f"gs://{bucket}/dataproc-notebooks/wrapper_papermill.py",
            gcpProjectId=self.project_id,
            gcpRegion=self.region_id,
            input_notebook=input_notebook,
            output_notebook=f"gs://{bucket}/dataproc-output/{params.name}/output-notebooks/{params.name}_",
            owner=owner,
            schedule_interval=schedule_interval,
            start_date=start_date,
            parameters="\n".join(params.parameters) if params.parameters else "",
            time_zone=params.time_zone,
            multi_tenant_service_account=multi_tenant_sa,
            cluster_name=params.cluster_name,
        )

        has_impersonation = "impersonation_chain" in content
        return RenderResult(
            template_used=template_name,
            multi_tenant_service_account=multi_tenant_sa,
            has_impersonation_chain=has_impersonation,
            diagnostics=diag,
            dag_content=content,
        )
