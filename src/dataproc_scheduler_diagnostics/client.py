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

import logging
import os
import subprocess
import warnings
from typing import Any, Dict, Optional, Tuple

import aiohttp
import tornado.ioloop

warnings.filterwarnings("ignore")

# Strict requirement: Must be running in an environment with scheduler_jupyter_plugin installed
try:
    import scheduler_jupyter_plugin
    from scheduler_jupyter_plugin import credentials as plugin_credentials
    from scheduler_jupyter_plugin.models.models import DescribeJob as PluginDescribeJob
    from scheduler_jupyter_plugin.services import airflow as plugin_airflow
    from scheduler_jupyter_plugin.services import executor as plugin_executor
    HAS_PLUGIN = True
    PLUGIN_VERSION = getattr(scheduler_jupyter_plugin, "__version__", "0.1.7")
except ImportError as e:
    HAS_PLUGIN = False
    PLUGIN_VERSION = None
    IMPORT_ERROR = str(e)

from .models import DiagnosticResult, JobScheduleParams


def check_plugin_installed():
    """Validates that scheduler_jupyter_plugin is installed in the active Python environment."""
    if not HAS_PLUGIN:
        raise RuntimeError(
            "CRITICAL: 'scheduler_jupyter_plugin' is NOT installed in this Python environment.\n"
            f"Import error: {IMPORT_ERROR}\n\n"
            "This tool is a pure native harness and requires the official plugin.\n"
            "Please run using the JupyterLab Python runtime inside your Workbench instance:\n"
            "  /opt/micromamba/envs/jupyterlab/bin/python3 -m dataproc_scheduler_diagnostics.cli <ACTION>\n"
        )


class NativePluginHarness:
    """Pure native harness for scheduler_jupyter_plugin on Vertex AI Workbench."""

    def __init__(self, verbose: bool = False):
        check_plugin_installed()
        self.verbose = verbose
        self.logger = logging.getLogger("SchedulerPluginHarness")
        if verbose:
            logging.basicConfig(level=logging.INFO)

    def diagnose(self, cluster_name: str) -> DiagnosticResult:
        """Runs pre-flight diagnostics by invoking the installed plugin services directly."""
        async def _async_diagnose():
            async with aiohttp.ClientSession() as session:
                creds = await plugin_credentials.get_cached()
                client = plugin_executor.Client(creds, self.logger, session)

                project_id = creds.get("project_id", "")
                region_id = creds.get("region_id", "")

                # 1. Fetch cluster details from Dataproc REST API via plugin
                cluster_data = await client.get_cluster_details(cluster_name)
                cluster_accessible = "error" not in cluster_data

                raw_props = (
                    cluster_data.get("config", {})
                    .get("softwareConfig", {})
                    .get("properties", {})
                ) if cluster_accessible else {}

                mt_enabled = raw_props.get("dataproc:dataproc.dynamic.multi.tenancy.enabled") == "true"

                mapping = (
                    cluster_data.get("config", {})
                    .get("securityConfig", {})
                    .get("identityConfig", {})
                    .get("userServiceAccountMapping", {})
                ) if cluster_accessible else {}

                # 2. Query active gcloud account
                cmd = "config get account"
                acc_proc = await plugin_executor.async_run_gcloud_subcommand(cmd)
                active_user = acc_proc.strip() if acc_proc else "unknown"

                # 3. Call official plugin's multi_tenant_user_service_account
                target_sa = await client.multi_tenant_user_service_account(cluster_name)

                # 4. Probe Token Creator IAM permissions if target SA resolved
                token_ok = False
                if target_sa:
                    probe_cmd = f"gcloud auth print-access-token --impersonate-service-account={target_sa}"
                    res = subprocess.run(probe_cmd, shell=True, capture_output=True, text=True)
                    token_ok = "ya29" in res.stdout

                skip_reason = None
                if not target_sa:
                    if not cluster_accessible:
                        skip_reason = f"Dataproc API error: {cluster_data.get('error')}"
                    elif not mt_enabled:
                        skip_reason = f"Property 'dataproc:dataproc.dynamic.multi.tenancy.enabled' is '{raw_props.get('dataproc:dataproc.dynamic.multi.tenancy.enabled')}' (must be exact string 'true')"
                    elif active_user not in mapping:
                        skip_reason = f"Active account '{active_user}' not found in cluster userServiceAccountMapping (Available: {list(mapping.keys())})"
                    else:
                        skip_reason = "Unknown resolution failure in official plugin logic"

                return DiagnosticResult(
                    plugin_version=PLUGIN_VERSION,
                    cluster_name=cluster_name,
                    active_account=active_user,
                    project_id=project_id,
                    region_id=region_id,
                    cluster_accessible=cluster_accessible,
                    dynamic_multi_tenancy_enabled=mt_enabled,
                    raw_properties=raw_props,
                    user_service_account_mapping=mapping,
                    resolved_target_sa=target_sa,
                    impersonation_chain_injected=bool(target_sa),
                    token_creator_verified=token_ok,
                    skip_reason=skip_reason,
                )

        return tornado.ioloop.IOLoop.current().run_sync(_async_diagnose)

    def render(self, params: JobScheduleParams) -> Tuple[str, bool, Optional[str]]:
        """Renders the Airflow DAG using the installed plugin's internal prepare_dag pipeline."""
        async def _async_render():
            async with aiohttp.ClientSession() as session:
                creds = await plugin_credentials.get_cached()
                client = plugin_executor.Client(creds, self.logger, session)
                project_id = creds.get("project_id", "")
                region_id = creds.get("region_id", "")

                payload = params.to_plugin_payload()
                job = PluginDescribeJob(**payload)

                dag_file_name = f"dag_{job.name}.py"
                mock_bucket = "composer-dryrun-bucket"

                # Execute official prepare_dag method from scheduler_jupyter_plugin
                dag_file_path = await client.prepare_dag(job, mock_bucket, dag_file_name, project_id, region_id)

                with open(dag_file_path, "r", encoding="utf-8") as f:
                    dag_content = f.read()

                has_impersonation = "impersonation_chain" in dag_content
                return dag_content, has_impersonation, dag_file_path

        return tornado.ioloop.IOLoop.current().run_sync(_async_render)

    def schedule(self, params: JobScheduleParams) -> Dict[str, Any]:
        """Live schedules the notebook job via the installed plugin execute pipeline."""
        async def _async_schedule():
            async with aiohttp.ClientSession() as session:
                creds = await plugin_credentials.get_cached()
                client = plugin_executor.Client(creds, self.logger, session)
                project_id = creds.get("project_id", "")
                region_id = creds.get("region_id", "")

                payload = params.to_plugin_payload()
                return await client.execute(payload, project_id, region_id)

        return tornado.ioloop.IOLoop.current().run_sync(_async_schedule)
