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

import argparse
import json
import os
import sys
from typing import List

from .client import NativePluginHarness
from .models import JobScheduleParams


def run_diagnose_action(harness: NativePluginHarness, cluster_name: str, as_json: bool = False):
    diag = harness.diagnose(cluster_name)

    if as_json:
        if hasattr(diag, "model_dump_json"):
            print(diag.model_dump_json(indent=2))
        else:
            print(diag.json(indent=2))
        return

    print("=" * 65)
    print("   NATIVE SCHEDULER PLUGIN PRE-FLIGHT & IMPERSONATION AUDIT      ")
    print("=" * 65)
    print(f"Plugin Version : {diag.plugin_version} (Official Installed Package)")
    print(f"Project ID     : {diag.project_id}")
    print(f"Region ID      : {diag.region_id}")
    print(f"Target Cluster : {diag.cluster_name}")
    print(f"Active Account : {diag.active_account}")
    print("-" * 65)
    print(f"1. Dataproc Cluster Accessible : {'[✓] PASS' if diag.cluster_accessible else '[✗] FAIL'}")
    mt_raw = diag.raw_properties.get("dataproc:dataproc.dynamic.multi.tenancy.enabled", "not set")
    print(f"2. Dynamic Multi-Tenancy Value : '{mt_raw}'")
    print(f"   -> Multi-Tenancy Enabled    : {'[✓] PASS' if diag.dynamic_multi_tenancy_enabled else '[✗] FAIL'}")
    print(f"3. User Mapping Configured     : {json.dumps(diag.user_service_account_mapping, indent=2)}")
    
    sa_display = diag.resolved_target_sa if diag.resolved_target_sa else "[None]"
    print(f"4. Target Service Account      : '{sa_display}'")

    if diag.impersonation_chain_injected:
        print(f"   -> Impersonation Chain Status: [✓] INJECTED ({diag.resolved_target_sa})")
        print("5. Probing Token Creator IAM Permissions...")
        status_str = "[✓] SUCCESS" if diag.token_creator_verified else "[✗] FAILED (Check Token Creator role on Target SA)"
        print(f"   -> Token Generation Test    : {status_str}")
    else:
        print("   -> Impersonation Chain Status: [✗] NOT INJECTED (Empty string returned)")
        print(f"   -> Skip Reason: {diag.skip_reason}")
    print("=" * 65)


def run_render_action(harness: NativePluginHarness, params: JobScheduleParams, output_file: str = None):
    dag_content, has_impersonation, local_path = harness.render(params)

    print("=" * 65)
    print("           NATIVE AIRFLOW DAG DRY-RUN RENDERING RESULT          ")
    print("=" * 65)
    print(f"Job Name          : {params.name}")
    print(f"Cluster           : {params.cluster_name}")
    print(f"Generated File    : {local_path}")
    print(f"Impersonation     : {'[✓] INJECTED' if has_impersonation else '[✗] OMITTED'}")
    print("-" * 65)

    if output_file:
        os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(dag_content)
        print(f"[✓] DAG exported to: {output_file}")
    else:
        print(dag_content)
    print("=" * 65)


def run_schedule_action(harness: NativePluginHarness, params: JobScheduleParams):
    print("=" * 65)
    print("        EXECUTING LIVE JOB CREATION VIA SCHEDULER PLUGIN        ")
    print("=" * 65)
    print(f"Job Name     : {params.name}")
    print(f"Cluster      : {params.cluster_name}")
    print(f"Composer Env : {params.composer_environment_name}")
    print(f"Notebook     : {params.input_filename}")
    print("-" * 65)

    result = harness.schedule(params)
    print("Execution Result:", json.dumps(result, indent=2))
    if result.get("status") == 0 or "status" in str(result):
        print("\n[✓] Job successfully created and synchronized to Composer!")
    else:
        print("\n[✗] Job creation reported error:", result)
    print("=" * 65)


def main(args: List[str] = None):
    parser = argparse.ArgumentParser(
        description="Native Scheduler Jupyter Plugin CLI for Vertex AI Workbench",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Pre-flight environment diagnostics using the installed plugin
  scheduler-plugin-diag diagnose --cluster=my-dataproc-cluster

  # Dry-run DAG generation and impersonation inspection
  scheduler-plugin-diag render --job-name=demo-job --cluster=my-dataproc-cluster

  # Live schedule job to Cloud Composer via installed plugin
  scheduler-plugin-diag schedule --job-name=demo-job --cluster=my-dataproc-cluster --composer-env=my-composer-env
        """,
    )

    parser.add_argument(
        "action",
        choices=["diagnose", "render", "schedule"],
        help="Action to perform using installed scheduler_jupyter_plugin",
    )
    parser.add_argument("--cluster", default="pyspark-cluster-dev-multitenant", help="Target Dataproc cluster name")
    parser.add_argument("--composer-env", default="my-airflow-composer", help="Composer Environment Name")
    parser.add_argument("--job-name", default="cli-scheduled-job", help="Job name identifier")
    parser.add_argument("--notebook", default="Basic Spark.ipynb", help="Notebook filename")
    parser.add_argument("--mode", default="cluster", choices=["cluster", "serverless"], help="Execution mode")
    parser.add_argument("--local-kernel", action="store_true", help="Run job locally in Airflow worker")
    parser.add_argument("--output-file", help="Filepath to export rendered DAG")
    parser.add_argument("--json", action="store_true", help="Output results in JSON format")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    parsed = parser.parse_args(args)
    harness = NativePluginHarness(verbose=parsed.verbose)

    if parsed.action == "diagnose":
        run_diagnose_action(harness, cluster_name=parsed.cluster, as_json=parsed.json)

    elif parsed.action == "render":
        params = JobScheduleParams(
            name=parsed.job_name,
            input_filename=parsed.notebook,
            cluster_name=parsed.cluster,
            composer_environment_name=parsed.composer_env,
            mode_selected=parsed.mode,
            local_kernel=parsed.local_kernel,
        )
        run_render_action(harness, params, output_file=parsed.output_file)

    elif parsed.action == "schedule":
        params = JobScheduleParams(
            name=parsed.job_name,
            input_filename=parsed.notebook,
            cluster_name=parsed.cluster,
            composer_environment_name=parsed.composer_env,
            mode_selected=parsed.mode,
            local_kernel=parsed.local_kernel,
        )
        run_schedule_action(harness, params)


if __name__ == "__main__":
    main()
