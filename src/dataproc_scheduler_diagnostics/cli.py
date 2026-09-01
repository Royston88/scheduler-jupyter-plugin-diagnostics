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

from .engine import DiagnosticEngine
from .models import JobScheduleParams


def run_diagnostics_cmd(engine: DiagnosticEngine, cluster_name: str, as_json: bool = False):
    target_sa, diag = engine.resolve_multi_tenant_sa(cluster_name)
    token_ok = engine.probe_token_creator_permission(target_sa) if target_sa else False
    diag.token_generation_success = token_ok

    if as_json:
        print(diag.model_dump_json(indent=2))
        return

    print("=" * 65)
    print("     SCHEDULER PLUGIN PRE-FLIGHT & IMPERSONATION DIAGNOSTICS     ")
    print("=" * 65)
    print(f"Project ID : {engine.project_id or '[Not configured]'}")
    print(f"Region ID  : {engine.region_id}")
    print(f"Cluster    : {cluster_name}")
    print(f"Active User: {diag.user_email}")
    print("-" * 65)
    print(f"1. Dataproc Cluster Accessible : {'[✓] PASS' if diag.cluster_accessible else '[✗] FAIL'}")
    print(f"2. Dynamic Multi-Tenancy Value : '{diag.dynamic_multi_tenancy_raw}'")
    print(f"   -> Evaluates to Enabled     : {'[✓] PASS' if diag.dynamic_multi_tenancy_enabled else '[✗] FAIL'}")
    print(f"3. User Mapping Configured     : {json.dumps(diag.user_service_account_mapping, indent=2)}")
    
    sa_display = target_sa if target_sa else "[None]"
    print(f"4. Target Service Account      : '{sa_display}'")

    if target_sa:
        print(f"   -> Impersonation Chain Status: [✓] INJECTED ({target_sa})")
        print("5. Probing Token Creator IAM Permissions...")
        status_str = "[✓] SUCCESS" if token_ok else "[✗] FAILED (Check Token Creator role)"
        print(f"   -> Token Generation Test    : {status_str}")
    else:
        print("   -> Impersonation Chain Status: [✗] NOT INJECTED")
        print(f"   -> Skip Reason: {diag.skip_reason}")
    print("=" * 65)


def run_render_cmd(engine: DiagnosticEngine, params: JobScheduleParams, output_file: str = None, as_json: bool = False):
    res = engine.render_dag(params)
    if as_json:
        print(res.model_dump_json(indent=2))
        return

    print("=" * 65)
    print("                 DAG RENDERING & DRY-RUN RESULT                  ")
    print("=" * 65)
    print(f"Template Used : {res.template_used}")
    sa_display = res.multi_tenant_service_account if res.multi_tenant_service_account else "[None]"
    print(f"Target SA     : {sa_display}")
    print(f"Impersonation : {'[✓] INJECTED' if res.has_impersonation_chain else '[✗] OMITTED'}")
    if not res.has_impersonation_chain:
        print(f"Skip Reason   : {res.diagnostics.skip_reason}")
    print("-" * 65)

    if output_file:
        os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(res.dag_content)
        print(f"[✓] Rendered DAG saved to: {output_file}")
    else:
        print(res.dag_content)
    print("=" * 65)


def run_test_matrix_cmd(engine: DiagnosticEngine, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    print("=" * 65)
    print("           EXECUTING 6-SCENARIO IMPERSONATION TEST MATRIX        ")
    print("=" * 65 + "\n")

    scenarios = [
        {
            "id": "CASE_1_NOMINAL_SUCCESS",
            "name": "Case 1: Standard Matching User on Multi-Tenant Cluster",
            "user": "user-1@example.com",
            "cluster": "pyspark-cluster-dev-multitenant",
            "params": JobScheduleParams(name="test-case-1-nominal", mode_selected="cluster", local_kernel=False),
            "cluster_override": {
                "config": {
                    "softwareConfig": {"properties": {"dataproc:dataproc.dynamic.multi.tenancy.enabled": "true"}},
                    "securityConfig": {
                        "identityConfig": {
                            "userServiceAccountMapping": {
                                "user-1@example.com": "data-user-sa@my-gcp-project.iam.gserviceaccount.com"
                            }
                        }
                    },
                }
            },
            "expected_injected": True,
        },
        {
            "id": "CASE_2_USER_MISMATCH",
            "name": "Case 2: User Email Mismatch (User not in mapping)",
            "user": "unmapped_engineer@example.com",
            "cluster": "pyspark-cluster-dev-multitenant",
            "params": JobScheduleParams(name="test-case-2-user-mismatch", mode_selected="cluster", local_kernel=False),
            "cluster_override": {
                "config": {
                    "softwareConfig": {"properties": {"dataproc:dataproc.dynamic.multi.tenancy.enabled": "true"}},
                    "securityConfig": {
                        "identityConfig": {
                            "userServiceAccountMapping": {
                                "user-1@example.com": "data-user-sa@my-gcp-project.iam.gserviceaccount.com"
                            }
                        }
                    },
                }
            },
            "expected_injected": False,
        },
        {
            "id": "CASE_3_MULTITENANCY_DISABLED",
            "name": "Case 3: Dataproc Multi-Tenancy Disabled (Property is false)",
            "user": "user-1@example.com",
            "cluster": "pyspark-cluster-dev-multitenant",
            "params": JobScheduleParams(name="test-case-3-no-multitenant", mode_selected="cluster", local_kernel=False),
            "cluster_override": {
                "config": {
                    "softwareConfig": {"properties": {"dataproc:dataproc.dynamic.multi.tenancy.enabled": "false"}},
                    "securityConfig": {
                        "identityConfig": {
                            "userServiceAccountMapping": {
                                "user-1@example.com": "data-user-sa@my-gcp-project.iam.gserviceaccount.com"
                            }
                        }
                    },
                }
            },
            "expected_injected": False,
        },
        {
            "id": "CASE_4_SERVERLESS_MODE",
            "name": "Case 4: Execution Mode is 'Serverless' (Batch template)",
            "user": "user-1@example.com",
            "cluster": "pyspark-cluster-dev-multitenant",
            "params": JobScheduleParams(name="test-case-4-serverless", mode_selected="serverless", local_kernel=False),
            "cluster_override": {
                "config": {
                    "softwareConfig": {"properties": {"dataproc:dataproc.dynamic.multi.tenancy.enabled": "true"}},
                    "securityConfig": {
                        "identityConfig": {
                            "userServiceAccountMapping": {
                                "user-1@example.com": "data-user-sa@my-gcp-project.iam.gserviceaccount.com"
                            }
                        }
                    },
                }
            },
            "expected_injected": False,
        },
        {
            "id": "CASE_5_LOCAL_KERNEL",
            "name": "Case 5: Local Kernel Selected (Local Papermill worker)",
            "user": "user-1@example.com",
            "cluster": "pyspark-cluster-dev-multitenant",
            "params": JobScheduleParams(name="test-case-5-local-kernel", mode_selected="cluster", local_kernel=True),
            "cluster_override": {
                "config": {
                    "softwareConfig": {"properties": {"dataproc:dataproc.dynamic.multi.tenancy.enabled": "true"}},
                    "securityConfig": {
                        "identityConfig": {
                            "userServiceAccountMapping": {
                                "user-1@example.com": "data-user-sa@my-gcp-project.iam.gserviceaccount.com"
                            }
                        }
                    },
                }
            },
            "expected_injected": False,
        },
        {
            "id": "CASE_6_API_FAILURE",
            "name": "Case 6: Dataproc API Failure / 401 Unauthorized",
            "user": "user-1@example.com",
            "cluster": "pyspark-cluster-dev-multitenant",
            "params": JobScheduleParams(name="test-case-6-api-error", mode_selected="cluster", local_kernel=False),
            "cluster_override": {"error": "HTTP 401 Unauthorized: Access token expired"},
            "expected_injected": False,
        },
    ]

    results = []
    for sc in scenarios:
        print(f"[*] Running {sc['name']}...")
        res = engine.render_dag(
            params=sc["params"],
            force_cluster_data=sc["cluster_override"],
            force_user_email=sc["user"],
        )

        dag_path = os.path.join(output_dir, f"dag_{sc['params'].name}.py")
        with open(dag_path, "w", encoding="utf-8") as f:
            f.write(res.dag_content)

        injected = res.has_impersonation_chain
        match_expected = injected == sc["expected_injected"]
        status = "PASSED" if match_expected else "UNEXPECTED"

        sa_val = res.multi_tenant_service_account if res.multi_tenant_service_account else "[None]"
        print(f"    Template                     : {res.template_used}")
        print(f"    Target Service Account       : '{sa_val}'")
        print(f"    Impersonation Chain Injected : {injected}")
        if not injected:
            print(f"    Skip Reason                  : {res.diagnostics.skip_reason}")
        print(f"    Output DAG                   : {dag_path}")
        print(f"    Validation Status            : [{'✓' if match_expected else '✗'}] {status}\n")

        results.append(
            {
                "id": sc["id"],
                "name": sc["name"],
                "template": res.template_used,
                "target_sa": res.multi_tenant_service_account,
                "impersonation_injected": injected,
                "skip_reason": res.diagnostics.skip_reason,
                "dag_file": dag_path,
                "status": status,
            }
        )

    summary_file = os.path.join(output_dir, "test_matrix_summary.json")
    with open(summary_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    print(f"[✓] Test matrix execution complete. Summary saved to {summary_file}\n")


def main(args: List[str] = None):
    parser = argparse.ArgumentParser(
        description="Dataproc & Composer Scheduler Diagnostics CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Pre-flight environment diagnostics
  dataproc-scheduler-diag diagnose --cluster=my-dataproc-cluster

  # Dry-run DAG generation and inspection
  dataproc-scheduler-diag render --job-name=demo-job --cluster=my-dataproc-cluster

  # Run the full 6-scenario impersonation test matrix
  dataproc-scheduler-diag test-matrix --output-dir=/tmp/test_outputs
        """,
    )

    parser.add_argument(
        "action",
        choices=["diagnose", "render", "test-matrix"],
        help="Diagnostic or simulation action to perform",
    )
    parser.add_argument("--project", help="GCP Project ID (default: gcloud config get project)")
    parser.add_argument("--region", help="GCP Region ID (default: gcloud config get dataproc/region)")
    parser.add_argument("--cluster", default="pyspark-cluster-dev-multitenant", help="Target Dataproc cluster name")
    parser.add_argument("--composer-bucket", default="", help="Composer GCS bucket name")
    parser.add_argument("--job-name", default="diagnostic-test-job", help="Job name identifier")
    parser.add_argument("--notebook", default="Basic Spark.ipynb", help="Notebook filename or GCS URI")
    parser.add_argument("--user-email", help="Override user identity to test mapping resolution")
    parser.add_argument("--output-file", help="Filepath to write rendered DAG file")
    parser.add_argument("--output-dir", default="./outputs", help="Output directory for test matrix results")
    parser.add_argument("--json", action="store_true", help="Output results in JSON format")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose debug logging")

    parsed = parser.parse_args(args)
    engine = DiagnosticEngine(
        project_id=parsed.project,
        region_id=parsed.region,
        user_email_override=parsed.user_email,
        verbose=parsed.verbose,
    )

    if parsed.action == "diagnose":
        run_diagnostics_cmd(engine, cluster_name=parsed.cluster, as_json=parsed.json)

    elif parsed.action == "render":
        params = JobScheduleParams(
            name=parsed.job_name,
            input_filename=parsed.notebook,
            cluster_name=parsed.cluster,
            composer_bucket=parsed.composer_bucket,
            mode_selected="cluster",
            local_kernel=False,
        )
        run_render_cmd(engine, params, output_file=parsed.output_file, as_json=parsed.json)

    elif parsed.action == "test-matrix":
        run_test_matrix_cmd(engine, output_dir=parsed.output_dir)


if __name__ == "__main__":
    main()
