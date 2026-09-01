#!/usr/bin/env python3
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

"""
Quick In-Process Smoke Test for scheduler_jupyter_plugin inside Workbench VM.
Directly imports the installed scheduler_jupyter_plugin package and queries Dataproc multi-tenancy.
"""

import argparse
import logging
import aiohttp
import tornado.ioloop

try:
    from scheduler_jupyter_plugin import credentials
    from scheduler_jupyter_plugin.services import executor
except ImportError:
    print("[!] Error: 'scheduler_jupyter_plugin' is not installed in this Python environment.")
    print("    Please run this script using the JupyterLab Python runtime:")
    print("    /opt/micromamba/envs/jupyterlab/bin/python scripts/test_installed_plugin.py --cluster=<CLUSTER_NAME>")
    exit(1)

logging.basicConfig(level=logging.INFO)


async def run_test(cluster_name: str):
    async with aiohttp.ClientSession() as session:
        creds = await credentials.get_cached()
        client = executor.Client(creds, logging.getLogger("InstalledPluginSmokeTest"), session)

        print("=" * 65)
        print("  DIRECT SMOKE TEST: INSTALLED SCHEDULER_JUPYTER_PLUGIN PACKAGE  ")
        print("=" * 65)
        print(f"Target Cluster : {cluster_name}")
        print(f"Project ID     : {creds.get('project_id')}")
        print(f"Region ID      : {creds.get('region_id')}")

        target_sa = await client.multi_tenant_user_service_account(cluster_name)
        sa_display = target_sa if target_sa else "[None]"
        print(f"Target SA      : '{sa_display}'")
        if target_sa:
            print(f"[✓] Impersonation Chain Injected: ['{target_sa}']")
        else:
            print("[✗] Impersonation Chain Omitted (empty string returned)")
            print("    Check: userServiceAccountMapping or dataproc.dynamic.multi.tenancy.enabled")
        print("=" * 65)


def main():
    parser = argparse.ArgumentParser(description="Quick smoke test for installed scheduler_jupyter_plugin")
    parser.add_argument("--cluster", default="pyspark-cluster-dev-multitenant", help="Target Dataproc cluster name")
    args = parser.parse_args()
    tornado.ioloop.IOLoop.current().run_sync(lambda: run_test(args.cluster))


if __name__ == "__main__":
    main()
