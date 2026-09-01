# Scheduler Jupyter Plugin Diagnostics CLI

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)

A pure, native diagnostic and dry-run CLI harness for the official **`scheduler_jupyter_plugin`** package installed on **Google Cloud Vertex AI Workbench**.

---

## 🎯 Overview & The Impersonation Challenge

When scheduling Jupyter notebooks to execute on multi-tenant Dataproc clusters via Cloud Composer, Cloud Composer's worker service account must impersonate the specific end-user's service account using `impersonation_chain=['<user-sa>']` in `DataprocSubmitJobOperator`.

However, the official Airflow DAG generation template inside the scheduler plugin wraps impersonation inside a conditional check:
```jinja
{% if multi_tenant_service_account %}
impersonation_chain=['{{multi_tenant_service_account}}'],
{% endif %}
```

> [!WARNING]
> If `multi_tenant_service_account` evaluates to an empty string (`""`), the template **silently omits** the impersonation chain without raising any errors, warnings, or UI alerts. The job is then submitted under the default Dataproc VM service account rather than the intended end user's identity.

This CLI directly invokes the **official, installed `scheduler_jupyter_plugin` package** inside your Workbench notebook environment to audit, dry-run render, and live-schedule jobs with zero false positives.

---

## 🏗️ Architecture & Decision Logic

The flowchart below illustrates how the official `scheduler_jupyter_plugin` evaluates whether to inject or silently omit the `impersonation_chain`:

```mermaid
flowchart TD
    A["Job Scheduling Triggered"] --> B{"local_kernel == True?"}
    B -- Yes --> B1["Render localPythonTemplate<br/>(No Dataproc operator - Impersonation OMITTED)"]
    B -- No --> C{"mode_selected == 'serverless'?"}
    C -- Yes --> C1["Render pysparkBatchTemplate<br/>(Batch template lacks impersonation support)"]
    C -- No --> D["Query Dataproc Cluster API via plugin"]
    D --> E{"API Call Succeeded?"}
    E -- No (401/403/404) --> E1["Silent Dict Fallback -> target_sa = ''<br/>(Impersonation OMITTED)"]
    E -- Yes --> F{"dataproc.dynamic.multi.tenancy.enabled == 'true'?"}
    F -- No / Missing --> F1["target_sa = ''<br/>(Impersonation OMITTED)"]
    F -- Yes --> G{"Active gcloud user in userServiceAccountMapping?"}
    G -- No (Key/Casing Mismatch) --> G1["target_sa = ''<br/>(Impersonation OMITTED)"]
    G -- Yes --> H["target_sa = mapped_service_account<br/>(Impersonation INJECTED in DataprocSubmitJobOperator)"]
```

---

## 💻 Installation on Vertex AI Workbench (Notebook Terminal)

Follow these steps inside any Vertex AI Workbench Notebook instance:

### Step 1: Open Terminal in JupyterLab
1. In your JupyterLab top navigation bar, click **File** → **New** → **Terminal**.

### Step 2: Clone or Copy Repository
```bash
git clone https://github.com/Royston88/scheduler-jupyter-plugin-diagnostics.git
cd scheduler-jupyter-plugin-diagnostics
```

*(Alternatively, copy or upload the directory to `/home/jupyter/scheduler-jupyter-plugin-diagnostics`)*.

### Step 3: Install into Notebook Managed Python Environment
Vertex AI Workbench JupyterLab executes inside `/opt/micromamba/envs/jupyterlab`. Install the CLI in editable mode using `--no-deps` to preserve all existing JupyterLab packages:

```bash
/opt/micromamba/envs/jupyterlab/bin/python3 -m pip install --no-deps -e .
```

---

## 🚀 Usage & Diagnostic Commands

### 1. Pre-Flight Diagnostic Audit (`diagnose`)
Validates Dataproc cluster accessibility, dynamic multi-tenancy properties, active gcloud user identity, and IAM Token Creator permissions directly through the installed plugin runtime:

```bash
/opt/micromamba/envs/jupyterlab/bin/python3 -m dataproc_scheduler_diagnostics.cli diagnose \
    --cluster=<DATAPROC_CLUSTER_NAME>
```

**Sample Output**:
```text
=================================================================
   NATIVE SCHEDULER PLUGIN PRE-FLIGHT & IMPERSONATION AUDIT      
=================================================================
Plugin Version : 0.1.7 (Official Installed Package)
Project ID     : my-gcp-project
Region ID      : us-central1
Target Cluster : my-dataproc-multitenant-cluster
Active Account : user-1@example.com
-----------------------------------------------------------------
1. Dataproc Cluster Accessible : [✓] PASS
2. Dynamic Multi-Tenancy Value : 'true'
   -> Multi-Tenancy Enabled    : [✓] PASS
3. User Mapping Configured     : {
  "user-1@example.com": "data-user-sa@my-gcp-project.iam.gserviceaccount.com"
}
4. Target Service Account      : 'data-user-sa@my-gcp-project.iam.gserviceaccount.com'
   -> Impersonation Chain Status: [✓] INJECTED (data-user-sa@my-gcp-project.iam.gserviceaccount.com)
5. Probing Token Creator IAM Permissions...
   -> Token Generation Test    : [✓] SUCCESS
=================================================================
```

---

### 2. Dry-Run Render Airflow DAG & Verify Impersonation (`render`)
Executes the official plugin's internal `prepare_dag()` pipeline locally. Renders the exact Airflow DAG Python code and verifies whether `impersonation_chain` is generated without uploading anything to Cloud Storage:

```bash
/opt/micromamba/envs/jupyterlab/bin/python3 -m dataproc_scheduler_diagnostics.cli render \
    --cluster=<DATAPROC_CLUSTER_NAME> \
    --composer-env=<COMPOSER_ENV_NAME> \
    --job-name="test-spark-job" \
    --notebook="notebook.ipynb"
```

**Key Parameters**:
- `--cluster`: Target Dataproc cluster name.
- `--composer-env`: Cloud Composer environment name.
- `--job-name`: Name identifier for the scheduled job (defaults to `scheduled-notebook-job`).
- `--notebook`: Name of the notebook file in the directory (defaults to `notebook.ipynb`).
- `--output-file`: (Optional) Local filepath to save the rendered Airflow DAG python script.
- `--mode`: (Optional) Execution mode: `cluster` (default) or `serverless`.
- `--local-kernel`: (Optional) Render DAG for execution inside the local Airflow worker container.

---

### 3. Live Schedule Job to Cloud Composer (`schedule`)
Executes the full end-to-end job creation, renders the DAG, and synchronizes the DAG and notebook payload to Cloud Composer's GCS bucket from the terminal:

```bash
/opt/micromamba/envs/jupyterlab/bin/python3 -m dataproc_scheduler_diagnostics.cli schedule \
    --cluster=<DATAPROC_CLUSTER_NAME> \
    --composer-env=<COMPOSER_ENV_NAME> \
    --job-name="daily-spark-analysis" \
    --notebook="notebook.ipynb"
```

---

## 📊 Impersonation Resolution Scenarios

| Scenario | Mode / Conditions | Target SA Resolved | Impersonation Injected | Reason / Mechanism |
| :--- | :--- | :---: | :---: | :--- |
| **Nominal Multi-Tenancy** | Cluster mode, Multi-tenancy enabled (`'true'`), Caller in mapping | `target-sa@...` | ✅ **INJECTED** | Matched `userServiceAccountMapping` key. `impersonation_chain=['<target-sa>']` written to DAG. |
| **User Identity Mismatch** | Cluster mode, Active `gcloud` account not in mapping table | `""` | ❌ **OMITTED** | Silent lookup miss in `userServiceAccountMapping`. Key is case-sensitive. |
| **Multi-Tenancy Disabled** | Cluster property `dataproc:dataproc.dynamic.multi.tenancy.enabled` is `false` or missing | `""` | ❌ **OMITTED** | Plugin skips mapping resolution when property is not the exact lowercase string `'true'`. |
| **Serverless Mode** | Serverless / Batch execution mode selected | `""` | ❌ **OMITTED** | Template `pysparkBatchTemplate-v1.txt` does not implement user impersonation chains. |
| **Local Kernel Mode** | `local_kernel == True` selected | `""` | ❌ **OMITTED** | Executes in Airflow worker container via `localPythonTemplate-v1.txt`. |
| **Dataproc API Failure** | Permission denied, expired token, or 401/403/404 on Dataproc API | `""` | ❌ **OMITTED** | Silent exception fallback returns empty dict, causing empty target SA string. |

---

## 🔍 Root Cause Troubleshooting Guide

| Issue Symptom | Underlying Cause in Plugin | Remediation |
| :--- | :--- | :--- |
| **`impersonation_chain` missing in generated DAG** | Active `gcloud` account does not match key in `userServiceAccountMapping`. | Ensure `gcloud config get account` matches the exact email (case-sensitive) mapped during cluster creation. |
| **`impersonation_chain` missing on multi-tenant cluster** | Property `dataproc:dataproc.dynamic.multi.tenancy.enabled` is missing or not lowercase `"true"`. | Recreate cluster with `--properties=dataproc:dataproc.dynamic.multi.tenancy.enabled=true`. |
| **Airflow DAG fails with `PERMISSION_DENIED: Failed to impersonate`** | Composer Worker SA does not have `Token Creator` role on Target SA. | Grant `roles/iam.serviceAccountTokenCreator` to the Composer Service Account on the Target Service Account: <br/>`gcloud iam service-accounts add-iam-policy-binding <TARGET_SA> --member="serviceAccount:<COMPOSER_SA>" --role="roles/iam.serviceAccountTokenCreator"` |
| **Impersonation missing when running Serverless** | Serverless mode uses batch templates which do not support user impersonation chains. | Use Execution Mode: `Cluster` instead of `Serverless`. |

---

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
