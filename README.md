# Scheduler Jupyter Plugin Diagnostics CLI

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)

A pure, native CLI harness for the official **`scheduler_jupyter_plugin`** installed on **Google Cloud Vertex AI Workbench**.

---

## 🎯 Overview

When scheduling Jupyter notebooks to run on multi-tenant Dataproc clusters via Cloud Composer, Cloud Composer's worker service account must impersonate the specific end-user's service account using `impersonation_chain=['<user-sa>']` in `DataprocSubmitJobOperator`.

This CLI directly invokes the **official, installed `scheduler_jupyter_plugin` package** running inside your Workbench notebook instance to:
1. **`diagnose`**: Perform pre-flight audits using the real plugin credentials, Dataproc API endpoints, and user account mapping.
2. **`render`**: Dry-run DAG rendering via the plugin's internal `prepare_dag()` pipeline to verify whether `impersonation_chain` is generated before submitting.
3. **`schedule`**: Live-create and upload the job, DAG, and notebook payload directly to Cloud Composer from the terminal without using the web UI.

> [!NOTE]
> This tool contains **zero simulations, mock overrides, or cloned templates**. It strictly executes the real, official plugin package installed on the notebook VM.

---

## 💻 Installation on Vertex AI Workbench (Notebook Terminal)

Follow these steps inside any Vertex AI Workbench Notebook instance:

### Step 1: Open Terminal in JupyterLab
In your JupyterLab navigation bar, click **File** → **New** → **Terminal**.

### Step 2: Clone or Copy Repository
```bash
git clone https://github.com/Royston88/scheduler-jupyter-plugin-diagnostics.git
cd scheduler-jupyter-plugin-diagnostics
```

### Step 3: Install into Notebook Managed Python Environment
Workbench JupyterLab runs in `/opt/micromamba/envs/jupyterlab`. Install the CLI in editable mode:

```bash
/opt/micromamba/envs/jupyterlab/bin/python3 -m pip install -e .
```

---

## 🚀 Usage & Commands

### 1. Pre-Flight Diagnostic Audit (`diagnose`)
Validates Dataproc cluster accessibility, dynamic multi-tenancy properties, and user identity mapping directly via the installed plugin:

```bash
/opt/micromamba/envs/jupyterlab/bin/python3 -m dataproc_scheduler_diagnostics.cli diagnose \
    --cluster=my-dataproc-multitenant-cluster
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

### 2. Dry-Run Render DAG & Check Impersonation (`render`)
Runs the official plugin's DAG generation pipeline locally and inspects if `impersonation_chain` was injected without uploading to Cloud Storage:

```bash
/opt/micromamba/envs/jupyterlab/bin/python3 -m dataproc_scheduler_diagnostics.cli render \
    --job-name="daily-spark-analysis" \
    --notebook="Basic Spark.ipynb" \
    --cluster=my-dataproc-multitenant-cluster
```

---

### 3. Live Schedule Job to Cloud Composer (`schedule`)
Executes the full in-process job creation, renders the DAG, and synchronizes all artifacts to Cloud Composer's GCS bucket:

```bash
/opt/micromamba/envs/jupyterlab/bin/python3 -m dataproc_scheduler_diagnostics.cli schedule \
    --job-name="daily-spark-analysis" \
    --notebook="Basic Spark.ipynb" \
    --cluster=my-dataproc-multitenant-cluster \
    --composer-env=my-airflow-composer
```

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
