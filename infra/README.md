# infra/ — Terraform bootstrap

Provisions the Azure resource group, Azure Databricks workspace (Premium SKU,
required for Unity Catalog), ADLS Gen2 storage for Delta tables, the
`bronze`/`silver`/`gold` Unity Catalog schemas, and the two scheduled
Databricks Jobs (`jobs.tf`) that run the pipeline — see
`../docs/batch_vs_streaming_memo.md` for why there are two.

## Prerequisites (manual, one-time)

1. An Azure subscription with permission to create resource groups and
   Databricks workspaces.
2. Azure CLI installed and authenticated: `az login`.
3. A Unity Catalog metastore already created and assigned to the target
   region for your Azure account (Databricks Account Console →
   Unity Catalog). This is an account-level, one-time action Terraform does
   not perform here.
4. A remote state backend storage account, created once, outside this
   config (chicken-and-egg: the backend can't provision the place it lives):

   ```bash
   az group create -n rg-tfstate -l eastus2
   az storage account create -n sttfstatelakehouse -g rg-tfstate -l eastus2 --sku Standard_LRS
   az storage container create -n tfstate --account-name sttfstatelakehouse
   ```

5. `jobs.tf`'s tasks run the `lakehouse` console script from a built wheel,
   not from workspace-synced source, so build and upload it before
   `terraform apply`, and point `lakehouse_wheel_path` (terraform.tfvars) at
   wherever it lands:

   ```bash
   uv build --wheel
   databricks fs cp dist/lakehouse-0.1.0-py3-none-any.whl dbfs:/FileStore/wheels/ --overwrite
   ```

## Cost scoping (read before `apply`)

By default this config provisions **only** the workspace, storage, Unity
Catalog schemas, and the single-node verification cluster. The two
continuously-billing pieces are behind flags, both defaulting to `false`:

| Variable | Creates | Why it's off by default |
|---|---|---|
| `enable_scheduled_jobs` | `jobs.tf`'s two Databricks Jobs | `lakehouse-alerts-snapshot` runs **every 15 minutes** against a cluster with `autotermination_minutes = 30`, so the cluster never idles down — effectively 24/7 compute. |
| `enable_sql_warehouse` | `sql_warehouse.tf`'s Power BI warehouse | Only needed when actually building the `.pbix` against Gold. Serverless with a 10-minute auto-stop, so cost is bursty rather than constant. |

Enable them deliberately in `terraform.tfvars` when you want that behavior:

```hcl
enable_scheduled_jobs = true
enable_sql_warehouse  = true
```

The verification cluster auto-terminates after 30 idle minutes, so with both
flags off an idle deployment costs storage plus the workspace itself.

Run `terraform destroy` when you're done demoing.

## Usage

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars   # fill in real values

terraform init \
  -backend-config="resource_group_name=rg-tfstate" \
  -backend-config="storage_account_name=sttfstatelakehouse" \
  -backend-config="container_name=tfstate" \
  -backend-config="key=lakehouse.tfstate"

terraform plan
terraform apply
```

Databricks provider auth uses your Azure CLI session (`azure_workspace_resource_id`
+ `az login`) — no PAT token is stored in this repo. A separate service
principal for CI (Phase 6 orchestration) is provisioned later, once DAB
deploys are wired up.
