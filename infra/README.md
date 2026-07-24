# infra/ — Terraform bootstrap

Provisions the Azure resource group, Azure Databricks workspace (Premium SKU,
required for Unity Catalog), ADLS Gen2 storage for Delta tables, and the
`bronze`/`silver`/`gold` Unity Catalog schemas.

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
