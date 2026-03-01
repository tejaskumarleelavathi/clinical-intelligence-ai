# =============================================================================
# Outputs — use these after 'terraform apply' (e.g. for Databricks/Spark config)
# =============================================================================

output "resource_group_name" {
  description = "Name of the created resource group"
  value       = azurerm_resource_group.rg.name
}

output "storage_account_name" {
  description = "Name of the Data Lake Storage Gen2 account"
  value       = azurerm_storage_account.datalake.name
}

output "storage_account_primary_dfs_endpoint" {
  description = "DFS endpoint for ADLS Gen2 (use this in Spark: abfss://container@account.dfs.core.windows.net/)"
  value       = azurerm_storage_account.datalake.primary_dfs_endpoint
}

output "raw_container_name" {
  description = "Bronze container name (raw CSVs)"
  value       = azurerm_storage_data_lake_gen2_filesystem.raw.name
}

output "silver_container_name" {
  description = "Silver container name (cleaned Parquet)"
  value       = azurerm_storage_data_lake_gen2_filesystem.silver.name
}

output "gold_container_name" {
  description = "Gold container name (transformed Parquet)"
  value       = azurerm_storage_data_lake_gen2_filesystem.gold.name
}

# Example full path for Spark (replace YOUR_STORAGE_ACCOUNT with actual name after apply)
output "raw_abfss_path" {
  description = "Example ABFSS path for raw data (use in 01_ingest.py)"
  value       = "abfss://${azurerm_storage_data_lake_gen2_filesystem.raw.name}@${azurerm_storage_account.datalake.name}.dfs.core.windows.net/"
}
