# =============================================================================
# Clinical Intelligence Platform — Azure Infrastructure (Terraform)
# =============================================================================
# This file defines the cloud resources so you can re-create them anywhere
# (dev, staging, prod) without clicking in the Azure Portal.
# =============================================================================

terraform {
  required_version = ">= 1.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

# -----------------------------------------------------------------------------
# Resource Group
# All resources for this project live in one group (billing, RBAC, cleanup).
# -----------------------------------------------------------------------------
resource "azurerm_resource_group" "rg" {
  name     = var.resource_group_name
  location = var.location
  tags     = var.tags
}

# -----------------------------------------------------------------------------
# Data Lake Storage Gen2 (ADLS Gen2)
# This is the Bronze/Silver/Gold storage. "Hierarchical namespace" = Gen2
# (directories, ACLs). Without it you get plain Blob Storage.
# -----------------------------------------------------------------------------
resource "azurerm_storage_account" "datalake" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true  # Required for Data Lake Gen2
  tags                     = var.tags
}

# -----------------------------------------------------------------------------
# Containers (filesystems) for Medallion Architecture
# - raw   = Bronze: raw CSVs (never modify)
# - silver = cleaned Parquet after 01_ingest
# - gold  = transformed Parquet after 02_transform, ready for Snowflake
# Subfolders (patients/, medications/, etc.) are created when you upload
# or when Spark writes; Terraform only creates the top-level containers.
# -----------------------------------------------------------------------------
resource "azurerm_storage_data_lake_gen2_filesystem" "raw" {
  name               = "raw"
  storage_account_id = azurerm_storage_account.datalake.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "silver" {
  name               = "silver"
  storage_account_id = azurerm_storage_account.datalake.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "gold" {
  name               = "gold"
  storage_account_id = azurerm_storage_account.datalake.id
}
