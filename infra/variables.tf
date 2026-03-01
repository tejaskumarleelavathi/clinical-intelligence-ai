# =============================================================================
# Input variables — change these or override via .tfvars / env (TF_VAR_*)
# =============================================================================

variable "resource_group_name" {
  description = "Name of the Azure Resource Group"
  type        = string
  default     = "clinical-intelligence-rg"
}

variable "location" {
  description = "Azure region (e.g. eastus2, westus2)"
  type        = string
  default     = "eastus2"
}

variable "storage_account_name" {
  description = "Storage account name (3–24 chars, alphanumeric only; must be globally unique)"
  type        = string
  default     = "clinicalintelstorage"  # Change if name is taken, e.g. clinicalintelstorage123
}

variable "tags" {
  description = "Tags applied to all resources"
  type        = map(string)
  default = {
    project = "clinical-intelligence"
    env     = "dev"
  }
}
