# infrastructure/terraform/main.tf
# Provisions all Snowflake resources for the GTM platform

terraform {
  required_version = ">= 1.0"

  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.87"
    }
  }
}

# ── Provider ──────────────────────────────────────────────────
provider "snowflake" {
  organization_name = var.snowflake_organization
  account_name      = var.snowflake_account_name
  user              = var.snowflake_user
  password          = var.snowflake_password
  role              = var.snowflake_role
}
# ── Warehouse ─────────────────────────────────────────────────
resource "snowflake_warehouse" "gtm_wh" {
  name           = "COMPUTE_WH"
  warehouse_size = "x-small"
  auto_suspend   = 60
  auto_resume    = true
  comment        = "GTM Platform warehouse - ${var.environment}"
}

# ── Database ──────────────────────────────────────────────────
resource "snowflake_database" "gtm_db" {
  name    = "GTM_DB"
  comment = "GTM Platform database - ${var.environment}"
}

# ── Schemas ───────────────────────────────────────────────────
resource "snowflake_schema" "raw" {
  database = snowflake_database.gtm_db.name
  name     = "RAW"
  comment  = "Raw ingested data"
}

resource "snowflake_schema" "bronze" {
  database = snowflake_database.gtm_db.name
  name     = "BRONZE"
  comment  = "Bronze layer - typed raw data"
}

resource "snowflake_schema" "silver" {
  database = snowflake_database.gtm_db.name
  name     = "SILVER"
  comment  = "Silver layer - cleaned data"
}

resource "snowflake_schema" "gold" {
  database = snowflake_database.gtm_db.name
  name     = "GOLD"
  comment  = "Gold layer - business metrics"
}

resource "snowflake_schema" "marts" {
  database = snowflake_database.gtm_db.name
  name     = "MARTS"
  comment  = "Data marts for reporting"
}