resource "aws_s3_bucket" "data_lake" {
  bucket = var.data_lake_bucket

  tags = {
    Project     = "UCV Big Data"
    Environment = var.environment
    ManagedBy   = "Terraform"
    CostCenter  = "BI"
  }
}


locals {
  domain_layer_pairs = flatten([
    for domain in var.domains : [
      for layer in var.layers : {
        domain = domain
        layer  = layer
        key    = "${domain}/${layer}/"
      }
    ]
  ])

  domain_layer_map = {
    for item in local.domain_layer_pairs :
    "${item.domain}-${item.layer}" => item
  }
}

resource "aws_s3_object" "layer_prefixes" {
  for_each = local.domain_layer_map

  bucket  = aws_s3_bucket.data_lake.id
  key     = each.value.key
  content = ""
}

resource "aws_glue_catalog_database" "domains" {
  for_each = var.domains

  catalog_id  = var.catalog_id
  name        = "${each.value}_domain"
  description = "Data Catalog for ${each.value} domain"

  tags = {
    Domain      = each.value
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}
