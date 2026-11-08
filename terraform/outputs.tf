output "data_lake_bucket" {
  description = "Bucket principal del Data Lake."
  value       = aws_s3_bucket.data_lake.bucket
}

output "domain_catalogs" {
  description = "Glue databases creadas por dominio."
  value = {
    for key, db in aws_glue_catalog_database.domains :
    key => db.name
  }
}

output "floci_endpoint" {
  description = "Endpoint AWS local."
  value       = "http://localhost:4566"
}
