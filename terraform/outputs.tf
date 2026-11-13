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

output "product_glue_jobs" {
  description = "Glue Jobs administrados por Terraform."
  value = {
    for key, job in aws_glue_job.product_jobs :
    key => job.name
  }
}

output "product_glue_scripts" {
  description = "Ubicaciones S3 de los scripts."
  value = {
    for key, obj in aws_s3_object.glue_scripts :
    key => "s3://${obj.bucket}/${obj.key}"
  }
}
