locals {
  product_glue_jobs = {
    bronze_to_silver = {
      job_name    = "products-bronze-to-silver"
      script_file = "${path.module}/../pipelines/products/glue_jobs/bronze_to_silver.py"
      script_key  = "scripts/products/bronze_to_silver.py"
    }
    silver_to_gold = {
      job_name    = "products-silver-to-gold"
      script_file = "${path.module}/../pipelines/products/glue_jobs/silver_to_gold.py"
      script_key  = "scripts/products/silver_to_gold.py"
    }
  }
}

resource "aws_s3_object" "glue_scripts" {
  for_each = local.product_glue_jobs

  bucket = aws_s3_bucket.data_lake.id
  key    = each.value.script_key
  source = each.value.script_file
  etag   = filemd5(each.value.script_file)

  content_type = "text/x-python"
}

resource "aws_glue_job" "product_jobs" {
  for_each = local.product_glue_jobs

  name              = each.value.job_name
  role_arn          = var.glue_role_arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = var.glue_job_run_timeout
  max_retries       = 0

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.data_lake.id}/${aws_s3_object.glue_scripts[each.key].key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"            = "python"
    "--enable-metrics"          = "true"
    "--enable-glue-datacatalog" = "true"
    "--datalake-formats"        = "iceberg"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  tags = {
    Domain      = "products"
    Environment = var.environment
    ManagedBy   = "Terraform"
    Laboratory  = "13"
  }

  depends_on = [aws_s3_object.glue_scripts]
}
