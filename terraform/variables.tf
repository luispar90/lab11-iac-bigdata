variable "aws_region" {
  description = "Region AWS simulada."
  type        = string
  default     = "us-east-1"
}

variable "data_lake_bucket" {
  description = "Nombre del bucket principal del Data Lake."
  type        = string
  default     = "ucv-data-platform"
}

variable "environment" {
  description = "Ambiente de trabajo."
  type        = string
  default     = "dev"
}

variable "domains" {
  description = "Dominios de datos de la plataforma."
  type        = set(string)

  default = [
    "sales",
    "customers",
    "products"
  ]
}

variable "layers" {
  description = "Capas de la arquitectura Medallion."
  type        = set(string)

  default = [
    "bronze",
    "silver",
    "gold"
  ]
}

variable "catalog_id" {
  description = "Catalog ID simulado para Glue."
  type        = string
  default     = "000000000000"
}