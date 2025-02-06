terraform {
  backend "gcs" {
    bucket = "terraform-bucket-dataproject-2425"
    prefix = "terraform/state"
  }
}