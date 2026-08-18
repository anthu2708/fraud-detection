terraform {
  required_providers {
    null = {
      source  = "hashicorp/null"
      version = "~> 3.0"
    }
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

locals {
  cluster_name = "fraud-detection"
  data_path    = "${path.root}/../data"
  models_path  = "${path.root}/../models"
}

resource "null_resource" "k3d_cluster" {
  triggers = {
    cluster_name = local.cluster_name
  }

  provisioner "local-exec" {
    interpreter = ["powershell", "-Command"]
    command     = <<-EOT
      k3d cluster create ${local.cluster_name} `
        --volume "${abspath(local.data_path)}:/fraud-data@all" `
        --volume "${abspath(local.models_path)}:/fraud-models@all" `
        -p "8000:30000@loadbalancer" `
        --agents 1
    EOT
  }

  provisioner "local-exec" {
    when        = destroy
    interpreter = ["powershell", "-Command"]
    command     = "k3d cluster delete ${self.triggers.cluster_name}"
  }
}
