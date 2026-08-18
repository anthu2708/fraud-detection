# Real GCP VM hosting the live demo (fraud.anphung.dev). Imported from a
# hand-created instance — see README for the import procedure. `metadata`
# is excluded from drift detection because `gcloud compute ssh` rewrites
# the `ssh-keys` item (with a fresh expiry) on every connection.

provider "google" {
  project = "fraud-detection-505506"
  region  = "us-west1"
  zone    = "us-west1-b"
}

resource "google_compute_address" "fraud_static" {
  name         = "fraud-static"
  region       = "us-west1"
  address_type = "EXTERNAL"
  address      = "34.105.54.6"
}

resource "google_compute_instance" "fraud" {
  name                       = "fraud"
  zone                       = "us-west1-b"
  machine_type               = "e2-standard-2"
  tags                       = ["http-server", "https-server"]
  enable_display             = false
  key_revocation_action_type = "NONE"

  boot_disk {
    auto_delete = true
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2404-lts"
      size  = 30
      type  = "pd-balanced"
    }
  }

  network_interface {
    network    = "default"
    subnetwork = "default"
    stack_type = "IPV4_ONLY"

    access_config {
      nat_ip       = google_compute_address.fraud_static.address
      network_tier = "PREMIUM"
    }
  }

  service_account {
    email = "223066301358-compute@developer.gserviceaccount.com"
    scopes = [
      "https://www.googleapis.com/auth/devstorage.read_only",
      "https://www.googleapis.com/auth/logging.write",
      "https://www.googleapis.com/auth/monitoring.write",
      "https://www.googleapis.com/auth/service.management.readonly",
      "https://www.googleapis.com/auth/servicecontrol",
      "https://www.googleapis.com/auth/trace.append",
    ]
  }

  scheduling {
    automatic_restart   = true
    on_host_maintenance = "MIGRATE"
    preemptible         = false
    provisioning_model  = "STANDARD"
  }

  shielded_instance_config {
    enable_integrity_monitoring = true
    enable_secure_boot          = false
    enable_vtpm                 = true
  }

  confidential_instance_config {
    enable_confidential_compute = false
  }

  reservation_affinity {
    type = "ANY_RESERVATION"
  }

  metadata = {
    enable-osconfig = "TRUE"
  }

  lifecycle {
    # labels: "goog-ops-agent-policy" is auto-managed by GCP's OS Config
    # Ops Agent policy, not by us — don't fight it for ownership.
    ignore_changes = [metadata, boot_disk, labels]
  }
}

output "fraud_vm_external_ip" {
  value = google_compute_address.fraud_static.address
}

# SSH restricted to the operator's current public IP — was open to
# 0.0.0.0/0 by default and pulled ~880 brute-force attempts/day.
# Actual value lives in terraform.tfvars (gitignored, not committed —
# it's someone's home/current public IP). When it changes, get the new
# one with `curl https://api.ipify.org` and update that file.
variable "ssh_allowed_source_ip" {
  type      = string
  sensitive = true
}

resource "google_compute_firewall" "default_allow_ssh" {
  name        = "default-allow-ssh"
  network     = "default"
  description = "SSH restricted to the operator's current public IP (see ssh_allowed_source_ip variable)."
  priority    = 65534
  direction   = "INGRESS"

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = [var.ssh_allowed_source_ip]
}
