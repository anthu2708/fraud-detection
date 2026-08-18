# Fraud Detection

Real-time credit-card fraud detection on streaming transactions — Kafka → Isolation Forest → FastAPI, deployed on Kubernetes via GitOps.

**Live:** [fraud.anphung.dev](https://fraud.anphung.dev) · **Monitoring:** [Grafana dashboard](https://fraud.anphung.dev/grafana/public-dashboards/3399bdb2fed14edbad0e6b7882afc452) (no login) · **About:** [fraud.anphung.dev/about](https://fraud.anphung.dev/about)

## What this is

An event-driven pipeline that scores credit-card transactions for fraud as they stream through Kafka, using an unsupervised anomaly detector (Isolation Forest) rather than a supervised classifier — the [Kaggle Credit Card Fraud dataset](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud) is only 0.17% fraud, so the model learns what "normal" spending looks like and flags outliers, rather than relying on scarce fraud labels.

```
Producer → Kafka → Scoring (Isolation Forest) → Query API (FastAPI) → Postgres + live dashboard
```

- **Producer** replays held-out transactions onto `transactions.raw` at a configurable rate.
- **Scoring** consumes `transactions.raw` (+ a priority `transactions.manual` topic), scores each transaction, publishes to `transactions.scored`. Exposes Prometheus metrics on its own port.
- **Query API** persists scored transactions to Postgres, serves the REST API + a Server-Sent Events stream to the live dashboard, and exposes its own `/metrics`.

## Infrastructure

- **Kubernetes**: single-node k3s on a GCP VM, Traefik ingress, cert-manager (Let's Encrypt).
- **GitOps**: GitHub Actions builds images → pushes to GHCR → commits the new tag into Helm values → ArgoCD auto-syncs. See `.github/workflows/ci.yml`.
- **IaC**: the VM itself, its static IP, and the SSH firewall rule are Terraform-managed (`terraform/gcp.tf`) — imported from a hand-created instance, `terraform plan` stays clean.
- **Observability**: Prometheus scrapes app-level metrics (throughput, scoring latency, fraud rate, API request/error rates); Grafana dashboards provisioned as code (`k8s/observability.yaml`).

## Known limitations

Written down on purpose, not hidden — this is a personal/demo deployment, not a production system:
- No auth on the API or the live-submit endpoint.
- Single VM, no HA, no failover.
- The threshold-tuning step in `train.py` currently selects its operating point using the test set's labels — a methodology gap (should use a held-out validation split instead) that's on the list to fix, not yet done.
- Model artifact is a committed `.pkl`, no model registry/experiment tracking.

## Local development

```bash
# 1. Get the dataset (writes data/creditcard.csv)
pip install kagglehub && python download_data.py

# 2. Train the model (writes models/isolation_forest_v2.pkl + data/creditcard_test.csv)
pip install -r requirements.txt
python train.py

# 3. Bring up the full stack
docker compose up -d --build
```

- Query API: http://localhost:8001 (`/demo` for the live dashboard, `/docs` for the API)
- Kafka: `localhost:9092`, Postgres: `localhost:5432`

Run tests: `pytest tests/ -v`. Lint: `ruff check src/ tests/ query_api/ scoring/ producer/`.

## Tech stack

Python 3.13 · FastAPI · scikit-learn · confluent-kafka · SQLAlchemy/Postgres · Kubernetes (k3s) · Helm · ArgoCD · Terraform · Prometheus/Grafana · GitHub Actions
