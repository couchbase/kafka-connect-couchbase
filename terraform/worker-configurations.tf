module "ndcb-worker-secrets-offset-topic" {
  source      = "github.com/NetDocs-Apps/tf-msk-connector-mod-iac?v0.1//mod/msk-worker-configuration"
  name        = "ndcb-worker-secrets-offset-topic"
  properties  = var.worker_configurations["ndcb-worker-secrets-offset-topic"]
  description = "Worker configuration with offset secrets topic provided."
}

module "ndcb-worker-with-offset-topic" {
  source      = "github.com/NetDocs-Apps/tf-msk-connector-mod-iac?v0.1//mod/msk-worker-configuration"
  name        = "ndcb-worker-with-offset-topic"
  properties  = var.worker_configurations["ndcb-worker-with-offset-topic"]
  description = "Worker configuration with offset topic provided."
}