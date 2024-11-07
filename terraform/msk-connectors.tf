module "cb-directory-acc-stream" {
  source         = "github.com/NetDocs-Apps/tf-msk-connector-mod-iac?v0.1//mod/msk-connector"
  connector_name = "cb-directory-acc-stream"
  msk_plugin = {
    arn      = module.msk-plugin.arn
    revision = module.msk-plugin.revision
  }
  kafka_bootstrap_servers = var.bootstrap_servers
  security_groups         = var.security_groups
  subnets                 = var.subnets
  env                     = var.env
  log_group               = var.log_group
  tags                    = var.tags
  connector_configuration = var.connector_configurations["cb-directory-acc-stream"]
  provisioned_capacity = {
    mcu_count    = 1
    worker_count = 1
  }
  worker_configuration = {
    arn      = module.ndcb-worker-with-offset-topic.arn
    revision = module.ndcb-worker-with-offset-topic.revision
  }
  iam_role = var.iam_role
}

module "cb-directory-search-stream" {
  source         = "github.com/NetDocs-Apps/tf-msk-connector-mod-iac?v0.1//mod/msk-connector"
  connector_name = "cb-directory-search-stream"
  msk_plugin = {
    arn      = module.msk-plugin.arn
    revision = module.msk-plugin.revision
  }
  kafka_bootstrap_servers = var.bootstrap_servers
  security_groups         = var.security_groups
  subnets                 = var.subnets
  env                     = var.env
  log_group               = var.log_group
  tags                    = var.tags
  connector_configuration = var.connector_configurations["cb-directory-search-stream"]
  provisioned_capacity = {
    mcu_count    = 1
    worker_count = 1
  }
  worker_configuration = {
    arn      = module.ndcb-worker-with-offset-topic.arn
    revision = module.ndcb-worker-with-offset-topic.revision
  }
  iam_role = var.iam_role
}

module "cb-nmd-acl-stream" {
  source         = "github.com/NetDocs-Apps/tf-msk-connector-mod-iac?v0.1//mod/msk-connector"
  connector_name = "cb-nmd-acl-stream"
  msk_plugin = {
    arn      = module.msk-plugin.arn
    revision = module.msk-plugin.revision
  }
  kafka_bootstrap_servers = var.bootstrap_servers
  security_groups         = var.security_groups
  subnets                 = var.subnets
  env                     = var.env
  log_group               = var.log_group
  tags                    = var.tags
  connector_configuration = var.connector_configurations["cb-nmd-acl-stream"]
  provisioned_capacity = {
    mcu_count    = 1
    worker_count = 1
  }
  worker_configuration = {
    arn      = module.ndcb-worker-with-offset-topic.arn
    revision = module.ndcb-worker-with-offset-topic.revision
  }
  iam_role = var.iam_role
}

module "cb-nmd-history-stream-split" {
  source         = "github.com/NetDocs-Apps/tf-msk-connector-mod-iac?v0.1//mod/msk-connector"
  connector_name = "cb-nmd-history-stream-split"
  msk_plugin = {
    arn      = module.msk-plugin.arn
    revision = module.msk-plugin.revision
  }
  kafka_bootstrap_servers = var.bootstrap_servers
  security_groups         = var.security_groups
  subnets                 = var.subnets
  env                     = var.env
  log_group               = var.log_group
  tags                    = var.tags
  connector_configuration = var.connector_configurations["cb-nmd-history-stream-split"]
  provisioned_capacity = {
    mcu_count    = 2
    worker_count = 1
  }
  worker_configuration = {
    arn      = module.ndcb-worker-secrets-offset-topic.arn
    revision = module.ndcb-worker-secrets-offset-topic.revision
  }
  iam_role = var.iam_role
}

module "cb-nmd-metadata-stream" {
  source         = "github.com/NetDocs-Apps/tf-msk-connector-mod-iac?v0.1//mod/msk-connector"
  connector_name = "cb-nmd-metadata-stream"
  msk_plugin = {
    arn      = module.msk-plugin.arn
    revision = module.msk-plugin.revision
  }
  kafka_bootstrap_servers = var.bootstrap_servers
  security_groups         = var.security_groups
  subnets                 = var.subnets
  env                     = var.env
  log_group               = var.log_group
  tags                    = var.tags
  connector_configuration = var.cb-nmd-metadata-stream
  provisioned_capacity = {
    mcu_count    = 2
    worker_count = 1
  }
  worker_configuration = {
    arn      = module.ndcb-worker-secrets-offset-topic.arn
    revision = module.ndcb-worker-secrets-offset-topic.revision
  }
  iam_role = var.iam_role
}

module "cb-nmd-metadata-stream-split" {
  source         = "github.com/NetDocs-Apps/tf-msk-connector-mod-iac?v0.1//mod/msk-connector"
  connector_name = "cb-nmd-metadata-stream-split"
  msk_plugin = {
    arn      = module.msk-plugin.arn
    revision = module.msk-plugin.revision
  }
  kafka_bootstrap_servers = var.bootstrap_servers
  security_groups         = var.security_groups
  subnets                 = var.subnets
  env                     = var.env
  log_group               = var.log_group
  tags                    = var.tags
  connector_configuration = var.connector_configurations["cb-nmd-metadata-stream-split"]
  provisioned_capacity = {
    mcu_count    = 2
    worker_count = 1
  }
  worker_configuration = {
    arn      = module.ndcb-worker-secrets-offset-topic.arn
    revision = module.ndcb-worker-secrets-offset-topic.revision
  }
  iam_role = var.iam_role
}