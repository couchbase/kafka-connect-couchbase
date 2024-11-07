module "msk-plugin" {
  source      = "github.com/NetDocs-Apps/tf-msk-connector-mod-iac?v0.1//mod/msk-plugin"
  tags        = {}
  env         = var.env
  file_name   = "kafka-connect-couchbase-4.2.4-SNAPSHOT.jar"
  plugin_name = "kafka-connect-couchbase"
}