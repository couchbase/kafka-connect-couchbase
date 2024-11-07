region = "us-west-2"
tags   = {}
env    = "non-prod"
locals {
  subnets = [
    "subnet-0977bacd712bb451a",
    "subnet-060e02de25cdc9c54",
    "subnet-0c8462d6423307dca"
  ]
  security_groups = [
    "sg-0a69ed71d8fd50e3b"
  ]
  log_group = "/aws/msk-connect/msk-couchbase-events"
  bootstrap_servers = [
    "b-1.couchbasebucketstreams.uo4au4.c5.kafka.us-west-2.amazonaws.com:9092",
    "b-2.couchbasebucketstreams.uo4au4.c5.kafka.us-west-2.amazonaws.com:9092",
    "b-3.couchbasebucketstreams.uo4au4.c5.kafka.us-west-2.amazonaws.com:9092"
  ]
  iam_role = "arn:aws:iam::730335459224:role/msk-couchbase-events-connector-role"
  connector_configurations = {
    cb-directory-acc-stream = {
      "connector.class"                           = "com.couchbase.connect.kafka.CouchbaseSourceConnector",
      "couchbase.persistence.polling.interval"    = "100ms",
      "couchbase.enable.hostname.verification"    = "false",
      "couchbase.bootstrap.timeout"               = "10s",
      "couchbase.custom.handler.nd.fields"        = "type,guid,admins,groups,members,cabinets",
      "tasks.max"                                 = "1",
      "transforms.setPartition.type"              = "com.netdocuments.connect.kafka.transforms.SetKafkaPartitionFromKey",
      "couchbase.enable.certificate.verification" = "false",
      "transforms"                                = "setPartition",
      "producer.retries"                          = "2147483647",
      "couchbase.env.timeout.kvTimeout"           = "10s",
      "couchbase.replicate.to"                    = "NONE",
      "couchbase.source.handler"                  = "com.netdocuments.connect.kafka.handler.source.NDSourceHandler",
      "couchbase.bucket"                          = "directory",
      "couchbase.flow.control.buffer"             = "16m",
      "couchbase.password"                        = "ei38kah3#$,ejw2",
      "transforms.setPartition.partitions"        = "6",
      "couchbase.topic"                           = "directory-acc",
      "errors.retry.timeout"                      = "10000",
      "couchbase.custom.handler.nd.key.regex"     = "^(UG-|NG-).*",
      "topics"                                    = "directory-acc",
      "errors.retry.delay.max.ms"                 = "100",
      "couchbase.log.document.lifecycle"          = "false",
      "couchbase.custom.handler.nd.output.format" = "cloudevent",
      "couchbase.seed.nodes"                      = "dev-dircb08.ndlab.local,dev-dircb09.ndlab.local,dev-dircb10.ndlab.local",
      "couchbase.enable.tls"                      = "true",
      "retries"                                   = "2147483647",
      "classloader.check.plugin.dependencies"     = "true",
      "couchbase.stream.from"                     = "SAVED_OFFSET_OR_NOW",
      "couchbase.username"                        = "msk",
      "errors.tolerance"                          = "all"
    }
    cb-directory-search-stream = {
      "connector.class"                           = "com.couchbase.connect.kafka.CouchbaseSourceConnector",
      "couchbase.persistence.polling.interval"    = "100ms",
      "couchbase.enable.hostname.verification"    = "false",
      "couchbase.bootstrap.timeout"               = "10s",
      "couchbase.custom.handler.nd.fields"        = "type,guid,repositories,repository,cabinets,groups,membership",
      "tasks.max"                                 = "1",
      "transforms.setPartition.type"              = "com.netdocuments.connect.kafka.transforms.SetKafkaPartitionFromKey",
      "couchbase.enable.certificate.verification" = "false",
      "transforms"                                = "setPartition",
      "producer.retries"                          = "2147483647",
      "couchbase.env.timeout.kvTimeout"           = "10s",
      "couchbase.replicate.to"                    = "NONE",
      "couchbase.source.handler"                  = "com.netdocuments.connect.kafka.handler.source.NDSourceHandler",
      "couchbase.bucket"                          = "directory",
      "couchbase.flow.control.buffer"             = "16m",
      "couchbase.password"                        = "ei38kah3#$,ejw2",
      "transforms.setPartition.partitions"        = "6",
      "couchbase.topic"                           = "directory-search",
      "errors.retry.timeout"                      = "10000",
      "couchbase.custom.handler.nd.key.regex"     = "^(DUCOT-|UG-|NG-|CA-).*",
      "topics"                                    = "directory-search",
      "errors.retry.delay.max.ms"                 = "100",
      "couchbase.log.document.lifecycle"          = "false",
      "couchbase.custom.handler.nd.output.format" = "cloudevent",
      "couchbase.seed.nodes"                      = "dev-dircb08.ndlab.local,dev-dircb09.ndlab.local,dev-dircb10.ndlab.local",
      "couchbase.enable.tls"                      = "true",
      "retries"                                   = "2147483647",
      "classloader.check.plugin.dependencies"     = "true",
      "couchbase.stream.from"                     = "SAVED_OFFSET_OR_NOW",
      "couchbase.username"                        = "msk",
      "errors.tolerance"                          = "all"
    }

    cb-nmd-acl-stream = {
      "connector.class"                           = "com.couchbase.connect.kafka.CouchbaseSourceConnector",
      "couchbase.persistence.polling.interval"    = "100ms",
      "couchbase.enable.hostname.verification"    = "false",
      "couchbase.bootstrap.timeout"               = "10s",
      "couchbase.custom.handler.nd.fields"        = "documents.1.docProps.id,envProps.url,envProps.acl,envProps.containingcabs",
      "tasks.max"                                 = "1",
      "transforms.setPartition.type"              = "com.netdocuments.connect.kafka.transforms.SetKafkaPartitionFromKey",
      "producer.retries"                          = "2147483647",
      "couchbase.enable.certificate.verification" = "false",
      "transforms"                                = "setPartition",
      "couchbase.env.timeout.kvTimeout"           = "10s",
      "couchbase.replicate.to"                    = "NONE",
      "couchbase.source.handler"                  = "com.netdocuments.connect.kafka.handler.source.NDSourceHandler",
      "couchbase.bucket"                          = "nmd",
      "couchbase.flow.control.buffer"             = "16m",
      "couchbase.password"                        = "$${secretsmanager:lab/cb/msk:couchbase_password}",
      "transforms.setPartition.partitions"        = "6",
      "couchbase.topic"                           = "nmd-acl",
      "errors.retry.timeout"                      = "10000",
      "topics"                                    = "nmd-acl",
      "couchbase.event.filter.regex"              = "^M.*",
      "errors.retry.delay.max.ms"                 = "100",
      "couchbase.log.document.lifecycle"          = "false",
      "couchbase.seed.nodes"                      = "dev-nmdcb01.ndlab.local,dev-nmdcb02.ndlab.local,dev-nmdcb03.ndlab.local",
      "couchbase.enable.tls"                      = "true",
      "retries"                                   = "2147483647",
      "classloader.check.plugin.dependencies"     = "true",
      "couchbase.stream.from"                     = "SAVED_OFFSET_OR_NOW",
      "couchbase.username"                        = "$${secretsmanager:lab/cb/msk:couchbase_username}",
      "errors.tolerance"                          = "all",
      "couchbase.event.filter"                    = "com.netdocuments.connect.kafka.filter.RegexKeyFilter"
    }
    cb-nmd-history-stream-split = {
      "connector.class"                           = "com.couchbase.connect.kafka.CouchbaseSourceConnector",
      "couchbase.persistence.polling.interval"    = "100ms",
      "couchbase.custom.handler.nd.s3.bucket"     = "nd-us-west-2-dev-nmd-dcp-messages",
      "couchbase.enable.hostname.verification"    = "false",
      "couchbase.bootstrap.timeout"               = "10s",
      "tasks.max"                                 = "8",
      "transforms.setPartition.type"              = "com.netdocuments.connect.kafka.transforms.SetKafkaPartitionFromKey",
      "producer.retries"                          = "2147483647",
      "couchbase.enable.certificate.verification" = "false",
      "transforms"                                = "setPartition",
      "couchbase.env.timeout.kvTimeout"           = "10s",
      "couchbase.replicate.to"                    = "NONE",
      "couchbase.source.handler"                  = "com.netdocuments.connect.kafka.handler.source.NDSourceHandler",
      "couchbase.bucket"                          = "nmd",
      "couchbase.flow.control.buffer"             = "16m",
      "couchbase.password"                        = "$${secretsmanager:lab/cb/msk:couchbase_password}",
      "transforms.setPartition.partitions"        = "6",
      "couchbase.custom.handler.nd.s3.region"     = "us-west-2",
      "couchbase.topic"                           = "nmd-history-split",
      "errors.retry.timeout"                      = "10000",
      "topics"                                    = "nmd-history-split",
      "couchbase.event.filter.regex"              = "^H.*",
      "errors.retry.delay.max.ms"                 = "100",
      "couchbase.log.document.lifecycle"          = "false",
      "couchbase.seed.nodes"                      = "dev-nmdcb01.ndlab.local,dev-nmdcb02.ndlab.local,dev-nmdcb03.ndlab.local",
      "couchbase.enable.tls"                      = "true",
      "retries"                                   = "2147483647",
      "classloader.check.plugin.dependencies"     = "true",
      "couchbase.stream.from"                     = "SAVED_OFFSET_OR_NOW",
      "couchbase.username"                        = "$${secretsmanager:lab/cb/msk:couchbase_username}",
      "errors.tolerance"                          = "all",
      "couchbase.event.filter"                    = "com.netdocuments.connect.kafka.filter.RegexKeyFilter"
    }
    cb-nmd-metadata-stream = {
      "connector.class"                           = "com.couchbase.connect.kafka.CouchbaseSourceConnector",
      "couchbase.persistence.polling.interval"    = "100ms",
      "couchbase.enable.hostname.verification"    = "false",
      "couchbase.bootstrap.timeout"               = "10s",
      "couchbase.custom.handler.nd.fields"        = "*",
      "tasks.max"                                 = "1",
      "transforms.setPartition.type"              = "com.netdocuments.connect.kafka.transforms.SetKafkaPartitionFromKey",
      "couchbase.enable.certificate.verification" = "false",
      "transforms"                                = "setPartition",
      "producer.retries"                          = "2147483647",
      "couchbase.env.timeout.kvTimeout"           = "10s",
      "couchbase.replicate.to"                    = "NONE",
      "couchbase.source.handler"                  = "com.netdocuments.connect.kafka.handler.source.NDSourceHandler",
      "couchbase.bucket"                          = "nmd",
      "couchbase.flow.control.buffer"             = "16m",
      "couchbase.password"                        = "ei38kah3#$,ejw2",
      "transforms.setPartition.partitions"        = "6",
      "couchbase.topic"                           = "nmd-metadata",
      "couchbase.custom.filter.size"              = "1000000",
      "errors.retry.timeout"                      = "10000",
      "couchbase.custom.handler.nd.key.regex"     = "^M.*",
      "topics"                                    = "nmd-metadata",
      "errors.retry.delay.max.ms"                 = "100",
      "couchbase.log.document.lifecycle"          = "false",
      "couchbase.custom.handler.nd.output.format" = "cloudevent",
      "couchbase.seed.nodes"                      = "dev-nmdcb01.ndlab.local,dev-nmdcb02.ndlab.local,dev-nmdcb03.ndlab.local",
      "couchbase.enable.tls"                      = "true",
      "retries"                                   = "2147483647",
      "classloader.check.plugin.dependencies"     = "true",
      "couchbase.stream.from"                     = "SAVED_OFFSET_OR_NOW",
      "couchbase.username"                        = "msk",
      "couchbase.event.filter"                    = "com.couchbase.connect.kafka.filter.PassSmallFilter",
      "errors.tolerance"                          = "all"
    }
    cb-nmd-metadata-stream-split = {
      "connector.class"                           = "com.couchbase.connect.kafka.CouchbaseSourceConnector",
      "couchbase.persistence.polling.interval"    = "100ms",
      "couchbase.custom.handler.nd.s3.bucket"     = "nd-us-west-2-dev-nmd-dcp-messages",
      "couchbase.enable.hostname.verification"    = "false",
      "couchbase.bootstrap.timeout"               = "10s",
      "tasks.max"                                 = "8",
      "transforms.setPartition.type"              = "com.netdocuments.connect.kafka.transforms.SetKafkaPartitionFromKey",
      "producer.retries"                          = "2147483647",
      "couchbase.enable.certificate.verification" = "false",
      "transforms"                                = "setPartition",
      "couchbase.env.timeout.kvTimeout"           = "10s",
      "couchbase.replicate.to"                    = "NONE",
      "couchbase.source.handler"                  = "com.netdocuments.connect.kafka.handler.source.NDSourceHandler",
      "couchbase.bucket"                          = "nmd",
      "couchbase.flow.control.buffer"             = "16m",
      "couchbase.password"                        = "$${secretsmanager:lab/cb/msk:couchbase_password}",
      "transforms.setPartition.partitions"        = "6",
      "couchbase.custom.handler.nd.s3.region"     = "us-west-2",
      "couchbase.topic"                           = "nmd-metadata-split",
      "errors.retry.timeout"                      = "10000",
      "topics"                                    = "nmd-metadata-split",
      "couchbase.event.filter.regex"              = "^M.*",
      "errors.retry.delay.max.ms"                 = "100",
      "couchbase.log.document.lifecycle"          = "false",
      "couchbase.seed.nodes"                      = "dev-nmdcb01.ndlab.local,dev-nmdcb02.ndlab.local,dev-nmdcb03.ndlab.local",
      "couchbase.enable.tls"                      = "true",
      "retries"                                   = "2147483647",
      "classloader.check.plugin.dependencies"     = "true",
      "couchbase.stream.from"                     = "SAVED_OFFSET_OR_NOW",
      "couchbase.username"                        = "$${secretsmanager:lab/cb/msk:couchbase_username}",
      "errors.tolerance"                          = "all",
      "couchbase.event.filter"                    = "com.netdocuments.connect.kafka.filter.RegexKeyFilter"
    }
  }
  worker_configurations = {
    ndcb-worker-secrets-offset-topic = {
      "key.converter"                         = "org.apache.kafka.connect.storage.StringConverter"
      "value.converter"                       = "org.apache.kafka.connect.converters.ByteArrayConverter"
      "errors.tolerance"                      = "all"
      "errors.retry.timeout"                  = "10000"
      "errors.retry.delay.max.ms"             = "100"
      "retries"                               = "2147483647"
      "classloader.check.plugin.dependencies" = "true"
      "offset.storage.topic"                  = "__msk_connect_offsets"
      # define name of config provider:
      "config.providers" = "secretsmanager"
      # provide implementation classes for secrets manager:
      "config.providers.secretsmanager.class" = "com.amazonaws.kafka.config.providers.SecretsManagerConfigProvider"
      # configure a config provider (if it needs additional initialization), for example you can provide a region where the secrets or parameters are located:
      "config.providers.secretsmanager.param.region"           = "us-west-2"
      "config.providers.secretsmanager.param.NotFoundStrategy" = "fail"
    }
    ndcb-worker-with-offset-topic = {
      "key.converter"                         = "org.apache.kafka.connect.storage.StringConverter"
      "value.converter"                       = "org.apache.kafka.connect.converters.ByteArrayConverter"
      "errors.tolerance"                      = "all"
      "errors.retry.timeout"                  = "10000"
      "errors.retry.delay.max.ms"             = "100"
      "retries"                               = "2147483647"
      "classloader.check.plugin.dependencies" = "true"
      "offset.storage.topic"                  = "__msk_connect_offsets"
    }
  }
}