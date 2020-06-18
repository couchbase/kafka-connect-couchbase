#!/usr/bin/env sh

# This shell script modifies a kafka-connect-couchbase version 3.x config file
# (in either *.properties or *.json format) and renames the properties
# so the config file may be used with version 4.x of the connector.

set -e
set -u

BACKUP_SUFFIX=.backup

if [ "$#" -ne 1 ]; then
  echo "This script takes one argument: the path of the config file to migrate."
  echo "The config file will be modified in place."
  echo "A backup will be created with filename suffix '$BACKUP_SUFFIX'."
  exit 1
fi

INPUT_FILE="$1"

case $INPUT_FILE in *.json | *.properties) true ;; *)
  echo "ERROR: Expected filename to end with .json or .properties"
  exit 1
esac

BACKUP_FILE="$INPUT_FILE$BACKUP_SUFFIX"

if [ -f "$BACKUP_FILE" ]; then
    echo "ERROR: Backup file '$BACKUP_FILE' already exists."
    exit 1
fi

sed -i "$BACKUP_SUFFIX" \
-e "s/connection.cluster_address/couchbase.seed.nodes/g" \
-e "s/couchbase.network/couchbase.network/g" \
-e "s/connection.bucket/couchbase.bucket/g" \
-e "s/connection.username/couchbase.username/g" \
-e "s/connection.password/couchbase.password/g" \
-e "s/connection.timeout.ms/couchbase.bootstrap.timeout/g" \
-e "s/connection.ssl.enabled/couchbase.enable.tls/g" \
-e "s/connection.ssl.keystore.password/couchbase.trust.store.password/g" \
-e "s/connection.ssl.keystore.location/couchbase.trust.store.path/g" \
-e "s/couchbase.log_redaction/couchbase.log.redaction/g" \
-e "s/topic.name/couchbase.topic/g" \
-e "s/dcp.message.converter.class/couchbase.source.handler/g" \
-e "s/event.filter.class/couchbase.event.filter/g" \
-e "s/batch.size.max/couchbase.batch.size.max/g" \
-e "s/compat.connector_name_in_offsets/couchbase.connector.name.in.offsets/g" \
-e "s/couchbase.stream_from/couchbase.stream.from/g" \
-e "s/couchbase.log_redaction/couchbase.log.redaction/g" \
-e "s/couchbase.compression/couchbase.compression/g" \
-e "s/couchbase.persistence_polling_interval/couchbase.persistence.polling.interval/g" \
-e "s/couchbase.flow_control_buffer/couchbase.flow.control.buffer/g" \
-e "s/couchbase.document.id/couchbase.document.id/g" \
-e "s/couchbase.remove.document.id/couchbase.remove.document.id/g" \
-e "s/couchbase.durability.persist_to/couchbase.persist.to/g" \
-e "s/couchbase.durability.replicate_to/couchbase.replicate.to/g" \
-e "s/couchbase.subdocument.path/couchbase.subdocument.path/g" \
-e "s/couchbase.document.mode/couchbase.document.mode/g" \
-e "s/couchbase.subdocument.operation/couchbase.subdocument.operation/g" \
-e "s/couchbase.n1ql.operation/couchbase.n1ql.operation/g" \
-e "s/couchbase.n1ql.where_fields/couchbase.n1ql.where.fields/g" \
-e "s/couchbase.subdocument.create_path/couchbase.subdocument.create.path/g" \
-e "s/couchbase.subdocument.create_document/couchbase.create.document/g" \
-e "s/couchbase.document.expiration/couchbase.document.expiration/g" \
"$INPUT_FILE"

echo "The original config was backed up to '$BACKUP_FILE'"
echo "SUCCESS: The connector config file '$INPUT_FILE' has been migrated successfully."

for OBSOLETE_PROPERTY in "use_snapshots" "couchbase.forceIPv4"
do
  if grep -q "$OBSOLETE_PROPERTY" "$INPUT_FILE"; then
  echo "*** Please manually remove the obsolete '$OBSOLETE_PROPERTY' property."
  fi
done

if grep -q couchbase.bootstrap.timeout "$INPUT_FILE"; then
  echo "*** Please manually update the 'couchbase.bootstrap.timeout' property and append 'ms' to the value to indicate the value is in milliseconds."
fi
