#!/usr/bin/env sh
# Populate Consul KV with config used by facade/counter/logging services.
# Run once after Consul is up.
set -e
CONSUL=${CONSUL_URL:-http://consul:8500}

put() {
  echo "  $1 = $2"
  curl -s -X PUT --data "$2" "$CONSUL/v1/kv/$1" >/dev/null
}

echo "Seeding Consul KV at $CONSUL"

# Hazelcast (used by logging-service for distributed map)
put config/hazelcast/cluster_members "hazelcast-1:5701,hazelcast-2:5701,hazelcast-3:5701"
put config/hazelcast/cluster_name "dev"
put config/hazelcast/map_name "transactions"

# Message queue (used by facade-service producer + counter-service consumer)
put config/mq/cluster_members "hazelcast-1:5701,hazelcast-2:5701,hazelcast-3:5701"
put config/mq/cluster_name "dev"
put config/mq/queue_name "transactions-queue"

echo "Done."
