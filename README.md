# Lab 5 — Microservices with Consul (Service Discovery + KV Config)

Continuation of Lab 4. The static `config-server` from Lab 4 is replaced by
**HashiCorp Consul**, which now provides:

- **Service registry / discovery** — every microservice self-registers with
  Consul on startup, including an HTTP health check. `facade-service` discovers
  `logging-service` and `counter-service` instances dynamically — no hard-coded
  addresses.
- **Key-value config store** — Hazelcast (cluster members, map name) and
  message-queue (queue name, members) settings are stored in Consul KV, read by
  the relevant services on startup.

## Services

| Service          | Hostname / instances           | Port |
|------------------|--------------------------------|------|
| consul           | consul (1)                     | 8500 |
| facade-service   | facade-service (1)             | 8082 |
| counter-service  | counter-service (1)            | 8002 |
| logging-service  | logging-service-1/2/3          | 8001 |
| hazelcast        | hazelcast-1/2/3                | 5701 |
| postgres         | postgres                       | 5433 |

## Run

```bash
docker compose up --build
# Consul UI: http://localhost:8500/ui
# Swagger:   http://localhost:8082/apidocs/
```

## Smoke test

```bash
# View what facade discovers via Consul
curl -s http://localhost:8082/services | jq .

# 10 transactions
for i in $(seq 1 10); do
  curl -s -X POST http://localhost:8082/transaction \
    -H 'Content-Type: application/json' \
    -d "{\"user_id\":\"user1\",\"amount\":$i}"; echo
done

curl -s http://localhost:8082/user/user1 | jq .
```

## Fault tolerance

```bash
docker compose stop logging-service-2
# Consul UI now shows logging-service-2 as critical;
# facade routes around it — POST/GET still succeed.
docker compose start logging-service-2
```

## Performance test

```bash
python3 perf_test.py --clients 10 --per-client 200 --out perf_lab5.json
```
