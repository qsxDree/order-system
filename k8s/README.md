# Kubernetes deployment

Manifests to run the whole system on a local cluster (kind / minikube). They
demonstrate a Deployment + Service per Go microservice, config/secrets, health
probes, and a HorizontalPodAutoscaler on the order service.

## Prerequisites

- A local cluster: `kind create cluster` (or `minikube start`)
- For the HPA to actually scale: `metrics-server` installed

## Build & load images

Each Go service builds a minimal statically-linked binary on Alpine:

```bash
# from the repo root
for svc in order inventory payment shipping notification; do
  docker build -t ordersys-${svc}-service:latest -f services/${svc}-service/Dockerfile .
  kind load docker-image ordersys-${svc}-service:latest
done
```

## Apply

```bash
kubectl apply -f k8s/          # applies every manifest in order
kubectl -n ordersys get pods -w
```

## Access

```bash
# Order API + ops endpoints
kubectl -n ordersys port-forward svc/order-service 4001:4001

# RabbitMQ management UI
kubectl -n ordersys port-forward svc/rabbitmq 15672:15672
```

## Autoscaling demo

```bash
kubectl -n ordersys get hpa order-service -w
# generate load against the order API and watch replicas scale 2 → 6
```

## Notes

- Infra (Postgres/RabbitMQ/Redis) runs as single-replica Deployments — fine for
  a demo. In production use managed services or operators (CloudNativePG, the
  RabbitMQ cluster operator).
- Schema migrations run automatically upon database initialization from `infra/postgres/init.sql`.
