# Health Checks

## Vue d'ensemble

Chaque service expose un endpoint de health check pour vérifier son état de santé. Ces endpoints ne nécessitent pas d'authentification.

## Endpoints de Health Check

### Orders Service

```
GET http://localhost/api/orders/health
```

**Response** :
```json
{
  "status": "OK",
  "service": "orders"
}
```

### Inventory Service

```
GET http://localhost/api/inventory/health
```

**Response** :
```json
{
  "status": "OK",
  "service": "inventory"
}
```

### Payments Service

```
GET http://localhost/api/payments/health
```

**Response** :
```json
{
  "status": "OK",
  "service": "payments"
}
```

## Vérification de Tous les Services

### Script Bash

```bash
#!/bin/bash

echo "Vérification des health checks..."

echo -n "Orders: "
curl -s http://localhost/api/orders/health | jq -r '.status'

echo -n "Inventory: "
curl -s http://localhost/api/inventory/health | jq -r '.status'

echo -n "Payments: "
curl -s http://localhost/api/payments/health | jq -r '.status'
```

### Résultat Attendu

```
Vérification des health checks...
Orders: OK
Inventory: OK
Payments: OK
```

## Codes de Réponse

| Code | Description |
|------|-------------|
| 200 | Service healthy |
| 503 | Service unhealthy (si implémenté) |

## Utilisation dans les déploiements

Dans Docker Compose, Kafka possède un healthcheck utilisé par les dépendances des microservices. Les services FastAPI ne déclarent pas de healthcheck Compose dédié dans `infra/docker-compose.yml`.

Dans Kubernetes, les charts `orders`, `inventory` et `payments` utilisent ces endpoints pour leurs probes de readiness et de liveness :

```yaml
readinessProbe:
  httpGet:
    path: /health
    port: http
livenessProbe:
  httpGet:
    path: /health
    port: http
```

## Monitoring

### Via Grafana

Les health checks peuvent être monitorés via Prometheus :

```promql
up{job="orders"}
```

### Via Alerting

Créer une alerte si un service n'est pas healthy :

```promql
up{job="orders"} == 0
```

## Limitations Connues

1. **Health check simple** : Seulement vérifie que le service répond
2. **Pas de vérification Kafka** : Ne vérifie pas la connexion à Kafka
3. **Pas de vérification OTel** : Ne vérifie pas l'export de télémétrie
4. **Toujours 200 OK** : Ne retourne jamais 503

## Pour la Production

Implémenter des health checks plus complets :

```python
@app.get("/health")
async def health_check(
    kafka_health: bool = Depends(check_kafka),
    db_health: bool = Depends(check_database)
):
    if not kafka_health or not db_health:
        raise HTTPException(status_code=503, detail="Service unhealthy")
    return {"status": "OK", "service": "orders"}
```
