# Orders API Reference

## Vue d'ensemble

Le **Orders Service** expose une API REST pour la création et la gestion des commandes.

### Base URL

```
http://localhost/api/orders
```

Le service FastAPI est déclaré avec `root_path="/api/orders"`. En Docker Compose, Traefik retire ce préfixe avant le forwarding ; en Kubernetes, le comportement du chemin doit être vérifié avec la configuration Ingress actuelle.

### Authentification

Tous les endpoints (sauf `/health`) nécessitent un token JWT valide.

```
Authorization: Bearer <JWT_TOKEN>
```

## Endpoints

### POST /

Crée une nouvelle commande.

#### Request

**Headers** :
```
Authorization: Bearer <JWT_TOKEN>
Content-Type: application/json
```

**Body** :
```json
{
  "product_id": "LAPTOP-001",
  "customer_id": "user123",
  "quantity": 1
}
```

**Schema** :

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `product_id` | string | Yes | Identifiant du produit |
| `customer_id` | string | Yes | Identifiant du client |
| `quantity` | integer | Yes | Quantité à commander |

#### Response

**201 Created** :
```json
{
  "order_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "pending",
  "message": "Order created successfully",
  "data": {
    "product_id": "LAPTOP-001",
    "customer_id": "user123",
    "quantity": 1
  }
}
```

**422 Unprocessable Entity** :
```json
{
  "detail": [
    {
      "type": "missing",
      "loc": ["body", "product_id"],
      "msg": "Field required"
    }
  ]
}
```

**401 Unauthorized** :
```json
{
  "detail": "Token invalide ou expiré"
}
```

#### Exemple

```bash
curl -X POST http://localhost/api/orders/ \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "product_id": "LAPTOP-001",
    "customer_id": "user123",
    "quantity": 1
  }'
```

### GET /health

Health check du service.

#### Request

Aucun header requis.

#### Response

**200 OK** :
```json
{
  "status": "OK",
  "service": "orders"
}
```

#### Exemple

```bash
curl http://localhost/api/orders/health
```

## Événements Kafka

### Topic: `orders.created`

Publié après la création d'une commande.

**Schema** :
```json
{
  "order_id": "uuid",
  "product_id": "string",
  "quantity": "integer",
  "customer_id": "string",
  "status": "pending"
}
```

## Codes d'Erreur

| Code | Description |
|------|-------------|
| 201 | Commande créée avec succès |
| 422 | Données de requête invalides selon la validation FastAPI |
| 401 | Token manquant ou invalide |
| 500 | Erreur interne du service |

## Exemples d'Utilisation

### Créer une commande

```bash
# Obtenir un token
TOKEN=$(curl -X POST http://localhost/auth/realms/ecommerce/protocol/openid-connect/token \
  -d "grant_type=password" \
  -d "client_id=ecomm-front" \
  -d "username=${KEYCLOAK_USER:?Définir KEYCLOAK_USER}" \
  -d "password=${KEYCLOAK_PASSWORD:?Définir KEYCLOAK_PASSWORD}" \
  | jq -r '.access_token')

# Créer une commande
curl -X POST http://localhost/api/orders/ \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"product_id": "PHONE-002", "customer_id": "user123", "quantity": 2}'
```

### Vérifier le health

```bash
curl http://localhost/api/orders/health
```

## Limitations

1. **Pas de validation produit** : Ne vérifie pas si le produit existe
2. **Pas de persistance** : Les commandes ne sont pas stockées
3. **Quantité entière** : Ne supporte pas les décimales
4. **Customer ID** : Utilise le username Keycloak sans validation

## Voir aussi

- [Architecture événementielle](../architecture/event-driven.md)
- [Flux des requêtes](../architecture/request-flow.md)
- [Inventory API](./inventory-api.md)
