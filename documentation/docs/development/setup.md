# Guide de Mise en Place

## Prérequis

Avant de commencer, assurez-vous d'avoir installé :

| Outil | Version | Description |
|-------|---------|-------------|
| Docker | 24+ | Moteur de conteneurs |
| Docker Compose | 2.0+ | Orchestration de conteneurs |
| Git | 2.0+ | Gestion de version |
| curl | - | Pour tester les APIs |
| jq | - | Pour parser le JSON (optionnel) |

### Vérification des Prérequis

```bash
# Vérifier Docker
docker --version
# Docker version 24.0.7

# Vérifier Docker Compose
docker compose version
# Docker Compose version v2.21.0

# Vérifier Git
git --version
# git version 2.42.0
```

## Cloner le Repository

```bash
# Cloner le repository
git clone <repository-url>
cd ECommerce-Platform

# Vérifier la structure
ls -la
```

## Démarrer la Plateforme

### Démarrage Complet

```bash
# Construire et démarrer tous les services depuis la racine du dépôt
docker compose -f infra/docker-compose.yml up --build

# Ou démarrer en arrière-plan
docker compose -f infra/docker-compose.yml up -d --build
```

### Services Démarrés

La commande démarre les services suivants :

| Service | Port | Description |
|---------|------|-------------|
| traefik | 80, 8090 | API Gateway |
| frontend | - | Interface React |
| orders | - | Service de commandes |
| inventory | - | Service de stock |
| payments | - | Service de paiement |
| keycloak | - | Authentification |
| kafka | 9092 | Message broker |
| kafka-ui | 8081 | Interface Kafka |
| otel-collector | 4317, 4318 | Collecteur OTel |
| prometheus | 9090 | Metrics |
| tempo | 3200 | Traces |
| loki | 3100 | Logs |
| grafana | 3000 | Dashboards |

### Vérifier le Démarrage

```bash
# Voir l'état des services
docker compose -f infra/docker-compose.yml ps

# Voir les logs
docker compose -f infra/docker-compose.yml logs -f
```

## Accès aux Services

Une fois tous les services démarrés, accédez-y via :

| Service | URL | Credentials |
|---------|-----|-------------|
| **Frontend** | http://localhost | None |
| **Traefik Dashboard** | http://localhost:8090 | None |
| **Kafka UI** | http://localhost:8081 | None |
| **Grafana** | http://localhost:3000 | Anonymous (Admin) |
| **Prometheus** | http://localhost:9090 | None |
| **Keycloak Admin** | http://localhost/auth | Identifiants configurés localement |

## Premier Utilisation

### 1. Créer un Utilisateur Keycloak

1. Aller sur http://localhost/auth/admin
2. Se connecter avec les identifiants administrateur configurés pour l'environnement local
3. Sélectionner le realm `ecommerce`
4. Aller dans **Users** → **Create new user**
5. Remplir :
   - Username: `user`
   - Email: `user@example.com`
6. Aller dans **Credentials** → **Set password**
    - Password: choisir un mot de passe de développement non partagé
   - Temporary: **OFF**

### 2. Tester le Frontend

1. Aller sur http://localhost
2. Se connecter avec Keycloak :
   - Username: `user`
   - Password: valeur définie lors de la création de l'utilisateur
3. Naviguer dans les onglets :
   - **Boutique** : Commander des produits
   - **Stocks** : Voir les niveaux de stock
   - **Caisse** : Payer les commandes

### 3. Vérifier les Logs

1. Aller sur http://localhost:3000
2. **Explore** → **Loki**
3. Requête : `{service_name="orders"}`
4. Voir les logs en temps réel

## Arrêter la Plateforme

```bash
# Arrêter tous les services
docker compose -f infra/docker-compose.yml down

# Arrêter et supprimer les volumes (données perdues)
docker compose -f infra/docker-compose.yml down -v
```

## Redémarrer un Service Spécifique

```bash
# Redémarrer le service orders
docker compose -f infra/docker-compose.yml restart orders

# Voir les logs d'un service
docker compose -f infra/docker-compose.yml logs -f orders
```

## Développer Localement

### Frontend

Le frontend est monté en volume pour le HMR (Hot Module Reload) :

```bash
# Modifier un fichier dans frontend/src/
# Les changements sont automatiquement rechargés
```

### Microservices

Les services Python sont aussi montés en volume :

```bash
# Modifier un fichier dans services/orders/
# Le service se recharge automatiquement (--reload)
```

## Tests Manuels

### Tester l'API Orders

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
  -d '{
    "product_id": "LAPTOP-001",
    "customer_id": "user",
    "quantity": 1
  }'
```

### Tester le Health Check

```bash
curl http://localhost/api/orders/health
curl http://localhost/api/inventory/health
curl http://localhost/api/payments/health
```

## Dépannage Rapide

### Service ne démarre pas

```bash
# Voir les logs du service
docker compose -f infra/docker-compose.yml logs orders

# Vérifier les dépendances
docker compose -f infra/docker-compose.yml ps
```

### Kafka non disponible

```bash
# Vérifier le health check
docker exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# Redémarrer Kafka
docker compose -f infra/docker-compose.yml restart kafka
```

### Keycloak inaccessible

```bash
# Vérifier les logs
docker compose -f infra/docker-compose.yml logs keycloak

# Redémarrer
docker compose -f infra/docker-compose.yml restart keycloak
```

## Commandes Utiles

```bash
# Voir les ressources Docker utilisées
docker stats

# Nettoyer les conteneurs arrêtés
docker system prune

# Voir les volumes Docker
docker volume ls

# Supprimer tous les volumes du projet
docker volume rm ECommerce-Platform_keycloak_data
```

## Environnement de Développement

### Variables d'Environnement

Les services utilisent les variables suivantes :

**Keycloak** :
- `KEYCLOAK_ADMIN` : nom du compte administrateur local
- `KEYCLOAK_ADMIN_PASSWORD` : mot de passe fourni localement, à ne pas versionner

**Grafana** :
- `GF_AUTH_ANONYMOUS_ENABLED=true`
- `GF_AUTH_ANONYMOUS_ORG_ROLE=Admin`

### Ports Utilisés

| Port | Usage |
|------|-------|
| 80 | Traefik (API) |
| 8090 | Traefik (Dashboard) |
| 8081 | Kafka UI |
| 9090 | Prometheus |
| 3000 | Grafana |
| 3100 | Loki |
| 3200 | Tempo |
| 9092 | Kafka |

## Prochaines Étapes

1. Lire la documentation [Architecture](../architecture/overview.md)
2. Explorer les [Dashboards Grafana](../observability/grafana.md)
3. Comprendre les [Flux Événementiels](../architecture/event-driven.md)
4. Consulter l'[API Reference](../api/orders-api.md)

Pour un déploiement Kubernetes, suivre le [guide Minikube](./minikube.md) plutôt que cette procédure Compose.
