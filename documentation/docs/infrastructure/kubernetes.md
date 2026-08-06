# Kubernetes

## Périmètre

La cible Kubernetes est un environnement de démonstration local, principalement prévu pour Minikube. Les manifests sont générés par les charts Helm du répertoire `infra/helm/`, puis orchestrés par Helmfile.

Le déploiement Kubernetes ne remplace pas automatiquement le parcours Docker Compose : les deux parcours utilisent des configurations proches, mais leurs mécanismes de découverte, de routage et de persistance sont différents.

## Namespaces

| Namespace | Contenu |
|-----------|---------|
| `ecommerce` | Kafka, Keycloak, microservices, frontend et observabilité |
| `ingress-system` | Release Traefik et sa `IngressClass` |

Les namespaces ne sont pas créés automatiquement par Helmfile : `createNamespace` vaut `false`. Ils doivent donc exister avant le déploiement.

## Releases déployées

Helmfile installe les releases suivantes :

| Release | Namespace | Chart |
|---------|-----------|-------|
| `kafka` | `ecommerce` | `infra/helm/kafka` |
| `observability` | `ecommerce` | `infra/helm/observability` |
| `keycloak` | `ecommerce` | `infra/helm/keycloak` |
| `traefik` | `ingress-system` | `infra/helm/traefik-config` |
| `orders` | `ecommerce` | `infra/helm/charts/orders` |
| `payments` | `ecommerce` | `infra/helm/charts/payments` |
| `inventory` | `ecommerce` | `infra/helm/charts/inventory` |
| `frontend` | `ecommerce` | `infra/helm/charts/frontend` |

Les dépendances `needs` de Helmfile ordonnent notamment Kafka avant l'observabilité et les services métier, puis Keycloak, l'observabilité et Traefik avant les applications.

## Services internes

Les composants communiquent par services Kubernetes `ClusterIP` et par DNS interne :

| Composant | Adresse utilisée dans le namespace `ecommerce` |
|-----------|-----------------------------------------------|
| Kafka | `kafka:9092` pour les clients |
| OTel Collector | `otel-collector:4317` |
| Orders | `orders:8000` |
| Inventory | `inventory:8000` |
| Payments | `payments:8000` |
| Keycloak | `keycloak:8080` |

Kafka est un `StatefulSet` avec un service headless. Son advertised listener utilise l'identité stable `kafka-0.kafka.ecommerce.svc.cluster.local:9092`.

## Routage HTTP

Les charts applicatifs créent des objets `Ingress` classiques avec l'annotation `kubernetes.io/ingress.class: traefik` et `ingressClassName: traefik`.

| Préfixe | Service | Port |
|---------|---------|------|
| `/api/orders` | `orders` | 8000 |
| `/api/inventory` | `inventory` | 8000 |
| `/api/payments` | `payments` | 8000 |
| `/` | `frontend` | 5173 |
| `/auth` | `keycloak` | 8080 |

Les hosts sont vides dans les valeurs dev : le routage se fait donc par chemin, sans nom DNS obligatoire. Les valeurs `root_path` des applications FastAPI sont respectivement `/api/orders`, `/api/inventory` et `/api/payments`. Les templates Ingress actuels ne déclarent pas de middleware de réécriture ; ce point doit être vérifié par un test HTTP après déploiement, car la présence du préfixe côté proxy et la valeur `root_path` doivent rester cohérentes.

## Persistance

- Kafka utilise un `StatefulSet` et une réclamation de volume de `2Gi` en environnement dev.
- Keycloak utilise une réclamation de volume de `1Gi` en environnement dev.
- Les composants d'observabilité n'ont pas de volume persistant configuré dans le chart actuel.
- Les données métier des services restent en mémoire dans cette démonstration.

## Sécurité Kubernetes

Les charts des applications désactivent le montage automatique du token des ServiceAccounts, utilisent `RuntimeDefault` pour le profil seccomp et suppriment les capabilities Linux. Les conteneurs ne disposent donc pas, par défaut, de privilèges Kubernetes ou Linux supplémentaires.

Cette configuration reste une base de démonstration : Kafka et l'observabilité utilisent du trafic interne non chiffré, Keycloak démarre en mode dev et Grafana autorise l'accès anonyme avec le rôle `Admin`.

## Limites actuelles

- Un seul replica pour Kafka, Keycloak, Traefik et les services.
- Images applicatives locales taguées `latest`.
- Pas de registry ni de stratégie de promotion d'images.
- Pas de NetworkPolicies.
- Pas de TLS Ingress configuré.
- Pas de dashboard Grafana provisionné par le chart Kubernetes.

## Références

- [Helm](helm.md)
- [Helmfile](helmfile.md)
- [Minikube](../development/minikube.md)
- [Ingress et DNS](ingress-dns.md)
