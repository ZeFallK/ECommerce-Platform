# Helm

## Rôle

Helm empaquette les manifests Kubernetes et permet de paramétrer les déploiements sans dupliquer les ressources pour chaque microservice. Le dépôt contient des charts locaux, pas un chart généré à partir de Docker Compose.

## Organisation

```text
infra/helm/
├── charts/
│   ├── common/
│   ├── frontend/
│   ├── inventory/
│   ├── orders/
│   └── payments/
├── kafka/
├── keycloak/
├── observability/
├── traefik-config/
├── environments/dev/
└── helmfile.yaml.gotmpl
```

Les charts `orders`, `inventory`, `payments` et `frontend` réutilisent la library chart `common` pour les noms, labels, images et variables communes.

## Paramétrage dev

Les valeurs de l'environnement sont réparties dans `infra/helm/environments/dev/` :

- `common.yaml` définit `kafka:9092` et `otel-collector:4317` ;
- `kafka.yaml` active la persistance Kafka ;
- `keycloak.yaml` active la persistance Keycloak ;
- `traefik.yaml` configure le provider Ingress et le `NodePort` `30080` ;
- les fichiers applicatifs sélectionnent les images locales `frontend`, `orders`, `inventory` et `payments`, avec `pullPolicy: IfNotPresent`.

## Charts applicatifs

Chaque service métier possède :

- un `Deployment` avec une replica par défaut ;
- un service `ClusterIP` sur le port `8000` ;
- des probes HTTP sur `/health` ;
- un `Ingress` sur son préfixe `/api/...` ;
- un ServiceAccount dont le token n'est pas monté automatiquement ;
- un contexte de sécurité non privilégié.

Le frontend expose le serveur Vite sur le port `5173` et l'Ingress `/`.

## Kafka et Keycloak

Le chart Kafka crée un `StatefulSet`, un service headless, un Secret contenant le cluster ID et un Job Helm post-install/post-upgrade pour créer les topics `orders.created` et `payments.processed`.

Le chart Keycloak crée :

- un `Deployment` en `start-dev` ;
- un Secret pour le compte administrateur ;
- un Secret contenant le realm généré à partir des valeurs Helm ;
- un PVC optionnel, activé en dev ;
- un Ingress `/auth`.

Le realm Kubernetes est généré par le chart. Il ne s'agit pas d'un montage direct de `infra/keycloak/realm-export.json`, utilisé par Docker Compose.

## Rendu et validation

Depuis le répertoire `infra/helm/`, les opérations de diagnostic habituelles sont :

```bash
helm lint ./kafka
helm lint ./keycloak
helm lint ./observability
helm lint ./charts/orders
helm lint ./charts/inventory
helm lint ./charts/payments
helm lint ./charts/frontend
```

Le rendu final est piloté par Helmfile ; il faut donc aussi vérifier la combinaison de l'environnement `dev` et des valeurs de chaque release. Aucun `helm install`, `helm upgrade` ou `kubectl apply` n'est exécuté par cette documentation.

## Limites

- Les valeurs dev utilisent `latest` pour les images applicatives et plusieurs images d'infrastructure.
- Les secrets par défaut du chart Keycloak sont adaptés au développement uniquement.
- Les charts n'implémentent pas de TLS, NetworkPolicy, autoscaling ou stratégie de registry complète.
- Le chart observability crée les datasources Grafana, mais ne monte pas `infra/otel/ecommerce.json`.

## Références

- [Helmfile](helmfile.md)
- [Kubernetes](kubernetes.md)
- [Déploiement Minikube](../development/minikube.md)
