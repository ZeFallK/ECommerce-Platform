# Dépannage

## Problèmes Courants

Les commandes Docker Compose ci-dessous sont à lancer depuis la racine du dépôt avec `-f infra/docker-compose.yml`. Les problèmes Kubernetes sont traités dans la section dédiée en fin de page.

### Services ne Démarrant Pas

#### Symptôme
```bash
 docker compose -f infra/docker-compose.yml ps
# orders service est "unhealthy"
```

#### Causes Possibles

1. **Kafka non disponible**
   ```bash
   docker compose -f infra/docker-compose.yml logs kafka
   # Vérifier si Kafka est healthy
   ```

2. **Port déjà utilisé**
   ```bash
   # Vérifier les ports utilisés
   netstat -tulpn | grep :80
   netstat -tulpn | grep :9092
   ```

3. **Erreur de construction Docker**
   ```bash
   docker compose -f infra/docker-compose.yml build --no-cache
   ```

#### Solution

```bash
# Redémarrer tous les services
docker compose -f infra/docker-compose.yml down
docker compose -f infra/docker-compose.yml up --build

# Vérifier l'état
docker compose -f infra/docker-compose.yml ps
```

### Kafka Inaccessible

#### Symptôme
```
Producer Kafka connect failed
Error connecting to kafka:9092
```

#### Vérifications

1. **Vérifier le health check**
   ```bash
   docker exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092
   ```

2. **Vérifier les logs**
   ```bash
    docker compose -f infra/docker-compose.yml logs kafka
   ```

3. **Vérifier le réseau**
   ```bash
   docker network inspect ECommerce-Platform_ecommerce-net
   ```

#### Solution

```bash
# Attendre que Kafka soit healthy (peut prendre 40s)
docker compose -f infra/docker-compose.yml logs kafka | grep "Kafka Server started"

# Redémarrer Kafka
docker compose -f infra/docker-compose.yml restart kafka
```

### Keycloak Inaccessible

#### Symptôme
```
401 Unauthorized
Token invalide
```

#### Vérifications

1. **Vérifier si Keycloak est démarré**
   ```bash
    docker compose -f infra/docker-compose.yml logs keycloak | grep "Running the server"
   ```

2. **Vérifier l'URL**
   - Correct : `http://localhost/auth`
   - Incorrect : `http://localhost:8080/auth`

3. **Vérifier le realm**
   - Le realm doit être `ecommerce`

#### Solution

```bash
# Redémarrer Keycloak
docker compose -f infra/docker-compose.yml restart keycloak

# Attendre le démarrage complet
sleep 30

# Tester l'accès
curl http://localhost/auth/realms/ecommerce/.well-known/openid-configuration
```

### Grafana Non Accessible

#### Symptôme
```
Unable to connect to Grafana
```

#### Vérifications

1. **Vérifier le port**
   ```bash
   curl http://localhost:3000/api/health
   ```

2. **Vérifier les datasources**
   ```bash
   curl http://localhost:3000/api/datasources
   ```

#### Solution

```bash
# Redémarrer Grafana
docker compose -f infra/docker-compose.yml restart grafana

# Vérifier les logs
docker compose -f infra/docker-compose.yml logs grafana
```

### Logs Non Apparaissant dans Loki

#### Symptôme
```
Aucun log dans Grafana Explore
```

#### Vérifications

1. **Vérifier OTel Collector**
   ```bash
    docker compose -f infra/docker-compose.yml logs otel-collector
   ```

2. **Vérifier Loki**
   ```bash
    docker compose -f infra/docker-compose.yml logs loki
   ```

3. **Vérifier le label service_name**
   ```logql
   {service_name="orders"}
   ```

#### Solution

```bash
# Redémarrer OTel Collector
docker compose -f infra/docker-compose.yml restart otel-collector

# Vérifier que les services envoient des logs
docker compose -f infra/docker-compose.yml logs orders | head -20
```

### Metrics Non Apparaissant dans Prometheus

#### Symptôme
```
Aucune metric dans Prometheus
```

#### Vérifications

1. **Vérifier les targets**
   ```bash
   curl http://localhost:9090/api/v1/targets | jq
   ```

2. **Vérifier l'exporter OTel**
   ```bash
   curl http://localhost:9090/api/v1/query?query=up
   ```

#### Solution

```bash
# Redémarrer OTel Collector
docker compose -f infra/docker-compose.yml restart otel-collector

# Vérifier l'endpoint metrics
curl http://localhost:8889/metrics
```

### Traces Non Apparaissant dans Tempo

#### Symptôme
```
Aucune trace dans Grafana Explore
```

#### Vérifications

1. **Vérifier Tempo**
   ```bash
   curl http://localhost:3200/ready
   ```

2. **Vérifier les services**
   ```bash
    docker compose -f infra/docker-compose.yml logs orders | grep "trace"
   ```

#### Solution

```bash
# Redémarrer Tempo
docker compose -f infra/docker-compose.yml restart tempo

# Redémarrer un service pour générer des traces
docker compose -f infra/docker-compose.yml restart orders
```

### Frontend Ne Charge Pas

#### Symptôme
```
Blank page ou erreur de chargement
```

#### Vérifications

1. **Vérifier le navigateur console**
   - Ouvrir DevTools (F12)
   - Vérifier les erreurs JavaScript

2. **Vérifier les appels API**
   - Onglet Network dans DevTools
   - Vérifier les requêtes échouées

3. **Vérifier l'authentification**
   - Être connecté à Keycloak
   - Token valide

#### Solution

```bash
# Redémarrer le frontend
docker compose -f infra/docker-compose.yml restart frontend

# Vider le cache du navigateur
Ctrl + Shift + Delete
```

### Erreurs de Connexion à Kafka

#### Symptôme
```
aiokafka.errors.KafkaConnectionError: Kafka connection failed
```

#### Causes

1. **Kafka en cours de démarrage**
2. **Réseau Docker incorrect**
3. **Configuration advertised listeners**

#### Solution

```bash
# Attendre que Kafka soit healthy
docker compose -f infra/docker-compose.yml logs kafka | grep "Kafka Server started"

# Vérifier la configuration
docker exec kafka cat /opt/kafka/config/server.properties | grep advertised
```

### Erreurs JWT / Authentification

#### Symptôme
```
401 Unauthorized
Token invalide ou expiré
```

#### Vérifications

1. **Token expiré**
   ```javascript
   // Vérifier l'expiration
   console.log(keycloak.timeSkew);
   ```

2. **Realm incorrect**
   - Vérifier que le realm est `ecommerce`

3. **Client ID incorrect**
   - Vérifier que le client est `ecomm-front`

#### Solution

```bash
# Se déconnecter et reconnecter
keycloak.logout();
keycloak.login();

# Vérifier Keycloak
curl http://localhost/auth/realms/ecommerce/.well-known/openid-configuration
```

## Commandes de Diagnostic

### État des Services

```bash
docker compose -f infra/docker-compose.yml ps
```

### Logs Récents

```bash
# Tous les services
docker compose -f infra/docker-compose.yml logs --tail=50

# Service spécifique
docker compose -f infra/docker-compose.yml logs --tail=50 orders
```

### Santé des Services

```bash
# Health checks
docker inspect --format='{{.State.Health.Status}}' orders
docker inspect --format='{{.State.Health.Status}}' kafka
docker inspect --format='{{.State.Health.Status}}' keycloak
```

### Réseau

```bash
# Liste des réseaux
docker network ls

# Inspecter le réseau
docker network inspect ECommerce-Platform_ecommerce-net
```

### Ressources

```bash
# Utilisation CPU/Mémoire
docker stats
```

## Nettoyage Complet

```bash
# Arrêter tous les services
docker compose -f infra/docker-compose.yml down -v

# Supprimer les images
docker compose -f infra/docker-compose.yml down --rmi all

# Supprimer les volumes
docker volume rm ECommerce-Platform_keycloak_data

# Reconstruire tout
docker compose -f infra/docker-compose.yml up --build
```

## Obtenir de l'Aide

### Logs Complètes

```bash
# Collecter tous les logs
docker compose -f infra/docker-compose.yml logs > logs.txt

# Partager les logs pour debug
```

### Configuration

```bash
# Vérifier la configuration Docker Compose
docker compose -f infra/docker-compose.yml config

# Vérifier les variables d'environnement
docker compose -f infra/docker-compose.yml --env-file .env config
```

## Diagnostic Kubernetes / Minikube

Pour le parcours Helm, commencer par identifier le namespace et la release concernés :

```bash
kubectl get pods -n ecommerce
kubectl get pods -n ingress-system
kubectl get events -n ecommerce --sort-by=.lastTimestamp
kubectl get ingress -n ecommerce
```

### Image locale indisponible

Si un pod affiche `ImagePullBackOff` pour `frontend:latest`, `orders:latest`, `inventory:latest` ou `payments:latest`, reconstruire les images dans le daemon Docker de Minikube avec `./deploy-images.sh`. Les valeurs dev utilisent `IfNotPresent` et n'utilisent pas de registry externe pour ces images.

### Kafka non prêt dans Kubernetes

```bash
kubectl get statefulset kafka -n ecommerce
kubectl logs statefulset/kafka -n ecommerce
kubectl get jobs -n ecommerce
kubectl describe pod -n ecommerce -l app.kubernetes.io/instance=kafka
```

Le chart utilise un StatefulSet mono-réplique, un service headless et un Job Helm pour créer les topics. Vérifier la probe Kafka et la terminaison du Job avant d'analyser les services métier.

### Ingress inaccessible

```bash
kubectl get ingressclass traefik
kubectl get svc traefik -n ingress-system
minikube ip
curl -H 'Host: ecommerce.test' http://$(minikube ip):30080/api/orders/health
```

La configuration dev expose Traefik par le NodePort `30080`, pas par le port 80 de l'hôte. Les hosts Ingress sont vides par défaut ; le header `Host` permet de reproduire un accès par hostname pendant le diagnostic.

### Keycloak non prêt

```bash
kubectl get pvc -n ecommerce
kubectl get secret -n ecommerce | grep keycloak
kubectl logs deploy/keycloak -n ecommerce
kubectl describe pod -n ecommerce -l app.kubernetes.io/instance=keycloak
```

Le chart monte un Secret realm et un PVC de développement. Ne pas afficher ou copier les valeurs des Secrets dans un ticket ou dans la documentation.

## Ressources

- [Documentation Docker](https://docs.docker.com/)
- [Documentation Keycloak](https://www.keycloak.org/documentation)
- [Documentation Kafka](https://kafka.apache.org/documentation/)
- [Documentation Grafana](https://grafana.com/docs/)
