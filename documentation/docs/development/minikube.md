# Déploiement avec Minikube

## Prérequis

- Docker ;
- Minikube ;
- `kubectl` ;
- Helm ;
- Helmfile ;
- `curl` pour les tests HTTP.

Les versions exactes ne sont pas verrouillées par le dépôt. Utiliser des versions compatibles avec le chart Traefik et Kubernetes disponibles sur la machine.

## Démarrer Minikube

```bash
minikube start
kubectl create namespace ecommerce
kubectl create namespace ingress-system
```

Les namespaces sont nécessaires car Helmfile utilise `createNamespace: false`.

## Construire les images dans Minikube

Depuis la racine du dépôt :

```bash
./deploy-images.sh
```

Le script :

1. configure le shell Docker avec `minikube docker-env` ;
2. construit `frontend:latest` ;
3. construit `orders:latest`, `inventory:latest` et `payments:latest`.

Les valeurs dev des charts utilisent `pullPolicy: IfNotPresent`. Les images locales doivent donc être construites dans le daemon Docker de Minikube, et non uniquement dans le daemon Docker de l'hôte.

## Déployer avec Helmfile

```bash
cd infra/helm
helmfile -e dev template
helmfile -e dev sync
```

La première commande rend les manifests sans les appliquer. La seconde déploie Kafka, l'observabilité, Keycloak, Traefik, les microservices et le frontend selon les dépendances déclarées.

## Vérifier les ressources

```bash
kubectl get pods -n ecommerce
kubectl get jobs -n ecommerce
kubectl get ingress -n ecommerce
kubectl get svc -n ingress-system
```

Attendre notamment le Job `kafka-topics` et la readiness des pods avant les tests HTTP.

## Tester Traefik

```bash
minikube ip
curl -H 'Host: ecommerce.test' http://$(minikube ip):30080/api/orders/health
curl -H 'Host: ecommerce.test' http://$(minikube ip):30080/api/inventory/health
curl -H 'Host: ecommerce.test' http://$(minikube ip):30080/api/payments/health
```

Le host est optionnel avec les valeurs dev, mais l'exemple permet de tester un hostname local. Voir [Ingress et DNS](../infrastructure/ingress-dns.md) pour les limites liées au frontend et à Keycloak.

## Diagnostic

```bash
kubectl describe pod -n ecommerce <pod>
kubectl logs -n ecommerce deploy/orders
kubectl logs -n ecommerce statefulset/kafka
kubectl get events -n ecommerce --sort-by=.lastTimestamp
```

Problèmes fréquents :

- `ImagePullBackOff` : l'image n'a pas été construite dans le daemon Minikube ou le nom ne correspond pas aux valeurs Helm ;
- Kafka non prêt : attendre le démarrage du StatefulSet et vérifier le Job de topics ;
- route Ingress indisponible : vérifier l'`IngressClass`, le service Traefik et le NodePort `30080` ;
- Keycloak non prêt : vérifier le PVC, le Secret realm et la probe sur `/auth/realms/master`.

## Nettoyage

Les commandes de suppression de ressources modifient le cluster. Les exécuter uniquement après validation de l'environnement concerné :

```bash
helmfile -e dev destroy
minikube stop
```

## Limites

- Le script de build ne construit pas les images Kafka, Keycloak ou observability ; elles sont tirées depuis leurs registries.
- Le déploiement est mono-réplique et orienté développement.
- Les services métier conservent leurs données en mémoire.
- Les URLs `localhost` du frontend ne sont pas paramétrables par Helm.
