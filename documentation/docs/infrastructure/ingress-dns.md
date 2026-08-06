# Ingress et DNS

## Routage actuel

Traefik est installé par le chart officiel comme contrôleur Ingress. Le chart local `infra/helm/traefik-config` active le provider `kubernetesIngress`, désactive les providers CRD et Gateway, et expose HTTP avec un service `NodePort` :

| Élément | Valeur dev |
|---------|------------|
| `IngressClass` | `traefik` |
| Port HTTP du service | `80` |
| NodePort Minikube | `30080` |
| HTTPS | Non exposé |
| Dashboard | Désactivé |

Les objets Ingress classiques portent l'annotation `kubernetes.io/ingress.class: traefik`.

## Table des routes

| Préfixe | Backend | Port |
|---------|---------|------|
| `/api/orders` | `orders` | 8000 |
| `/api/payments` | `payments` | 8000 |
| `/api/inventory` | `inventory` | 8000 |
| `/auth` | `keycloak` | 8080 |
| `/` | `frontend` | 5173 |

Les valeurs `host` des Ingress applicatifs sont vides en dev. Kubernetes crée donc des règles sans contrainte de hostname.

## Accès avec Minikube

Le service Traefik est un `NodePort`. L'adresse à utiliser est l'IP de Minikube avec le port `30080`, ou une adresse résolue par un mécanisme local de DNS :

```bash
minikube ip
curl -H 'Host: ecommerce.test' http://$(minikube ip):30080/api/orders/health
```

Pour que le navigateur envoie ce hostname sans option `Host`, le nom `ecommerce.test` doit résoudre vers l'IP de Minikube.

## DNS local

Une résolution DNS locale peut être mise en place avec `ingress-dns` ou une entrée statique dans `/etc/hosts`. Le principe est le même :

```text
<IP_MINIKUBE> ecommerce.test
```

Après modification du fichier hosts, tester la résolution puis la route :

```bash
getent hosts ecommerce.test
curl http://ecommerce.test:30080/api/orders/health
```

Le port `30080` reste nécessaire avec la configuration actuelle. Utiliser `http://ecommerce.test` sur le port 80 nécessite un mapping supplémentaire de l'hôte vers le NodePort ou une autre configuration du service Traefik.

## Limitation frontend et Keycloak

Le frontend et sa configuration Keycloak utilisent actuellement `http://localhost` en dur. Un hostname DNS personnalisé peut donc servir à tester les routes Ingress et les APIs, mais le parcours frontend complet n'est pas garanti avec `ecommerce.test` sans modifier la configuration applicative et les URLs autorisées du client Keycloak.

La procédure la plus fidèle au code actuel est donc :

1. utiliser `localhost` pour le parcours frontend prévu ;
2. utiliser l'IP/NodePort ou un hostname avec en-tête `Host` pour tester l'Ingress ;
3. considérer l'usage d'un hostname dédié comme une évolution de configuration, pas comme une fonctionnalité déjà implémentée.

## Vérifications

```bash
kubectl get ingress -n ecommerce
kubectl describe ingress orders -n ecommerce
kubectl get svc traefik -n ingress-system
```

Les templates actuels ne déclarent pas de middleware de réécriture de chemin. Les routes et les `root_path` FastAPI doivent donc être testés ensemble après déploiement, en particulier pour `/api/orders/health`, `/api/inventory/health` et `/api/payments/health`.

## Références

- [Traefik](traefik.md)
- [Kubernetes](kubernetes.md)
- [Minikube](../development/minikube.md)
