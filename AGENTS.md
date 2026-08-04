# Contexte du Projet
Il s'agit du prototype d'architecture microservices e-commerce.
Le dépôt est un monorepo contenant :
- Frontend : React (Vite)
- Backend : 3 Microservices (Orders, Inventory, Payments) en Python (FastAPI).
- Asynchrone : Apache Kafka.
- Infrastructure cible : Kubernetes avec Helm.

- **Nom du projet :** ECommerce-Platform 
- **Monorepo :**
  - Frontend : React (Vite) dans `frontend/`
  - Backend : Microservices Python (FastAPI) dans `services/`
  - Infrastructure : Docker Compose dans `infra/` -> Migration vers Helm dans `infra/helm/`
# Directives Strictes pour l'Agent

# Fichiers à Ignorer / Hors Périmètre
Ne scrute pas et ne modifie pas les dossiers suivants :
- `**/node_modules/**`
- `**/.venv/**`
- `**/__pycache__/**`
- `**/dist/**`
- `**/.git/**`

## 1. Infrastructure (Kubernetes & Helm)
- L'objectif actuel est d'écrire des Charts Helm dynamiques de zéro.
- INTERDICTION ABSOLUE d'utiliser ou de suggérer l'outil `kompose`. 
- Les fichiers YAML de Helm doivent exploiter au maximum les fonctions de templating Go (boucles `range`, conditions `if`) pour éviter la duplication de code entre les microservices.
- Le routage K8s doit se faire via des objets `Ingress` classiques, en remplacement des anciens labels Traefik.

### Ingress Controller
- Traefik est l'Ingress Controller cible sur Kubernetes
- Il sera déployé via son chart Helm officiel (https://helm.traefik.io/traefik)
- Utiliser des objets Ingress classiques avec l'annotation :
  kubernetes.io/ingress.class: traefik
- Le routage doit respecter les root_path FastAPI :
  /api/orders    → service orders:8000
  /api/payments  → service payments:8000
  /api/inventory → service inventory:8000

## 2. Sanctuarisation du Code Applicatif (Lecture Seule)
- **RÈGLE ABSOLUE :** Tu as l'interdiction formelle de modifier, proposer de modifier, ou générer du code source applicatif (fichiers `.py`, `.jsx`, `.js`). Le code actuel est 100% fonctionnel et validé.
- Ton périmètre d'action est STRICTEMENT limité au dossier `infra/helm/` et à l'écriture de manifests Kubernetes/Helm.
- **Contexte technique (Pour information uniquement) :**
  - Les microservices communiquent exclusivement de manière asynchrone via Kafka (tu dois donc t'assurer que tes manifests Helm permettent cette connexion réseau interne).
  - Les APIs FastAPI utilisent un paramètre `root_path`. Ton configuration d'Ingress Kubernetes devra respecter cette logique de routage sans demander de changement côté backend.

## 3. Comportement Global
- Tes réponses doivent être concises, directes et techniques.
- Ne propose pas de recréer l'architecture existante, concentre-toi sur la traduction Docker Compose -> Helm.
- NE JAMAIS exécuter automatiquement de commande modifiant le cluster ou le système (`kubectl apply`, `kubectl delete`, `helm install`, `helm upgrade`, `rm`, `git commit`, etc.).
- Pour TOUTE commande de modification, tu dois proposer la commande dans ta réponse et attendre la validation explicite de l'utilisateur avant de l'exécuter.
- Tu es autorisé à exécuter UNIQUEMENT des commandes d'inspection passives si nécessaire (`kubectl get`, `kubectl describe`, `helm list`, `docker ps`).
- Demande de confirmation : Si tu dois vérifier l'état du système, demande TOUJOURS confirmation à l'utilisateur au préalable (ex: *"Puis-je exécuter `kubectl get pods` pour vérifier ?"*).