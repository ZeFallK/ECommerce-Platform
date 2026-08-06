# Keycloak - Authentification

## Vue d'ensemble

**Keycloak** est le serveur d'identité qui gère l'authentification et l'autorisation pour toute la plateforme. Il implémente les protocoles OAuth2 et OpenID Connect (OIDC).

Les valeurs affichées dans cette page sont des exemples. Les identifiants d'administration et ceux des utilisateurs doivent être fournis localement et ne doivent pas être versionnés. Le parcours Docker Compose importe `infra/keycloak/realm-export.json`, tandis que le chart Helm génère un realm à partir de ses valeurs.

### Rôles de Keycloak

1. **Identity Provider** : Gère les utilisateurs et leurs credentials
2. **Token Issuer** : Émet des JWT tokens pour l'authentification
3. **Authorization Server** : Gère les scopes et permissions
4. **Single Sign-On** : Authentification unique pour tous les services

## Configuration

### Image et Version

```yaml
image: quay.io/keycloak/keycloak:24.0.1
```

### Mode de Démarrage

```yaml
command: start-dev
```

**Mode développement** :
- HTTPS désactivé
- Logs détaillés
- Base de données in-memory ou fichier

### Variables d'Environnement

```yaml
environment:
  KEYCLOAK_ADMIN: <ADMIN_USERNAME>
  KEYCLOAK_ADMIN_PASSWORD: <ADMIN_PASSWORD>
  KC_DB: dev-file
  KC_PROXY: edge
  KC_HTTP_RELATIVE_PATH: /auth
  KC_HOSTNAME_URL: http://localhost/auth
```

**Explications** :

| Variable | Description |
|----------|-------------|
| `KEYCLOAK_ADMIN` | Username administrateur |
| `KEYCLOAK_ADMIN_PASSWORD` | Password administrateur |
| `KC_DB` | Type de base de données (dev-file) |
| `KC_PROXY` | Mode proxy (edge pour derrière Traefik) |
| `KC_HTTP_RELATIVE_PATH` | Chemin d'accès (/auth) |
| `KC_HOSTNAME_URL` | URL publique de Keycloak |

### Volumes

```yaml
volumes:
  - keycloak_data:/opt/keycloak/data
```

Persistance des données Keycloak (utilisateurs, realms, etc.).

## Accès

### Console d'Administration

```
http://localhost/auth/admin
```

**Credentials** :
- Username: valeur de `KEYCLOAK_ADMIN` ou du Secret Helm
- Password: valeur de `KEYCLOAK_ADMIN_PASSWORD` ou du Secret Helm

### Endpoint OIDC

```
http://localhost/auth/realms/ecommerce/.well-known/openid-configuration
```

## Realm: `ecommerce`

Un realm est un espace d'isolation qui contient :
- Utilisateurs
- Clients (applications)
- Roles
- Groupes

### Configuration du Realm

1. **Nom** : ecommerce
2. **Login Theme** : keycloak (default)
3. **Password Policy** : Default (dev)

### Client: `ecomm-front`

Configuration pour l'application React :

```javascript
// frontend/src/keycloak.js
const keycloakConfig = {
  url: 'http://localhost/auth',
  realm: 'ecommerce',
  clientId: 'ecomm-front'
};
```

**Configuration dans Keycloak** :
- **Client ID** : ecomm-front
- **Client Protocol** : openid-connect
- **Access Type** : Public, adapté à une SPA sans secret client
- **Standard Flow** : Activé
- **Direct Access Grants** : Activé dans les configurations de développement
- **Valid Redirect URIs** : `http://localhost/*`
- **Web Origins** : `http://localhost`

### Endpoints OIDC

| Endpoint | URL |
|----------|-----|
| Authorization | `http://localhost/auth/realms/ecommerce/protocol/openid-connect/auth` |
| Token | `http://localhost/auth/realms/ecommerce/protocol/openid-connect/token` |
| JWKS | `http://localhost/auth/realms/ecommerce/protocol/openid-connect/certs` |
| Userinfo | `http://localhost/auth/realms/ecommerce/protocol/openid-connect/userinfo` |
| Logout | `http://localhost/auth/realms/ecommerce/protocol/openid-connect/logout` |

## Flux d'Authentification

### 1. Initialisation

```javascript
import Keycloak from 'keycloak-js';

const keycloak = new Keycloak({
  url: 'http://localhost/auth',
  realm: 'ecommerce',
  clientId: 'ecomm-front'
});

await keycloak.init({ onLoad: 'check-sso' });
```

### 2. Login

```javascript
if (!keycloak.authenticated) {
  keycloak.login();
}
```

Redirection vers :
```
http://localhost/auth/realms/ecommerce/protocol/openid-connect/auth?
  client_id=ecomm-front&
  redirect_uri=http://localhost&
  response_type=code&
  scope=openid
```

### 3. Échange Code pour Token

Après login, Keycloak redirige avec un code :
```
http://localhost?code=xyz...
```

Le frontend échange le code pour un token :
```bash
curl -X POST http://localhost/auth/realms/ecommerce/protocol/openid-connect/token \
  -d "grant_type=authorization_code" \
  -d "client_id=ecomm-front" \
  -d "code=xyz..." \
  -d "redirect_uri=http://localhost"
```

**Response** :
```json
{
  "access_token": "eyJhbGciOiJSUzI1NiIs...",
  "refresh_token": "eyJhbGciOiJSUzI1NiIs...",
  "id_token": "eyJhbGciOiJSUzI1NiIs...",
  "expires_in": 300,
  "token_type": "Bearer"
}
```

### 4. Utilisation du Token

```javascript
axios.get('http://localhost/api/orders/', {
  headers: { Authorization: `Bearer ${keycloak.token}` }
});
```

### 5. Refresh du Token

```javascript
keycloak.updateToken(60).then(refreshed => {
  if (refreshed) {
    console.log('Token refreshed');
  }
}).catch(() => {
  console.log('Token expired, user logged out');
  keycloak.logout();
});
```

### 6. Déconnexion

```javascript
keycloak.logout({ redirectUri: 'http://localhost' });
```

## Structure du JWT Token

### Access Token (décodé)

```json
{
  "exp": 1721721600,
  "iat": 1721721300,
  "auth_time": 1721721200,
  "jti": "uuid",
  "iss": "http://localhost/auth/realms/ecommerce",
  "aud": "ecomm-front",
  "sub": "uuid",
  "typ": "Bearer",
  "azp": "ecomm-front",
  "session_state": "uuid",
  "acr": "1",
  "preferred_username": "user",
  "name": "User Name",
  "given_name": "User",
  "family_name": "Name",
  "email": "user@example.com"
}
```

### Champs Importants

| Champ | Description |
|-------|-------------|
| `exp` | Expiration timestamp |
| `iss` | Issuer (Keycloak) |
| `aud` | Audience (client) |
| `sub` | Subject (user ID) |
| `preferred_username` | Username |
| `email` | Email de l'utilisateur |

## Création d'Utilisateurs

### Via la Console

1. Se connecter à `http://localhost/auth/admin`
2. Sélectionner le realm `ecommerce`
3. Aller dans "Users"
4. Cliquer "Create new user"
5. Remplir les informations
6. Aller dans "Credentials" pour définir le mot de passe

### Via l'API

```bash
curl -X POST http://localhost/auth/admin/realms/ecommerce/users \
  -H "Authorization: Bearer <ADMIN_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "user",
    "email": "user@example.com",
    "enabled": true,
    "credentials": [{
      "type": "password",
      "value": "<USER_PASSWORD>",
      "temporary": false
    }]
  }'
```

Dans Kubernetes, le chart `infra/helm/keycloak` crée un Ingress classique `/auth`, un Secret realm et, en dev, un PVC de `1Gi`. Il utilise les mêmes paramètres de chemin relatif (`/auth`) mais ne monte pas directement le fichier d'export Compose.

## Intégration avec Traefik

Keycloak est exposé via Traefik avec le chemin `/auth` :

```yaml
keycloak:
  environment:
    KC_PROXY: edge
    KC_HTTP_RELATIVE_PATH: /auth
    KC_HOSTNAME_URL: http://localhost/auth
  labels:
    - "traefik.enable=true"
    - "traefik.http.routers.keycloak.rule=PathPrefix(`/auth`)"
    - "traefik.http.services.keycloak.loadbalancer.server.port=8080"
```

## Sécurité

### Pour la Production

1. **HTTPS** : Activer HTTPS partout
2. **Base de données** : Utiliser PostgreSQL/MySQL
3. **Mot de passe admin** : Changer le password par défaut
4. **Email** : Configurer un serveur SMTP
5. **Password Policy** : Renforcer la politique de mot de passe
6. **Token Lifespan** : Ajuster la durée de vie des tokens
7. **CORS** : Configurer les origins autorisées

### Variables pour Production

```yaml
environment:
  KC_DB: postgres
  KC_DB_URL: jdbc:postgresql://postgres:5432/keycloak
  KC_DB_USERNAME: keycloak
  KC_DB_PASSWORD: <SECRET>
  KEYCLOAK_ADMIN: <ADMIN_USERNAME>
  KEYCLOAK_ADMIN_PASSWORD: <STRONG_PASSWORD>
```

## Monitoring

### Logs

```bash
docker compose -f infra/docker-compose.yml logs -f keycloak
```

### Exemple de Log

```
Executing the 'start-dev' command...
Running the server in development mode. DO NOT use this configuration in production.
...
Realm 'ecommerce' imported
Administrator account created
```

## Troubleshooting

### Token Non Validé

Vérifier :
1. Le realm est correctement configuré
2. L'issuer dans le token correspond à l'URL configurée
3. La clé publique JWKS est accessible

### Redirection Échouée

Vérifier :
1. Les Redirect URIs sont corrects dans le client
2. Le wildcard `http://localhost/*` est autorisé

### Session Expired

Vérifier :
1. La durée de vie du token (`expires_in`)
2. Le refresh token est valide
3. L'utilisateur n'est pas déconnecté

## Limitations Connues

1. **Mode dev** : Configuration non sécurisée
2. **Base de données** : Fichier local (non scalable)
3. **HTTPS** : Non configuré
4. **Email** : Pas de serveur SMTP
5. **Single realm** : Tous les utilisateurs dans un seul realm

## Pour aller plus loin

- Configurer un provider LDAP/Active Directory
- Implémenter l'authentification sociale (Google, GitHub)
- Configurer les roles et permissions granulaires
- Mettre en place le MFA (Multi-Factor Authentication)
- Configurer les events et audit logs
- Implémenter le User Federation
