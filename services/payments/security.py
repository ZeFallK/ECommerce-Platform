from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import jwt, JWTError
import requests

# L'URL publique utilisée par Swagger pour récupérer un token
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="http://localhost/auth/realms/ecommerce/protocol/openid-connect/token")

# L'URL interne (réseau Docker) pour vérifier la signature cryptographique du token
JWKS_URL = "http://keycloak:8080/auth/realms/ecommerce/protocol/openid-connect/certs"

def verify_token(token: str = Depends(oauth2_scheme)):
    try:
        # 1. Récupérer les clés publiques de Keycloak
        jwks = requests.get(JWKS_URL).json()
        unverified_header = jwt.get_unverified_header(token)        
        # 2. Trouver la clé qui a servi à signer ce token
        rsa_key = next((key for key in jwks["keys"] if key["kid"] == unverified_header["kid"]), None)
        if not rsa_key:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Clé de signature introuvable")
        # 3. Décoder et valider le token
        payload = jwt.decode(
            token,
            rsa_key,
            algorithms=["RS256"],
            options={"verify_aud": False}, # Simplification pour l'environnement de développement
            issuer="http://localhost/auth/realms/ecommerce"
        )
        return payload 
        
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide ou expiré")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e))