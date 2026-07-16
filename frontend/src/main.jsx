import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.jsx'
import keycloak from './keycloak';

keycloak.init({ 
  onLoad: 'login-required', // Force la connexion immédiatement
  checkLoginIframe: false   // Désactive la vérification de session pour éviter les problèmes de CORS
}).then((authenticated) => {
  if (!authenticated) {
    window.location.reload();
  } else {
    console.info("Utilisateur authentifié !");
    
    createRoot(document.getElementById('root')).render(
      <StrictMode>
        <App />
      </StrictMode>,
    )
  }
}).catch((erreur) => {
  console.error("Erreur réelle :", erreur);
});

