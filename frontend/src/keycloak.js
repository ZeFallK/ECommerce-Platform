import Keycloak from 'keycloak-js';

const keycloakConfig = {
  url: 'http://localhost/auth', 
  realm: 'ecommerce',           
  clientId: 'ecomm-front'
};

const keycloak = new Keycloak(keycloakConfig);

export default keycloak;