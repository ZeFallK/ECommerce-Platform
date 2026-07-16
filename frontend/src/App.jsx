import keycloak from './keycloak';

function App() {
  return (
    <div style={{ padding: '40px', fontFamily: 'sans-serif', maxWidth: '600px', margin: '0 auto' }}>
      <h1 style={{ color: '#2c3e50' }}>Bienvenue sur ton E-Commerce ! 🛒</h1>
      
      <div style={{ background: '#ecf0f1', padding: '20px', borderRadius: '8px', marginTop: '20px' }}>
        <h3 style={{ marginTop: 0 }}>Profil Utilisateur</h3>
        <p><strong>Identifiant :</strong> {keycloak.tokenParsed?.preferred_username}</p>
        <p><strong>ID Unique :</strong> {keycloak.tokenParsed?.sub}</p>
        
        <h3 style={{ marginTop: '20px' }}>Ton "Bracelet VIP" (Token JWT) :</h3>
        <textarea 
          readOnly 
          rows="6" 
          style={{ width: '100%', fontFamily: 'monospace', fontSize: '12px', padding: '10px' }}
          value={keycloak.token}
        />
        
        <button 
          onClick={() => keycloak.logout()}
          style={{ 
            marginTop: '20px', padding: '10px 20px', background: '#e74c3c', 
            color: 'white', border: 'none', borderRadius: '5px', cursor: 'pointer', fontWeight: 'bold'
          }}
        >
          Se déconnecter
        </button>
      </div>
    </div>
  );
}

export default App;