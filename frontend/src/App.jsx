import { useState, useEffect } from 'react';
import keycloak from './keycloak';
import axios from 'axios';

const CATALOGUE = [
  { id: 'LAPTOP-001', name: 'Ordinateur Portable Pro', icon: '💻', price: 1299 },
  { id: 'PHONE-002', name: 'Smartphone Z-Fold', icon: '📱', price: 899 },
  { id: 'MUG-003', name: 'Mug Développeur', icon: '☕', price: 15 }
];

function App() {
  const [activeTab, setActiveTab] = useState('boutique'); // 'boutique', 'inventory', 'payments'
  const [notification, setNotification] = useState(null);
  const [recentOrders, setRecentOrders] = useState([]); // Stockage local des commandes créées pour le paiement
  const [stocks, setStocks] = useState({});
  
  // États de chargement
  const [loadingOrder, setLoadingOrder] = useState(null);
  const [loadingStocks, setLoadingStocks] = useState(false);
  const [loadingPayment, setLoadingPayment] = useState(null);

  // 📦 Fonction pour interroger l'API Inventory pour tous les produits
  const fetchStocks = async () => {
    setLoadingStocks(true);
    const updatedStocks = {};
    for (const produit of CATALOGUE) {
      try {
        const response = await axios.get(`http://localhost/api/inventory/stock/${produit.id}`, {
          headers: { Authorization: `Bearer ${keycloak.token}` }
        });
        updatedStocks[produit.id] = {
          quantity: response.data.quantity,
          in_stock: response.data.in_stock
        };
      } catch (error) {
        console.error(`Erreur lors de la récupération du stock de ${produit.id}:`, error);
        updatedStocks[produit.id] = { quantity: 'N/A', in_stock: false };
      }
    }
    setStocks(updatedStocks);
    setLoadingStocks(false);
  };

  // Charger les stocks au démarrage et à chaque changement d'onglet vers "inventory"
  useEffect(() => {
    fetchStocks();
  }, [activeTab]);
  useEffect(() => {
    const interval = setInterval(() => {
      keycloak.updateToken(60).catch(() => keycloak.logout());
    }, 4 * 60 * 1000);
    return () => clearInterval(interval);
  }, []);
  // 🛒 Service 1 : Commander (Orders Service)
  const commanderProduit = async (productId, price) => {
    setLoadingOrder(productId);
    setNotification(null);
    
    const customerId = keycloak.tokenParsed?.preferred_username || "utilisateur_inconnu";
    const orderPayload = {
      product_id: productId,
      customer_id: customerId,
      quantity: 1
    };

    try {
      const response = await axios.post('http://localhost/api/orders/', orderPayload, {
        headers: { Authorization: `Bearer ${keycloak.token}` }
      });
      
      const newOrder = {
        order_id: response.data.order_id,
        product_id: productId,
        customer_id: customerId,
        quantity: 1,
        price: price,
        status: 'pending' // Statut initial
      };

      // Sauvegarde locale de la commande pour pouvoir la payer dans l'onglet Paiements
      setRecentOrders(prev => [newOrder, ...prev]);
      
      setNotification({
        type: 'success',
        message: `Commande créée (Statut: PENDING) ! ID: ${newOrder.order_id.substring(0, 8)}... Allez dans l'onglet 'Stocks' pour voir la réservation.`
      });

      // Rafraîchir les stocks pour voir la réservation immédiate effectuée par le Consumer Kafka
      setTimeout(() => fetchStocks(), 1000);

    } catch (error) {
      setNotification({
        type: 'error',
        message: `Échec de la commande : ${error.response?.status || error.message}`
      });
    } finally {
      setLoadingOrder(null);
    }
  };

  // 💳 Service 3 : Payer (Payments Service)
  const payerCommande = async (order) => {
    setLoadingPayment(order.order_id);
    setNotification(null);

    const paymentPayload = {
      order_id: order.order_id,
      product_id: order.product_id,
      quantity: order.quantity,
      amount: order.price
    };

    try {
      // Appel de ton API Payments (ajuste l'URL/méthode selon tes routes réelles)
      await axios.post('http://localhost/api/payments/pay', paymentPayload, {
        headers: { Authorization: `Bearer ${keycloak.token}` }
      });

      // Mise à jour locale du statut de la commande
      setRecentOrders(prev => 
        prev.map(o => o.order_id === order.order_id ? { ...o, status: 'paid' } : o)
      );

      setNotification({
        type: 'success',
        message: `Paiement validé pour la commande ${order.order_id.substring(0, 8)}... Événement 'payments.processed' publié !`
      });

      // Rafraîchir les stocks pour voir la déduction définitive
      setTimeout(() => fetchStocks(), 1500);

    } catch (error) {
      setNotification({
        type: 'error',
        message: `Échec du paiement : ${error.response?.status || error.message}`
      });
    } finally {
      setLoadingPayment(null);
    }
  };

  return (
    <div style={{ backgroundColor: '#f3f4f6', minHeight: '100vh', padding: '2rem', fontFamily: 'system-ui, sans-serif' }}>
      
      {/* HEADER GLOBALE */}
      <header style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', backgroundColor: 'white', padding: '1rem 2rem', borderRadius: '10px', boxShadow: '0 4px 6px rgba(0,0,0,0.05)', marginBottom: '2rem' }}>
        <div>
          <h1 style={{ margin: 0, color: '#1f2937', fontSize: '1.5rem' }}>E-Commerce Multi-Services Hub 🚀</h1>
          <p style={{ margin: '5px 0 0 0', fontSize: '0.875rem', color: '#6b7280' }}>Simulateur d'architecture microservices & Kafka</p>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
          <span style={{ color: '#4b5563' }}>Client connecté : <strong>{keycloak.tokenParsed?.preferred_username}</strong></span>
          <button onClick={() => keycloak.logout()} style={{ padding: '0.5rem 1rem', backgroundColor: '#ef4444', color: 'white', border: 'none', borderRadius: '5px', cursor: 'pointer', fontWeight: 'bold' }}>
            Déconnexion
          </button>
        </div>
      </header>

      {/* NOTIFICATION ZONE */}
      {notification && (
        <div style={{ padding: '1rem', marginBottom: '2rem', borderRadius: '8px', textAlign: 'center', fontWeight: 'bold', color: notification.type === 'success' ? '#065f46' : '#991b1b', backgroundColor: notification.type === 'success' ? '#d1fae5' : '#fee2e2', border: `1px solid ${notification.type === 'success' ? '#34d399' : '#f87171'}` }}>
          {notification.message}
        </div>
      )}

      {/* NAVIGATION DES ONGLETS (SERVICES) */}
      <nav style={{ display: 'flex', gap: '1rem', marginBottom: '2rem' }}>
        <button 
          onClick={() => setActiveTab('boutique')} 
          style={{ padding: '0.75rem 1.5rem', borderRadius: '8px', border: 'none', fontWeight: 'bold', cursor: 'pointer', backgroundColor: activeTab === 'boutique' ? '#3b82f6' : 'white', color: activeTab === 'boutique' ? 'white' : '#4b5563', boxShadow: '0 2px 4px rgba(0,0,0,0.05)' }}
        >
          🛒 1. Boutique (Orders Service)
        </button>
        <button 
          onClick={() => setActiveTab('inventory')} 
          style={{ padding: '0.75rem 1.5rem', borderRadius: '8px', border: 'none', fontWeight: 'bold', cursor: 'pointer', backgroundColor: activeTab === 'inventory' ? '#10b981' : 'white', color: activeTab === 'inventory' ? 'white' : '#4b5563', boxShadow: '0 2px 4px rgba(0,0,0,0.05)' }}
        >
          📦 2. Stocks (Inventory Service)
        </button>
        <button 
          onClick={() => setActiveTab('payments')} 
          style={{ padding: '0.75rem 1.5rem', borderRadius: '8px', border: 'none', fontWeight: 'bold', cursor: 'pointer', backgroundColor: activeTab === 'payments' ? '#f59e0b' : 'white', color: activeTab === 'payments' ? 'white' : '#4b5563', boxShadow: '0 2px 4px rgba(0,0,0,0.05)' }}
        >
          💳 3. Caisse (Payments Service)
        </button>
      </nav>

      {/* CONTENU DE L'ONGLET : BOUTIQUE (ORDERS) */}
      {activeTab === 'boutique' && (
        <div>
          <h2 style={{ color: '#1f2937', marginBottom: '1.5rem' }}>Passer une commande</h2>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: '2rem' }}>
            {CATALOGUE.map((produit) => (
              <div key={produit.id} style={{ backgroundColor: 'white', borderRadius: '10px', padding: '2rem', textAlign: 'center', boxShadow: '0 4px 6px rgba(0,0,0,0.05)', display: 'flex', flexDirection: 'column', gap: '1rem' }}>
                <div style={{ fontSize: '4rem' }}>{produit.icon}</div>
                <h3 style={{ margin: 0, color: '#1f2937' }}>{produit.name}</h3>
                <div style={{ color: '#6b7280', fontSize: '0.875rem' }}>Réf : {produit.id}</div>
                <div style={{ fontSize: '1.5rem', fontWeight: 'bold', color: '#111827' }}>{produit.price} €</div>
                
                <button 
                  onClick={() => commanderProduit(produit.id, produit.price)}
                  disabled={loadingOrder === produit.id}
                  style={{ marginTop: 'auto', padding: '0.75rem', backgroundColor: loadingOrder === produit.id ? '#9ca3af' : '#3b82f6', color: 'white', border: 'none', borderRadius: '8px', cursor: loadingOrder === produit.id ? 'not-allowed' : 'pointer', fontWeight: 'bold' }}
                >
                  {loadingOrder === produit.id ? 'Publication orders.created...' : 'Commander'}
                </button>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* CONTENU DE L'ONGLET : STOCKS (INVENTORY) */}
      {activeTab === 'inventory' && (
        <div style={{ backgroundColor: 'white', padding: '2rem', borderRadius: '10px', boxShadow: '0 4px 6px rgba(0,0,0,0.05)' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1.5rem' }}>
            <h2 style={{ color: '#1f2937', margin: 0 }}>Niveaux de stocks en temps réel</h2>
            <button 
              onClick={fetchStocks} 
              disabled={loadingStocks}
              style={{ padding: '0.5rem 1rem', backgroundColor: '#10b981', color: 'white', border: 'none', borderRadius: '5px', cursor: 'pointer', fontWeight: 'bold' }}
            >
              {loadingStocks ? 'Rafraîchissement...' : '🔄 Actualiser'}
            </button>
          </div>

          <table style={{ width: '100%', borderCollapse: 'collapse', textAlign: 'left' }}>
            <thead>
              <tr style={{ borderBottom: '2px solid #e5e7eb', color: '#4b5563' }}>
                <th style={{ padding: '1rem 0.5rem' }}>Produit</th>
                <th style={{ padding: '1rem 0.5rem' }}>Réf</th>
                <th style={{ padding: '1rem 0.5rem' }}>Stock Disponible</th>
                <th style={{ padding: '1rem 0.5rem' }}>Statut</th>
              </tr>
            </thead>
            <tbody>
              {CATALOGUE.map((item) => {
                const stockData = stocks[item.id] || { quantity: 'Chargement...', in_stock: false };
                return (
                  <tr key={item.id} style={{ borderBottom: '1px solid #f3f4f6' }}>
                    <td style={{ padding: '1rem 0.5rem', display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                      <span style={{ fontSize: '1.5rem' }}>{item.icon}</span> {item.name}
                    </td>
                    <td style={{ padding: '1rem 0.5rem', fontFamily: 'monospace' }}>{item.id}</td>
                    <td style={{ padding: '1rem 0.5rem', fontWeight: 'bold', fontSize: '1.1rem' }}>
                      {stockData.quantity} unités
                    </td>
                    <td style={{ padding: '1rem 0.5rem' }}>
                      <span style={{ 
                        padding: '0.25rem 0.75rem', borderRadius: '9999px', fontSize: '0.85rem', fontWeight: 'bold',
                        backgroundColor: stockData.quantity > 0 ? '#d1fae5' : '#fee2e2',
                        color: stockData.quantity > 0 ? '#065f46' : '#991b1b'
                      }}>
                        {stockData.quantity > 0 ? 'En Stock' : 'Rupture / Réservé'}
                      </span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {/* CONTENU DE L'ONGLET : CAISSE (PAYMENTS) */}
      {activeTab === 'payments' && (
        <div style={{ backgroundColor: 'white', padding: '2rem', borderRadius: '10px', boxShadow: '0 4px 6px rgba(0,0,0,0.05)' }}>
          <h2 style={{ color: '#1f2937', marginBottom: '1.5rem' }}>Terminal de Paiement (Simulation)</h2>
          
          {recentOrders.length === 0 ? (
            <p style={{ color: '#6b7280', textAlign: 'center', padding: '2rem' }}>
              Aucune commande récente en mémoire. Retournez sur l'onglet <strong>Boutique</strong> pour créer des commandes !
            </p>
          ) : (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '1.5rem' }}>
              {recentOrders.map((order) => (
                <div key={order.order_id} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '1.5rem', borderRadius: '8px', border: '1px solid #e5e7eb', backgroundColor: order.status === 'paid' ? '#f0fdf4' : '#fffbeb' }}>
                  <div>
                    <div style={{ fontWeight: 'bold', color: '#1f2937' }}>
                      Commande # {order.order_id.substring(0, 8)}... ({order.product_id})
                    </div>
                    <div style={{ fontSize: '0.875rem', color: '#6b7280', marginTop: '4px' }}>
                      Client : {order.customer_id} | Qté : {order.quantity} | Montant : <strong>{order.price} €</strong>
                    </div>
                  </div>

                  <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
                    <span style={{ 
                      padding: '0.25rem 0.75rem', borderRadius: '9999px', fontSize: '0.85rem', fontWeight: 'bold',
                      backgroundColor: order.status === 'paid' ? '#d1fae5' : '#fef3c7',
                      color: order.status === 'paid' ? '#065f46' : '#d97706'
                    }}>
                      {order.status === 'paid' ? 'PAIEMENT VALIDÉ' : 'EN ATTENTE'}
                    </span>
                    
                    {order.status !== 'paid' && (
                      <button 
                        onClick={() => payerCommande(order)}
                        disabled={loadingPayment === order.order_id}
                        style={{ padding: '0.5rem 1rem', backgroundColor: '#f59e0b', color: 'white', border: 'none', borderRadius: '5px', cursor: 'pointer', fontWeight: 'bold' }}
                      >
                        {loadingPayment === order.order_id ? 'Paiement...' : '💳 Payer'}
                      </button>
                    )}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      )}

    </div>
  );
}

export default App;