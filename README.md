# ECommerce-Platform

> Plateforme e-commerce construite sur une architecture microservices event-driven, avec observabilité complète et authentification centralisée — 100% open source, 100% local.

---

## Vue d'ensemble
 
Ce projet est une **démonstration complète d'architecture microservices** appliquée à un cas e-commerce. Il simule le cycle de vie d'une commande, de sa création jusqu'à la confirmation du paiement et la mise à jour du stock, de façon **entièrement asynchrone via Apache Kafka**.
 
L'objectif est d'appliquer mes connaissances sur les technologies cloud-native modernes acquises en entreprise et produire quelque chose de fonctionnel et réaliste.

---

## Architecture

┌─────────────┐
│ Utilisateur │
└──────┬──────┘
       │
       ▼
┌──────────────────────────────────────────────┐
│              Frontend React + Vite           │
│         Dashboard · Authentication · APIs    │
└────────────┬──────────────────────┬───────────┘
             │                      │
             │ OIDC                 │ JWT Bearer Token
             ▼                      ▼
      ┌─────────────┐        ┌─────────────┐
      │  Keycloak   │        │   Traefik   │
      │ Identity    │        │ API Gateway │
      │ Provider    │        │  / Ingress  │
      └─────────────┘        └──────┬──────┘
                                    │
                 ┌──────────────────┼──────────────────┐
                 │                  │                  │
                 ▼                  ▼                  ▼
          ┌────────────┐     ┌────────────┐     ┌────────────┐
          │   Orders   │     │  Payments  │     │ Inventory  │
          │  FastAPI   │     │  FastAPI   │     │  FastAPI   │
          └─────┬──────┘     └─────┬──────┘     └─────┬──────┘
                │                  │                  │
                └──────────────┬───┴──────────────────┘
                               │ Events
                               ▼
                      ┌─────────────────┐
                      │      Kafka      │
                      │   KRaft Mode    │
                      │    Event Bus    │
                      └─────────────────┘

                ─ ─ ─ ─ OBSERVABILITY ─ ─ ─ ─

          Orders ───────┐
          Payments ─────┼──── Telemetry ────┐
          Inventory ────┘                   │
                                           ▼
                                ┌──────────────────────┐
                                │ OpenTelemetry        │
                                │ Collector            │
                                └────┬──────┬──────┬───┘
                                     │      │      │
                         Metrics     │      │      │ Logs
                                     ▼      │      ▼
                              ┌──────────┐   │   ┌──────────┐
                              │Prometheus│   │   │   Loki   │
                              └────┬─────┘   │   └────┬─────┘
                                   │         ▼        │
                                   │    ┌─────────┐   │
                                   │    │  Tempo  │   │
                                   │    └────┬────┘   │
                                   └─────────┼────────┘
                                             ▼
                                      ┌───────────┐
                                      │  Grafana  │
                                      │ Dashboards│
                                      └───────────┘

---
 
## 🧰 Stack technique
 
| Couche | Technologie | Rôle |
|---|---|---|
| **Frontend** | React + Vite + Tailwind | Dashboard utilisateur |
| **Auth** | Keycloak 24 | SSO, JWT, gestion des rôles |
| **API Gateway** | Traefik | Routage, Ingress Kubernetes |
| **Microservices** | Python / FastAPI | Logique métier |
| **Event Bus** | Apache Kafka (KRaft) | Communication asynchrone |
| **Traces** | OpenTelemetry + Tempo | Traces distribuées |
| **Métriques** | OpenTelemetry + Prometheus | Métriques applicatives |
| **Logs** | OpenTelemetry + Loki | Logs centralisés |
| **Visualisation** | Grafana | Dashboards, alerting |
| **Conteneurs** | Docker + Docker Compose | Dev local |
| **Orchestration** | Kubernetes (Minikube) | Déploiement |
 
---
