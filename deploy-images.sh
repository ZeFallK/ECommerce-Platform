#!/usr/bin/env bash

set -e

eval "$(minikube docker-env)"

echo "🔨 Building frontend..."
docker build -t frontend:latest ./frontend

echo "🔨 Building orders..."
docker build -t orders:latest ./services/orders

echo "🔨 Building inventory..."
docker build -t inventory:latest ./services/inventory

echo "🔨 Building payments..."
docker build -t payments:latest ./services/payments

echo "♻️ Restarting deployments..."
kubectl rollout restart deployment/frontend -n ecommerce
kubectl rollout restart deployment/orders -n ecommerce
kubectl rollout restart deployment/inventory -n ecommerce
kubectl rollout restart deployment/payments -n ecommerce

echo "⏳ Waiting for rollouts..."
kubectl rollout status deployment/frontend -n ecommerce
kubectl rollout status deployment/orders -n ecommerce
kubectl rollout status deployment/inventory -n ecommerce
kubectl rollout status deployment/payments -n ecommerce

echo "✅ Deployment terminé"
kubectl get pods -n ecommerce