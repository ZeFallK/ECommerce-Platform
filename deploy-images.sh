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

