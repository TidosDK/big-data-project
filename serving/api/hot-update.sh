#!/usr/bin/env bash

set -e

IMAGE="macbech/backend-api"
PLATFORM="linux/amd64"
NAMESPACE="bd-bd-gr-02"
DEPLOYMENT="backend-api"
CONTAINER="backend"
APP_LABEL="backend-api"

TAG=$(date +%Y%m%d%H%M%S)

echo "Building image: $IMAGE:$TAG"
docker build --platform $PLATFORM -t $IMAGE:$TAG .

echo "Pushing image: $IMAGE:$TAG"
docker push $IMAGE:$TAG

echo "Updating Deployment image"
kubectl set image deployment/$DEPLOYMENT \
  $CONTAINER=$IMAGE:$TAG \
  -n $NAMESPACE

echo "Restarting pods with label app=$APP_LABEL"
kubectl delete pod -l app="$APP_LABEL"

echo "Hot update completed successfully!"
kubectl get pods -l app="$APP_LABEL" 

sleep 15
echo "Fetching logs from updated pods..."
kubectl logs -l app=backend-api



# set -e

# docker build --platform linux/amd64 -t macbech/backend-api:latest .
# docker push macbech/backend-api:latest
# kubectl delete pod -l app=backend-api

# echo "Hot update completed successfully!..."