#!/usr/bin/env bash
# ============================================================
# run_databricks_job.sh
# Lance ngrok + met à jour et déclenche le job Databricks
# Usage : ./run_databricks_job.sh
# ============================================================

set -e

JOB_ID="329083345032947"
MINIO_PORT=9000

echo "🚀 Démarrage du tunnel ngrok sur le port $MINIO_PORT..."
# Lancer ngrok en arrière-plan s'il n'est pas déjà actif
if ! curl -s http://localhost:4040/api/tunnels > /dev/null 2>&1; then
  ngrok http $MINIO_PORT > /dev/null &
  sleep 4
fi

# Récupérer l'URL HTTPS publique
NGROK_URL=$(curl -s http://localhost:4040/api/tunnels \
  | python3 -c "import sys,json; t=json.load(sys.stdin)['tunnels']; [print(x['public_url']) for x in t if x['proto']=='https']")

if [ -z "$NGROK_URL" ]; then
  echo "❌ Impossible de récupérer l'URL ngrok. Vérifiez que ngrok est lancé."
  exit 1
fi

echo "✅ URL ngrok : $NGROK_URL"

# Vérifier que MinIO est joignable
STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "ngrok-skip-browser-warning: 1" \
  "$NGROK_URL/minio/health/live")

if [ "$STATUS" != "200" ]; then
  echo "❌ MinIO non joignable (HTTP $STATUS). Vérifiez que Docker est lancé."
  exit 1
fi

echo "✅ MinIO joignable (HTTP $STATUS)"

# Déclencher le job Databricks avec la nouvelle URL
echo "🔄 Lancement du job Databricks ($JOB_ID)..."
databricks jobs run-now "$JOB_ID" \
  --json "{\"notebook_params\": {\"ngrok_url\": \"$NGROK_URL\"}}"

echo "✅ Job lancé ! Consultez la progression sur :"
echo "   https://dbc-6090890b-e77d.cloud.databricks.com/?o=7474646401140418#job/$JOB_ID"
