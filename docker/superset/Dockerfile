# Image Superset custom pour le projet 6.
# Base : image officielle Apache Superset + drivers PostgreSQL (psycopg2)
# pour connecter Superset à la couche Gold du projet.

FROM apache/superset:latest

# Passe en root le temps de l'install (nécessaire pour pip system-wide)
USER root

# Installe les drivers DB dont Superset a besoin :
# - psycopg2-binary : connecteur PostgreSQL (indispensable pour Gold Football)
# IMPORTANT : Superset tourne dans son propre venv (/app/.venv/), et cette
# image ne fournit PAS de binaire "pip" dans ce venv (ni le module pip) :
# le gestionnaire de paquets utilisé est "uv". Il faut donc installer via
# "uv pip install --python /app/.venv/bin/python" pour cibler le bon venv.
RUN uv pip install --python /app/.venv/bin/python --no-cache psycopg2-binary==2.9.9

# Retour au user superset par défaut (bonne pratique de sécurité)
USER superset