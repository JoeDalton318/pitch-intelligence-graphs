# Projet 6 - Graphe de performance et reseaux de passes

Pipeline Big Data local pour reconstruire des reseaux de passes de football.
Le projet suit une architecture medaillon :

- `bronze` : donnees brutes JSON dans MinIO.
- `silver` : passes nettoyees et agregees par match/equipe.
- `gold` : indicateurs analytiques pour Superset.

## Repartition rapide

- Membre A : Airflow, MinIO, orchestration, ingestion Bronze.
- Membre B : MongoDB, Neo4j, Cypher, GDS.
- Membre C : DuckDB/PostgreSQL/Superset, Silver et Gold.

## Etape 1 - Ingestion Bronze

Cette etape lance uniquement les services utiles au socle d'ingestion :
MinIO, PostgreSQL et Airflow.

```bash
docker compose -f docker/docker-compose.yml up -d minio minio-init postgres airflow
```

Interfaces utiles :

- MinIO Console : http://localhost:9001
- Airflow : http://localhost:8080

Identifiants MinIO :

- utilisateur : `minio`
- mot de passe : `minio12345`

Airflow cree un compte admin au demarrage `standalone`. Le mot de passe est
affiche dans les logs du conteneur Airflow.

```bash
docker compose -f docker/docker-compose.yml logs airflow
```

### DAG Bronze

Le DAG `01_ingestion_bronze_statsbomb` telecharge des fichiers JSON bruts depuis
StatsBomb open-data et les stocke dans le bucket MinIO `bronze`.

Objets produits par defaut :

- `statsbomb/competitions.json`
- `statsbomb/matches/competition_id=43/season_id=106/matches.json`
- `statsbomb/events/match_id=<id>.json`
- `statsbomb/lineups/match_id=<id>.json`
- `statsbomb/manifests/latest_ingestion.json`

Variables configurables dans `docker/docker-compose.yml` :

- `STATSBOMB_COMPETITION_ID` : competition StatsBomb, `43` par defaut.
- `STATSBOMB_SEASON_ID` : saison StatsBomb, `106` par defaut.
- `STATSBOMB_MATCH_LIMIT` : nombre de matchs a ingerer, `1` par defaut.
- `STATSBOMB_MATCH_IDS` : liste optionnelle d'identifiants de matchs separes par des virgules.

Pour respecter la contrainte de RAM, les services Neo4j, MongoDB, Superset et
Ollama sont places dans des profils Docker Compose et ne demarrent pas avec la
commande de l'etape 1.
