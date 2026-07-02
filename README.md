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

---

## 🧑‍💻 Section Membre A : Ingestion Bronze

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

---

## 🧑‍💻 Section Membre B : Graph Data Engineering (MongoDB & Neo4j)

Cette section documente le travail de la partie "Membre B", centré sur le stockage documentaire des métadonnées et la modélisation en graphe des réseaux de passes.

### 1. Ingestion Documentaire (MongoDB)
Les métadonnées globales des matchs (date, compétition, équipes, managers) sont extraites de la couche Bronze et stockées dans une base de données NoSQL orientée document (MongoDB).
- **Script d'ingestion** : `src/storage/mongo_writer.py`
- **Exécution** : 
  ```bash
  python src/storage/mongo_writer.py
  ```
*(Note : Le script gère les doublons grâce à un système d'Upsert basé sur le `match_id` et s'appuie sur des index optimisés créés via `database/mongo/indexes.js`)*.

### 2. Modélisation du Graphe (Neo4j)
La couche Silver, contenant les données de passes agrégées, est modélisée sous forme de graphe :
- **Nœuds (`Joueur`)** : Représentent les joueurs sur le terrain.
- **Relations (`PASSE`)** : Lient un passeur à un receveur. Elles sont pondérées par la propriété `nb` (volume de passes) et contextualisées par le `match_id`.
- **Scripts d'importation** :
  1. `database/neo4j/schema_constraints.cypher` : Garantit l'unicité des nœuds.
  2. `database/neo4j/load_nodes.cypher` : Insère les joueurs.
  3. `database/neo4j/load_edges.cypher` : Crée les arêtes pondérées.

### 3. Analytique Avancée & Graph Data Science (GDS)
Une fois le graphe modélisé, nous utilisons la librairie Neo4j GDS pour extraire la véritable valeur tactique métier.

- **PageRank** (`pagerank_score`) : 
  *Signification* : Identifie les véritables "plaques tournantes" (Hubs) de l'équipe. Un joueur a un score élevé s'il reçoit beaucoup de passes, particulièrement de la part d'autres joueurs eux-mêmes très influents dans la construction du jeu.
  
- **Betweenness Centrality / Intermédiarité** (`betweenness_score`) : 
  *Signification* : Détecte les joueurs "ponts". Un joueur a un score élevé s'il se situe très souvent sur le chemin le plus court entre deux autres joueurs. Il s'agit typiquement des profils clés pour la transition entre la défense et l'attaque.

- **Exécution de l'algorithmique** :
  1. Lancer le script Cypher `database/neo4j/gds_metrics.cypher` dans l'interface Neo4j pour calculer et inscrire les scores.
  2. Lancer l'extraction via Python pour afficher le Top 5 : 
     ```bash
     python src/storage/neo4j_loader.py
     ```
