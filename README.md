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

## 🧑‍💻 Section Membre B : Gills Daryl KETCHA (Graph Data Engineer)

Cette section documente le travail réalisé par **Gills Daryl KETCHA (Membre B)**, centré sur le stockage documentaire des métadonnées et la modélisation en graphe des réseaux de passes.

### 1. Architecture & Choix Techniques
- **MongoDB (Ingestion Documentaire)** : Les métadonnées globales des matchs (date, compétition, équipes) sont extraites de la couche Bronze. Pour garantir l'idempotence (éviter les doublons de matchs si le pipeline est relancé), j'ai utilisé la commande `UpdateOne` avec `upsert=True` basée sur le `match_id`. Des index de performance (`database/mongo/indexes.js`) ont été mis en place pour accélérer les futures requêtes.
- **Neo4j (Modélisation Graphe)** : La couche Silver est modélisée en réseau de passes avec la structure `(Joueur)-[PASSE]->(Joueur)`. L'arête `PASSE` est pondérée par le volume d'échanges (`nb`).
- **Image Docker Neo4j** : J'ai fixé la version `neo4j:5.21.0-community` avec la variable d'environnement `NEO4J_PLUGINS=["graph-data-science"]` pour garantir la compatibilité et le support natif et stable des algorithmes GDS.

### 2. Guide de démarrage rapide

- **Ingérer les métadonnées dans MongoDB** :
  ```bash
  python src/storage/mongo_writer.py
  ```
- **Charger le graphe (Neo4j)** :
  1. Copier et exécuter `database/neo4j/schema_constraints.cypher` pour l'unicité des noeuds.
  2. Exécuter `database/neo4j/load_nodes.cypher` (Joueurs).
  3. Exécuter `database/neo4j/load_edges.cypher` (Arêtes de passes pondérées).
- **Calculer et afficher les métriques** :
  1. Calculer les scores en mémoire avec : `database/neo4j/gds_metrics.cypher`
  2. Afficher le Top 5 tactique avec Python :
     ```bash
     python src/storage/neo4j_loader.py
     ```

### 3. Explication Mathématique & Métier des Algorithmes GDS

- **PageRank** : Identifie les "plaques tournantes" (Hubs). Un joueur obtient un score de PageRank élevé s'il reçoit beaucoup de passes de la part d'autres joueurs qui sont eux-mêmes très influents. 
**Métier** : Permet de trouver le vrai meneur/constructeur du jeu.
- **Betweenness Centrality (Intermédiarité)** : Détecte les joueurs "ponts". Il calcule à quelle fréquence un joueur se trouve sur le chemin de passes le plus court entre deux autres joueurs. 
**Métier** : Permet d'identifier le joueur indispensable pour la transition défense-attaque (souvent un milieu récupérateur/relai).

---

## Section Membre C : Data Transformation & Restitution BI (DuckDB, PostgreSQL, Superset)

Cette section documente le travail realise par la partie Membre C du projet. L'objectif etait de transformer les donnees brutes (Silver), de les stocker dans un environnement analytique robuste (Gold) et de les restituer.

### Architecture Technique
1. **Couche Silver (DuckDB)** : Lecture des donnees brutes JSON depuis le bucket Bronze, filtrage des passes et aggregation, sauvegarde au format Parquet dans MinIO.
2. **Couche Gold (PostgreSQL)** : Upsert massif des donnees agregees via DuckDB et psycopg2 vers une base relationnelle PostgreSQL.
3. **Restitution (Superset)** : Connexion a Postgres pour la generation de Dashboards de Business Intelligence.

### Guide de configuration BI (Superset)
Pour configurer et acceder a la restitution visuelle des passes :

1. Acceder a l'interface web Apache Superset a l'adresse : `http://localhost:8088`.
2. Ajouter PostgreSQL comme source de donnees analytique. Utiliser la chaine de connexion SQLAlchemy suivante :
   `postgresql+psycopg2://app:app12345@postgres:5432/gold`
3. Exposer la table `fact_passes` pour construire les graphiques et le tableau de bord final.
