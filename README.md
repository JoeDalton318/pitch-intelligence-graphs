# Projet 6 - Graphe de performance et reseaux de passes

Pipeline Big Data local pour reconstruire des reseaux de passes de football.
Le projet suit une architecture medaillon :

- `bronze` : donnees brutes JSON dans MinIO.
- `silver` : passes nettoyees et agregees par match/equipe.
- `gold` : indicateurs analytiques pour Superset.

> 📖 **[Voir la Documentation Complète de l'Architecture (Mermaid)](docs/ARCHITECTURE.md)**

## 🚀 Comment tester l'intégralité du projet de A à Z

Pour lancer l'intégralité du pipeline, depuis l'ingestion de la donnée jusqu'à sa visualisation en graphes et BI :

1. **Lancer les services d'infrastructure**
   Démarrez l'orchestrateur (Airflow), le stockage (MinIO, Postgres), la base documentaire (MongoDB) et le graphe (Neo4j) :
   ```bash
   docker compose -f docker/docker-compose.yml --profile metadata --profile graph --profile bi up -d
   ```
2. **Déclencher les Pipelines Airflow**
   Allez sur **[Airflow](http://localhost:8080)** (admin/admin par défaut ou mot de passe dans les logs `docker compose logs airflow`). Activez et lancez les deux DAGs :
   - `01_world_cup_ingestion` (télécharge la donnée en masse dans MinIO).
   - `02_world_cup_processing` (prend le relais automatiquement, transforme via DuckDB, charge PostgreSQL, upsert MongoDB et exécute les algos GDS dans Neo4j).
3. **Visualiser avec Superset (BI)**
   Allez sur **[Superset](http://localhost:8088)** (admin/admin). Connectez PostgreSQL avec `postgresql+psycopg2://app:app12345@postgres:5432/gold` et créez des graphiques sur `fact_passes`.
4. **Visualiser les Graphes (Neo4j)**
   Allez sur **[Neo4j](http://localhost:7474)** (neo4j/neo4j12345) et exécutez `MATCH (n) RETURN n` pour voir votre réseau tactique top 5 !

---

## Repartition rapide

- Membre A : Airflow, MinIO, orchestration, ingestion Bronze.
- Membre B : MongoDB, Neo4j, Cypher, GDS.
- Membre C : DuckDB/PostgreSQL/Superset, Silver et Gold.

---

## 🧑‍💻 Section Membre A : Ingestion Bronze & Orchestration

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

### Architecture des DAGs Airflow

Afin d'optimiser l'orchestration, le pipeline a été divisé en deux DAGs synchronisés par un `S3KeySensor` :

1. **DAG 1 : `01_world_cup_ingestion`**
   - Télécharge les compétitions et les matchs.
   - **Ingestion massive multithreadée** : Traite de manière concurrente (jusqu'à 10 workers) les centaines de fichiers d'événements de la Coupe du Monde (2018 et 2022).
   - Dépose un fichier marqueur `_SUCCESS` dans le bucket `bronze` à la fin.

2. **DAG 2 : `02_world_cup_processing`**
   - Attend l'apparition du fichier `_SUCCESS` via un capteur (Sensor).
   - Traite les données avec DuckDB (Bronze -> Silver).
   - Charge les données agrégées dans Postgres (Couche Gold).
   - Automatise l'insertion en base de données documentaire MongoDB.
   - Automatise la modélisation en graphe et le calcul des algorithmes dans Neo4j.

---

## 🧑‍💻 Section Membre B : Gills Daryl KETCHA (Graph Data Engineer)

Cette section documente le travail réalisé par **Gills Daryl KETCHA (Membre B)**, centré sur le stockage documentaire des métadonnées et la modélisation en graphe des réseaux de passes.

### 1. Architecture & Choix Techniques
- **MongoDB (Ingestion Documentaire)** : Les métadonnées globales des matchs (date, compétition, équipes) sont extraites de la couche Bronze. L'idempotence est garantie par des requêtes `UpdateOne` avec `upsert=True`.
- **Neo4j (Modélisation Graphe)** : La couche Silver est modélisée en réseau de passes avec la structure `(Joueur)-[PASSE]->(Joueur)`. L'arête `PASSE` est pondérée par le volume d'échanges (`nb`).
- **Image Docker Neo4j** : Version `neo4j:5.21.0-community` avec `NEO4J_PLUGINS=["graph-data-science"]`.

### 2. Guide de démarrage rapide

- L'ingestion des métadonnées (MongoDB) et le chargement du graphe (Neo4j) **sont désormais entièrement automatisés** par le DAG Airflow `02_world_cup_processing`.
- Interface Neo4j : `http://localhost:7474` (neo4j / neo4j12345)
- Requête pour visualiser le réseau de passes d'un match (Top 5) :
  ```cypher
  MATCH (n) RETURN n
  ```

### 3. Explication Mathématique & Métier des Algorithmes GDS (Automatisés)

Les algorithmes suivants sont projetés et calculés en mémoire de façon automatique via le pipeline :
- **PageRank** : Identifie les "plaques tournantes" (Hubs). Un joueur obtient un score élevé s'il reçoit beaucoup de passes de la part d'autres joueurs très influents.
- **Betweenness Centrality (Intermédiarité)** : Détecte les joueurs "ponts". Calcule à quelle fréquence un joueur se trouve sur le chemin de passes le plus court entre deux autres joueurs (souvent un milieu récupérateur/relai).

---

## 🧑‍💻 Section Membre C : Data Transformation & Restitution BI (DuckDB, PostgreSQL, Superset)

Cette section documente le travail réalisé pour transformer les données brutes (Silver), les stocker dans un environnement analytique robuste (Gold) et les restituer.

### Architecture Technique
1. **Couche Silver (DuckDB)** : Lecture des donnees brutes JSON depuis MinIO, filtrage des passes et aggregation, sauvegarde au format Parquet.
2. **Couche Gold (PostgreSQL)** : Upsert massif des donnees agregees vers une base relationnelle PostgreSQL.
3. **Restitution (Superset)** : Connexion a Postgres pour la generation de Dashboards de Business Intelligence. Le connecteur `psycopg2` a été ajouté manuellement à l'image pour permettre la connexion.

### Guide de configuration BI (Superset)
Pour configurer et accéder à la restitution visuelle des passes :

1. Accéder à l'interface web Apache Superset à l'adresse : `http://localhost:8088`.
2. Se connecter avec les identifiants administrateur : `admin` / `admin`.
3. Ajouter PostgreSQL comme source de donnees analytique. Utiliser la chaine de connexion SQLAlchemy suivante :
   `postgresql+psycopg2://app:app12345@postgres:5432/gold`
4. Exposer la table `fact_passes` (schéma `public`) pour construire les graphiques et le tableau de bord final.
