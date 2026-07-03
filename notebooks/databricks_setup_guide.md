# Guide de configuration : Connexion Databricks Free Edition ➔ MinIO Local

Ce guide explique comment configurer votre espace **Databricks Free Edition** pour lire et écrire des données dans votre instance **MinIO locale** (Docker), en respectant la contrainte d'architecture hybride du projet.

---

## 1. Le Défi technique : Local ➔ Cloud
Puisque **MinIO** tourne sur votre machine locale (`localhost:9000`) et que **Databricks** s'exécute dans le Cloud, Databricks ne peut pas accéder à `localhost`. 

Il faut donc rendre votre port MinIO local accessible de manière sécurisée depuis internet.

### Option recommandée : Utiliser ngrok (gratuit)
1. Installez ngrok sur votre poste local :
   ```bash
   sudo apt install ngrok  # Linux (ou téléchargez le binaire)
   ```
2. Configurez votre jeton d'authentification ngrok (depuis votre compte gratuit ngrok) :
   ```bash
   ngrok config add-authtoken <VOTRE_TOKEN>
   ```
3. Exposez le port API de MinIO (`9000`) :
   ```bash
   ngrok http 9000
   ```
4. ngrok va générer une URL publique de type : `https://xxxx-xx-xx-xx.ngrok-free.app`.
   * **Note** : C'est cette URL qu'il faudra fournir à Databricks comme `MINIO_ENDPOINT_URL`.

---

## 2. Configuration dans Databricks

### Méthode 1 : Configuration directe Spark (Recommandée)
Dans votre notebook Databricks PySpark, insérez et exécutez la configuration suivante dans la première cellule :

```python
# Remplacer par l'URL fournie par ngrok (sans le '/' final)
MINIO_ENDPOINT = "https://xxxx-xx-xx-xx.ngrok-free.app"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio12345"

# Configuration Hadoop S3A
sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
hadoop_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
hadoop_conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# SSL obligatoire si l'endpoint ngrok est en https
is_secure = "true" if MINIO_ENDPOINT.startswith("https://") else "false"
hadoop_conf.set("fs.s3a.connection.ssl.enabled", is_secure)
```

### Méthode 2 : Variables d'environnement globales
Si vous configurez un cluster Databricks (Free Edition / Serverless), vous pouvez définir ces variables d'environnement dans les paramètres avancés du compute :
* `MINIO_ENDPOINT_URL` = `https://xxxx-xx-xx-xx.ngrok-free.app`
* `MINIO_ACCESS_KEY` = `minio`
* `MINIO_SECRET_KEY` = `minio12345`

---

## 3. Exemple de lecture de la couche Bronze

Une fois connecté, vous pouvez interroger directement le Datalake MinIO au format `s3a://` :

```python
# Exemple de lecture du fichier competitions.json dans le bucket bronze
df_competitions = spark.read.json("s3a://bronze/statsbomb/competitions.json")
df_competitions.show(5)

# Exemple de lecture des matchs
df_matches = spark.read.json("s3a://bronze/statsbomb/matches/competition_id=43/season_id=106/matches.json")
df_matches.printSchema()
```

---

## 4. Écriture dans la couche Silver (Parquet)

Après nettoyage des données par le **Membre C**, le résultat peut être écrit directement dans le bucket `silver` au format Parquet :

```python
# Exemple d'écriture du dataframe de passes nettoyées
(df_passes_clean.write
    .mode("overwrite")
    .parquet("s3a://silver/pass_networks/match_id=3857286.parquet"))
```
