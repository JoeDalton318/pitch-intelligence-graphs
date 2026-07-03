import json
import logging
from pymongo import MongoClient, UpdateOne
from pymongo.errors import ConnectionFailure

# Configuration basique du logger pour un suivi clair (bonne pratique)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


import os


def ingest_match_metadata(
    json_filepath: str,
    db_uri: str = os.getenv(
        "MONGO_URI", "mongodb://app:app12345@localhost:27017/?authSource=admin"
    ),
    db_name: str = "pitch_intelligence",
    collection_name: str = "match_metadata",
):
    """
    Lit un fichier JSON local contenant les métadonnées d'un ou plusieurs matchs
    (ex: StatsBomb open data) et les insère ou met à jour dans MongoDB.
    """
    client = None
    try:
        # 1. Connexion au client MongoDB
        client = MongoClient(db_uri)
        client.admin.command("ping")  # Vérification rapide de la disponibilité
        logging.info("Connexion à MongoDB réussie.")

        db = client[db_name]
        collection = db[collection_name]

        # 2. Lecture du fichier JSON depuis MinIO (Couche Bronze)
        import sys, os

        sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
        from dags.utils.s3_client import get_minio_client

        minio_client = get_minio_client()
        bronze_bucket = os.getenv("BRONZE_BUCKET", "bronze")
        # On lit le fichier des matchs de la coupe du monde par defaut
        response = minio_client.get_object(
            bronze_bucket,
            "statsbomb/matches/competition_id=43/season_id=106/matches.json",
        )
        matches_data = json.loads(response.read().decode("utf-8"))
        response.close()
        response.release_conn()

        # Uniformisation : on s'assure d'avoir une liste même si on a un seul match
        if isinstance(matches_data, dict):
            matches_data = [matches_data]

        operations = []
        for match in matches_data:
            # 3. Extraction des champs clés pertinents (adapté à StatsBomb)
            match_id = match.get("match_id")

            if not match_id:
                logging.warning("Un match sans 'match_id' a été ignoré.")
                continue

            metadata_doc = {
                "match_id": match_id,
                "match_date": match.get("match_date"),
                "competition": match.get("competition", {}).get("competition_name"),
                "season": match.get("season", {}).get("season_name"),
                "home_team": match.get("home_team", {}).get("home_team_name"),
                "away_team": match.get("away_team", {}).get("away_team_name"),
                "home_team_managers": match.get("home_team", {}).get("managers", []),
                "away_team_managers": match.get("away_team", {}).get("managers", []),
            }

            # 4. Utilisation de UpdateOne avec upsert=True
            # Cela évite les doublons si l'ingestion est relancée plusieurs fois par Airflow
            operations.append(
                UpdateOne({"match_id": match_id}, {"$set": metadata_doc}, upsert=True)
            )

        # 5. Exécution de toutes les opérations en batch pour la performance
        if operations:
            result = collection.bulk_write(operations)
            logging.info(
                f"Ingestion terminée : {result.upserted_count} insérés, {result.modified_count} mis à jour."
            )
        else:
            logging.warning("Aucune donnée de match trouvée dans le fichier.")

    except FileNotFoundError:
        logging.error(f"Le fichier spécifié est introuvable : {json_filepath}")
    except ConnectionFailure:
        logging.error(
            "Impossible de se connecter à MongoDB. Vérifiez que le conteneur tourne."
        )
    except Exception as e:
        logging.error(f"Erreur inattendue lors de l'ingestion : {e}")
    finally:
        # 6. Clôture de la connexion
        if client is not None:
            client.close()
            logging.info("Connexion MongoDB fermée.")


if __name__ == "__main__":
    # Chemin vers les vraies données StatsBomb
    SAMPLE_JSON = "data/bronze/statsbomb_wc2022_matches.json"
    ingest_match_metadata(SAMPLE_JSON)
