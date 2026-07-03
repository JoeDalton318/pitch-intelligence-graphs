import logging
import os
import csv
from neo4j import GraphDatabase

# Configuration du logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def load_graph_to_neo4j(uri, user, password, csv_path):
    """
    Se connecte à Neo4j et injecte les nœuds (Joueurs) et les relations (Passes)
    à partir du fichier CSV généré par la couche Silver.
    """
    if not os.path.exists(csv_path):
        logging.error(f"Fichier CSV introuvable : {csv_path}")
        return

    # Lecture du fichier CSV
    records = []
    with open(csv_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(
                {
                    "match_id": row["match_id"],
                    "passeur_nom": row["passeur_nom"],
                    "receveur_nom": row["receveur_nom"],
                    "equipe": row["equipe"],
                    "nombre_passes": int(row["nombre_passes"]),
                }
            )

    logging.info(f"-> {len(records)} lignes lues depuis le CSV local.")

    try:
        with GraphDatabase.driver(uri, auth=(user, password)) as driver:
            logging.info("Connexion à Neo4j établie avec succès.")

            with driver.session() as session:
                # 1. Contraintes et Index
                logging.info("1. Création des contraintes et index...")
                session.run(
                    "CREATE CONSTRAINT unique_joueur_nom IF NOT EXISTS FOR (j:Joueur) REQUIRE j.nom IS UNIQUE;"
                )
                session.run(
                    "CREATE INDEX index_joueur_equipe IF NOT EXISTS FOR (j:Joueur) ON (j.equipe);"
                )

                # 2. Chargement des Noeuds
                logging.info("2. Chargement des noeuds (Joueurs)...")
                session.run(
                    """
                UNWIND $records AS row
                MERGE (p:Joueur {nom: row.passeur_nom})
                ON CREATE SET p.equipe = row.equipe
                MERGE (r:Joueur {nom: row.receveur_nom})
                ON CREATE SET r.equipe = row.equipe
                """,
                    records=records,
                )

                # 3. Chargement des Arêtes
                logging.info("3. Chargement des arêtes (Passes)...")
                session.run(
                    """
                UNWIND $records AS row
                MATCH (p:Joueur {nom: row.passeur_nom})
                MATCH (r:Joueur {nom: row.receveur_nom})
                MERGE (p)-[rel:PASSE {match_id: row.match_id}]->(r)
                SET rel.nb = row.nombre_passes
                """,
                    records=records,
                )

                # 4. Exécution automatique des algorithmes GDS (Graph Data Science)
                logging.info(
                    "4. Exécution des algorithmes GDS (PageRank et Betweenness)..."
                )
                try:
                    # On supprime la projection si elle existe déjà (relance du DAG)
                    session.run(
                        "CALL gds.graph.drop('reseau_passes', false) YIELD graphName;"
                    )

                    # Projection en mémoire
                    session.run("""
                    CALL gds.graph.project(
                        'reseau_passes',
                        'Joueur',
                        {PASSE: {orientation: 'NATURAL', properties: 'nb'}}
                    );
                    """)

                    # PageRank
                    session.run("""
                    CALL gds.pageRank.write(
                        'reseau_passes',
                        {
                            maxIterations: 20,
                            dampingFactor: 0.85,
                            relationshipWeightProperty: 'nb',
                            writeProperty: 'pagerank_score'
                        }
                    );
                    """)

                    # Betweenness
                    session.run("""
                    CALL gds.betweenness.write(
                        'reseau_passes',
                        {writeProperty: 'betweenness_score'}
                    );
                    """)

                    # Nettoyage de la mémoire
                    session.run("CALL gds.graph.drop('reseau_passes');")
                    logging.info(
                        "-> Algorithmes GDS terminés et scores sauvegardés dans Neo4j."
                    )
                except Exception as e:
                    logging.error(f"Erreur lors de l'exécution de GDS : {e}")

            logging.info("✅ Étape C1.5 (Graphe Neo4j) terminée avec succès !")

    except Exception as e:
        logging.error(f"Erreur lors de l'interaction avec Neo4j : {e}")


def get_top_players_by_pagerank(uri, user, password, limit=5):
    """
    Exécute une requête pour récupérer le Top 5 des joueurs selon le score PageRank.
    Attention : Nécessite que l'algorithme GDS PageRank ait été préalablement exécuté.
    """
    query = """
    MATCH (j:Joueur)
    WHERE j.pagerank_score IS NOT NULL
    RETURN j.nom AS joueur, j.equipe AS equipe, j.pagerank_score AS score
    ORDER BY score DESC
    LIMIT $limit
    """
    try:
        with GraphDatabase.driver(uri, auth=(user, password)) as driver:
            with driver.session() as session:
                result = session.run(query, limit=limit)

                # Vérifie si le résultat est vide (GDS non exécuté)
                records = list(result)
                if records:
                    print(f"\n--- TOP {limit} PLAQUES TOURNANTES (PageRank) ---")
                    for i, record in enumerate(records, 1):
                        score = round(record["score"], 3)
                        print(
                            f"{i}. {record['joueur']} ({record['equipe']}) - Score: {score}"
                        )
                    print("-" * 50 + "\n")
                else:
                    logging.info(
                        "Aucun score PageRank trouvé. L'algorithme GDS doit être exécuté pour voir le top joueurs."
                    )
    except Exception as e:
        logging.error(f"Erreur lors de la lecture Neo4j : {e}")


if __name__ == "__main__":
    # Paramètres de connexion avec variable d'environnement (Docker vs Local)
    NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
    NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "neo4j12345")

    # Résolution dynamique du chemin vers le CSV
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_dir, "..", ".."))
    csv_file = os.path.join(project_root, "passes_agg.csv")

    load_graph_to_neo4j(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, csv_file)
    get_top_players_by_pagerank(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
