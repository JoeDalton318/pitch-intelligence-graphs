import logging
from neo4j import GraphDatabase

# Configuration du logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_top_players_by_pagerank(uri, user, password, limit=5):
    """
    Se connecte à Neo4j, exécute une requête pour récupérer le Top 5
    des joueurs selon le score PageRank (calculé par GDS), et affiche le résultat.
    """
    # Requête Cypher de lecture
    query = """
    MATCH (j:Joueur)
    WHERE j.pagerank_score IS NOT NULL
    RETURN j.nom AS joueur, j.equipe AS equipe, j.pagerank_score AS score
    ORDER BY score DESC
    LIMIT $limit
    """

    try:
        # L'utilisation du pattern 'with' sur le driver ET la session garantit
        # la fermeture automatique et propre de la connexion (anti fuite de mémoire).
        with GraphDatabase.driver(uri, auth=(user, password)) as driver:
            logging.info("Connexion à Neo4j établie avec succès.")

            with driver.session() as session:
                result = session.run(query, limit=limit)

                print(f"\n🏆 --- TOP {limit} PLAQUES TOURNANTES (PageRank) --- 🏆")
                for i, record in enumerate(result, 1):
                    joueur = record["joueur"]
                    equipe = record["equipe"]
                    score = round(record["score"], 3)
                    print(f"{i}. {joueur} ({equipe}) - Score: {score}")
                print("-" * 50 + "\n")

    except Exception as e:
        logging.error(f"Erreur lors de l'interaction avec Neo4j : {e}")


if __name__ == "__main__":
    # Identifiants locaux tels que définis dans les consignes d'architecture
    NEO4J_URI = "bolt://localhost:7687"
    NEO4J_USER = "neo4j"
    NEO4J_PASSWORD = "neo4j12345"

    get_top_players_by_pagerank(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
