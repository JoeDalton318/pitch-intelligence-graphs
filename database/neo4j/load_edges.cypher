// Chargement des relations (arêtes) de PASSES entre les joueurs
LOAD CSV WITH HEADERS FROM 'file:///passes_agg.csv' AS row

// Récupération des noeuds passeur et receveur existants
MATCH (p:Joueur {nom: row.passeur_nom})
MATCH (r:Joueur {nom: row.receveur_nom})

// Création de la relation PASSE. On inclut le match_id pour avoir un graphe filtrable par match.
// MERGE garantit qu'on ne duplique pas l'arête si on relance le script.
MERGE (p)-[rel:PASSE {match_id: row.match_id}]->(r)

// On met à jour le volume (poids) de la relation
// L'utilisation de SET sans condition ON CREATE/ON MATCH garantit la mise à jour idempotente
SET rel.nb = toInteger(row.nombre_passes);