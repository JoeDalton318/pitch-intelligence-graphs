// 1. Chargement des noeuds Joueurs (Passeurs)
// L'utilisation de MERGE garantit l'idempotence (le noeud n'est créé que s'il n'existe pas)
LOAD CSV WITH HEADERS FROM 'file:///passes_agg.csv' AS row
MERGE (p:Joueur {nom: row.passeur_nom})
  ON CREATE SET p.equipe = row.equipe;

// 2. Chargement des noeuds Joueurs (Receveurs)
// Dans une action de passe réussie, le receveur est généralement dans la même équipe
LOAD CSV WITH HEADERS FROM 'file:///passes_agg.csv' AS row
MERGE (r:Joueur {nom: row.receveur_nom})
  ON CREATE SET r.equipe = row.equipe;