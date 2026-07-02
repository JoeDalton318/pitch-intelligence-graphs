// 1. Création de la projection du graphe en mémoire (GDS)
// On sélectionne les noeuds 'Joueur' et les relations 'PASSE'.
// On inclut la propriété 'nb' (le volume de passes) pour l'utiliser comme poids.
CALL
  gds.graph.project(
    'reseau_passes',
    'Joueur',
    {PASSE: {orientation: 'NATURAL', properties: 'nb'}}
  );

// 2. Algorithme de PageRank (Plaques tournantes / Hubs)
// Objectif : Identifier les joueurs les plus influents du réseau (ceux qui reçoivent beaucoup
// de passes, notamment de la part d'autres joueurs eux-mêmes très impliqués).
// Le mode 'write' inscrit le score directement comme propriété 'pagerank_score' dans la base.
CALL
  gds.pageRank.write(
    'reseau_passes',
    {
      maxIterations: 20,
      dampingFactor: 0.85,
      relationshipWeightProperty: 'nb',
      writeProperty: 'pagerank_score'
    }
  );

// 3. Algorithme de Betweenness Centrality (Intermédiarité)
// Objectif : Identifier les joueurs "ponts" qui connectent différentes parties de l'équipe
// (ex: le milieu défensif clé pour la transition défense/attaque).
// Il calcule la fréquence à laquelle un joueur se trouve sur le plus court chemin entre d'autres joueurs.
CALL
  gds.betweenness.write(
    'reseau_passes',
    {writeProperty: 'betweenness_score'}
  );

// 4. Nettoyage
// Suppression de la projection en mémoire pour libérer la RAM du conteneur Docker.
CALL gds.graph.drop('reseau_passes');