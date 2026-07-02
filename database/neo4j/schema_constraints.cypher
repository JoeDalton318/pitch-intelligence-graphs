// Création d'une contrainte d'unicité sur le nom du joueur.
// Cela crée automatiquement un index pour accélérer les requêtes (MERGE/MATCH).
CREATE CONSTRAINT unique_joueur_nom IF NOT EXISTS
FOR (j:Joueur)
REQUIRE j.nom IS UNIQUE;

// Optionnel mais recommandé : Index sur l'équipe pour accélérer les filtres de visualisation
CREATE INDEX index_joueur_equipe IF NOT EXISTS
FOR (j:Joueur)
ON (j.equipe);