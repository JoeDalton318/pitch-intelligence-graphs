// Script de création des index pour la collection match_metadata
// Exécution: mongosh pitch_intelligence database/mongo/indexes.js

print("Début de la création des index pour la base 'pitch_intelligence'...");

const dbName = "pitch_intelligence";
const collectionName = "match_metadata";

// Sélection de la base de données
db = db.getSiblingDB(dbName);

// 1. Index unique sur l'ID du match
// Garantit l'unicité et accélère les recherches d'un match spécifique
db[collectionName].createIndex(
    { "match_id": 1 },
    { unique: true, name: "idx_unique_match_id" }
);
print("- Index unique créé sur 'match_id'");

// 2. Index composé sur les équipes
// Permet de filtrer rapidement les matchs impliquant deux équipes spécifiques
db[collectionName].createIndex(
    { "home_team": 1, "away_team": 1 },
    { name: "idx_teams" }
);
print("- Index composé créé sur 'home_team' et 'away_team'");

// 3. Index sur la compétition
db[collectionName].createIndex(
    { "competition": 1 },
    { name: "idx_competition" }
);
print("- Index créé sur 'competition'");

// 4. Index sur la date du match
// Utile pour trier chronologiquement les rencontres (-1 = descendant)
db[collectionName].createIndex(
    { "match_date": -1 },
    { name: "idx_match_date_desc" }
);
print("- Index descendant créé sur 'match_date'");

print("Création des index terminée avec succès.");
