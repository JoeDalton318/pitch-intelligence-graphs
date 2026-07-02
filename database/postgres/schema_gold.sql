-- =========================================================================
-- Couche GOLD — Schéma en étoile pour Superset
-- =========================================================================
-- Modélisation Kimball : dimensions + tables de faits.
-- Alimentée depuis Neo4j (scores de centralité) et MongoDB (métadonnées match).
-- Consommée par Superset via des vues métier (voir views_superset.sql).
-- =========================================================================

-- Séparation propre : tout le gold vit dans son schéma dédié.
CREATE SCHEMA IF NOT EXISTS gold;

-- =========================================================================
-- DIMENSIONS
-- =========================================================================

-- Dimension Joueur : un joueur unique par ligne.
-- Clé métier = nom StatsBomb (unique), clé technique auto-générée pour les jointures.
CREATE TABLE IF NOT EXISTS gold.dim_joueur (
    joueur_id     SERIAL PRIMARY KEY,
    nom           TEXT UNIQUE NOT NULL,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension Équipe : une sélection ou un club unique par ligne.
CREATE TABLE IF NOT EXISTS gold.dim_equipe (
    equipe_id     SERIAL PRIMARY KEY,
    nom           TEXT UNIQUE NOT NULL,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension Match : enrichie par les métadonnées MongoDB (compétition, date...).
-- match_id vient directement de StatsBomb (identifiant naturel), pas de clé auto.
CREATE TABLE IF NOT EXISTS gold.dim_match (
    match_id      BIGINT PRIMARY KEY,
    competition   TEXT,
    saison        TEXT,
    date_match    DATE,
    equipe_dom    TEXT,
    equipe_ext    TEXT,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =========================================================================
-- FAITS
-- =========================================================================

-- Table de faits : score de centralité par joueur et par match.
-- Une ligne = un joueur a joué dans un match, avec ses scores calculés par GDS.
-- C'est LA table centrale du dashboard.
CREATE TABLE IF NOT EXISTS gold.fact_centralite (
    fact_centralite_id  SERIAL PRIMARY KEY,
    match_id            BIGINT NOT NULL REFERENCES gold.dim_match(match_id),
    joueur_id           INT NOT NULL REFERENCES gold.dim_joueur(joueur_id),
    equipe_id           INT NOT NULL REFERENCES gold.dim_equipe(equipe_id),
    pagerank            DOUBLE PRECISION,       -- score PageRank pondéré (poids = nb passes)
    betweenness         DOUBLE PRECISION,       -- score Betweenness Centrality
    computed_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Un joueur ne peut apparaître qu'une seule fois par match
    UNIQUE (match_id, joueur_id)
);

-- Table de faits agrégée : style de jeu par équipe et par match.
-- Calculée à partir de fact_centralite (via une vue ou un job d'agrégation).
-- Utilisée pour le radar de comparaison "jeu centralisé vs distribué" en soutenance.
CREATE TABLE IF NOT EXISTS gold.fact_style (
    match_id                BIGINT NOT NULL REFERENCES gold.dim_match(match_id),
    equipe_id               INT NOT NULL REFERENCES gold.dim_equipe(equipe_id),
    indice_centralisation   DOUBLE PRECISION,   -- écart-type des PageRank : élevé = jeu centralisé
    pagerank_max            DOUBLE PRECISION,   -- score du joueur pivot (le "cerveau")
    pagerank_moyen          DOUBLE PRECISION,   -- score moyen de l'équipe
    nb_joueurs              INT,                -- nombre de joueurs impliqués
    computed_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (match_id, equipe_id)
);

-- =========================================================================
-- INDEX pour accélérer les jointures Superset (analyses par équipe/match)
-- =========================================================================
CREATE INDEX IF NOT EXISTS idx_fact_centralite_match  ON gold.fact_centralite(match_id);
CREATE INDEX IF NOT EXISTS idx_fact_centralite_equipe ON gold.fact_centralite(equipe_id);
CREATE INDEX IF NOT EXISTS idx_fact_style_match       ON gold.fact_style(match_id);