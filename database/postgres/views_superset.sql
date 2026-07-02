-- =========================================================================
-- Vues métier pour Superset
-- =========================================================================
-- Chaque vue = un chart du dashboard. On expose des noms parlants et
-- toutes les jointures sont préfaites : Superset n'a qu'à SELECT * FROM.
-- =========================================================================

-- -------------------------------------------------------------------------
-- VUE 1 — Centralités par joueur et par match
-- Utilisée par : bar chart "PageRank par joueur", tableau détaillé.
-- Contient : nom du joueur, équipe, match, ses 2 scores.
-- -------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.v_centralites_joueurs AS
SELECT
    fc.match_id,
    dm.competition,
    dm.date_match,
    de.nom            AS equipe,
    dj.nom            AS joueur,
    fc.pagerank,
    fc.betweenness
FROM       gold.fact_centralite fc
INNER JOIN gold.dim_joueur      dj ON dj.joueur_id = fc.joueur_id
INNER JOIN gold.dim_equipe      de ON de.equipe_id = fc.equipe_id
INNER JOIN gold.dim_match       dm ON dm.match_id  = fc.match_id;


-- -------------------------------------------------------------------------
-- VUE 2 — Style de jeu par équipe et par match
-- Utilisée par : radar "Comparaison de styles (centralisé vs distribué)".
-- Contient : indice de centralisation, pagerank max/moyen, nb joueurs.
-- -------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.v_style_par_equipe AS
SELECT
    fs.match_id,
    dm.competition,
    dm.date_match,
    de.nom                          AS equipe,
    fs.indice_centralisation,
    fs.pagerank_max,
    fs.pagerank_moyen,
    fs.nb_joueurs,
    -- Interprétation métier prête à l'affichage
    CASE
        WHEN fs.indice_centralisation >= 0.05 THEN 'Centralisé'
        WHEN fs.indice_centralisation >= 0.02 THEN 'Équilibré'
        ELSE                                       'Distribué'
    END                             AS style_de_jeu
FROM       gold.fact_style   fs
INNER JOIN gold.dim_equipe   de ON de.equipe_id = fs.equipe_id
INNER JOIN gold.dim_match    dm ON dm.match_id  = fs.match_id;


-- -------------------------------------------------------------------------
-- VUE 3 — Top pivots (les "cerveaux" du jeu)
-- Utilisée par : tableau "Joueurs pivots par match".
-- Un pivot = joueur avec forte betweenness (celui par qui transite le jeu).
-- Classe les joueurs par match et retient le top.
-- -------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.v_top_pivots AS
SELECT
    fc.match_id,
    dm.competition,
    de.nom              AS equipe,
    dj.nom              AS joueur,
    fc.pagerank,
    fc.betweenness,
    -- Classement par équipe/match sur la betweenness (pivot = rang 1)
    RANK() OVER (
        PARTITION BY fc.match_id, fc.equipe_id
        ORDER BY     fc.betweenness DESC
    )                   AS rang_pivot
FROM       gold.fact_centralite fc
INNER JOIN gold.dim_joueur      dj ON dj.joueur_id = fc.joueur_id
INNER JOIN gold.dim_equipe      de ON de.equipe_id = fc.equipe_id
INNER JOIN gold.dim_match       dm ON dm.match_id  = fc.match_id;