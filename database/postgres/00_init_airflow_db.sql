-- Crée une base dédiée aux métadonnées Airflow, séparée de "gold" (indicateurs métier).
-- Exécuté automatiquement au premier init du volume Postgres, une seule fois.
CREATE DATABASE airflow;