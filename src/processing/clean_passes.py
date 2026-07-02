"""Nettoyage : events StatsBomb bruts -> passes réussies (passeur, receveur, équipe)."""
from typing import List, Dict


def extract_clean_passes(events: List[Dict], match_id: int) -> List[Dict]:
    """
    Garde uniquement les passes RÉUSSIES et en extrait les champs utiles
    pour construire le réseau (passeur -> receveur, avec équipe et match).

   une passe réussie n'a PAS de 'pass.outcome'.
    Si 'outcome' est renseigné (Incomplete, Out, Injury Clearance...), la passe a échoué.
    """
    rows = []
    for e in events:
        # 1) On ne garde que les events de type "Pass"
        if e.get("type", {}).get("name") != "Pass":
            continue

        p = e.get("pass", {})

        # 2) Filtre passes réussies : pas de clé 'outcome'
        if p.get("outcome"):
            continue

        # 3) Extraction des champs utiles
        passeur = (e.get("player") or {}).get("name")
        receveur = (p.get("recipient") or {}).get("name")
        equipe = (e.get("team") or {}).get("name")

        # 4) Robustesse : on jette les lignes incomplètes (rares mais possibles)
        if not (passeur and receveur and equipe):
            continue

        rows.append({
            "passeur": passeur,
            "receveur": receveur,
            "equipe": equipe,
            "match_id": match_id,
        })
    return rows