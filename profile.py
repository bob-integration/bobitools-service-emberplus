# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

"""Profil Ember+ canonique — « moule IPG » de Bobi.Tools.

Ce module détient la SOURCE UNIQUE DE VÉRITÉ de la numérotation Ember+ canonique.
Les outils (SNP, Neuron…) ne réinventent aucun `id` : ils déclarent, par device et
par paramètre, une CLÉ CANONIQUE `"<bloc>.<param>"` (ex. `"framesync.delay"`) et un
slot logique ; le service assemble l'arbre Glow depuis CE profil, avec des `id`
figés → deux devices d'un même slot produisent un arbre byte-identique côté VSM.

Structure d'un profil :
    { "version": int, "label": str, "lanes": int,   # voies logiques 1..N
      "blocks": [ { "key": str, "label": str, "id": int,
                    "params": [ { "key": str, "label": str, "id": int,
                                  "type": "string|int|real|bool|enum",
                                  "unit"?: str, "enum"?: [str],
                                  "writable"?: bool } ] } ] }
Le drapeau optionnel "writable" (défaut True si absent) déclare si le paramètre est
inscriptible côté VSM (SetValue autorisé) ; à False pour un statut en lecture seule
(verrouillage, présence de signal, PTP, voie affectée…).

Le contenu par défaut ci-dessous est PROVISOIRE (« NAP vidéo » minimal) : il valide
le mécanisme. Le catalogue définitif viendra de l'analyse croisée SNP/Neuron, et sera
éditable en réglages (setting `emberplus_profile`, JSON) sans toucher au code.
"""
import json
import logging

log = logging.getLogger(__name__)

# ─── Profil par défaut « NAP vidéo » ────────────────────────────────────
# Union des blocs curatés SNP + Neuron ; chaque device remplit ce qu'il expose (ou
# qu'on lui mappe). Le routage de source est HORS profil (géré par la matrice, phase 2).
# Numérotation FIGÉE : bloc.id et param.id ne doivent jamais bouger (chemins VSM stables).
#
# Enum canonique des formats de sortie : liste PROVISOIRE, à recaler sur un relevé device
# réel (`GET /neurons/{id}/object/{oid}` → options, et MetaData SNP). L'ordre = l'index vu
# par VSM ; chaque plugin fournit un enum_map (index canonique → valeur native).
_VIDEO_FORMATS = [
    "1080i50", "1080i5994", "1080i60",
    "1080p50", "1080p5994", "1080p60", "1080p25", "1080p2997", "1080p30", "1080p24", "1080p2398",
    "2160p50", "2160p5994", "2160p60", "2160p25", "2160p2997", "2160p30", "2160p24", "2160p2398",
    "720p50", "720p5994", "720p60",
]

DEFAULT_PROFILE = {
    "version": 1,
    "label": "NAP vidéo",
    "lanes": 32,                      # voies logiques 1..32 (Neuron A1..H4 ; SNP 4 proc × 8 prog HD)
    "blocks": [
        {"key": "channel", "label": "Voie", "id": 1, "params": [
            {"key": "enable", "label": "Voie active", "id": 1, "type": "bool"},
        ]},
        {"key": "colorcorr", "label": "Correcteur couleur", "id": 2, "params": [
            {"key": "gain_r", "label": "Gain R", "id": 1, "type": "real"},
            {"key": "gain_g", "label": "Gain V", "id": 2, "type": "real"},
            {"key": "gain_b", "label": "Gain B", "id": 3, "type": "real"},
            {"key": "black_r", "label": "Niveau noir R", "id": 4, "type": "real"},
            {"key": "black_g", "label": "Niveau noir V", "id": 5, "type": "real"},
            {"key": "black_b", "label": "Niveau noir B", "id": 6, "type": "real"},
        ]},
        {"key": "framesync", "label": "Synchroniseur", "id": 3, "params": [
            {"key": "delay", "label": "Retard image", "id": 1, "type": "int", "unit": "frames"},
            {"key": "h_phase", "label": "Phase H", "id": 2, "type": "int"},
            {"key": "v_phase", "label": "Phase V", "id": 3, "type": "int"},
        ]},
        {"key": "functions", "label": "Fonctions", "id": 4, "params": [
            {"key": "freeze", "label": "Gel image", "id": 1, "type": "bool"},
            {"key": "black", "label": "Forçage noir", "id": 2, "type": "bool"},
            {"key": "testpattern", "label": "Mire", "id": 3, "type": "bool"},
            {"key": "testpattern_sel", "label": "Motif de mire", "id": 4, "type": "string"},
        ]},
        {"key": "output", "label": "Sortie", "id": 5, "params": [
            {"key": "video_format", "label": "Format vidéo", "id": 1,
             "type": "enum", "enum": _VIDEO_FORMATS},
            {"key": "media_type", "label": "Type de média", "id": 2,
             "type": "enum", "enum": ["SDI", "ST 2110", "NMOS"]},   # provisoire (Neuron only)
        ]},
        {"key": "audio", "label": "Audio", "id": 6, "params": [
            {"key": "delay", "label": "Retard audio", "id": 1, "type": "int", "unit": "ms"},
        ]},
    ],
}


def get_profile():
    """Renvoie le profil courant : le JSON du réglage `emberplus_profile` s'il est
    valide, sinon le profil par défaut du code (fallback → une MAJ du DEFAULT_PROFILE
    se propage tant que l'opérateur n'a rien surchargé)."""
    from app import settings
    raw = settings.get("emberplus_profile")
    if raw:
        try:
            p = raw if isinstance(raw, dict) else json.loads(raw)
            if isinstance(p, dict) and isinstance(p.get("blocks"), list) and p["blocks"]:
                return p
        except Exception as e:
            log.warning("emberplus: profil réglage invalide (%s) → défaut", e)
    return DEFAULT_PROFILE


def build_index(prof=None):
    """Indexe le profil : { "<bloc>.<param>" : {block_id, block_label, param_id,
    param_label, type, enum, writable} }. Utilisé pour résoudre une clé canonique en `id`."""
    prof = prof or get_profile()
    idx = {}
    for block in prof.get("blocks") or []:
        bkey, bid = block.get("key"), block.get("id")
        blabel = block.get("label") or bkey
        if not bkey or bid is None:
            continue
        for p in block.get("params") or []:
            pkey, pid = p.get("key"), p.get("id")
            if not pkey or pid is None:
                continue
            idx["%s.%s" % (bkey, pkey)] = {
                "block_id": int(bid), "block_label": str(blabel),
                "param_id": int(pid), "param_label": str(p.get("label") or pkey),
                "type": str(p.get("type") or "string").lower(),
                "enum": [str(x) for x in (p.get("enum") or [])],
                "writable": bool(p.get("writable", True)),
            }
    return idx


def keys(prof=None):
    """Liste ordonnée des clés canoniques (pour peupler l'UI de mapping des plugins)."""
    prof = prof or get_profile()
    out = []
    for block in prof.get("blocks") or []:
        bkey = block.get("key")
        for p in block.get("params") or []:
            if bkey and p.get("key"):
                out.append({"key": "%s.%s" % (bkey, p["key"]),
                            "label": "%s › %s" % (block.get("label") or bkey,
                                                  p.get("label") or p["key"]),
                            "type": str(p.get("type") or "string").lower()})
    return out
