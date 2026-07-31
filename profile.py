# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

"""Accès au catalogue canonique — le service le LIT, il ne le détient plus.

⚠ CHANGEMENT DU 2026-07-31. Le catalogue (« moule IPG ») vivait ici, en dur, jusqu'à
`emberplus` 0.16.0. Il est parti dans `plugins/ipg_generique/profile.py`, avec le registre
d'affectation. La raison est celle du §12.11 pour le slot : un catalogue qui habite chez un
PROTOCOLE oblige tout nouveau protocole de contrôle à venir le chercher dans les réglages de
celui-là — le SW-P-08 (§15) a rendu le sujet concret le jour même où il est né.

**Il n'y a donc plus AUCUNE copie du catalogue ici, et c'est délibéré.** Un repli en dur
serait une seconde vérité, et ce projet s'est fait mordre deux fois par exactement ça : le
profil v1 figé dans un réglage qui masquait la v2 du code pendant des semaines (§1.1), et le
`slot` déclaré par un convertisseur qui rallumait des slots vidés (§12.11). Une copie de
secours qui diverge est plus dangereuse qu'une absence, parce qu'elle marche.

Ce qui reste ici : la LECTURE (avec cache et dernier connu), et les deux fonctions PURES de
dérivation — `build_index` et `keys` — qui ne portent aucune valeur métier et n'ont donc rien
à dupliquer.

Comportement quand la couche IPG ne répond pas :

  * on sert le DERNIER catalogue connu, même périmé. Un catalogue d'il y a une minute vaut
    mieux qu'un arbre qui change de forme sous le contrôleur — c'est la règle du cache SW-P-08
    (§15.5), pour la même raison ;
  * à froid (jamais lu depuis le démarrage), on rend un profil VIDE, et le contributeur « ipg »
    le dit en toutes lettres dans l'état du service. Sans couche IPG il n'y a de toute façon
    aucun slot affecté, donc aucune voie occupée : un moule vide est alors la vérité, pas une
    dégradation. Ça doit se voir tout de suite plutôt que de ressembler à un parc éteint.
"""
import logging
import time

from app import plugins as _plugins
from app import tools

log = logging.getLogger(__name__)

TTL_S = 30.0                  # le catalogue ne bouge qu'à la main : inutile de le relire à 5 s
CALL_TIMEOUT_S = 6            # la couche IPG est in-process, mais elle interroge le parc

_EMPTY = {"version": 0, "label": "IPG", "blocks": []}

_cache = None                 # dernier catalogue connu (jamais réinitialisé : c'est le filet)
_cache_at = 0.0
_origin = "jamais lu"
_WARNED = False               # l'alerte d'indisponibilité n'est dite qu'une fois


def profile_types():
    """Types déclarant `ipg_profile: true` — la ou les couches IPG installées.

    Lu dans le REGISTRE DES OUTILS, jamais déduit des devices collectés : c'est la règle que
    le §12.11 a tirée d'un incident réel côté affectation. Un parc entièrement dépeuplé ne
    publie aucun device ; en déduire le contributeur ferait disparaître le catalogue au pire
    moment, celui où l'on cherche justement à reposer un matériel."""
    out = [m.get("type") for m in _plugins.all()
           if m.get("ipg_profile") and not _plugins.is_disabled(m.get("type"))]
    return sorted(t for t in out if t)


def get_profile(force=False):
    """Le catalogue courant. Ne lève jamais."""
    global _cache, _cache_at, _origin, _WARNED
    now = time.monotonic()
    if not force and _cache is not None and (now - _cache_at) < TTL_S:
        return _cache
    for type_ in profile_types():
        try:
            status, data = tools.call(type_, "ipg/profile", "GET",
                                      actor="Service Ember+", timeout=CALL_TIMEOUT_S)
        except Exception as e:
            log.warning("emberplus: %s ipg/profile a levé : %s", type_, e)
            continue
        if status != 200 or not isinstance(data, dict):
            continue
        prof = data.get("profile")
        if isinstance(prof, dict) and prof.get("blocks"):
            _cache, _cache_at = prof, now
            _origin = "%s (%s)" % (type_, data.get("origin") or "défaut")
            _WARNED = False
            return prof
    # Personne n'a répondu. On garde le dernier connu SANS toucher à `_cache_at` : la relecture
    # sera retentée au prochain tour plutôt que dans trente secondes.
    if not _WARNED:
        _WARNED = True
        log.warning("emberplus: aucune couche IPG ne sert le catalogue (`ipg_profile: true`) → "
                    "%s. Installer ou réactiver l'outil « IPG Générique ».",
                    "dernier catalogue connu conservé" if _cache is not None
                    else "moule VIDE tant qu'il n'aura pas répondu")
    return _cache if _cache is not None else _EMPTY


def origin():
    """D'où vient le catalogue servi — pour l'afficher dans l'état du service."""
    return _origin if _cache is not None else "indisponible"


def build_index(prof=None):
    """Indexe le profil : { "<bloc>.<param>" : {block_id, block_label, param_id, param_label,
    type, enum, writable, min, max} }. Fonction PURE de dérivation : elle ne connaît aucune
    clé, elle ne fait que retourner le catalogue qu'on lui donne. `min`/`max` valent `None`
    quand la borne est absente (à distinguer d'une borne valant 0)."""
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
                "min": p.get("min"),
                "max": p.get("max"),
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
