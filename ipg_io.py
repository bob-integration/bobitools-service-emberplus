# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

"""Matrices de flux IPG, SDP et affectation des slots — cf. EMBERPLUS-IPG.md §12.

Tout est bâti par le SERVICE (surtout pas par les plugins : la numérotation EST le contrat
VSM, un plugin qui choisirait ses numéros ruinerait l'interchangeabilité) :

  1010         grille d'**affectation** des IPG sur les slots
  1011 / 1012  grilles **vidéo**        entrées de voies / sorties
  1013 / 1014  grilles **audio**        les DEUX audio dans la même grille (cf. §12.10)
  1015 / 1016  grilles **ancillaire**
  1100         RÉSERVÉ — l'arbre des SDP a été retiré le 2026-08-12, les SDP vivent
               désormais dans la voie qui les porte (§17). Ne jamais réattribuer.

Les paires sont contiguës et se lisent en bloc ; `1002..1009` et `1017..1099` sont RÉSERVÉS, de
sorte qu'une essence de plus — ou un futur arbre de paramètres — s'insère sans renuméroter. Ce plan a été arrêté le
2026-07-29, alors qu'aucun VSM n'était encore câblé — après, il ne se reprend plus.

Le plan de numérotation (§12.2) tient en une phrase : **un bloc de 100 par slot**, les
centaines donnent le slot, les unités le signal dans la machine. Les sources et les
destinations d'une matrice sont deux espaces INDÉPENDANTS — le même 101 vaut « SDI in 1 » en
source et « entrée voie 1 » en destination, ce n'est pas une collision.

Les deux audio partagent une grille pour rester **intervertibles** : un point de croisement
n'existe qu'à l'intérieur d'une matrice. D'où l'offset `+10000` sur l'audio 2, seul palier qui
ne recouvre pas les numéros de base (`101..9999`, 99 slots) — `+1000` ferait entrer en collision
l'audio 2 du slot 1 et la vidéo du slot 11.

Contrat plugin : `GET ember/io` (§12.4, étendu aux essences en §12.10). Un outil qui ne répond
pas 200 est ignoré en silence.
"""
import logging
import re

from app import settings, tools
from app import plugins as _plugins
from . import emberplus_glow as glow

log = logging.getLogger(__name__)

EMBER_ACTOR = "Service Ember+"
CALL_TIMEOUT_S = 4            # LECTURES (`ember/io`) : rapides, un contributeur lent est sauté
# ÉCRITURES (`ember/connect`) : un crosspoint touche plusieurs objets du matériel — 4 programmes
# pour une section de SNP, 8 canaux pour une essence audio de Neuron — et dépasse largement le
# délai de lecture. Mesuré le 2026-07-29 : le premier crosspoint réel a été perdu sur un
# `Read timed out (4 s)`, ce qui est le pire cas — le service croit avoir échoué alors que le
# matériel a peut-être appliqué. Un délai d'écriture généreux vaut mieux qu'un doute.
WRITE_TIMEOUT_S = 30

# ─── Racines (chemins VSM). NE JAMAIS renuméroter. ──────────────────────────
GRID_SLOT_ROOT_ID = 1010      # grille d'affectation IPG ↔ slot
GRID_IN_ROOT_ID = 1011        # grille vidéo, entrées de voies
GRID_OUT_ROOT_ID = 1012       # grille vidéo, sorties
GRID_AUDIO_IN_ROOT_ID = 1013
GRID_AUDIO_OUT_ROOT_ID = 1014
GRID_ANC_IN_ROOT_ID = 1015
GRID_ANC_OUT_ROOT_ID = 1016
# 1017..1099 RÉSERVÉS : les paires d'essence à venir ET les futurs arbres de paramètres
# s'y insèrent sans rien renuméroter. Large à dessein : renuméroter est fatal (§12.2).
SDP_ROOT_ID = 1100            # ancien arbre des SDP — RETIRÉ, racine conservée réservée
MATRIX_ID = 1                 # sous chaque racine de grille : 1 = la matrice, 2 = « Infos »
INFOS_ID = 2

# ─── Plan de numérotation des signaux (§12.2 et §12.10) ────────────────────
SLOT_BLOCK = 100              # un bloc de 100 numéros par slot
SIG_SDI_MAX = 50              # 100×slot + 1..50   = SDI / BNC / voies
SIG_IP_OFFSET = 50            # 100×slot + 51..99  = Rx IP / Tx 2110
SIG_IP_MAX = 49
LANE_MAX = 50                 # voies réservées par slot (le PLAN ; l'émission est bornée à part)
SLOT_MAX = 99
PSEUDO_NONE = 1               # bloc 0 (1..99) : les pseudo-signaux, sans slot
PSEUDO_PATTERN = 2
DEV_SRC_OFFSET = 10           # grille d'affectation : le device n est la source 10 + n
AUDIO2_OFFSET = 10000         # §12.10 : l'audio 2 vit au-dessus de tous les numéros de base

# ─── Essences ──────────────────────────────────────────────────────────────
ESSENCE_VIDEO = "video"
ESSENCES = (ESSENCE_VIDEO, "audio1", "audio2", "anc")
ESSENCE_ID = {ESSENCE_VIDEO: 1, "audio1": 2, "audio2": 3, "anc": 4}   # ordre GELÉ : il numérote
                                                                     # les feuilles SDP de la voie
ESSENCES_AUDIO = ("audio1", "audio2")   # les seules à porter un résumé de format (cf. §21)
ESSENCE_LABEL = {ESSENCE_VIDEO: "Vidéo", "audio1": "Audio 1", "audio2": "Audio 2", "anc": "ANC"}
ESSENCE_TAG = {ESSENCE_VIDEO: "", "audio1": " A1", "audio2": " A2", "anc": " ANC"}

# ─── Bornes d'ÉMISSION (§12.9.4) — le plan réserve plus large que ce qu'on émet ──
SLOTS_COUNT_DEFAULT = 16
LANES_PER_SLOT_DEFAULT = 32

# ─── Sens d'un signal IP ────────────────────────────────────────────
# (Les ids de l'ancien arbre SDP sont partis avec lui : les SDP sont dans la voie, §17.)
SDP_DIR_RX = 1
SDP_DIR_TX = 2

# ─── Les six grilles de flux ───────────────────────────────────────────────
# `essences` = les couples (essence, offset) que la grille porte. Une grille à plusieurs
# essences les rend intervertibles ; c'est tout l'intérêt de la grille audio.
GRID_SPECS = (
    {"canon": "video_in", "root": GRID_IN_ROOT_ID, "dir": "in", "label": "Entrées de voies",
     "essences": ((ESSENCE_VIDEO, 0),), "pattern": True},
    {"canon": "video_out", "root": GRID_OUT_ROOT_ID, "dir": "out", "label": "Sorties",
     "essences": ((ESSENCE_VIDEO, 0),), "pattern": False},
    {"canon": "audio_in", "root": GRID_AUDIO_IN_ROOT_ID, "dir": "in", "label": "Entrées audio",
     "essences": (("audio1", 0), ("audio2", AUDIO2_OFFSET)), "pattern": False},
    {"canon": "audio_out", "root": GRID_AUDIO_OUT_ROOT_ID, "dir": "out", "label": "Sorties audio",
     "essences": (("audio1", 0), ("audio2", AUDIO2_OFFSET)), "pattern": False},
    {"canon": "anc_in", "root": GRID_ANC_IN_ROOT_ID, "dir": "in", "label": "Entrées ANC",
     "essences": (("anc", 0),), "pattern": False},
    {"canon": "anc_out", "root": GRID_ANC_OUT_ROOT_ID, "dir": "out", "label": "Sorties ANC",
     "essences": (("anc", 0),), "pattern": False},
)
_SPEC_BY_CANON = {s["canon"]: s for s in GRID_SPECS}


# ═════════════════════════════════════════════════════════════════════
# Réglages et registres persistés
# ═════════════════════════════════════════════════════════════════════

def _int_setting(key, default, lo, hi):
    try:
        v = int(settings.get(key) or default)
    except (TypeError, ValueError):
        v = default
    return max(lo, min(hi, v))

def num_slots():
    """Nombre de slots ÉMIS dans les grilles (réglage `emberplus_slots_count`)."""
    return _int_setting("emberplus_slots_count", SLOTS_COUNT_DEFAULT, 1, SLOT_MAX)

def lanes_per_slot():
    """Nombre de voies ÉMISES par slot (réglage `emberplus_lanes_per_slot`)."""
    return _int_setting("emberplus_lanes_per_slot", LANES_PER_SLOT_DEFAULT, 1, LANE_MAX)


def io_types():
    """Types déclarant `ember_io: true` : seuls ceux-là sont interrogés sur GET ember/io —
    évite un aller-retour 404 vers les contributeurs qui n'ont que ember/tree."""
    out = [m.get("type") for m in _plugins.all()
           if m.get("ember_io") and not _plugins.is_disabled(m.get("type"))]
    return sorted(t for t in out if t)


def dev_key(type_, device):
    """Clé d'un matériel : le type d'OUTIL CONTRIBUTEUR + l'identifiant qu'il publie. Depuis le
    §12.11 le contributeur est la couche IPG, qui publie déjà une identité globale — la clé vaut
    donc `ipg_generique:<famille>:<id matériel>`. Elle reste ce qu'elle a toujours été : stable,
    opaque, et seule à désigner un matériel dans tout le plan de numérotation."""
    return "%s:%s" % (type_, device)


# NOTE (§12.11) — Il n'y a PLUS de registre d'affectation ici. Le slot et le numéro collant d'un
# matériel appartiennent à la couche IPG (l'outil « IPG Générique »), qui les tient dans son
# propre store et les publie dans `ember/io`. Ce service ne fait que les lire.
#
# Ce n'est pas un déplacement cosmétique : le slot commande le plan de numérotation, donc il
# relève de la logique IPG, pas du protocole. Le laisser ici obligeait chaque futur protocole à
# venir chercher son affectation dans les réglages d'Ember+ — ou à s'en inventer une autre.


# ═════════════════════════════════════════════════════════════════════
# Numérotation des signaux (§12.2)
# ═════════════════════════════════════════════════════════════════════

def lane_signal(slot, lane):
    """Numéro AFFICHÉ (VSM) d'une voie. Identique dans toutes les grilles : la voie garde son
    numéro en destination d'une grille d'entrées (son entrée) comme en source d'une grille de
    sorties, ce qui rend la liaison « sortie voie N ↔ entrée voie N » à déclarer côté VSM
    triviale."""
    return SLOT_BLOCK * slot + lane

def phys_signal(slot, kind, index):
    """Numéro AFFICHÉ d'un connecteur. `kind` : "sdi" (1..50) ou "ip" (1..49)."""
    return SLOT_BLOCK * slot + (index if kind == "sdi" else SIG_IP_OFFSET + index)

def split_signal(num):
    """(slot, reste) d'un numéro affiché. Le reste vaut 0 si le numéro n'a pas de slot."""
    return num // SLOT_BLOCK, num % SLOT_BLOCK

def phys_kind(rest):
    """(kind, index) depuis le reste d'un numéro de connecteur, ou (None, 0) si hors plan."""
    if 1 <= rest <= SIG_SDI_MAX:
        return "sdi", rest
    if SIG_IP_OFFSET < rest <= SIG_IP_OFFSET + SIG_IP_MAX:
        return "ip", rest - SIG_IP_OFFSET
    return None, 0


def _e(n):
    """Numéro ÉMIS = numéro affiché − 1. Ember+ numérote les signaux à partir de 0 et VSM
    réaffiche +1 ; même convention que switch_ports, même piège (§12.2)."""
    return int(n) - 1

def _d(n):
    """Réciproque : numéro affiché depuis le numéro reçu de VSM."""
    return int(n) + 1


def _ess_offset(spec, essence):
    """Offset de numérotation d'une essence DANS une grille, ou None si elle n'y figure pas."""
    for name, off in spec["essences"]:
        if name == essence:
            return off
    return None


def _split_essence(spec, num):
    """(essence, numéro de base) d'un numéro reçu dans `spec`. On essaie les offsets du plus
    grand au plus petit : un numéro de base vaut au moins `SLOT_BLOCK + 1`, donc l'offset le
    plus élevé qui laisse un reste plausible est le bon."""
    for name, off in sorted(spec["essences"], key=lambda x: -x[1]):
        if num - off > SLOT_BLOCK:
            return name, num - off
    return None, num


# ═════════════════════════════════════════════════════════════════════
# Collecte : GET ember/io
# ═════════════════════════════════════════════════════════════════════

def _essence_block(d):
    return {"sdp": d.get("sdp"), "present": d.get("present"),
            "enabled": d.get("enabled"), "ref": d.get("ref")}


def _norm_essences(d, builder):
    """{essence: bloc} d'un objet du contrat. Les clés de PREMIER NIVEAU sont la vidéo (§12.10)
    — un plugin qui ignore les essences reste donc valide, il ne décrit que de la vidéo."""
    out = {ESSENCE_VIDEO: builder(d)}
    for name, blk in (d.get("essences") or {}).items():
        if name in ESSENCES and name != ESSENCE_VIDEO and isinstance(blk, dict):
            out[name] = builder(blk)
    return out


def _norm_signals(raw, where):
    """Normalise une liste inputs/outputs. Écarte (avec log) ce qui sort du plan : un index
    hors bornes n'a pas de numéro possible, l'accepter produirait un signal fantôme."""
    out = []
    for s in raw or []:
        if not isinstance(s, dict):
            continue
        kind = str(s.get("kind") or "").lower()
        if kind not in ("sdi", "ip"):
            continue
        try:
            index = int(s.get("index"))
        except (TypeError, ValueError):
            continue
        limit = SIG_SDI_MAX if kind == "sdi" else SIG_IP_MAX
        if not (1 <= index <= limit):
            log.info("emberplus/io: %s %s %s hors plan (1..%d), ignoré",
                     where, kind, index, limit)
            continue
        out.append({"kind": kind, "index": index, "label": str(s.get("label") or ""),
                    "essences": _norm_essences(s, _essence_block)})
    return out


def _in_block(d):
    return {"src": d.get("src") if isinstance(d.get("src"), dict) else None,
            "fixed": bool(d.get("fixed")),
            "allowed": d.get("allowed") if isinstance(d.get("allowed"), list) else None}


def _out_block(d):
    return {"dst": [x for x in (d.get("dst") or []) if isinstance(x, dict)],
            "fixed": bool(d.get("fixed"))}


def _norm_lanes(raw):
    """Normalise `lanes` en {voie locale: {"in": {essence: bloc}, "out": {essence: bloc}}}."""
    out = {}
    for l in raw or []:
        if not isinstance(l, dict):
            continue
        try:
            lane = int(l.get("lane"))
        except (TypeError, ValueError):
            continue
        if not (1 <= lane <= LANE_MAX):
            log.info("emberplus/io: voie locale %s hors plan (1..%d), ignorée", lane, LANE_MAX)
            continue
        din = l.get("in") if isinstance(l.get("in"), dict) else {}
        dout = l.get("out") if isinstance(l.get("out"), dict) else {}
        out[lane] = {"in": _norm_essences(din, _in_block),
                     "out": _norm_essences(dout, _out_block),
                     # Désignation CONSTRUCTEUR de la voie, facultative : « A1 » sur un SNP
                     # (processeur + position), « A1 »…« H4 » sur un Neuron (path). Elle vient
                     # du matériel, qui est seul à la connaître — un service qui la
                     # recalculerait devrait connaître chaque famille.
                     "name": str(l.get("name") or "") or None}
    return out


def _norm_device(type_, dev):
    """(clé, entrée) d'un device remonté par un plugin, ou (None, None) s'il est inexploitable.
    L'identifiant `device` est OBLIGATOIRE : sans lui le service ne peut ni le numéroter, ni
    savoir qui il déplace d'un slot à l'autre.

    Un `slot` que remonterait encore un plugin est IGNORÉ : depuis le §12.11 l'affectation
    n'appartient qu'au registre, et un vœu du matériel serait un second verrou d'exposition —
    exactement ce qu'on vient de supprimer."""
    if not isinstance(dev, dict):
        return None, None
    device = dev.get("device")
    if device in (None, ""):
        log.info("emberplus/io: %s remonte un device sans identifiant `device` — ignoré", type_)
        return None, None
    def _int(v):
        try:
            return int(v)
        except (TypeError, ValueError):
            return None
    return dev_key(type_, device), {
        "type": type_, "device": str(device), "label": str(dev.get("label") or device),
        "ref": dev.get("ref"), "slot": _int(dev.get("slot")),
        "number": _int(dev.get("number")),
        # Nom court saisi dans la couche IPG : les protocoles de contrôleur plafonnent les
        # libellés (4, 8 ou 12 caractères en SW-P-08), donc le nom long ne passe pas.
        "short": str(dev.get("short") or "") or None,
        "inputs": _norm_signals(dev.get("inputs"), "%s entrée" % type_),
        "outputs": _norm_signals(dev.get("outputs"), "%s sortie" % type_),
        "lanes": _norm_lanes(dev.get("lanes")),
    }


def _resolve_slots(devices):
    """Indexe par slot ce que la couche IPG a DÉCIDÉ (§12.11).

    Le slot et le numéro collant arrivent tout faits dans `ember/io` : ce service ne les
    choisit pas, il les lit. Un device sans slot n'est pas exposé — c'est la couche IPG qui
    porte le sens de « exposé », en n'attribuant un slot que sur geste d'un opérateur.

    **Rien ne s'affecte tout seul.** Tant qu'un matériel se posait sur le premier slot libre,
    brancher un convertisseur suffisait à le publier, et le numéro qu'il recevait ne voulait
    rien dire — alors qu'il commande tout le plan de numérotation (§12.2)."""
    by_slot, slots, numbers = {}, {}, {}
    for key in sorted(devices):
        entry = devices[key]
        n = entry.get("number")
        if n is not None:
            numbers[key] = n
        s = entry.get("slot")
        if s is None:
            continue
        if not (1 <= s <= SLOT_MAX):
            log.info("emberplus/io: %s annoncé sur le slot %s, hors plan (1..%d) — ignoré",
                     key, s, SLOT_MAX)
            entry["slot"] = None
            continue
        if s in by_slot:
            # La couche IPG l'interdit ; deux contributeurs IPG concurrents, eux, pourraient
            # le produire. Premier arrivé dans l'ordre des clés, et on le dit.
            log.warning("emberplus/io: slot %d revendiqué par %s et %s — %s ignoré",
                        s, by_slot[s], key, key)
            entry["slot"] = None
            continue
        slots[key] = s
        by_slot[s] = key
    return {"devices": devices, "by_slot": by_slot, "slots": slots, "numbers": numbers}


def collect():
    """État complet des entrées/sorties du parc. Ne lève jamais : un contributeur muet ou
    lent est simplement absent de l'état, ses signaux reviendront au tour suivant."""
    devices = {}
    for type_ in io_types():
        try:
            status, data = tools.call(type_, "ember/io", "GET", actor=EMBER_ACTOR,
                                      timeout=CALL_TIMEOUT_S)
        except Exception as e:
            log.warning("emberplus/io: %s ember/io a levé : %s", type_, e)
            continue
        if status != 200 or not isinstance(data, dict):
            log.info("emberplus/io: %s ember/io → %s (ignoré)", type_, status)
            continue
        for dev in data.get("devices") or []:
            key, entry = _norm_device(type_, dev)
            if key:
                devices[key] = entry
    return _resolve_slots(devices)


def reload_type(state, type_):
    """Recharge le SEUL contributeur `type_` dans une copie de l'état — l'analogue de
    `_reload_matrix` du mode libre : après un crosspoint, on veut le tally tout de suite sans
    relire les contributeurs lents en I/O réseau. Renvoie None si le rechargement échoue."""
    try:
        status, data = tools.call(type_, "ember/io", "GET", actor=EMBER_ACTOR,
                                  timeout=CALL_TIMEOUT_S)
    except Exception as e:
        log.warning("emberplus/io: rechargement %s a levé : %s", type_, e)
        return None
    if status != 200 or not isinstance(data, dict):
        return None
    devices = {k: v for k, v in (state.get("devices") or {}).items()
               if v.get("type") != type_}
    for dev in data.get("devices") or []:
        key, entry = _norm_device(type_, dev)
        if key:
            devices[key] = entry
    return _resolve_slots(devices)


# ═════════════════════════════════════════════════════════════════════
# Construction des grilles
# ═════════════════════════════════════════════════════════════════════

def _connect(connections, seen, target, source, where):
    """Pose une connexion en garantissant UNE seule source par destination : la matrice est
    `oneToN`, deux connexions sur la même cible donneraient un tally ambigu au contrôleur. Un
    plugin qui se contredit (deux voies revendiquant la même sortie) est journalisé, pas suivi."""
    if target in seen:
        log.warning("emberplus/io: %s — destination %d revendiquée deux fois (source %d "
                    "ignorée, %d conservée)", where, target + 1, source + 1, seen[target] + 1)
        return
    seen[target] = source
    connections.append({"target": target, "sources": [source]})


def _essence_src(src, default_essence):
    """Essence portée par une extrémité de crosspoint, vidéo par défaut (§12.10)."""
    e = str((src or {}).get("essence") or default_essence)
    return e if e in ESSENCES else default_essence


def _source_number(spec, slot, src, essence):
    """Numéro affiché de la source d'entrée d'une voie, depuis l'état remonté par le plugin.
    L'essence de la SOURCE peut différer de celle de la destination (interversion audio)."""
    if not isinstance(src, dict):
        return None
    kind = str(src.get("kind") or "").lower()
    if kind == "none":
        return PSEUDO_NONE
    if kind == "pattern":
        return PSEUDO_PATTERN if spec["pattern"] else None
    if kind in ("sdi", "ip"):
        try:
            index = int(src.get("index"))
        except (TypeError, ValueError):
            return None
        limit = SIG_SDI_MAX if kind == "sdi" else SIG_IP_MAX
        if not (1 <= index <= limit):
            return None
        off = _ess_offset(spec, _essence_src(src, essence))
        if off is None:
            return None
        return off + phys_signal(slot, kind, index)
    return None


# ⚠ Les constructeurs de MATRICES Ember+ ont été retirés le 2026-07-31 — `build_grid`,
# `build_grid_slots`, `build_matrices`. Le routage des signaux et l'affectation des slots
# passent par SW-P-08 (cf. §15 de EMBERPLUS-IPG.md) : exposer les mêmes croisements en Ember+
# aurait entretenu deux vérités sur le même point, et c'est justement pour éviter de câbler
# mille paramètres un par un au contrôleur qu'on a pris un protocole de routeur.
#
# Ce qui RESTE, et qui n'a rien à voir : `GRID_SPECS`, `_SPEC_BY_CANON` et `apply_connect`.
# C'est la LOGIQUE d'application d'un croisement, appelée par le service SW-P-08 — les
# supprimer avec l'exposition aurait coupé le routage. Les racines 1010 à 1016 ne sont donc
# plus émises, mais restent RÉSERVÉES : les réattribuer casserait des chemins de contrôleur.

def lane_ip_essence(dev, lane, essence, direction):
    """Bloc d'essence du signal IP porté par une VOIE (`{sdp, present, enabled, ref}`), ou None.

    L'association voie ↔ signal IP n'a jamais eu besoin d'être publiée à part : elle est déjà
    dans le contrat du §12.4, où une voie nomme ses sources (`in.src`, `in.allowed`) et ses
    destinations (`out.dst`) par `{kind, index}`. On la LIT donc, plutôt que de demander aux
    plugins une clé de plus — les trois familles qui ont des signaux IP la publient déjà.

    Côté réception on regarde la source COURANTE puis les sources possibles : une voie dont
    l'entrée est commutée sur son BNC garde son récepteur IP, et son SDP reste ce qu'il faut
    écrire pour l'y abonner. Côté émission, le SNP comme le CDE épinglent leur Tx 2110 dans
    `out.dst` — il n'y a rien à choisir.

    ⚠ REPLI SUR LA VIDÉO (2026-08-17). Cette promesse ne tenait que pour la vidéo. Une essence
    dont la commutation N'EST PAS séparable de celle de la vidéo n'a rien à déclarer dans
    `allowed` — le SNP publie donc `allowed: []` sur audio 1/2 et ANC (`_lane_essences`), et
    leur `src` recopie celui de la vidéo. Dès qu'une voie était commutée sur son BNC, le seul
    candidat était SDI : plus aucun candidat IP, et les SDP audio/ANC de cette voie sortaient
    vides ET non inscriptibles — exactement l'abonnement qu'on voulait pouvoir armer d'avance.
    Mesuré le 2026-08-17 sur SNP 1 : 15 feuilles perdues sur 128 (voies 2 et 25-28).

    Le repli ne s'applique QUE si l'essence ne déclare aucun candidat IP : une famille qui
    porte réellement son audio sur un autre signal IP le dit dans son propre bloc, et garde
    donc la main. Il ne peut rien écraser — il ne remplit que ce qui était vide."""
    l = (dev.get("lanes") or {}).get(lane) if dev else None
    if not l:
        return None
    side = l.get("in" if direction == SDP_DIR_RX else "out") or {}
    blk = side.get(essence)
    if not blk:
        return None
    index = _ip_candidate(blk, direction)
    if index is None and essence != ESSENCE_VIDEO:
        index = _ip_candidate(side.get(ESSENCE_VIDEO) or {}, direction)
    if index is None:
        return None
    sigs = dev.get("inputs" if direction == SDP_DIR_RX else "outputs") or []
    sig = next((s for s in sigs if s.get("kind") == "ip" and s.get("index") == index), None)
    return (sig.get("essences") or {}).get(essence) if sig else None


def _ip_candidate(blk, direction):
    """Index ENTIER du premier signal IP nommé par une extrémité de voie, ou None.

    ⚠ La coercition en entier n'est pas cosmétique. `_norm_signals` normalise l'index d'un
    SIGNAL (`int(s.get("index"))`), alors que `src`/`allowed`/`dst` traversent `_in_block` /
    `_out_block` tels que le plugin les a écrits. Un contributeur qui publierait `"index": "3"`
    d'un côté et `3` de l'autre ne serait jamais raccordé : `"3" == 3` est faux, et le SDP de
    la voie sortirait vide sans le moindre message — le même silence que le repli ci-dessus."""
    cands = ([blk.get("src")] + list(blk.get("allowed") or [])
             if direction == SDP_DIR_RX else list(blk.get("dst") or []))
    for c in cands:
        if isinstance(c, dict) and c.get("kind") == "ip":
            try:
                return int(c.get("index"))
            except (TypeError, ValueError):
                continue
    return None


# Première section audio du SDP, puis sa ligne `a=rtpmap` (RFC 4566 :
# `<encodage>/<fréquence>[/<canaux>]`). Deux motifs plutôt qu'un seul : la borne `m=audio` est
# ce qui empêche de résumer la vidéo d'un SDP mixte comme si c'était de l'audio.
_M_AUDIO_RE = re.compile(r"^m=audio\b", re.I | re.M)
_RTPMAP_RE = re.compile(r"^a=rtpmap:\s*\d+\s+([^/\s]+)/(\d+)(?:/(\d+))?", re.I | re.M)
_PCM_RE = re.compile(r"^L(\d+)$", re.I)


def audio_sdp_summary(sdp):
    """« 48 kHz / 24 bits / 8 ch » depuis le SDP d'un signal audio, ou `""` (§21).

    Demandé par l'exploitant le 2026-08-19 : au contrôleur, le nombre de canaux d'un flux audio
    ne se lit NULLE PART — il faut ouvrir le SDP, qui pèse un à deux kilo-octets. L'information
    y est pourtant déjà, dans la ligne `a=rtpmap`, et c'est la seule source COMMUNE aux
    familles : le SNP la tient de NMOS, le Newt et le Neuron la lisent sur le matériel. On la
    résume donc ici plutôt que de demander une clé de plus au contrat du §12.4 — un paramètre
    dérivé ne coûte rien à personne, une extension de contrat coûte à toutes les familles.

    ⚠ On ne lit QUE la première section `m=audio`. Un flux redondant ST 2022-7 en porte deux,
    identiques par construction (groupe DUP) : les concaténer afficherait « 8 ch / 8 ch ».

    ⚠ L'encodage n'est pas toujours une largeur d'échantillon. `L16`/`L24` sont du PCM
    (ST 2110-30), `AM824` est le transport non-PCM du ST 2110-31. On rend alors le mot du SDP
    tel quel plutôt qu'un nombre de bits inventé — même règle que `status.input_valid` au
    catalogue : on relaie ce que le flux déclare, on ne le traduit pas au mieux.

    ⚠ Le CDE 1922 n'est PAS servi par cette fonction, et c'est SU. Son SDP est FABRIQUÉ par
    notre propre plugin (`cde1922/backend.py:_sdp_text`) à partir d'un REST qui ne décrit pas
    l'essence : il ne porte aucune ligne `a=rtpmap`. Ses voies rendront donc une chaîne vide —
    un blanc qui se voit, plutôt qu'un « 48 kHz / 24 bits / 2 ch » plausible et faux."""
    texte = str(sdp or "")
    debut = _M_AUDIO_RE.search(texte)
    if not debut:
        return ""
    m = _RTPMAP_RE.search(texte, debut.end())
    if not m:
        return ""
    encodage, freq, canaux = m.group(1), m.group(2), m.group(3)
    bouts = []
    try:
        # `%g` rend « 48 » et non « 48.0 », et garde « 44.1 » quand la fréquence l'exige.
        bouts.append("%g kHz" % (int(freq) / 1000.0))
    except (TypeError, ValueError):
        pass
    pcm = _PCM_RE.match(encodage)
    bouts.append("%s bits" % pcm.group(1) if pcm else encodage)
    # Canaux ABSENTS ≠ un canal. La RFC 4566 dit bien « 1 par défaut », mais un SDP 2110-30 qui
    # tait son compte est assez anormal pour qu'on ne l'affirme pas à la place du matériel.
    if canaux:
        bouts.append("%s ch" % canaux)
    return " / ".join(bouts)


# ═════════════════════════════════════════════════════════════════════
# Application d'un crosspoint
# ═════════════════════════════════════════════════════════════════════

def _dev_on_slot(state, slot):
    key = (state.get("by_slot") or {}).get(slot)
    return state.get("devices", {}).get(key) if key else None


def _call_connect(dev, payload):
    """Appelle le plugin propriétaire. TOUT refus est un `error` côté plugin (§12.3) : on ne
    « fait au mieux » jamais, le service rediffusera l'état vrai et le tally claquera en
    arrière dans VSM."""
    payload = dict(payload)
    payload["ref"] = dev.get("ref")
    try:
        status, data = tools.call(dev["type"], "ember/connect", "POST", payload,
                                  actor=EMBER_ACTOR, timeout=WRITE_TIMEOUT_S)
    except Exception as e:
        log.warning("emberplus/io: connect %s a levé : %s", dev["type"], e)
        return False
    ok = status == 200 and isinstance(data, dict) and not data.get("error")
    if not ok:
        log.info("emberplus/io: connect %s refusé → %s %s", dev["type"], status, data)
    return ok


def apply_connect(canon, target, sources, operation, state):
    """Route un crosspoint d'une grille canonique. `target`/`sources` sont les numéros ÉMIS
    reçus de VSM. Renvoie (ok, type d'outil touché) — le type sert au rechargement ciblé."""
    tgt = _d(target)
    src = _d(sources[0]) if sources else PSEUDO_NONE
    if operation == "disconnect":
        src = PSEUDO_NONE
    if canon == "slot":
        return _apply_slot(tgt, src, state), None
    spec = _SPEC_BY_CANON.get(canon)
    if not spec:
        return False, None

    # L'essence est portée par CHAQUE extrémité (§12.10) : dans la grille audio, croiser
    # l'audio 2 d'une source vers l'entrée audio 1 d'une voie est le geste recherché.
    tess, tnum = _split_essence(spec, tgt)
    if tess is None:
        log.info("emberplus/io: destination %s hors plan de la grille %s", tgt, canon)
        return False, None
    slot, rest = split_signal(tnum)
    if not (1 <= slot <= SLOT_MAX):
        log.info("emberplus/io: destination %s hors plan", tgt)
        return False, None
    dev = _dev_on_slot(state, slot)
    if not dev:
        log.info("emberplus/io: aucun IPG sur le slot %d, crosspoint refusé", slot)
        return False, None

    if spec["dir"] == "in":
        lane = rest
        if not (1 <= lane <= lanes_per_slot()):
            log.info("emberplus/io: voie %s hors bornes d'émission", tgt)
            return False, None
        source = _local_source(spec, slot, src)
        if source is None:
            return False, None
        payload = {"grid": "in", "lane": lane, "source": source}
        if tess != ESSENCE_VIDEO:
            payload["essence"] = tess
        return _call_connect(dev, payload), dev["type"]

    kind, index = phys_kind(rest)
    if kind is None:
        log.info("emberplus/io: sortie %s hors plan", tgt)
        return False, None
    output = {"kind": kind, "index": index}
    if tess != ESSENCE_VIDEO:
        output["essence"] = tess
    if src == PSEUDO_NONE:
        source = {"kind": "none"}
    else:
        sess, snum = _split_essence(spec, src)
        sslot, srest = split_signal(snum)
        if sess is None or sslot != slot or not (1 <= srest <= lanes_per_slot()):
            log.info("emberplus/io: la sortie %s ne peut être alimentée que par une voie "
                     "du slot %d (demandé : %s)", tgt, slot, src)
            return False, None
        source = {"kind": "lane", "lane": srest}
        if sess != ESSENCE_VIDEO:
            source["essence"] = sess
    return _call_connect(dev, {"grid": "out", "output": output, "source": source}), dev["type"]


def _local_source(spec, slot, src):
    """Traduit un numéro de source d'une grille d'entrées en termes LOCAUX au plugin, ou None
    si la demande est irrecevable (source d'un autre slot, numéro hors plan)."""
    if src == PSEUDO_NONE:
        return {"kind": "none"}
    if src == PSEUDO_PATTERN:
        if not spec["pattern"]:
            log.info("emberplus/io: la mire n'est pas une source de la grille %s", spec["canon"])
            return None
        return {"kind": "pattern"}
    sess, snum = _split_essence(spec, src)
    if sess is None:
        log.info("emberplus/io: source %s hors plan de la grille %s", src, spec["canon"])
        return None
    sslot, srest = split_signal(snum)
    if sslot != slot:
        log.info("emberplus/io: source %s refusée — une voie du slot %d ne peut être alimentée "
                 "que par un connecteur de son propre slot", src, slot)
        return None
    kind, index = phys_kind(srest)
    if kind is None:
        log.info("emberplus/io: source %s hors plan", src)
        return None
    out = {"kind": kind, "index": index}
    if sess != ESSENCE_VIDEO:
        out["essence"] = sess
    return out


def _ipg_contributor(state):
    """Le type d'outil qui porte la couche IPG. On ne le code pas en dur : le service n'a pas à
    connaître le nom de l'outil qui l'alimente, seulement à savoir à qui renvoyer une
    affectation.

    ⚠ Il se lit dans le REGISTRE DES OUTILS, pas dans les devices collectés. La couche IPG ne
    publie que les matériels POSÉS sur un slot : les déduire d'eux ferait qu'un parc entièrement
    dépeuplé n'aurait plus de contributeur connu, donc plus aucun moyen de réaffecter quoi que
    ce soit. Vider le dernier slot serait irréversible depuis le contrôleur."""
    t = next(iter(io_types()), None)
    if t:
        return t
    for entry in (state.get("devices") or {}).values():
        if entry.get("type"):
            return entry["type"]
    return None


def _apply_slot(slot, src, state):
    """Affecte (ou libère) un slot depuis la GRILLE 1010 (le geste au contrôleur).

    Le service ne décide plus rien ici : il TRANSMET à la couche IPG, seule propriétaire du
    registre (§12.11). C'est ce qui garantit que l'écran de l'outil et le contrôleur appliquent
    exactement les mêmes règles — déplacement plutôt que duplication, un seul matériel par
    slot — au lieu de deux implémentations qui divergeraient au premier oubli."""
    if not (1 <= slot <= num_slots()):
        log.info("emberplus/io: slot %s hors bornes", slot)
        return False
    type_ = _ipg_contributor(state)
    if not type_:
        log.info("emberplus/io: aucune couche IPG joignable, affectation impossible")
        return False
    payload = {"slot": slot,
               "number": None if src == PSEUDO_NONE else src - DEV_SRC_OFFSET}
    try:
        status, data = tools.call(type_, "ipg/assign", "POST", payload,
                                  actor=EMBER_ACTOR, timeout=WRITE_TIMEOUT_S)
    except Exception as e:
        log.warning("emberplus/io: affectation renvoyée à %s a levé : %s", type_, e)
        return False
    if status != 200 or not isinstance(data, dict) or data.get("error"):
        log.info("emberplus/io: affectation refusée par %s → %s %s", type_, status, data)
        return False
    if not data.get("changed"):
        return False        # rien n'a bougé : inutile de renuméroter tout l'arbre
    log.info("emberplus/io: slot %d — %s", slot,
             "libéré" if payload["number"] is None else "matériel n° %d posé" % payload["number"])
    return True
