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
  1100         arbre des **SDP**        le connection management, RW sur un récepteur

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
import json
import logging

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
SDP_ROOT_ID = 1100            # arbre des SDP
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
ESSENCE_ID = {ESSENCE_VIDEO: 1, "audio1": 2, "audio2": 3, "anc": 4}   # niveau de l'arbre des SDP
ESSENCE_LABEL = {ESSENCE_VIDEO: "Vidéo", "audio1": "Audio 1", "audio2": "Audio 2", "anc": "ANC"}
ESSENCE_TAG = {ESSENCE_VIDEO: "", "audio1": " A1", "audio2": " A2", "anc": " ANC"}

# ─── Bornes d'ÉMISSION (§12.9.4) — le plan réserve plus large que ce qu'on émet ──
SLOTS_COUNT_DEFAULT = 16
LANES_PER_SLOT_DEFAULT = 32

# ─── Arbre SDP ──────────────────────────────────────────────────────
SDP_DIR_RX = 1
SDP_DIR_TX = 2
SDP_PARAM_SDP = 1
SDP_PARAM_PRESENT = 2
SDP_PARAM_ENABLED = 3

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
    """Clé d'un matériel : le type d'outil + son identifiant STABLE côté plugin. C'est la clé
    des deux registres ci-dessous, donc de tout le plan de numérotation."""
    return "%s:%s" % (type_, device)


def _load_map(key):
    raw = settings.get(key)
    if isinstance(raw, str):
        try:
            raw = json.loads(raw or "{}")
        except ValueError:
            raw = {}
    return raw if isinstance(raw, dict) else {}


def slot_registry():
    """Affectation device → slot. **Le service est autoritaire** (§12.8) : le `slot` que
    remonte un plugin n'est plus qu'un défaut, consulté pour un matériel jamais affecté."""
    out = {}
    for k, v in _load_map("emberplus_slots").items():
        try:
            s = int(v)
        except (TypeError, ValueError):
            continue
        if 1 <= s <= SLOT_MAX:
            out[str(k)] = s
    return out

def save_slot_registry(reg):
    settings.set("emberplus_slots", {k: int(v) for k, v in reg.items()})


def device_registry():
    """Numérotation COLLANTE des devices pour la grille d'affectation : un numéro attribué ne bouge
    plus, et n'est pas réattribué après suppression du matériel."""
    out = {}
    for k, v in _load_map("emberplus_devices").items():
        try:
            out[str(k)] = int(v)
        except (TypeError, ValueError):
            continue
    return out

def save_device_registry(reg):
    settings.set("emberplus_devices", {k: int(v) for k, v in reg.items()})


def _device_number(reg, key):
    """Numéro collant d'un device ; l'attribue s'il est neuf. Renvoie (n, modifié)."""
    if key in reg:
        return reg[key], False
    n = max(reg.values()) + 1 if reg else 1
    reg[key] = n
    return n, True


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
                     "out": _norm_essences(dout, _out_block)}
    return out


def _norm_device(type_, dev):
    """(clé, entrée) d'un device remonté par un plugin, ou (None, None) s'il est inexploitable.
    L'identifiant `device` est OBLIGATOIRE : sans lui le service ne peut ni le numéroter, ni
    savoir qui il déplace d'un slot à l'autre."""
    if not isinstance(dev, dict):
        return None, None
    device = dev.get("device")
    if device in (None, ""):
        log.info("emberplus/io: %s remonte un device sans identifiant `device` — ignoré", type_)
        return None, None
    slot = dev.get("slot")
    try:
        slot = int(slot) if slot is not None else None
    except (TypeError, ValueError):
        slot = None
    if slot is not None and not (1 <= slot <= SLOT_MAX):
        slot = None
    return dev_key(type_, device), {
        "type": type_, "device": str(device), "label": str(dev.get("label") or device),
        "want_slot": slot, "ref": dev.get("ref"),
        "inputs": _norm_signals(dev.get("inputs"), "%s entrée" % type_),
        "outputs": _norm_signals(dev.get("outputs"), "%s sortie" % type_),
        "lanes": _norm_lanes(dev.get("lanes")),
    }


def _resolve_slots(devices):
    """Pose chaque device sur son slot. Le REGISTRE prime ; le `slot` souhaité par le plugin
    ne sert qu'à un matériel jamais affecté, et seulement si la place est libre."""
    reg = slot_registry()
    dreg = device_registry()
    changed = dirty = False
    taken = {}
    # 1. Ce que le registre dit déjà, dans un ordre déterministe.
    for key in sorted(devices):
        s = reg.get(key)
        if s is not None and s not in taken:
            taken[s] = key
    # 2. Les nouveaux venus : vœu du plugin s'il est libre, sinon premier slot libre.
    for key in sorted(devices):
        if key in reg and reg[key] in taken and taken[reg[key]] == key:
            continue
        want = devices[key].get("want_slot")
        s = want if (want and want not in taken) else None
        if s is None:
            for cand in range(1, SLOT_MAX + 1):
                if cand not in taken:
                    s = cand
                    break
        if s is None:
            log.warning("emberplus/io: plus un seul slot libre, %s non affecté", key)
            continue
        reg[key] = s
        taken[s] = key
        changed = True
    # 3. Numérotation collante pour la grille d'affectation.
    for key in sorted(devices):
        _, added = _device_number(dreg, key)
        dirty = dirty or added
    if changed:
        try:
            save_slot_registry(reg)
        except Exception as e:
            log.warning("emberplus/io: sauvegarde des slots échouée : %s", e)
    if dirty:
        try:
            save_device_registry(dreg)
        except Exception as e:
            log.warning("emberplus/io: sauvegarde des numéros de device échouée : %s", e)
    by_slot = {}
    for key, entry in devices.items():
        s = reg.get(key)
        if s is None:
            continue
        entry["slot"] = s
        by_slot[s] = key
    return {"devices": devices, "by_slot": by_slot, "slots": reg, "numbers": dreg}


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
    `oneToN`, deux connexions sur la même cible donneraient un tally ambigu au pupitre. Un
    plugin qui se contredit (deux voies revendiquant la même sortie) est journalisé, pas suivi."""
    if target in seen:
        log.warning("emberplus/io: %s — destination %d revendiquée deux fois (source %d "
                    "ignorée, %d conservée)", where, target + 1, source + 1, seen[target] + 1)
        return
    seen[target] = source
    connections.append({"target": target, "sources": [source]})


def _infos_node(elements, root, legende):
    """Nœud « Infos » voisin de la matrice : la légende de numérotation, en LECTURE SEULE.
    La `description` d'une matrice sert de NOM DE GRILLE au pupitre (§9) — y entasser une
    légende la rend illisible, d'où ce nœud séparé. Et puisque les libellés de signaux ne
    partent PAS sur le fil (§12.9.1), cette légende est la seule documentation que VSM voit."""
    elements.append(([root, INFOS_ID], "node", "Infos", ""))
    elements.append(([root, INFOS_ID, 1], "param", "Légende", "", legende, glow.PT_STRING, False))


_LEGENDE_COMMUNE = ("Bloc de 100 par slot : les centaines donnent le slot IPG, les unités le "
                    "signal (1..50 SDI/BNC ou voies, 51..99 Rx/Tx IP). 1 = Désactivé. "
                    "Sources et destinations sont deux numérotations indépendantes.")
_LEGENDE_AUDIO = (" Audio 1 garde les numéros de base ; AUDIO 2 = numéro + 10000 (10101, "
                  "10153…). Les deux vivent dans la même grille pour rester intervertibles : "
                  "croiser l'audio 2 d'une source vers l'entrée audio 1 d'une voie est un "
                  "geste normal.")


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


def _build_grid_in(spec, elements, state):
    """Grille d'entrées de voies. Destinations = l'entrée de chaque voie ; sources = les
    connecteurs d'entrée + les pseudo-sources. GRILLE PLEINE : tous les numéros du plan sont
    présents, qu'un device soit posé ou non — VSM reste configurable d'avance et la forme de
    la matrice ne bouge jamais."""
    nslots, nlanes = num_slots(), lanes_per_slot()
    targets, sources, connections, seen = [], [], [], {}
    sources.append({"number": _e(PSEUDO_NONE), "label": "Désactivé"})
    if spec["pattern"]:
        sources.append({"number": _e(PSEUDO_PATTERN), "label": "Mire"})
    for slot in range(1, nslots + 1):
        key = state["by_slot"].get(slot)
        dev = state["devices"].get(key) if key else None
        for essence, off in spec["essences"]:
            tag = ESSENCE_TAG[essence]
            for lane in range(1, nlanes + 1):
                targets.append({"number": _e(off + lane_signal(slot, lane)),
                                "label": "S%d voie %d%s" % (slot, lane, tag)})
            for index in range(1, SIG_SDI_MAX + 1):
                sources.append({"number": _e(off + phys_signal(slot, "sdi", index)),
                                "label": "S%d SDI %d%s" % (slot, index, tag)})
            for index in range(1, SIG_IP_MAX + 1):
                sources.append({"number": _e(off + phys_signal(slot, "ip", index)),
                                "label": "S%d Rx IP %d%s" % (slot, index, tag)})
            if not dev:
                continue
            for lane, info in (dev.get("lanes") or {}).items():
                if lane > nlanes:
                    continue
                blk = (info.get("in") or {}).get(essence)
                if not blk:
                    continue
                num = _source_number(spec, slot, blk.get("src"), essence)
                if num is not None:
                    _connect(connections, seen, _e(off + lane_signal(slot, lane)), _e(num),
                             spec["label"])
    return targets, sources, connections


def _build_grid_out(spec, elements, state):
    """Grille de sorties. Destinations = les connecteurs de sortie ; sources = la sortie de
    chaque voie. Les sorties étant les DESTINATIONS, une même voie peut en alimenter
    plusieurs : le « SDI et/ou IP » sort gratuitement du fan-out d'une matrice oneToN."""
    nslots, nlanes = num_slots(), lanes_per_slot()
    targets, sources, connections, seen = [], [], [], {}
    sources.append({"number": _e(PSEUDO_NONE), "label": "Désactivé"})
    for slot in range(1, nslots + 1):
        key = state["by_slot"].get(slot)
        dev = state["devices"].get(key) if key else None
        for essence, off in spec["essences"]:
            tag = ESSENCE_TAG[essence]
            for index in range(1, SIG_SDI_MAX + 1):
                targets.append({"number": _e(off + phys_signal(slot, "sdi", index)),
                                "label": "S%d BNC %d%s" % (slot, index, tag)})
            for index in range(1, SIG_IP_MAX + 1):
                targets.append({"number": _e(off + phys_signal(slot, "ip", index)),
                                "label": "S%d Tx IP %d%s" % (slot, index, tag)})
            for lane in range(1, nlanes + 1):
                sources.append({"number": _e(off + lane_signal(slot, lane)),
                                "label": "S%d voie %d%s" % (slot, lane, tag)})
            if not dev:
                continue
            for lane, info in (dev.get("lanes") or {}).items():
                if lane > nlanes:
                    continue
                blk = (info.get("out") or {}).get(essence)
                if not blk:
                    continue
                for dst in blk.get("dst") or []:
                    kind = str(dst.get("kind") or "").lower()
                    try:
                        index = int(dst.get("index"))
                    except (TypeError, ValueError):
                        continue
                    limit = SIG_SDI_MAX if kind == "sdi" else SIG_IP_MAX
                    if kind not in ("sdi", "ip") or not (1 <= index <= limit):
                        continue
                    doff = _ess_offset(spec, _essence_src(dst, essence))
                    if doff is None:
                        continue
                    _connect(connections, seen, _e(doff + phys_signal(slot, kind, index)),
                             _e(off + lane_signal(slot, lane)), spec["label"])
    return targets, sources, connections


def build_grid(spec, elements, state):
    """Une grille de flux : son nœud racine, sa légende, et l'entrée matrix_map qui va avec."""
    if spec["dir"] == "in":
        targets, sources, connections = _build_grid_in(spec, elements, state)
        legende = _LEGENDE_COMMUNE + (" Une voie ne peut être alimentée que par un connecteur "
                                      "de SON slot ; toute autre demande est refusée.")
    else:
        targets, sources, connections = _build_grid_out(spec, elements, state)
        legende = _LEGENDE_COMMUNE + (" Une sortie ne peut être alimentée que par une voie de "
                                      "SON slot. Une même voie peut alimenter plusieurs sorties.")
    if len(spec["essences"]) > 1:
        legende += _LEGENDE_AUDIO
    elements.append(([spec["root"]], "node", spec["label"], ""))
    _infos_node(elements, spec["root"], legende)
    return {"type": None, "canon": spec["canon"], "ref": None, "label": spec["label"],
            "decl": {"type": "oneToN", "description": spec["label"],
                     "targets": targets, "sources": sources, "connections": connections}}


def build_grid_slots(elements, state):
    """Grille d'affectation (racine 1010) « Affectation IPG ». Destinations = les slots ; sources = les devices connus.
    C'est elle qui rend le slot pilotable au pupitre — et comme le numéro d'un signal DIT son
    slot, déplacer un device ici renumérote tous ses signaux et toutes ses voies (§12.8)."""
    nslots = num_slots()
    targets = [{"number": _e(s), "label": "Slot %d" % s} for s in range(1, nslots + 1)]
    sources = [{"number": _e(PSEUDO_NONE), "label": "Aucun"}]
    connections = []
    numbers = state.get("numbers") or {}
    for key in sorted(numbers, key=lambda k: numbers[k]):
        dev = state["devices"].get(key)
        label = dev.get("label") if dev else key
        sources.append({"number": _e(DEV_SRC_OFFSET + numbers[key]),
                        "label": "%s%s" % (label, "" if dev else " (absent)")})
    for slot, key in (state.get("by_slot") or {}).items():
        if slot <= nslots and key in numbers:
            connections.append({"target": _e(slot),
                                "sources": [_e(DEV_SRC_OFFSET + numbers[key])]})
    elements.append(([GRID_SLOT_ROOT_ID], "node", "Affectation IPG", ""))
    _infos_node(elements, GRID_SLOT_ROOT_ID,
                "Pose un IPG sur un slot. 1 = Aucun (vide le slot), puis 11, 12… = les devices "
                "connus. Un slot porte un device à la fois ; affecter un device déjà posé "
                "ailleurs le DÉPLACE. Attention : déplacer un device renumérote tous ses "
                "signaux et toutes ses voies — opération hors service, pas un geste de "
                "production.")
    return {"type": None, "canon": "slot", "ref": None, "label": "Affectation IPG",
            "decl": {"type": "oneToN", "description": "Affectation IPG",
                     "targets": targets, "sources": sources, "connections": connections}}


def build_matrices(elements, state):
    """Les sept matrices canoniques, indexées par leur chemin (prêtes pour matrix_map)."""
    out = {(spec["root"], MATRIX_ID): build_grid(spec, elements, state) for spec in GRID_SPECS}
    out[(GRID_SLOT_ROOT_ID, MATRIX_ID)] = build_grid_slots(elements, state)
    return out


# ═════════════════════════════════════════════════════════════════════
# Arbre des SDP (racine 1100)
# ═════════════════════════════════════════════════════════════════════

def build_sdp(elements, path_map, state):
    """`<racine SDP> / slot / (1 = Rx, 2 = Tx) / index / essence / {SDP, Flux présent, Actif}`.

    ⚠ EXCEPTION ASSUMÉE au principe de grille pleine (§12.7) : on n'émet QUE les signaux
    réellement déclarés. Un SDP pèse 1 à 2 ko ; le vivier complet en ferait plusieurs Mo à
    chaque GetDirectory. Les CHEMINS restent déterministes — VSM peut être câblé d'avance —
    seule la présence suit le parc."""
    nslots = num_slots()
    root_done = False
    for slot in sorted(state.get("by_slot") or {}):
        if slot > nslots:
            continue
        key = state["by_slot"][slot]
        dev = state["devices"].get(key)
        if not dev:
            continue
        slot_done = False
        for direction, signals in ((SDP_DIR_RX, dev.get("inputs")),
                                   (SDP_DIR_TX, dev.get("outputs"))):
            ip_sigs = [s for s in (signals or []) if s.get("kind") == "ip"]
            if not ip_sigs:
                continue
            if not root_done:
                elements.append(([SDP_ROOT_ID], "node", "SDP", ""))
                root_done = True
            if not slot_done:      # une seule fois : les deux sens partagent le nœud de slot
                elements.append(([SDP_ROOT_ID, slot], "node",
                                 dev.get("label") or "Slot %d" % slot, ""))
                slot_done = True
            elements.append(([SDP_ROOT_ID, slot, direction], "node",
                             "Récepteurs" if direction == SDP_DIR_RX else "Émetteurs", ""))
            for s in ip_sigs:
                base = [SDP_ROOT_ID, slot, direction, SIG_IP_OFFSET + s["index"]]
                default = ("Rx %d" if direction == SDP_DIR_RX else "Tx %d") % s["index"]
                elements.append((base, "node", s.get("label") or default, ""))
                for essence in ESSENCES:
                    blk = (s.get("essences") or {}).get(essence)
                    if not blk:
                        continue
                    _emit_sdp_essence(elements, path_map, base, essence, blk, dev, direction)


def _emit_sdp_essence(elements, path_map, base, essence, blk, dev, direction):
    """Les trois paramètres d'une essence d'un signal IP."""
    epath = base + [ESSENCE_ID[essence]]
    elements.append((epath, "node", ESSENCE_LABEL[essence], ""))
    ref = blk.get("ref")
    # SDP inscriptible sur un RÉCEPTEUR : l'écrire EST l'acte de routage. Sur un émetteur il
    # décrit ce qu'on produit — lecture seule.
    writable = bool(ref is not None and direction == SDP_DIR_RX)
    elements.append((epath + [SDP_PARAM_SDP], "param", "SDP", "",
                     str(blk.get("sdp") or ""), glow.PT_STRING, writable))
    if writable:
        path_map[tuple(epath + [SDP_PARAM_SDP])] = (dev["type"], ref, "sdp")
    elements.append((epath + [SDP_PARAM_PRESENT], "param", "Flux présent", "",
                     bool(blk.get("present")), glow.PT_BOOLEAN, False))
    en_writable = ref is not None and blk.get("enabled") is not None
    elements.append((epath + [SDP_PARAM_ENABLED], "param", "Actif", "",
                     bool(blk.get("enabled")), glow.PT_BOOLEAN, bool(en_writable)))
    if en_writable:
        path_map[tuple(epath + [SDP_PARAM_ENABLED])] = (dev["type"], ref, "enabled")


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


def _apply_slot(slot, src, state):
    """Affecte (ou libère) un slot. Purement interne au service : aucun plugin n'est appelé,
    c'est le registre qui fait foi (§12.8)."""
    if not (1 <= slot <= num_slots()):
        log.info("emberplus/io: slot %s hors bornes", slot)
        return False
    reg = slot_registry()
    numbers = state.get("numbers") or {}
    if src == PSEUDO_NONE:
        freed = [k for k, s in reg.items() if s == slot]
        for k in freed:
            del reg[k]
        if not freed:
            return False
        save_slot_registry(reg)
        log.info("emberplus/io: slot %d libéré (%s)", slot, ", ".join(freed))
        return True
    wanted = src - DEV_SRC_OFFSET
    key = next((k for k, n in numbers.items() if n == wanted), None)
    if key is None:
        log.info("emberplus/io: source %s inconnue dans la grille d'affectation", src)
        return False
    for k, s in list(reg.items()):
        if s == slot and k != key:
            del reg[k]                      # un slot porte UN device à la fois
    reg[key] = slot                         # et un device déménage plutôt que de se dupliquer
    save_slot_registry(reg)
    log.info("emberplus/io: %s posé sur le slot %d", key, slot)
    return True
