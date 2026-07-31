# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

"""Service Ember+ — provider générique, agrégateur des contributions d'outils.

Ce service N'A AUCUNE sémantique métier. Chaque outil-plugin qui veut être piloté
en Ember+ déclare `"ember": true` dans son plugin.json et expose deux routes
(atteintes via `tools.call`, donc indifféremment docker ou inprocess) :

  GET  ember/tree  → sous-arbre déclaratif :
      { "label": "...", "nodes": [ {id, label, desc?, nodes?, params?}, ... ] }
    où params : { id, label, desc?, type: string|int|real|bool|enum,
                  value, writable?, enum?:[{value,label}], ref:<opaque> }

  POST ember/set   ← { "ref": <opaque>, "value": <v> } → applique côté outil.

Le service monte le sous-arbre de chaque outil sous un nœud racine `[i]`, encode
l'arbre Glow (cf. emberplus_glow), et route les écritures vers l'outil via le `ref`
opaque qu'il rejoue tel quel. Toute écriture est journalisée comme « Service Ember+ ».

Socle serveur (boucle TCP, S101, debounce) repris du provider Ember+ de Bobi.Studio.
"""
import json
import logging
import os
import socket
import threading
import time

from app import config, settings, tools
from app import plugins as _plugins
from app.database import audit_log
from . import emberplus_glow as glow
from . import ipg_io
from . import profile as _profile

log = logging.getLogger(__name__)

# Acteur virtuel attribué dans l'audit pour toute modification venant d'Ember+.
EMBER_ACTOR = "Service Ember+"

# Racine FIXE du « moule IPG » (mode canonique). Valeur haute et stable, distincte des racines
# par-plugin (1..k) : garantit un chemin VSM stable quels que soient les plugins activés.
# Structure sous cette racine : [IPG, voie, bloc, param], où la voie est ADRESSÉE PAR SLOT —
# `100×slot + n° local` (§12.2). Une voie ne s'attribue donc plus, elle se calcule : le
# registre collant d'antan a disparu, et avec lui sa dérive (cf. §12.9.4).
IPG_ROOT_ID = 1000
# Racine du nœud de service (le provider s'expose lui-même). Distincte des racines
# par-plugin (1..k) et de la racine IPG : aucun risque de collision.
SERVICE_ROOT_ID = 1001
# Bloc « identité de voie », monté par le SERVICE (hors profil). id réservé, très au-dessus des
# blocs du catalogue (1..k) : décrit à qui une voie est affectée, en lecture seule.
IDENTITY_BLOCK_ID = 100
_VERSION_CACHE = None          # version lue une fois dans le manifeste (cf. _service_version)
_last_push_ts = None           # horodatage de la dernière trame réellement émise

# Paramètres du nœud de service dont la valeur bouge TOUTE SEULE (horloge). Ils sont exclus
# de la détection de changement : sinon chaque tick produirait un delta, et le silence à état
# stable — la raison d'être de la diffusion incrémentale — serait perdu. Cas de la « dernière
# poussée », qui s'AUTO-ENTRETIENT : pousser met à jour l'horodatage, donc le tick suivant voit
# une différence, donc pousse à nouveau, indéfiniment. Ces valeurs restent émises dès qu'un
# changement RÉEL survient, et sont fraîches à chaque GetDirectory.
# L'uptime n'y figure PAS : à la minute près il ne bavarde plus, et c'est justement lui qui
# doit vivre à l'écran. Seule reste la « dernière poussée », intrinsèquement auto-entretenue.
VOLATILE_SERVICE_PATHS = {(SERVICE_ROOT_ID, 10)}

NOTIFY_DEBOUNCE_S = 1.0        # max 1 broadcast / seconde
TREE_TTL_S = 5.0               # ré-agrégation de l'arbre au plus toutes les 5 s
IO_TTL_S = 15.0                # cadence PROPRE à `ember/io`, plus lente que l'arbre : avec les
                               # essences, la collecte pèse 260 ko pour un seul SNP (quatre SDP
                               # par signal), soit plusieurs Mo sur un parc chargé. La refaire à
                               # chaque reconstruction serait du gaspillage — un SDP bouge
                               # rarement. Le tally après crosspoint n'en dépend PAS : il passe
                               # par `ipg_io.reload_type`, qui court-circuite ce cache.
PUSH_INTERVAL_DEFAULT_S = 5    # cadence du pousseur périodique (réglage emberplus_push_interval)
PUSH_INTERVAL_MIN_S = 1        # bornes de garde : un intervalle absurde ne doit pas noyer le parc
PUSH_INTERVAL_MAX_S = 3600
TREE_CALL_TIMEOUT_S = 10       # garde-fou : un contributeur lent à `ember/tree` est sauté.
                               # Porté de 4 à 10 s le 2026-07-29 : le SNP met 4 à 9 s pour
                               # 687 ko et débordait une fois sur deux. Le dépassement n'efface
                               # plus rien pour autant, cf. `_subtree_cache`.

# Mapping type déclaratif → type de paramètre Glow.
_TYPE_MAP = {
    "string": glow.PT_STRING, "str": glow.PT_STRING, "text": glow.PT_STRING,
    "int": glow.PT_INTEGER, "integer": glow.PT_INTEGER,
    "real": glow.PT_REAL, "float": glow.PT_REAL,
    "bool": glow.PT_BOOLEAN, "boolean": glow.PT_BOOLEAN,
    "enum": glow.PT_INTEGER,
}

# Mapping type de matrice déclaratif → constante Glow.
_MATRIX_TYPE = {
    "oneton": glow.MATRIX_ONE_TO_N, "onetoone": glow.MATRIX_ONE_TO_ONE,
    "nton": glow.MATRIX_N_TO_N,
}

# ─── État serveur ───────────────────────────────────────────
_lock = threading.Lock()
_clients = set()               # connexions actives
_subscribed = set()            # sockets ayant souscrit au broadcast
_server_thread = None
_push_thread = None            # pousseur périodique (cf. _push_loop)
_server_socket = None
_running = False
_notify_timer = None
_notify_pending = False
_status = {
    "running": False, "port": 0, "clients": 0, "subscribed": 0,
    "last_error": None, "started_at": None, "contributors": [],
}

# Cache de l'arbre agrégé : body (racine, nœuds/params + matrices en contents-seuls),
# path_map {tuple(path): (type, ref)} pour SetValue, matrix_map {tuple(path): {...}} pour
# le routage des crosspoints, contributors.
# `elements` {tuple(path): élément} est l'index de la DERNIÈRE agrégation : il sert de point
# de comparaison pour n'émettre que ce qui a bougé (cf. _broadcast_update).
_tree_lock = threading.Lock()
# Dernier sous-arbre CONNU par contributeur. Sert de repli quand `ember/tree` échoue ou déborde
# du délai : mesuré le 2026-07-29, le `ember/tree` du SNP met 4 à 9 s pour 687 ko et sautait donc
# une fois sur deux, faisant disparaître tout son sous-arbre du contrôleur.
#
# Un cache en MÉMOIRE : il ne protège donc qu'APRÈS un premier succès.
# Redémarrer l'application pendant qu'un équipement est éteint faisait disparaître son
# sous-arbre du contrôleur — et c'est la STRUCTURE qui casse une configuration VSM, pas des
# valeurs périmées. On persiste donc la RÉPONSE BRUTE de `ember/tree` par contributeur, et
# pas les structures dérivées : c'est plus petit, et le rechargement repasse par exactement
# le même code (`_walk_node`) que si le plugin venait de répondre.
_TREES_FILE = os.path.join(os.path.dirname(config.DB_PATH), "emberplus_trees.json")
_raw_trees = {}                # {type: réponse ember/tree}, miroir mémoire du fichier
_raw_trees_dirty = False

_tree_cache = {"ts": 0.0, "body": None, "path_map": {}, "matrix_map": {}, "contributors": [],
               "elements": {}, "elements_list": None, "io": None, "io_ts": 0.0}


# ═════════════════════════════════════════════════════════════════════
# Agrégation des contributions d'outils → éléments Glow plats
# ═════════════════════════════════════════════════════════════════════

def _load_raw_trees():
    """Recharge les sous-arbres persistés. Silencieux si le fichier n'existe pas encore."""
    global _raw_trees
    try:
        with open(_TREES_FILE, encoding="utf-8") as f:
            d = json.load(f)
        if isinstance(d, dict):
            _raw_trees = {k: v for k, v in d.items() if isinstance(v, dict)}
            log.info("emberplus: %d sous-arbre(s) rechargé(s) depuis le disque", len(_raw_trees))
    except FileNotFoundError:
        pass
    except Exception as e:
        log.warning("emberplus: relecture de %s échouée : %s", _TREES_FILE, e)


def _save_raw_trees():
    """Écrit les sous-arbres connus, par remplacement atomique — un fichier tronqué par une
    coupure serait pire que pas de fichier du tout."""
    global _raw_trees_dirty
    if not _raw_trees_dirty:
        return
    tmp = _TREES_FILE + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_raw_trees, f, ensure_ascii=False)
        os.replace(tmp, _TREES_FILE)
        _raw_trees_dirty = False
    except Exception as e:
        log.warning("emberplus: écriture de %s échouée : %s", _TREES_FILE, e)


def _ember_types():
    """Types des outils activés déclarant `ember: true`, triés (index racine stable)."""
    out = [m.get("type") for m in _plugins.all()
           if m.get("ember") and not _plugins.is_disabled(m.get("type"))]
    return sorted(t for t in out if t)

def _bindings_types():
    """Types déclarant `ember_bindings: true` (mode « moule IPG ») : seuls ceux-là sont
    interrogés sur GET ember/bindings — évite un aller-retour 404 (potentiellement lent) vers
    les contributeurs qui n'implémentent que ember/tree (switch_ports…)."""
    out = [m.get("type") for m in _plugins.all()
           if m.get("ember_bindings") and not _plugins.is_disabled(m.get("type"))]
    return sorted(t for t in out if t)

def _walk_node(parent_path, node, elements, path_map, matrix_map, type_):
    nid = node.get("id")
    if nid is None:
        return
    path = parent_path + [int(nid)]
    # Un nœud portant `matrix` EST une matrice à ce chemin (pas un Node).
    if node.get("matrix"):
        matrix_map[tuple(path)] = {
            "type": type_, "decl": node["matrix"], "ref": node["matrix"].get("ref"),
            "label": str(node.get("label") or nid),
        }
        return
    elements.append((path, "node", str(node.get("label") or nid), str(node.get("desc") or "")))
    for child in node.get("nodes") or []:
        _walk_node(path, child, elements, path_map, matrix_map, type_)
    for param in node.get("params") or []:
        pid = param.get("id")
        if pid is None:
            continue
        ppath = path + [int(pid)]
        ptype_decl = str(param.get("type") or "string").lower()
        ptype = _TYPE_MAP.get(ptype_decl, glow.PT_STRING)
        value = param.get("value")
        enumeration = None
        if ptype_decl == "enum":
            enumeration = [str(e.get("label")) for e in (param.get("enum") or [])]
            try:
                value = int(value or 0)
            except (TypeError, ValueError):
                value = 0
        writable = bool(param.get("writable"))
        el = (ppath, "param", str(param.get("label") or pid),
              str(param.get("desc") or ""), value, ptype, writable)
        if enumeration:
            el = el + (enumeration,)
        elements.append(el)
        if writable and param.get("ref") is not None:
            path_map[tuple(ppath)] = (type_, param.get("ref"))

def _matrix_signals(decl):
    """(numéros targets, numéros sources, connexions [(t,[s]),…]) depuis la déclaration."""
    tnums = [int(t["number"]) for t in decl.get("targets") or []]
    snums = [int(s["number"]) for s in decl.get("sources") or []]
    conns = [(int(c["target"]), [int(x) for x in c.get("sources") or []])
             for c in decl.get("connections") or []]
    return tnums, snums, conns

def _encode_matrix(mpath, m, *, with_axes):
    """Encode la QualifiedMatrix d'une entrée matrix_map (axes complets ou contents-seuls)."""
    decl = m["decl"]
    tnums, snums, conns = _matrix_signals(decl)
    mtype = _MATRIX_TYPE.get(str(decl.get("type") or "oneToN").lower(), glow.MATRIX_ONE_TO_N)
    return glow.qualified_matrix(list(mpath), m["label"], str(decl.get("description") or ""),
                                 mtype, tnums, snums, conns, with_axes=with_axes)

def _matrix_body(mpath, m):
    """Réponse à GetDirectory(matrice) : la QualifiedMatrix COMPLÈTE (axes + connexions)."""
    return glow.build_collection([], extra=[_encode_matrix(mpath, m, with_axes=True)])

def _ensure_node(elements, seen, path, label):
    """Émet un nœud (path, label) une seule fois (les nœuds intermédiaires slot/voie/bloc
    sont partagés par plusieurs paramètres)."""
    key = tuple(path)
    if key in seen:
        return
    seen.add(key)
    elements.append((list(path), "node", str(label), ""))

_CANON_DEFAULTS = {"bool": False, "boolean": False, "int": 0, "integer": 0,
                   "real": 0.0, "float": 0.0, "enum": 0}


def _emit_canon_param(elements, path_map, ppath, res, value, ref, minimum, maximum, ident=None):
    """Émet un paramètre canonique (forme positionnelle enum/bornes de `_encode_element`) et,
    s'il est inscriptible ET porte un `ref`, l'inscrit dans `path_map` (routage du SetValue)."""
    ptype = _TYPE_MAP.get(res["type"], glow.PT_STRING)
    # Un paramètre que le device courant NE MAPPE PAS (aucun `ref`) est annoncé en LECTURE
    # SEULE, même si le catalogue le dit inscriptible. Sans ça, la grille pleine produisait
    # 416 « faux boutons » par slot : VSM acceptait l'édition, le service la jetait faute de
    # route, et la valeur revenait à la poussée suivante — sans le moindre message. C'est le
    # miroir du faux verrou, et il érode autant la confiance dans la surface de contrôle.
    #
    # Le drapeau varie donc d'un device à l'autre, et l'arbre n'est plus byte-identique entre
    # deux machines de couverture différente. C'est assumé : VSM s'accroche au CHEMIN
    # (RELATIVE-OID, §2), pas au drapeau — les chemins câblés sur le contrôleur survivent au
    # remplacement, ce qui est la promesse réelle du moule.
    writable = bool(res.get("writable", True)) and ref is not None
    if res["type"] == "enum":
        try:
            value = int(value or 0)
        except (TypeError, ValueError):
            value = 0
    # IDENTIFIANT et DESCRIPTION sont deux champs distincts, et on s'en sert enfin comme tel :
    # l'identifiant est machine (« L01_Color_GainR » — anglais, sans espace ni accent, c'est lui
    # qui se retrouve dans une configuration de contrôleur), la description est humaine
    # (« Gain R »). Jusqu'ici le libellé servait aux deux, ce qui mettait des accents et des
    # espaces dans des chemins censés être stables.
    el = (ppath, "param", ident or res["param_label"], res["param_label"] if ident else "",
          value, ptype, writable)
    enumeration = res["enum"] if (res["type"] == "enum" and res["enum"]) else None
    # `enumeration` doit précéder les bornes, même à None : forme positionnelle attendue.
    if enumeration is not None or minimum is not None or maximum is not None:
        el = el + (enumeration,)
    if minimum is not None or maximum is not None:
        el = el + (minimum, maximum)
    elements.append(el)
    if writable and ref is not None:
        path_map[tuple(ppath)] = ref


def _append_canonical(elements, path_map, contributors, io_state):
    """Voie CANONIQUE (« moule IPG »), modèle VIVIER. La racine 1000 porte un nombre FIXE de
    voies (grille pleine, `_num_lanes`), chacune émettant TOUT le catalogue du profil — qu'un
    device y soit affecté ou non. Ainsi VSM peut être configuré avant qu'un équipement soit
    présent, et un swap de device ne change JAMAIS la forme de l'arbre.

        1000 / <voie 1..N> / <bloc.id> / <param.id>

    Chaque voie porte aussi un bloc « identité » (id réservé, lecture seule) disant à quel
    device/canal elle est affectée. La voie ne s'attribue plus : elle se CALCULE depuis le
    slot (§12.9.4), donc un device qui en remplace un autre sur le même slot hérite
    exactement de ses voies — ce que le slot promettait sans le tenir jusqu'ici.

    Contrat plugin (GET ember/bindings) — plus de `slot` : le matériel décrit ce qu'il A,
    jamais où il est posé (§12.11) :
        { "devices": [ { "device": str, "label"?: str,
                         "bindings": [ { "key": "<bloc>.<param>", "lane"?: int,
                                         "value": <v>, "ref": <opaque>,
                                         "min"?: number, "max"?: number }, ... ] } ] }
    Un outil qui ne répond pas 200 est ignoré. `min`/`max` (binding) priment sur le profil :
    une borne décrit le matériel, pas le catalogue commun à toutes les familles.

    Le CATALOGUE ne vient plus d'ici mais de la couche IPG (`plugins/ipg_generique`), lu par
    `_profile.get_profile()` — même déménagement que le registre d'affectation en 0.17.0, et
    pour la même raison (§12.11). S'il est indisponible ET jamais lu depuis le démarrage, on
    n'émet RIEN sous la racine 1000 : mieux vaut une racine absente, qui se voit, qu'une
    grille pleine de voies sans un seul paramètre, qui ressemble à un parc éteint."""
    prof = _profile.get_profile()
    if not (prof.get("blocks") or []):
        contributors.append({"type": "ipg", "label": "IPG — catalogue indisponible "
                                                     "(outil « IPG Générique » absent ou muet)"})
        return
    index = _profile.build_index(prof)
    nslots, nlanes = ipg_io.num_slots(), ipg_io.lanes_per_slot()
    slots = (io_state or {}).get("slots") or {}
    # Devices connus par `ember/io`, indexés par slot : ils portent l'identité d'une voie même
    # quand ils ne contribuent aucune clé canonique.
    io_devices = {s: (io_state.get("devices") or {}).get(k)
                  for s, k in ((io_state or {}).get("by_slot") or {}).items()}

    # 1. Collecte des bindings, indexés par (slot, voie locale). Le slot vient du REGISTRE et
    #    de LUI SEUL (§12.11) : un device que le registre ignore n'entre pas dans le moule,
    #    quoi qu'il déclare. Il n'y a plus de repli sur un vœu du plugin — c'était le second
    #    verrou d'exposition, celui qui rendait le slot affiché imprévisible.
    channels = {}        # (slot, lane) -> { canon_key: binding }
    dev_labels = {}      # slot -> label lisible du device
    for type_ in _bindings_types():
        status, data = tools.call(type_, "ember/bindings", "GET", actor=EMBER_ACTOR,
                                  timeout=TREE_CALL_TIMEOUT_S)
        if status != 200 or not isinstance(data, dict):
            continue
        for dev in data.get("devices") or []:
            device = dev.get("device")
            if device in (None, ""):
                continue
            slot = slots.get(ipg_io.dev_key(type_, device))
            if slot is None or not (1 <= slot <= ipg_io.SLOT_MAX):
                continue
            if dev.get("label"):
                dev_labels.setdefault(slot, str(dev["label"]))
            for b in dev.get("bindings") or []:
                try:
                    lane = int(b.get("lane") or 1)
                except (TypeError, ValueError):
                    continue
                if 1 <= lane <= ipg_io.LANE_MAX:
                    ch = channels.setdefault((slot, lane), {"type": type_, "binds": {}})
                    ch["binds"][b.get("key")] = b

    # 2. Grille pleine : IPG → Slot → Lane, et TOUS les paramètres à plat dans la lane.
    #
    # ⚠ La profondeur n'est pas un choix esthétique, elle se paie au pupitre. Constaté par
    # l'exploitant le 2026-07-31 : tout ce qui concerne un IPG doit tenir sous UNE branche,
    # sinon le câblage se fait branche par branche, en glisser-déposer. L'arbre d'avant
    # dispersait un même IPG entre sept racines de grilles, un arbre SDP séparé et des voies
    # pendues à la racine sans niveau slot — soit 288 branches par IPG. Ici, une par IPG.
    #
    # Le nœud d'un IPG s'appelle « Slot01 », JAMAIS du nom du matériel qui l'occupe : le chemin
    # doit survivre à un déménagement de slot (§12.8), sinon une réaffectation casserait tout le
    # câblage du contrôleur. Le nom du matériel vit dans `Ident_Device`, qui est un paramètre et
    # a donc le droit de changer.
    seen = set()
    _ensure_node(elements, seen, [IPG_ROOT_ID], prof.get("label") or "IPG")
    nassigned = 0
    for slot in range(1, nslots + 1):
        _ensure_node(elements, seen, [IPG_ROOT_ID, slot], "Slot%02d" % slot)
        for lane in range(1, nlanes + 1):
            ch = channels.get((slot, lane))
            binds = ch["binds"] if ch else {}
            if ch:
                nassigned += 1
            lpath = [IPG_ROOT_ID, slot, lane]
            _ensure_node(elements, seen, lpath, "L%02d" % lane)
            pref = "L%02d_" % lane          # rappelé sur CHAQUE feuille, cf. plus bas

            # Identité (hors profil) : à qui la voie est affectée, en lecture seule.
            # L'occupation vient du SLOT, pas des bindings : une passerelle pure (CDE, Newt)
            # tient un slot et des voies dans les grilles sans mapper une seule clé du
            # catalogue. La dire « non affectée » aurait été un mensonge au contrôleur.
            io_dev = io_devices.get(slot)
            occupied = ch is not None or io_dev is not None
            label = ((io_dev or {}).get("label") or dev_labels.get(slot)
                     or (ch["type"] if ch else "")) if occupied else ""
            # id de feuille = bloc×100 + param. Les `id` du catalogue sont gelés (§ en tête de
            # `profile.py`), donc ce calcul l'est aussi — et il laisse les blocs à deux chiffres
            # sans collision possible avec le bloc d'identité, qui vaut 100.
            # PREMIER champ de la voie, et le seul qu'on lise d'un coup d'œil : « SNPF1 - C2 »,
            # soit le nom de l'IPG suivi de la désignation CONSTRUCTEUR du canal. Demandé par
            # l'exploitant : au pupitre, une voie doit se reconnaître sans avoir à recouper deux
            # paramètres. Vide tant que le slot n'est pas occupé — un nom sur une voie libre
            # laisserait croire à une affectation.
            natif0 = ((io_dev or {}).get("lanes") or {}).get(lane, {}).get("name") \
                if io_dev else None
            elements.append(([IPG_ROOT_ID, slot, lane, IDENTITY_BLOCK_ID * 100], "param",
                             pref + "Ident", "",
                             ("%s - %s" % (label, natif0 or ("L%02d" % lane))
                              if label else "") if occupied else "",
                             glow.PT_STRING, False))
            elements.append(([IPG_ROOT_ID, slot, lane, IDENTITY_BLOCK_ID * 100 + 1], "param",
                             pref + "Ident_Assigned", "", occupied, glow.PT_BOOLEAN, False))
            elements.append(([IPG_ROOT_ID, slot, lane, IDENTITY_BLOCK_ID * 100 + 2], "param",
                             pref + "Ident_Device", "", label, glow.PT_STRING, False))
            # « Canal natif » : la désignation que le CONSTRUCTEUR donne à cette voie — « A1 »
            # sur un SNP (processeur + position) ou un Neuron (path). C'est elle que
            # l'exploitant lit sur la face avant, donc c'est elle qui doit apparaître ici ;
            # le couple slot/voie ne fait que la situer dans notre plan. Les familles qui ne
            # nomment pas leurs voies (CDE, Newt) retombent sur ce couple.
            natif = ((io_dev or {}).get("lanes") or {}).get(lane, {}).get("name") \
                if io_dev else None
            elements.append(([IPG_ROOT_ID, slot, lane, IDENTITY_BLOCK_ID * 100 + 3], "param",
                             pref + "Ident_Channel", "",
                             ("%s · slot %d voie %d" % (natif, slot, lane) if natif
                              else "slot %d · voie %d" % (slot, lane)) if occupied else "",
                             glow.PT_STRING, False))

            # Catalogue complet du profil, dans l'ordre. Une voie affectée remplit ce que le
            # device expose (valeur + ref → pilotable) ; le reste, et toute voie libre, tombe
            # au défaut.
            for block in prof.get("blocks") or []:
                bkey, bid = block.get("key"), block.get("id")
                if not bkey or bid is None:
                    continue
                for pm in block.get("params") or []:
                    pkey, pid = pm.get("key"), pm.get("id")
                    if not pkey or pid is None:
                        continue
                    res = index.get("%s.%s" % (bkey, pkey))
                    if not res:
                        continue
                    b = binds.get("%s.%s" % (bkey, pkey))
                    if b is not None:
                        value = b.get("value")
                        minimum = b.get("min") if b.get("min") is not None else res.get("min")
                        maximum = b.get("max") if b.get("max") is not None else res.get("max")
                        ref = (ch["type"], b.get("ref")) if b.get("ref") is not None else None
                    else:
                        value = _CANON_DEFAULTS.get(res["type"], "")
                        minimum, maximum = res.get("min"), res.get("max")
                        ref = None
                    _emit_canon_param(elements, path_map,
                                      [IPG_ROOT_ID, slot, lane, int(bid) * 100 + int(pid)],
                                      res, value, ref, minimum, maximum,
                                      ident=pref + res["block_ident"] + "_" + res["param_ident"])

    contributors.append({"type": "ipg", "label": "%s (%d slots × %d voies, %d affectée%s)" % (
        prof.get("label") or "IPG", nslots, nlanes, nassigned, "s" if nassigned != 1 else "")})

_EMPTY_IO = {"devices": {}, "by_slot": {}, "slots": {}, "numbers": {}}

def _io_state(force=False):
    """État `ember/io` du parc, avec sa péremption propre (IO_TTL_S). Ne lève jamais : un
    échec de collecte rend le dernier état connu plutôt que de vider les grilles — un
    contributeur momentanément muet ne doit pas faire disparaître ses crosspoints du contrôleur."""
    with _tree_lock:
        state = _tree_cache.get("io")
        fresh = state is not None and (time.monotonic() - (_tree_cache.get("io_ts") or 0)) < IO_TTL_S
    if fresh and not force:
        return state
    try:
        state = ipg_io.collect()
    except Exception as e:
        log.warning("emberplus: collecte ember/io échouée : %s", e)
        return state if state is not None else dict(_EMPTY_IO)
    with _tree_lock:
        _tree_cache["io"] = state
        _tree_cache["io_ts"] = time.monotonic()
    return state


def _build_tree():
    """Agrège les sous-arbres : (body racine, path_map, matrix_map, contributors).
    Deux voies coexistent : mode LIBRE (ember/tree, monté par plugin) + mode CANONIQUE
    (ember/bindings, monté par slot sous la racine IPG). Le body racine contient
    nœuds/params + matrices en CONTENTS-SEULS (annonce sans le payload, pour éviter la
    déconnexion VSM ; les axes/connexions viennent au GetDirectory)."""
    elements = []
    path_map = {}
    matrix_map = {}
    contributors = []
    global _raw_trees_dirty
    for idx, type_ in enumerate(_ember_types(), start=1):
        status, data = tools.call(type_, "ember/tree", "GET", actor=EMBER_ACTOR,
                                  timeout=TREE_CALL_TIMEOUT_S)
        stale = False
        if not (status == 200 and isinstance(data, dict)):
            # Un contributeur muet, lent ou cassé ne doit PAS faire DISPARAÎTRE son sous-arbre
            # du contrôleur : un nœud qui s'évanouit est bien pire qu'un nœud en retard — VSM perd
            # ses chemins, et l'opérateur croit le matériel absent. On rejoue donc le dernier
            # état connu, y compris APRÈS UN REDÉMARRAGE grâce au fichier (le cache mémoire, lui,
            # ne protège qu'après un premier succès dans le processus courant).
            log.warning("emberplus: %s ember/tree → %s", type_, status)
            data = _raw_trees.get(type_)
            if not isinstance(data, dict):
                continue
            stale = True
            log.warning("emberplus: %s indisponible — sous-arbre précédent rejoué", type_)
        elif _raw_trees.get(type_) != data:
            _raw_trees[type_] = data
            _raw_trees_dirty = True
        label = str(data.get("label") or type_)
        base = [idx]
        try:
            sub, pm, mm = [(base, "node", label, "")], {}, {}
            for node in data.get("nodes") or []:
                _walk_node(base, node, sub, pm, mm, type_)
        except Exception as e:
            log.warning("emberplus: conversion arbre %s échouée : %s", type_, e)
            continue
        if stale:
            label = "%s (dernier état connu)" % label
            sub[0] = (base, "node", label, "")
        elements += sub
        path_map.update(pm)
        matrix_map.update(mm)
        contributors.append({"type": type_, "label": label})
    # État des entrées/sorties du parc : il sert au moule (résolution des slots), aux six
    # grilles de flux, à l'arbre des SDP et à la grille d'affectation. Une seule collecte,
    # avec sa propre péremption (cf. IO_TTL_S) — c'est de loin le contributeur le plus lourd.
    io_state = _io_state()
    try:
        _append_canonical(elements, path_map, contributors, io_state)
    except Exception as e:
        log.warning("emberplus: agrégation canonique (IPG) échouée : %s", e)
    # ⚠ Les GRILLES DE FLUX Ember+ ont été retirées le 2026-07-31 : le routage des signaux et
    # l'affectation des slots passent désormais par SW-P-08 (§15). Les garder aurait entretenu
    # deux vérités sur le même crosspoint — et c'est justement pour ne PAS câbler mille
    # paramètres à la main au pupitre qu'on a choisi un protocole de routeur.
    #
    # `ipg_io.apply_connect` RESTE, et ne doit pas partir avec : c'est elle que le service
    # SW-P-08 appelle pour appliquer un croisement. Seule l'EXPOSITION en matrices disparaît.
    #
    # `build_sdp` reste aussi, faute de mieux : les SDP devaient rejoindre chaque voie, mais
    # aucun plugin ne publie l'association voie ↔ signal IP (une voie porte son routage, pas
    # ses SDP). Les supprimer d'abord aurait détruit l'information sans remplacement.
    try:
        ipg_io.build_sdp(elements, path_map, io_state)
        ndev = len(io_state.get("devices") or {})
        contributors.append({"type": "sdp", "label": "SDP (%d IPG)" % ndev})
    except Exception as e:
        log.warning("emberplus: arbre SDP échoué : %s", e)
    try:
        _append_service_node(elements, path_map)
    except Exception as e:
        log.warning("emberplus: nœud de service échoué : %s", e)
    # Index par chemin : base de la comparaison incrémentale. Un élément porte à la fois sa
    # valeur et son libellé, donc comparer les tuples suffit à détecter tout ce qui bouge.
    _save_raw_trees()      # après agrégation : un sous-arbre neuf survivra au prochain arrêt
    el_index = {tuple(el[0]): el for el in elements}
    # PAS d'encodage ici : la comparaison n'a besoin que de l'index. Le corps encodé ne sert
    # qu'à répondre à un GetDirectory ou à pousser un arbre complet, cas rares — l'encoder à
    # chaque cycle coûtait des centaines de ms pour rien sur un gros arbre (cf. _encoded_body).
    return elements, path_map, matrix_map, contributors, el_index, io_state


def _encoded_body():
    """Corps racine encodé, calculé À LA DEMANDE et mémoïsé jusqu'à la prochaine agrégation.
    Contient nœuds/params + matrices en CONTENTS-SEULS (annonce sans le payload, pour éviter
    la déconnexion VSM ; les axes/connexions viennent au GetDirectory)."""
    with _tree_lock:
        if _tree_cache.get("body") is not None:
            return _tree_cache["body"]
        elements = _tree_cache.get("elements_list") or []
        matrix_map = _tree_cache.get("matrix_map") or {}
    extras = [_encode_matrix(p, m, with_axes=False) for p, m in matrix_map.items()]
    body = glow.build_collection(elements, extra=extras)
    with _tree_lock:
        _tree_cache["body"] = body
    return body

def _children_body(path):
    """Corps d'un GetDirectory sur UN nœud : ses enfants DIRECTS, et rien d'autre.

    ⚠ Ce que faisait le provider avant le 2026-07-31 : répondre l'ARBRE ENTIER à tout
    GetDirectory, quelle que soit la profondeur demandée. Mesuré avec notre propre lecteur —
    18 secondes et plusieurs mégaoctets pour obtenir les cinq branches de la racine. Un
    consommateur qui descend nœud par nœud recevait donc tout l'arbre à chaque pas, et un
    pupitre qui se reconnecte le reprenait en entier.

    Les éléments portent leur chemin COMPLET (arbre plat qualifié), donc une tranche s'encode
    exactement comme le tout : on filtre, on encode, rien d'autre à faire."""
    with _tree_lock:
        elements = list(_tree_cache.get("elements_list") or [])
        matrix_map = dict(_tree_cache.get("matrix_map") or {})
    n = len(path)
    fils = [e for e in elements if len(e[0]) == n + 1 and list(e[0][:n]) == list(path)]
    # Les matrices s'annoncent en CONTENTS-SEULS, comme dans le corps racine : leurs axes et
    # connexions ne partent qu'au GetDirectory qui les vise, sinon le pupitre se déconnecte.
    extras = [_encode_matrix(p, m, with_axes=False) for p, m in matrix_map.items()
              if len(p) == n + 1 and list(p[:n]) == list(path)]
    return glow.build_collection(fils, extra=extras)


def _reaggregate(force=False):
    """Ré-agrège si le cache est expiré ou si `force`. N'ENCODE RIEN : le corps n'est produit
    qu'à la demande par `_encoded_body()`. Renvoie (path_map, matrix_map)."""
    with _tree_lock:
        fresh = (time.monotonic() - _tree_cache["ts"]) < TREE_TTL_S
        if not force and fresh and _tree_cache.get("elements_list") is not None:
            return _tree_cache["path_map"], _tree_cache["matrix_map"]
    elements, path_map, matrix_map, contributors, el_index, io_state = _build_tree()
    with _tree_lock:
        _tree_cache.update({"ts": time.monotonic(), "body": None, "path_map": path_map,
                            "matrix_map": matrix_map, "contributors": contributors,
                            "elements": el_index, "elements_list": elements, "io": io_state})
    with _lock:
        _status["contributors"] = contributors
    return path_map, matrix_map


def _current_tree(force=False):
    """Renvoie (body, path_map, matrix_map). Le corps est encodé à la demande — n'appeler
    que lorsqu'on en a réellement besoin (GetDirectory, push d'arbre complet)."""
    path_map, matrix_map = _reaggregate(force)
    return _encoded_body(), path_map, matrix_map

def _invalidate():
    with _tree_lock:
        _tree_cache["ts"] = 0.0

def refresh():
    """Force la ré-agrégation et re-pousse aux abonnés (à appeler après un changement)."""
    _invalidate()
    notify_change()


# ═════════════════════════════════════════════════════════════════════
# Nœud « Service Ember+ » : le provider s'expose lui-même
# ═════════════════════════════════════════════════════════════════════

def _service_version():
    """Version du service, lue dans son propre manifeste. Lecture directe du fichier voisin
    plutôt que via `core_plugins` : ce module est chargé PAR le registre, s'y adresser
    créerait une dépendance circulaire à l'import."""
    global _VERSION_CACHE
    if _VERSION_CACHE is None:
        try:
            import os
            with open(os.path.join(os.path.dirname(__file__), "manifest.json"),
                      encoding="utf-8") as f:
                _VERSION_CACHE = str(json.load(f).get("version") or "?")
        except Exception:
            _VERSION_CACHE = "?"
    return _VERSION_CACHE


def _uptime_str(started_at):
    """Durée depuis le démarrage, en texte court et lisible côté contrôleur.

    Granularité VOLONTAIREMENT à la minute : ce texte est comparé à chaque cycle pour décider
    d'une poussée. Avec des secondes il changeait à chaque tick, donc émettait un delta en
    permanence ; à la minute, il n'en produit qu'un par minute — assez pour que le compteur
    vive à l'écran, assez peu pour que la veille reste silencieuse. Ce delta fait aussi
    battement de cœur : le consumer voit que le provider est vivant."""
    if not started_at:
        return "—"
    s = int(max(0, time.time() - started_at))
    d, rem = divmod(s, 86400)
    h, rem = divmod(rem, 3600)
    m = rem // 60
    if d:
        return "%d j %d h" % (d, h)
    if h:
        return "%d h %02d min" % (h, m)
    if m:
        return "%d min" % m
    return "< 1 min"


def _append_service_node(elements, path_map):
    """Monte le nœud de service : cadence de poussée (réglable DEPUIS le contrôleur) et
    quelques compteurs de diagnostic en lecture seule.

    La cadence est AUTO-RÉFÉRENTE — elle règle l'intervalle auquel elle est elle-même
    repoussée. Sans danger grâce aux bornes annoncées (1 s–1 h), qui font refuser une
    saisie absurde par le consumer lui-même plutôt qu'après coup. Ce paramètre
    n'appartient à aucun outil : il n'entre donc PAS dans `path_map` (qui route vers un
    plugin) et son écriture est interceptée en amont par `_apply_service_setvalue`."""
    with _lock:
        nsub, ncli = len(_subscribed), len(_clients)
        contribs = ", ".join(c.get("type", "") for c in _status.get("contributors") or [])
        port = int(_status.get("port") or 0)
        started = _status.get("started_at")
        err = _status.get("last_error")
    # Voies émises : slots × voies par slot. Les « affectées » sont les slots réellement
    # occupés par un IPG — depuis que la voie se calcule (§12.9.4), c'est le slot qui porte
    # l'information, plus un registre de voies.
    with _tree_lock:
        io_state = _tree_cache.get("io") or {}
    nslots, nper = ipg_io.num_slots(), ipg_io.lanes_per_slot()
    nlanes = nslots * nper
    nassigned = sum(1 for s in (io_state.get("by_slot") or {}) if 1 <= s <= nslots) * nper
    # Compté sur l'agrégation en cours, + 1 : la cadence ci-dessous est le seul paramètre
    # inscriptible du nœud de service. Sert à repérer d'un coup d'œil un `writable` mal posé.
    nwrit = sum(1 for el in elements if el[1] == "param" and len(el) > 6 and el[6]) + 1
    _ensure_node(elements, set(), [SERVICE_ROOT_ID], "Service Ember+")
    # Identifiant seul, description vide — comme tous les autres paramètres de l'arbre :
    # VÉRIFIÉ, VSM recopie l'identifiant faute de description, donc un champ suffit.
    # Les `id` ci-dessous sont des CHEMINS VSM : on n'ajoute qu'EN FIN, jamais au milieu.
    elements.append(([SERVICE_ROOT_ID, 1], "param", "update interval (s)", "",
                     _push_interval(), glow.PT_INTEGER, True, None,
                     PUSH_INTERVAL_MIN_S, PUSH_INTERVAL_MAX_S))
    elements.append(([SERVICE_ROOT_ID, 2], "param", "Abonnés", "", nsub, glow.PT_INTEGER, False))
    elements.append(([SERVICE_ROOT_ID, 3], "param", "Clients", "", ncli, glow.PT_INTEGER, False))
    elements.append(([SERVICE_ROOT_ID, 4], "param", "Contributeurs", "", contribs,
                     glow.PT_STRING, False))
    elements.append(([SERVICE_ROOT_ID, 5], "param", "Version", "",
                     _service_version(), glow.PT_STRING, False))
    elements.append(([SERVICE_ROOT_ID, 6], "param", "Uptime", "",
                     _uptime_str(started), glow.PT_STRING, False))
    elements.append(([SERVICE_ROOT_ID, 7], "param", "Port", "", port, glow.PT_INTEGER, False))
    elements.append(([SERVICE_ROOT_ID, 8], "param", "Voies (affectées / total)", "",
                     "%d / %d" % (nassigned, nlanes), glow.PT_STRING, False))
    elements.append(([SERVICE_ROOT_ID, 9], "param", "Dernière erreur", "",
                     str(err) if err else "—", glow.PT_STRING, False))
    elements.append(([SERVICE_ROOT_ID, 10], "param", "Dernière poussée", "",
                     time.strftime("%H:%M:%S", time.localtime(_last_push_ts))
                     if _last_push_ts else "—", glow.PT_STRING, False))
    elements.append(([SERVICE_ROOT_ID, 11], "param", "Paramètres inscriptibles", "",
                     nwrit, glow.PT_INTEGER, False))


def _apply_service_setvalue(path, value):
    """Intercepte une écriture sur le nœud de service. Renvoie True si le chemin lui
    appartient (traité ou refusé), False pour laisser le routage normal opérer."""
    p = tuple(path)
    if not p or p[0] != SERVICE_ROOT_ID:
        return False
    if p == (SERVICE_ROOT_ID, 1):
        try:
            v = int(value)
        except (TypeError, ValueError):
            log.info("emberplus: cadence refusée (valeur %r non entière)", value)
            return True
        if not (PUSH_INTERVAL_MIN_S <= v <= PUSH_INTERVAL_MAX_S):
            log.info("emberplus: cadence %s hors bornes %d–%d, refusée",
                     v, PUSH_INTERVAL_MIN_S, PUSH_INTERVAL_MAX_S)
            return True
        settings.set("emberplus_push_interval", v)
        audit_log("emberplus", "push_interval", "%s s" % v, user_id=None, username=EMBER_ACTOR)
        log.info("emberplus: cadence de poussée portée à %s s depuis le contrôleur", v)
        refresh()
        return True
    log.info("emberplus: setvalue %s ignoré (paramètre de service en lecture seule)", path)
    return True


# ═════════════════════════════════════════════════════════════════════
# Application d'un SetValue → routage vers l'outil propriétaire
# ═════════════════════════════════════════════════════════════════════

def _write_allowed(addr):
    """Vrai si cette adresse a le droit d'ÉCRIRE (SetValue et crosspoints).

    Le provider est sans authentification — c'est le §11.5, et il pilote désormais des
    centaines de paramètres inscriptibles sur du matériel de production. À défaut de pouvoir
    authentifier (le protocole ne le prévoit pas), on restreint par adresse : seul le contrôleur
    déclaré écrit, tout le monde peut lire. Un consumer de diagnostic ne casse donc pas.

    Liste VIDE = aucune restriction, pour ne rien casser sur une installation existante — mais
    l'état est signalé au démarrage et dans le nœud de service, faute de quoi une protection
    jamais configurée serait exactement le genre de « livré mais pas en service » qui a déjà
    coûté cher à ce projet."""
    allow = _write_allow_list()
    if not allow:
        return True
    return (addr[0] if isinstance(addr, (tuple, list)) else str(addr)) in allow


def _write_allow_list():
    raw = settings.get("emberplus_write_allow") or ""
    return {p.strip() for p in str(raw).replace(";", ",").split(",") if p.strip()}


def _apply_setvalue(path, value):
    if _apply_service_setvalue(path, value):     # paramètres du service, avant tout routage
        return True
    path_map, _ = _reaggregate()        # tables seules : pas besoin d'encoder l'arbre
    entry = path_map.get(tuple(path))
    if not entry:
        log.info("emberplus: setvalue %s ignoré (inconnu ou lecture seule)", path)
        return False
    # Une entrée porte (type, ref) ; les paramètres de signal IP de l'arbre SDP y ajoutent un
    # `field`, car un même `ref` opaque couvre plusieurs valeurs inscriptibles (§12.7). Le
    # champ reste ABSENT du payload dans tous les autres cas : les plugins du mode libre et du
    # moule ne le voient jamais.
    type_, ref = entry[0], entry[1]
    field = entry[2] if len(entry) > 2 else None
    payload = {"ref": ref, "value": value}
    if field:
        payload["field"] = field
    status, data = tools.call(type_, "ember/set", "POST", payload, actor=EMBER_ACTOR)
    ok = status == 200 and isinstance(data, dict) and not data.get("error")
    if ok:
        detail = json.dumps(payload, ensure_ascii=False)[:400]
        audit_log(type_, "ember/set", detail, user_id=None, username=EMBER_ACTOR)
        log.info("emberplus: set %s %s = %r", type_, ref, value)
        refresh()
        return True
    log.warning("emberplus: set %s %s → %s %s", type_, ref, status, data)
    return False

_OP_NAME = {glow.CN_OP_ABSOLUTE: "absolute", glow.CN_OP_CONNECT: "connect",
            glow.CN_OP_DISCONNECT: "disconnect"}

def _reload_matrix(type_, mpath):
    """Recharge UNIQUEMENT le contributeur `type_` et renvoie son entrée matrix_map à jour
    pour `mpath`, sans ré-agréger tout l'arbre (donc sans relire les contributeurs lents en
    I/O réseau comme switch_ports). Renvoie None si introuvable/échec → repli sur refresh()."""
    try:
        idx = _ember_types().index(type_) + 1   # index racine 1-based, comme _build_tree
    except ValueError:
        return None
    status, data = tools.call(type_, "ember/tree", "GET", actor=EMBER_ACTOR,
                              timeout=TREE_CALL_TIMEOUT_S)
    if status != 200 or not isinstance(data, dict):
        return None
    local_mm = {}
    try:
        for node in data.get("nodes") or []:
            _walk_node([idx], node, [], {}, local_mm, type_)
    except Exception as e:
        log.warning("emberplus: reload matrice %s échoué : %s", type_, e)
        return None
    return local_mm.get(tuple(mpath))

def _apply_connect(matrix_path, target, sources, operation):
    """Route un crosspoint (consumer→provider) vers l'outil propriétaire de la matrice."""
    _, matrix_map = _reaggregate()      # tables seules : pas besoin d'encoder l'arbre
    m = matrix_map.get(tuple(matrix_path))
    if not m:
        log.info("emberplus: connect %s ignoré (matrice inconnue)", matrix_path)
        return False
    op = _OP_NAME.get(operation, "absolute")
    # Plus de matrice canonique : les grilles IPG ne sont plus exposées en Ember+ (SW-P-08).
    # Ne restent ici que les matrices des outils en mode LIBRE — un SNP exposé en direct, par
    # exemple —, qui gardent leur propre chemin d'écriture.
    status, data = tools.call(m["type"], "ember/connect", "POST",
                              {"ref": m["ref"], "target": target,
                               "sources": sources, "operation": op}, actor=EMBER_ACTOR)
    ok = status == 200 and isinstance(data, dict) and not data.get("error")
    if ok:
        detail = json.dumps({"ref": m["ref"], "target": target,
                             "sources": sources, "op": op}, ensure_ascii=False)[:400]
        audit_log(m["type"], "ember/connect", detail, user_id=None, username=EMBER_ACTOR)
        log.info("emberplus: connect %s tgt=%s src=%s op=%s", m["type"], target, sources, op)
        # Tally IMMÉDIAT vers VSM : on recharge la SEULE matrice touchée et on rediffuse sa
        # frame tout de suite — pas de débounce, pas de reconstruction complète (qui relirait
        # les switchs en live et coûtait ~3 s entre le crosspoint et sa validation).
        m2 = _reload_matrix(m["type"], matrix_path)
        if m2 is not None:
            with _tree_lock:
                if tuple(matrix_path) in _tree_cache["matrix_map"]:
                    _tree_cache["matrix_map"][tuple(matrix_path)] = m2
            _broadcast_matrix(matrix_path, m2)
        else:
            refresh()                       # repli : reconstruction + broadcast débouncé
        return True
    log.warning("emberplus: connect %s tgt=%s → %s %s", m["type"], target, status, data)
    return False


# ═════════════════════════════════════════════════════════════════════
# Serveur TCP (socle repris de Bobi.Studio)
# ═════════════════════════════════════════════════════════════════════

def status_dict():
    with _lock:
        _status["clients"] = len(_clients)
        _status["subscribed"] = len(_subscribed)
        return dict(_status)

def _send_frame(sock, body):
    try:
        wire = glow.s101_encode_ember(body)
        if glow.DEBUG:
            log.info("emberplus: → %s %d bytes (%d BER)", sock.getpeername(), len(wire), len(body))
        sock.sendall(wire)
        return True
    except Exception as e:
        log.debug("emberplus: send échoué : %s", e)
        return False

def _send_frames(frames):
    """Envoie une suite de frames à tous les abonnés ; retire ceux dont le socket est mort."""
    with _lock:
        dead = []
        for s in list(_subscribed):
            if not all(_send_frame(s, fr) for fr in frames):
                dead.append(s)
        for s in dead:
            _subscribed.discard(s)

def _diff_elements(old, new):
    """Compare deux index {chemin: élément} → (structure_changée, [éléments modifiés]).

    Un chemin ajouté ou retiré est STRUCTUREL : le consumer doit revoir l'arbre, on lui
    renvoie tout. À chemins constants, seuls les éléments dont le tuple diffère sont à
    repousser — un élément porte sa valeur ET son libellé, donc l'égalité de tuples suffit."""
    if old.keys() != new.keys():
        return True, []
    changed = [p for p in new if old.get(p) != new[p]]
    if changed and all(p in VOLATILE_SERVICE_PATHS for p in changed):
        return False, []          # seules des valeurs « horloge » ont bougé → on se tait
    return False, [new[p] for p in changed]

def _broadcast_update():
    """Rediffuse aux abonnés le STRICT nécessaire.

    L'arbre entier n'est réémis que si sa structure a changé (chemin ajouté/retiré : arrivée
    ou départ d'un device, ré-affectation, édition du profil). Sinon on n'émet que les
    éléments dont la valeur a bougé — la collection Ember+ étant plate et à chemins absolus,
    un sous-ensemble EST une trame de mise à jour valide. Et si rien n'a bougé, on n'envoie
    rien : l'ancien comportement rediffusait tout l'arbre même à valeurs identiques."""
    with _tree_lock:
        old = dict(_tree_cache.get("elements") or {})
        # Amorçage testé sur l'INDEX, surtout pas sur `body` : depuis l'encodage différé
        # celui-ci est presque toujours None, ce qui ferait passer chaque cycle pour un
        # premier passage — donc un arbre complet à chaque fois.
        primed = _tree_cache.get("elements_list") is not None
    try:
        _, matrix_map = _reaggregate(force=True)        # ré-agrège SANS encoder
    except Exception as e:
        log.error("emberplus: build arbre échoué : %s", e)
        return
    with _tree_lock:
        new = dict(_tree_cache.get("elements") or {})
    structural, changed = _diff_elements(old, new) if primed else (True, [])
    if structural:
        # Racine (nœuds/params + matrices contents-seuls) PUIS chaque matrice complète, pour
        # que les tallies de connexions remontent aux abonnés après un crosspoint.
        # L'encodage complet n'a lieu QUE dans ce cas : sur un tick ordinaire, seuls les
        # éléments modifiés sont encodés, ce qui rend la taille de l'arbre indolore.
        frames = [_encoded_body()] + [_matrix_body(p, m) for p, m in matrix_map.items()]
    elif changed:
        frames = [glow.build_collection(changed)]
    else:
        return
    if glow.DEBUG:
        log.info("emberplus: broadcast %s (%d élément(s))",
                 "arbre complet" if structural else "incrémental", len(changed))
    global _last_push_ts
    _last_push_ts = time.time()
    _send_frames(frames)

def _broadcast_matrix(mpath, m):
    """Envoie IMMÉDIATEMENT (sans débounce) la frame d'UNE matrice aux abonnés — fait remonter
    le tally d'un crosspoint à VSM sans attendre une reconstruction d'arbre."""
    _send_frames([_matrix_body(list(mpath), m)])

def notify_change():
    """Broadcast aux abonnés. Débounce : max 1 / NOTIFY_DEBOUNCE_S."""
    global _notify_timer, _notify_pending
    if not _running:
        return
    with _lock:
        if _notify_timer is not None:
            _notify_pending = True
            return
        _notify_pending = False
        def _fire():
            global _notify_timer, _notify_pending
            try:
                _broadcast_update()
            finally:
                with _lock:
                    _notify_timer = None
                    repeat = _notify_pending
                    _notify_pending = False
                if repeat:
                    notify_change()
        _notify_timer = threading.Timer(NOTIFY_DEBOUNCE_S, _fire)
        _notify_timer.daemon = True
        _notify_timer.start()

def _handle_client(sock, addr):
    log.info("emberplus: client %s connecté", addr)
    reader = glow.S101Reader()
    sock.settimeout(60.0)
    try:
        body, _, _ = _current_tree(force=True)     # push initial (valeurs fraîches)
        log.info("emberplus: push initial à %s (%d bytes BER)", addr, len(body))
        _send_frame(sock, body)
        while _running:
            try:
                data = sock.recv(4096)
            except socket.timeout:
                continue
            if not data:
                break
            if glow.DEBUG:
                log.info("emberplus: ← %s %d bytes: %s", addr, len(data), data.hex())
            for kind, payload in reader.feed(data):
                _process_message(sock, addr, kind, payload)
    except Exception as e:
        log.warning("emberplus: client %s erreur : %s", addr, e)
    finally:
        with _lock:
            _clients.discard(sock)
            _subscribed.discard(sock)
        try: sock.close()
        except Exception: pass
        log.info("emberplus: client %s déconnecté", addr)

def _process_message(sock, addr, kind, payload):
    if kind == "keepalive_req":
        try: sock.sendall(glow.s101_encode_keepalive_response())
        except Exception: pass
        return
    if kind != "payload":
        return
    try:
        actions = glow.parse_root(payload)
    except Exception as e:
        log.warning("emberplus: parse root erreur depuis %s : %s", addr, e)
        return
    if glow.DEBUG:
        log.info("emberplus: %s → actions %s", addr, actions)
    for a in actions:
        if a["kind"] in ("getdir", "subscribe"):
            with _lock:
                _subscribed.add(sock)
            # ⚠ On n'ENCODE PAS le corps racine ici : `_current_tree()` le produisait à chaque
            # GetDirectory, y compris pour répondre trois lignes. Seules les tables sont
            # nécessaires pour choisir la branche ; le corps complet n'est encodé que dans le
            # repli historique, ci-dessous.
            _, matrix_map = _reaggregate()
            mp = tuple(a.get("path") or [])
            if mp in matrix_map:                       # GetDirectory SUR une matrice
                _send_frame(sock, _matrix_body(mp, matrix_map[mp]))
            elif bool(settings.get("emberplus_lazy_dir")):
                # Réponse À LA DEMANDE : les enfants directs du nœud visé. C'est le
                # comportement attendu d'un provider, et il évite de repousser tout l'arbre
                # à chaque pas d'un consommateur qui descend.
                _send_frame(sock, _children_body(list(mp)))
            else:
                # Repli historique : l'arbre entier. Réglage de secours si un contrôleur
                # s'avérait dépendre de cette poussée massive — le mettre à faux et le dire.
                _send_frame(sock, _encoded_body())
        elif a["kind"] == "unsubscribe":
            with _lock:
                _subscribed.discard(sock)
        elif a["kind"] == "setvalue":
            if not _write_allowed(addr):
                log.warning("emberplus: SetValue REFUSÉ depuis %s (hors liste d'écriture)", addr)
                continue
            _apply_setvalue(a["path"], a["value"])
        elif a["kind"] == "connect":
            if not _write_allowed(addr):
                log.warning("emberplus: crosspoint REFUSÉ depuis %s (hors liste d'écriture)", addr)
                # On rediffuse la matrice telle qu'elle est : sans ça le contrôleur garderait
                # à l'écran un crosspoint qui n'a jamais eu lieu (même principe qu'au §12.3).
                _, matrix_map = _reaggregate()
                m = matrix_map.get(tuple(a["matrix_path"]))
                if m:
                    _send_frame(sock, _matrix_body(list(a["matrix_path"]), m))
                continue
            _apply_connect(a["matrix_path"], a["target"], a["sources"], a["operation"])

def _server_loop(port):
    global _server_socket, _running
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.bind(("0.0.0.0", port))
        s.listen(8)
        s.settimeout(1.0)
    except Exception as e:
        with _lock:
            _status["last_error"] = f"bind: {e}"
            _status["running"] = False
        _running = False
        log.error("emberplus: bind sur %s échoué : %s", port, e)
        return
    _server_socket = s
    with _lock:
        _status.update({"running": True, "port": port,
                        "last_error": None, "started_at": time.time()})
    log.info("emberplus: serveur lancé sur :%s", port)
    while _running:
        try:
            conn, addr = s.accept()
        except socket.timeout:
            continue
        except Exception as e:
            if _running:
                log.warning("emberplus: accept erreur : %s", e)
            break
        with _lock:
            _clients.add(conn)
        threading.Thread(target=_handle_client, args=(conn, addr), daemon=True).start()
    try: s.close()
    except Exception: pass
    with _lock:
        _status["running"] = False
    log.info("emberplus: serveur arrêté")

def _push_interval():
    """Cadence du pousseur, bornée : un réglage absurde ne doit pas noyer les contributeurs."""
    try:
        v = int(settings.get("emberplus_push_interval") or PUSH_INTERVAL_DEFAULT_S)
    except (TypeError, ValueError):
        v = PUSH_INTERVAL_DEFAULT_S
    return max(PUSH_INTERVAL_MIN_S, min(PUSH_INTERVAL_MAX_S, v))


def _push_loop():
    """Ré-agrège périodiquement et pousse CE QUI A CHANGÉ aux abonnés.

    Sans cela le provider ne reconstruit son arbre que sur sollicitation : un consumer qui
    reste sur une page voit des valeurs figées, et ne découvre les changements qu'en
    renavigant (constaté sur VSM). On ne travaille que s'il Y A des abonnés — sinon la
    ré-agrégation, qui interroge chaque contributeur en HTTP, serait pure dépense.
    Le coût par tick est faible : `_broadcast_update` n'émet que le delta, et rien du tout
    si rien n'a bougé."""
    while _running:
        time.sleep(_push_interval())
        if not _running:
            break
        with _lock:
            has_subs = bool(_subscribed)
        if not has_subs:
            continue
        try:
            notify_change()          # débouncé : se fond avec les autres déclencheurs
        except Exception as e:
            log.debug("emberplus: push périodique échoué : %s", e)


def start(port):
    """Démarre (ou redémarre) le serveur sur `port`."""
    global _server_thread, _push_thread, _running
    stop()
    _running = True
    _server_thread = threading.Thread(target=_server_loop, args=(int(port),), daemon=True)
    _server_thread.start()
    _push_thread = threading.Thread(target=_push_loop, daemon=True)
    _push_thread.start()

def stop():
    """Arrête le serveur ; ferme les clients."""
    global _running, _server_thread, _push_thread
    if not _running:
        return
    _running = False       # le pousseur sort de sa boucle au prochain réveil
    with _lock:
        for sock in list(_clients):
            try: sock.close()
            except Exception: pass
        _clients.clear()
        _subscribed.clear()
    if _server_socket:
        try: _server_socket.close()
        except Exception: pass
    if _server_thread:
        _server_thread.join(timeout=2)
    _server_thread = None
    _push_thread = None    # daemon : pas de join, il dort peut-être tout l'intervalle
    with _lock:
        _status["running"] = False

def is_running():
    return _running

def boot():
    """Démarrage au lancement de l'app : démarre le serveur si activé en réglages.
    Appelé par le boot générique des services (main.py) après init_db()."""
    _load_raw_trees()      # AVANT tout : un équipement éteint au démarrage garde ses chemins
    if settings.get("emberplus_enabled") and not _write_allow_list():
        log.warning("emberplus: AUCUNE restriction d'écriture — n'importe quelle adresse du "
                    "réseau peut écrire sur le matériel. Renseigner `emberplus_write_allow` "
                    "(Réglages → Ember+) avec l'adresse du contrôleur.")
    try:
        if settings.get("emberplus_enabled"):
            port = int(settings.get("emberplus_port") or 9000)
            start(port)
    except Exception as e:
        log.error("emberplus: boot échoué : %s", e)


# ═════════════════════════════════════════════════════════════════════
# Routes API du service (montées par core_plugins.register_all_routes)
# ═════════════════════════════════════════════════════════════════════

def register_routes(bp):
    """Expose les routes propres au service (statut + application des réglages).
    Le service POSSÈDE ses routes : elles ne vivent plus dans app/routes.py."""
    from flask import request, jsonify
    from app.auth import require_login, require_perm
    from app.database import audit_log as _audit_log

    @bp.route("/api/emberplus/status", methods=["GET"])
    @require_login
    def emberplus_status():
        out = status_dict()
        out["enabled_setting"] = bool(settings.get("emberplus_enabled"))
        out["port_setting"] = int(settings.get("emberplus_port") or 9000)
        out["push_interval_setting"] = _push_interval()
        out["slots_count_setting"] = ipg_io.num_slots()
        out["lanes_per_slot_setting"] = ipg_io.lanes_per_slot()
        return jsonify(out)

    @bp.route("/api/emberplus/apply", methods=["POST"])
    @require_perm("settings.edit")
    def emberplus_apply():
        data = request.json or {}
        enabled = bool(data.get("enabled"))
        port = int(data.get("port") or 9000)
        if not (1 <= port <= 65535):
            return jsonify({"error": "port invalide"}), 400
        push = data.get("push_interval")
        if push is not None:
            try:
                push = int(push)
            except (TypeError, ValueError):
                return jsonify({"error": "cadence invalide"}), 400
            if not (PUSH_INTERVAL_MIN_S <= push <= PUSH_INTERVAL_MAX_S):
                return jsonify({"error": "cadence hors bornes (%d–%d s)"
                                % (PUSH_INTERVAL_MIN_S, PUSH_INTERVAL_MAX_S)}), 400
            settings.set("emberplus_push_interval", push)
        # Bornes d'ÉMISSION (§12.9.4). Les AGRANDIR est sûr (on ajoute en fin) ; les réduire
        # fait disparaître des chemins peut-être déjà câblés sur le contrôleur — d'où l'avertissement
        # côté UI, pas un refus : c'est une décision d'exploitation, pas une erreur.
        structure = False
        for key, field, lo, hi, label in (
                ("emberplus_slots_count", "slots_count", 1, ipg_io.SLOT_MAX, "slots"),
                ("emberplus_lanes_per_slot", "lanes_per_slot", 1, ipg_io.LANE_MAX, "voies par slot")):
            v = data.get(field)
            if v is None:
                continue
            try:
                v = int(v)
            except (TypeError, ValueError):
                return jsonify({"error": "nombre de %s invalide" % label}), 400
            if not (lo <= v <= hi):
                return jsonify({"error": "nombre de %s hors bornes (%d–%d)"
                                % (label, lo, hi)}), 400
            settings.set(key, v)
            structure = True
        if structure:
            refresh()          # la taille des grilles change la STRUCTURE → réémettre l'arbre
        settings.set("emberplus_enabled", enabled)
        settings.set("emberplus_port", port)
        if enabled:
            start(port)
        else:
            stop()
        _audit_log("emberplus", "apply", f"enabled={enabled} port={port} push={_push_interval()}",
                   user_id=None, username="système")
        return jsonify(status_dict())

    @bp.route("/api/emberplus/profile", methods=["GET"])
    @require_login
    def emberplus_profile_get():
        """Profil canonique courant + liste des clés (consommé par l'UI d'exposition des
        plugins pour peupler le menu déroulant du « moule IPG »).

        L'URL est CONSERVÉE bien que le catalogue ait déménagé dans la couche IPG : les UI du
        SNP et du Neuron l'appellent en dur (`/api/emberplus/profile`). La casser aurait vidé
        leur menu de mapping — et un menu vide se lit comme « ce matériel ne mappe rien »,
        c'est-à-dire comme la panne qu'on cherche justement à rendre visible."""
        prof = _profile.get_profile()
        return jsonify({"profile": prof, "keys": _profile.keys(prof),
                        "origin": _profile.origin()})

    @bp.route("/api/emberplus/profile", methods=["POST"])
    @require_perm("settings.edit")
    def emberplus_profile_set():
        """RELAIS vers la couche IPG, qui détient le catalogue depuis emberplus 0.19.0.

        Le service n'écrit plus le réglage `emberplus_profile` : il n'en est plus le
        propriétaire, et deux écrivains sur un même catalogue redonneraient la double vérité
        que la 0.17.0 a supprimée pour le slot."""
        data = request.json or {}
        types = _profile.profile_types()
        if not types:
            return jsonify({"error": "aucune couche IPG installée pour recevoir le catalogue"}), 503
        status, out = tools.call(types[0], "ipg/profile", "POST", {"profile": data},
                                 actor="Service Ember+", timeout=10)
        if status != 200:
            return jsonify(out if isinstance(out, dict) else {"error": "refus %s" % status}), status
        _profile.get_profile(force=True)      # le catalogue vient de changer : on ne sert pas l'ancien
        refresh()
        _audit_log("emberplus", "profile",
                   f"maj profil relayée à {types[0]} ({len(data.get('blocks') or [])} blocs)",
                   user_id=None, username="système")
        return jsonify(out)
