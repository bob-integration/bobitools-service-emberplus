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
import tempfile
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
# Ancienne racine du nœud de service. RETIRÉE le 2026-08-12 : le provider se présente
# désormais SOUS la racine du moule (§19), pour qu'on n'ait pas à ouvrir une seconde branche
# afin de savoir à qui l'on parle. Conservée RÉSERVÉE, comme 1010-1016 et 1100 : une racine
# publiée un jour ne se réattribue jamais.
SERVICE_ROOT_ID = 1001
# Bloc « identité de voie », monté par le SERVICE (hors profil). id réservé, très au-dessus des
# blocs du catalogue (1..k) : décrit à qui une voie est affectée, en lecture seule.
IDENTITY_BLOCK_ID = 100
# Blocs SDP de la VOIE (§17). Ids réservés au-dessus du bloc d'identité (100) et très
# au-dessus du catalogue (1..9), donc sans collision possible : la feuille vaut
# `bloc×100 + essence×10 + champ`.
SDP_RX_BLOCK_ID = 101
SDP_TX_BLOCK_ID = 102
_SDP_FIELD_SDP, _SDP_FIELD_PRESENT, _SDP_FIELD_ACTIVE = 1, 2, 3
# Résumé lisible du flux audio (§21). Champ 4 : l'essence en réserve dix (feuille =
# bloc×100 + essence×10 + champ), il en restait six.
_SDP_FIELD_FORMAT = 4
# Plus haute racine attribuable en mode libre. Au-dessus commencent les racines RÉSERVÉES :
# 1000 (moule IPG), 1001 (nœud de service), 1010-1016 (anciennes grilles, jamais réattribuées),
# 1100 (SDP).
ROOT_MAX = 999
# Registre des racines du mode libre : {type d'outil: racine}. Cf. `_assign_roots`.
_ROOTS_SETTING = "emberplus_roots"
# Racines libérées par un DÉPLACEMENT manuel, définitivement hors circulation (cf. `set_root`).
_RETIRED_SETTING = "emberplus_roots_retired"
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
# Écritures refusées parce que le CONTRÔLEUR n'avait pas résolu le chemin (numéro négatif).
# Compté et exposé dans l'état du service : ce défaut-là ne fait aucun bruit tout seul.
_rejets_non_resolus = {"total": 0, "dernier": None}
_server_thread = None
_push_thread = None            # pousseur périodique (cf. _push_loop)
_server_socket = None
_running = False
_notify_timer = None
_notify_pending = False
_status = {
    "running": False, "port": 0, "clients": 0, "subscribed": 0,
    "unresolved_writes": 0, "unresolved_last": None,
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
# Qui a signalé un changement depuis la dernière diffusion (§26). Un seul contributeur
# distinct ⇒ relecture CIBLÉE ; plusieurs, ou un inconnu, ⇒ reconstruction complète.
_notify_qui = set()

_raw_bindings = {}             # {type: réponse ember/bindings}, jumeau de `_raw_trees` : sert
                               # à ne PAS réinterroger un contributeur qui n'a rien signalé
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
    # Temporaire PROPRE à cette écriture, dans le même répertoire (le renommage reste donc
    # atomique). Un nom fixe est partagé par toutes les écritures simultanées : la première à
    # renommer fait disparaître le temporaire de la seconde, qui échoue sur « No such file or
    # directory ». Ce writer n'est appelé que depuis l'agrégation, mais le défaut y était
    # latent — il s'est manifesté chez son jumeau (ipg_generique) le 2026-09-19.
    d = os.path.dirname(_TREES_FILE) or "."
    try:
        fd, tmp = tempfile.mkstemp(dir=d, prefix=os.path.basename(_TREES_FILE) + ".", suffix=".tmp")
    except Exception as e:
        log.warning("emberplus: écriture de %s échouée : %s", _TREES_FILE, e)
        return
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(_raw_trees, f, ensure_ascii=False)
        os.replace(tmp, _TREES_FILE)
        _raw_trees_dirty = False
    except Exception as e:
        try:
            os.unlink(tmp)              # pas de temporaire abandonné à côté du fichier
        except OSError:
            pass
        log.warning("emberplus: écriture de %s échouée : %s", _TREES_FILE, e)


def _ember_candidates():
    """Types déclarant `ember: true`, DÉSACTIVÉS COMPRIS : un outil désactivé garde sa racine
    réservée. La lui reprendre reviendrait à la donner à un autre, et le jour où on le
    réactive il atterrirait ailleurs — un aller-retour sur une case à cocher ne doit pas
    déplacer un sous-arbre."""
    return sorted({m.get("type") for m in _plugins.all() if m.get("ember") and m.get("type")})


def _roots_registry():
    """Registre {type: racine} tel qu'il est persisté, nettoyé de ce qui est inexploitable
    (racine hors plan, doublon). Les entrées d'outils DÉSINSTALLÉS sont conservées : leur
    numéro reste pris."""
    raw = settings.get(_ROOTS_SETTING)
    out, taken = {}, set()
    if isinstance(raw, dict):
        for t, n in sorted(raw.items()):
            try:
                n = int(n)
            except (TypeError, ValueError):
                continue
            if isinstance(t, str) and t and 1 <= n <= ROOT_MAX and n not in taken:
                out[t] = n
                taken.add(n)
    return out


def _assign_roots():
    """Registre à jour des racines du mode libre, attribuées UNE FOIS et persistées.

    Pourquoi ce registre existe : la racine d'un contributeur se déduisait de son RANG
    ALPHABÉTIQUE parmi les outils déclarant `ember: true`. Installer un outil renumérotait
    donc tout ce qui le suit — constaté le 2026-08-12, l'arrivée de `ptz` a déplacé le
    sous-arbre de `switch_ports` de la racine 2 à la 3, et tout câblage VSM posé dessus
    pointait dans le vide. Un ordre alphabétique n'est pas un plan de numérotation : il
    change quand le parc d'outils change, c'est-à-dire au pire moment.

    Trois règles, dans cet ordre d'importance :
      1. une racine attribuée ne bouge JAMAIS ;
      2. une racine libérée n'est JAMAIS réattribuée — un trou se voit, alors qu'un numéro
         recyclé donnerait en silence le câblage d'un outil au sous-arbre d'un autre ;
      3. un nouveau venu prend le plus petit numéro libre.

    Amorçage : le registre n'existe pas sur les instances en service, et le reconstituer par
    ordre alphabétique figerait justement le décalage qu'on vient de subir. Les outils DÉJÀ
    VUS dans l'arbre (ceux dont `_raw_trees` porte un sous-arbre, donc les seuls qui aient pu
    être câblés au contrôleur) passent devant les nouveaux venus, et gardent ainsi le numéro
    qu'ils avaient. D'où l'ordre du boot : `_load_raw_trees()` AVANT toute agrégation."""
    reg = _roots_registry()
    taken = set(reg.values()) | _retired_roots()
    manquants = [t for t in _ember_candidates() if t not in reg]
    if not manquants:
        return reg
    vus = [t for t in manquants if t in _raw_trees]
    for t in vus + [t for t in manquants if t not in _raw_trees]:
        n = next((i for i in range(1, ROOT_MAX + 1) if i not in taken), None)
        if n is None:
            log.warning("emberplus: aucune racine libre (1..%d) — %s ne sera pas exposé",
                        ROOT_MAX, t)
            continue
        reg[t] = n
        taken.add(n)
        log.info("emberplus: racine %d attribuée à %s (définitive)", n, t)
    settings.set(_ROOTS_SETTING, reg)
    return reg


def _retired_roots():
    """Racines RETIRÉES : elles ont porté un sous-arbre, puis leur outil a été déplacé
    ailleurs. Elles ne sont jamais réattribuées — sans ça, un déplacement rendrait le numéro
    disponible pour un nouveau venu, qui hériterait en silence du câblage laissé sur place.
    C'est la règle 2 du registre, appliquée au cas où c'est l'exploitant qui déplace."""
    raw = settings.get(_RETIRED_SETTING)
    out = set()
    if isinstance(raw, list):
        for n in raw:
            try:
                n = int(n)
            except (TypeError, ValueError):
                continue
            if 1 <= n <= ROOT_MAX:
                out.add(n)
    return out


def set_root(type_, n):
    """Impose la racine d'un contributeur. Renvoie (ok, message d'erreur).

    Le déplacement est un ACTE D'EXPLOITATION, jamais une conséquence d'autre chose : il
    déplace le sous-arbre chez le contrôleur, donc il se demande explicitement. Il sert le cas
    où un contrôleur est déjà câblé sur un numéro qu'on veut voir occupé par tel outil."""
    if type_ not in _ember_candidates():
        return False, "outil inconnu, ou ne déclarant pas `ember: true`"
    if not (1 <= n <= ROOT_MAX):
        return False, "racine hors plan (1..%d)" % ROOT_MAX
    reg = _assign_roots()
    occupant = {v: k for k, v in reg.items()}.get(n)
    if occupant is not None and occupant != type_:
        return False, "racine %d déjà attribuée à « %s »" % (n, occupant)
    if n in _retired_roots():
        return False, ("racine %d retirée du service : elle a déjà porté un sous-arbre, "
                       "la réattribuer donnerait un câblage existant à un autre outil" % n)
    ancienne = reg.get(type_)
    if ancienne == n:
        return True, None
    reg[type_] = n
    settings.set(_ROOTS_SETTING, reg)
    if ancienne is not None:
        settings.set(_RETIRED_SETTING, sorted(_retired_roots() | {ancienne}))
        log.info("emberplus: %s déplacé de la racine %d à %d ; %d retirée du service",
                 type_, ancienne, n, ancienne)
    else:
        log.info("emberplus: racine %d imposée à %s", n, type_)
    return True, None


def _ember_roots():
    """[(racine, type)] des contributeurs à ÉMETTRE, dans l'ordre des racines."""
    actifs = {t for t in _ember_candidates() if not _plugins.is_disabled(t)}
    return sorted((n, t) for t, n in _assign_roots().items() if t in actifs)


def _ember_root(type_):
    """Racine d'un contributeur, ou None s'il n'en a pas."""
    return _assign_roots().get(type_)

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
                   "real": 0.0, "float": 0.0, "enum": None}

# ─── Le « rien » d'un enum a enfin un nom (§22) ─────────────────────────────
# Une valeur d'enum inconnue retombait sur l'INDEX 0, donc sur la première étiquette du
# catalogue : « 1080i50 » s'affichait sur les 512 voies d'un parc où personne ne publiait de
# format vidéo. Un contrôleur ne peut pas distinguer ça d'une mesure — c'est le pire des deux
# mondes (§5), et l'exploitant l'a signalé le 2026-08-19.
#
# On AJOUTE donc une étiquette sentinelle à la FIN de chaque énumération émise. « En fin » est
# ce qui rend l'opération sûre : les index existants ne bougent pas, donc aucune configuration
# de contrôleur ne se met à désigner autre chose. L'index de `NC` lui-même n'est PAS un
# contrat — il se déplace si le catalogue s'enrichit — et il n'a pas à l'être : `NC` n'est
# jamais une valeur que le contrôleur ÉCRIT (cf. `_canon_enum_write` : elle est refusée), donc
# jamais une valeur qu'il enregistre.
ENUM_UNKNOWN_LABEL = "NC"

# Index de `NC` par chemin, pour les seuls enums INSCRIPTIBLES. Reconstruit avec l'arbre
# (`_build_tree` le vide), donc de même durée de vie que `path_map` et peuplé au même
# endroit. Il ne sert qu'à REFUSER une écriture : un opérateur voit `NC` dans la liste
# déroulante — Ember+ n'a aucun moyen de griser une entrée — et rien n'empêcherait de la
# choisir. L'envoyer au matériel n'aurait aucun sens : `NC` est notre mot, pas le sien.
_enum_nc = {}


def _enum_labels(res):
    """Énumération ÉMISE : celle du catalogue, plus la sentinelle `NC` en fin. Rendue vide si
    le catalogue n'en déclare pas — un paramètre sans énumération n'est pas un enum."""
    base = res.get("enum") or []
    return list(base) + [ENUM_UNKNOWN_LABEL] if base else []


def _enum_unknown(res):
    """Index de la sentinelle `NC` pour ce paramètre, ou 0 s'il n'a pas d'énumération (auquel
    cas rien ne s'affiche de toute façon : sans `enumeration`, VSM rend l'entier brut)."""
    return len(res.get("enum") or [])


def _enum_index(res, value):
    """Index canonique d'une valeur d'enum. Un contributeur publie soit l'INDEX déjà résolu,
    soit la VALEUR du device — les deux se rencontrent dans le parc, selon qu'un `enum_map`
    est aligné ou non dans le modèle d'exposition. Le SNP résout de son côté quand la clé en
    porte un ; sans `enum_map`, la chaîne arrive telle quelle et c'est ici qu'elle se résout.

    ⚠ Avant le 2026-08-17 la chaîne partait droit dans `int()`, et le `except` la ramenait à 0
    SANS UN MOT : « Input Source » annonçait BNC sur les 27 voies réellement en IP, et le
    retour d'un SetValue semblait ne jamais arriver — l'écriture passait pourtant, elle
    emprunte `path_map`. Un aplatissement muet sur une surface de contrôle est le pire des
    deux mondes : le contrôleur affiche une valeur fausse avec l'aplomb d'une vraie.

    Le repli reste — la FORME de l'arbre ne doit pas dépendre d'une valeur live (§5) — mais il
    ne vaut plus 0 : il vaut `NC` (§22). Un aplatissement sur l'index 0 rendait la PREMIÈRE
    étiquette du catalogue, indiscernable d'une mesure ; la sentinelle, elle, dit ce qu'elle
    est. Il se journalise toujours, comme le fait le SNP pour ses propres enums hors mapping."""
    # `None` / chaîne vide / False : absence de valeur. Ce n'est PLUS l'index 0 — c'est
    # exactement le cas que l'exploitant voulait voir cesser (une voie libre, ou un matériel
    # qui n'expose pas la clé, annonçait « 1080i50 »). Silencieux, en revanche : ce n'est pas
    # une anomalie, seulement un blanc, et le journaliser noierait le cas qui mérite de se voir.
    if value is None or value == "" or isinstance(value, bool):
        return _enum_unknown(res)
    try:
        return int(value)
    except (TypeError, ValueError):
        pass
    s = str(value).strip().lower()
    for i, v in enumerate(res.get("enum") or []):
        if str(v).strip().lower() == s:
            return i
    log.info("emberplus: enum hors catalogue %s=%r → NC", res.get("param_label"), value)
    return _enum_unknown(res)


def _desc_bloc(dpref, bloc_ident, libelle):
    """Description d'une feuille de catalogue : « L01 Color Gain R » (§23.4).

    Le bloc vient de l'IDENTIFIANT (`block_ident`) et non du libellé : c'est ce qui fait trier
    la description exactement comme l'identifiant — `L01_Color_GainR` ↔ « L01 Color Gain R » —
    et le tri alphabétique d'un contrôleur regroupe alors chaque bloc au lieu d'éparpiller le
    correcteur couleur entre « Black Level R » (sous B), « Gain R » (G) et « Luma » (L).

    Il n'est PAS répété quand le libellé l'ouvre déjà : la surcharge en service nomme
    `audio.delay` « Audio Delay » et `Entree.Source d entree` « Input Source », qui donneraient
    « L01 Audio Audio Delay » et « L01 Input Input Source ». Le mot est le même, le dire deux
    fois n'ajoute rien au classement et coûte une ligne illisible."""
    nu, bloc = libelle.strip(), bloc_ident.strip()
    if nu.lower() == bloc.lower() or nu.lower().startswith(bloc.lower() + " "):
        return dpref + nu
    return "%s%s %s" % (dpref, bloc, nu)


# Feuilles SDP RX annoncées inscriptibles alors qu'AUCUNE route ne leur correspond (§24) :
# { chemin : motif }. Même durée de vie que `path_map` et `_enum_nc`, reconstruit avec l'arbre.
# Sert à refuser l'écriture EN DISANT POURQUOI, au lieu du « inconnu ou lecture seule » générique.
_sdp_rx_noref = {}

# Texte SDP tel que le CONTRÔLEUR l'a écrit, par chemin de feuille (§25). Ce n'est PAS un
# cache de valeur : c'est la demande, gardée pour pouvoir la republier une fois — et seulement
# une fois — que le matériel a confirmé être abonné au MÊME flux. Durée de vie plus longue que
# `path_map` : une demande survit aux reconstructions d'arbre, elle n'est effacée que par une
# écriture suivante sur la même feuille. Bornée par construction (une entrée par feuille SDP RX).
_sdp_written = {}


def _emit_canon_param(elements, path_map, ppath, res, value, ref, minimum, maximum, ident=None,
                      desc=None):
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
        value = _enum_index(res, value)
    # IDENTIFIANT et DESCRIPTION sont deux champs distincts, et on s'en sert enfin comme tel :
    # l'identifiant est machine (« L01_Color_GainR » — anglais, sans espace ni accent, c'est lui
    # qui se retrouve dans une configuration de contrôleur), la description est humaine
    # (« L01 Gain R »). Jusqu'ici le libellé servait aux deux, ce qui mettait des accents et des
    # espaces dans des chemins censés être stables.
    #
    # `desc` est le libellé COMPLET, préfixé de la voie ou du slot par l'appelant (§23) ; à
    # défaut on retombe sur le libellé nu du catalogue, ce qu'émettait la version d'avant.
    el = (ppath, "param", ident or res["param_label"],
          desc or (res["param_label"] if ident else ""), value, ptype, writable)
    # Énumération ÉMISE = catalogue + sentinelle `NC` (§22). Elle est la MÊME qu'un device
    # remplisse la clé ou non : c'est ce qui garde l'arbre identique d'une machine à l'autre,
    # promesse du moule. Seule la VALEUR distingue « je ne sais pas » d'une mesure.
    enumeration = _enum_labels(res) if res["type"] == "enum" else None
    enumeration = enumeration or None
    # `enumeration` doit précéder les bornes, même à None : forme positionnelle attendue.
    if enumeration is not None or minimum is not None or maximum is not None:
        el = el + (enumeration,)
    if minimum is not None or maximum is not None:
        el = el + (minimum, maximum)
    elements.append(el)
    if writable and ref is not None:
        path_map[tuple(ppath)] = ref
        if enumeration:
            _enum_nc[tuple(ppath)] = _enum_unknown(res)


def _append_canonical(elements, path_map, contributors, io_state, seen, seulement=None):
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
    n'émet AUCUN SLOT : mieux vaut une racine sans voies, qui se voit, qu'une grille pleine de
    voies sans un seul paramètre, qui ressemble à un parc éteint. La racine elle-même est
    montée par le nœud de service (§19), qui y publie `IPG_Service_Catalog` à
    « (indisponible) » — c'est justement quand la couche IPG est muette qu'on a besoin de le
    lire depuis le contrôleur broadcast."""
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
    dev_binds = {}       # slot -> { canon_key: binding } de portée ÉQUIPEMENT (§18)
    dev_labels = {}      # slot -> label lisible du device
    for type_ in _bindings_types():
        # Relecture CIBLÉE (§26) : si un seul contributeur a signalé et que ce n'est pas
        # celui-ci, on rejoue son dernier état connu au lieu de l'interroger. Le diff décide
        # toujours de ce qui part sur le fil — on ne saute que l'INTERROGATION, jamais la
        # comparaison. Et la reconstruction périodique (TREE_TTL_S) rattrape tout ce qu'une
        # notification manquée aurait laissé filer.
        if seulement and type_ != seulement and _raw_bindings.get(type_) is not None:
            data = _raw_bindings[type_]
        else:
            status, data = tools.call(type_, "ember/bindings", "GET", actor=EMBER_ACTOR,
                                      timeout=TREE_CALL_TIMEOUT_S)
            if status != 200 or not isinstance(data, dict):
                continue
            _raw_bindings[type_] = data
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
                # `lane: 0` = portée ÉQUIPEMENT (§18) : la valeur décrit le châssis entier.
                # ⚠ Surtout pas `int(b.get("lane") or 1)` : 0 est faux en Python, et l'état PTP
                # d'un SNP se serait retrouvé sur sa seule voie 1 — vrai nulle part.
                brut = b.get("lane")
                try:
                    lane = 1 if brut is None else int(brut)
                except (TypeError, ValueError):
                    continue
                if lane == 0:
                    d = dev_binds.setdefault(slot, {"type": type_, "binds": {}})
                    d["binds"][b.get("key")] = b
                elif 1 <= lane <= ipg_io.LANE_MAX:
                    ch = channels.setdefault((slot, lane), {"type": type_, "binds": {}})
                    ch["binds"][b.get("key")] = b

    # 2. Grille pleine : IPG → Slot → Lane, et TOUS les paramètres à plat dans la lane.
    #
    # ⚠ La profondeur n'est pas un choix esthétique, elle se paie au contrôleur. Constaté par
    # l'exploitant le 2026-07-31 : tout ce qui concerne un IPG doit tenir sous UNE branche,
    # sinon le câblage se fait branche par branche, en glisser-déposer. L'arbre d'avant
    # dispersait un même IPG entre sept racines de grilles, un arbre SDP séparé et des voies
    # pendues à la racine sans niveau slot — soit 288 branches par IPG. Ici, une par IPG.
    #
    # Le nœud d'un IPG s'appelle « Slot01 », JAMAIS du nom du matériel qui l'occupe : le chemin
    # doit survivre à un déménagement de slot (§12.8), sinon une réaffectation casserait tout le
    # câblage du contrôleur. Le nom du matériel vit dans `Ident_Device`, qui est un paramètre et
    # a donc le droit de changer.
    _ensure_node(elements, seen, [IPG_ROOT_ID], prof.get("label") or "IPG")
    nassigned = 0
    for slot in range(1, nslots + 1):
        _ensure_node(elements, seen, [IPG_ROOT_ID, slot], "Slot%02d" % slot)
        _emit_slot_device(elements, path_map, slot, prof, index,
                          io_devices.get(slot), dev_binds.get(slot), dev_labels.get(slot))
        for lane in range(1, nlanes + 1):
            ch = channels.get((slot, lane))
            binds = ch["binds"] if ch else {}
            if ch:
                nassigned += 1
            lpath = [IPG_ROOT_ID, slot, lane]
            _ensure_node(elements, seen, lpath, "L%02d" % lane)
            pref = "L%02d_" % lane          # rappelé sur CHAQUE feuille, cf. plus bas
            # …et la DESCRIPTION porte le MÊME rappel (§23). Ce n'est pas de la redondance :
            # un contrôleur affiche la description et ne retombe sur l'identifiant que faute de
            # description (VÉRIFIÉ sur VSM, cf. `_append_service_node`). Une branche de voie
            # mélangeait donc deux façons de se nommer — « Gain R », qui ne dit pas sa voie, à
            # côté de « L01_SdpRx_Video », qui la dit. Signalé par l'exploitant le 2026-08-26.
            # Le libellé sorti de son arbre, dans une liste de contrôleur broadcast, ne
            # désignait alors plus rien : trente-deux voies portent le même « Gain R ».
            dpref = "L%02d " % lane

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
            # l'exploitant : au contrôleur, une voie doit se reconnaître sans avoir à recouper deux
            # paramètres. Vide tant que le slot n'est pas occupé — un nom sur une voie libre
            # laisserait croire à une affectation.
            natif0 = ((io_dev or {}).get("lanes") or {}).get(lane, {}).get("name") \
                if io_dev else None
            elements.append(([IPG_ROOT_ID, slot, lane, IDENTITY_BLOCK_ID * 100], "param",
                             pref + "Ident", dpref + "Ident",
                             ("%s - %s" % (label, natif0 or ("L%02d" % lane))
                              if label else "") if occupied else "",
                             glow.PT_STRING, False))
            elements.append(([IPG_ROOT_ID, slot, lane, IDENTITY_BLOCK_ID * 100 + 1], "param",
                             pref + "Ident_Assigned", dpref + "Ident Assigned", occupied,
                             glow.PT_BOOLEAN, False))
            elements.append(([IPG_ROOT_ID, slot, lane, IDENTITY_BLOCK_ID * 100 + 2], "param",
                             pref + "Ident_Device", dpref + "Ident Device", label,
                             glow.PT_STRING, False))
            # « Canal natif » : la désignation que le CONSTRUCTEUR donne à cette voie — « A1 »
            # sur un SNP (processeur + position) ou un Neuron (path). C'est elle que
            # l'exploitant lit sur la face avant, donc c'est elle qui doit apparaître ici ;
            # le couple slot/voie ne fait que la situer dans notre plan. Les familles qui ne
            # nomment pas leurs voies (CDE, Newt) retombent sur ce couple.
            natif = ((io_dev or {}).get("lanes") or {}).get(lane, {}).get("name") \
                if io_dev else None
            elements.append(([IPG_ROOT_ID, slot, lane, IDENTITY_BLOCK_ID * 100 + 3], "param",
                             pref + "Ident_Channel", dpref + "Ident Channel",
                             ("%s · slot %d voie %d" % (natif, slot, lane) if natif
                              else "slot %d · voie %d" % (slot, lane)) if occupied else "",
                             glow.PT_STRING, False))

            # Catalogue complet du profil, dans l'ordre. Une voie affectée remplit ce que le
            # device expose (valeur + ref → pilotable) ; le reste, et toute voie libre, tombe
            # au défaut.
            for block in prof.get("blocks") or []:
                # Portée ÉQUIPEMENT (§18) : le bloc décrit le châssis, il est monté UNE fois
                # sous le slot par `_emit_slot_device`. Sans ce filtre il repartait AUSSI dans
                # chacune des voies, où RIEN ne peut le remplir — un binding de portée
                # équipement porte `lane: 0` et va dans `dev_binds`, jamais dans `channels`.
                # `L01_PTP_State` annonçait donc « NC » et `L01_PTP_Master` du vide, juste à
                # côté d'un `S01_PTP_Master` exact : deux vérités pour une, sur 1 536 feuilles
                # (3 × 32 voies × 16 slots). C'est le défaut que le §22 vient de corriger pour
                # les valeurs, à sa racine cette fois — ces feuilles n'auraient jamais dû exister.
                if block.get("scope") == "device":
                    continue
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
                                      ident=pref + res["block_ident"] + "_" + res["param_ident"],
                                      desc=_desc_bloc(dpref, res["block_ident"],
                                                      res["param_label"]))

            # Les SDP de la voie, dans la même branche que tout le reste (§17).
            _emit_lane_sdp(elements, path_map, slot, lane, pref, dpref, io_dev)

    contributors.append({"type": "ipg", "root": IPG_ROOT_ID,
                         "label": "%s (%d slots × %d voies, %d affectée%s)" % (
        prof.get("label") or "IPG", nslots, nlanes, nassigned, "s" if nassigned != 1 else "")})

UI_URL_TTL_S = 300              # l'adresse locale ne change qu'à une reconfiguration réseau
_ui_url_cache = {"ts": 0.0, "url": ""}


def _ui_url():
    """Adresse de l'interface web, telle qu'un opérateur la taperait dans son navigateur.

    Le réglage `emberplus_ui_url` prime : sur une machine à plusieurs interfaces — le cas
    NORMAL ici, gestion d'un côté, média de l'autre — la détection automatique rend l'adresse
    de la route par défaut, qui n'est pas forcément celle par laquelle on joint l'UI. Mieux
    vaut pouvoir la corriger que publier une adresse plausible et fausse.

    À défaut, on demande au noyau quelle source il emploierait pour sortir : une socket UDP
    « connectée » ne fait que résoudre la route, elle n'émet AUCUN paquet (et l'adresse visée
    est TEST-NET-1, réservée à la documentation, donc injoignable par construction)."""
    forced = str(settings.get("emberplus_ui_url") or "").strip()
    if forced:
        return forced
    now = time.monotonic()
    if _ui_url_cache["url"] and (now - _ui_url_cache["ts"]) < UI_URL_TTL_S:
        return _ui_url_cache["url"]
    ip = ""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("192.0.2.1", 9))
            ip = s.getsockname()[0]
        finally:
            s.close()
    except Exception as e:
        log.debug("emberplus: adresse locale indéterminable : %s", e)
    url = "http://%s:%d" % (ip, config.HTTP_PORT) if ip else ""
    _ui_url_cache.update({"ts": now, "url": url})
    return url


def _emit_slot_device(elements, path_map, slot, prof, index, io_dev, binds, label_fallback):
    """Ce qui décrit l'IPG ENTIER, à plat sous son nœud de slot (§18).

    Manquait depuis le début : tout le catalogue était implicitement PAR VOIE, si bien que
    l'état PTP d'un châssis ou son adresse de gestion n'avaient nulle part où aller. Un SNP
    aurait dû publier trente-deux fois la même valeur — ce que personne n'a fait, et c'est une
    des raisons pour lesquelles les blocs 8 et 9 sont restés vides.

    Deux origines distinctes, et il faut les garder distinctes :
      · l'IDENTITÉ (ids 10000+) vient du SERVICE, qui la connaît par `ember/io` — nom, famille,
        occupation. Aucun plugin n'a à la mapper, exactement comme pour la voie ;
      · le CATALOGUE de portée `device` (blocs 10-11) vient du MATÉRIEL, par des bindings
        portant `"lane": 0`.

    Les feuilles sont montées à PLAT sous `Slot01`, à côté des nœuds de voies : leurs ids
    (bloc×100 + param, donc ≥ 1001) ne peuvent pas entrer en collision avec les numéros de
    voies (1..50), et l'exploitant voit l'état de l'IPG en ouvrant le slot, sans descendre."""
    pref = "S%02d_" % slot
    dpref = "S%02d " % slot         # côté description, même rappel que pour la voie (§23)
    base = [IPG_ROOT_ID, slot]
    entree = binds or {}
    btype = entree.get("type")          # outil à qui adresser une écriture (cf. `ref`)
    binds = entree.get("binds") or {}
    occupied = io_dev is not None or bool(binds)
    label = ((io_dev or {}).get("label") or label_fallback or "") if occupied else ""
    # La famille se lit dans l'identité GLOBALE publiée par la couche IPG (« snp:3 ») : c'est
    # elle qui distingue deux matériels que leurs plugins numérotent pareil. Le type d'outil
    # contributeur, lui, vaut `ipg_generique` pour tout le monde depuis le §12.11 — l'afficher
    # ne dirait rien à personne au contrôleur.
    ident_global = str((io_dev or {}).get("device") or "")
    famille = ident_global.split(":")[0] if ":" in ident_global else ""
    elements.append((base + [IDENTITY_BLOCK_ID * 100], "param", pref + "Ident", dpref + "Ident",
                     label, glow.PT_STRING, False))
    elements.append((base + [IDENTITY_BLOCK_ID * 100 + 1], "param", pref + "Ident_Assigned",
                     dpref + "Ident Assigned", occupied, glow.PT_BOOLEAN, False))
    elements.append((base + [IDENTITY_BLOCK_ID * 100 + 2], "param", pref + "Ident_Device",
                     dpref + "Ident Device", label, glow.PT_STRING, False))
    elements.append((base + [IDENTITY_BLOCK_ID * 100 + 3], "param", pref + "Ident_Family",
                     dpref + "Ident Family", famille if occupied else "",
                     glow.PT_STRING, False))

    # Catalogue de portée ÉQUIPEMENT. Grille pleine comme partout : un slot vide publie les
    # mêmes feuilles, aux mêmes chemins, avec les valeurs par défaut.
    for block in prof.get("blocks") or []:
        if block.get("scope") != "device":
            continue
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
                ref = (btype, b.get("ref")) if b.get("ref") is not None else None
            else:
                value = _CANON_DEFAULTS.get(res["type"], "")
                minimum, maximum = res.get("min"), res.get("max")
                ref = None
            _emit_canon_param(elements, path_map, base + [int(bid) * 100 + int(pid)],
                              res, value, ref, minimum, maximum,
                              ident=pref + res["block_ident"] + "_" + res["param_ident"],
                              desc=_desc_bloc(dpref, res["block_ident"],
                                              res["param_label"]))


def _sdp_publie(ppath, blk):
    """Texte publié pour une feuille SDP de RÉCEPTION (§25).

    Le contrôleur tient une écriture pour confirmée quand le provider lui rend SA valeur. Or un
    récepteur ne restitue jamais le SDP qu'on lui a donné : le SNP fabrique son propre transport
    file IS-05 — `o=`, `s=`, `i=` réécrits, `a=recvonly` ajouté, TTL du `c=` ramenée à 32,
    paramètres `fmtp` réordonnés, `b=AS:` perdu. Mesuré le 2026-09-17 : 505 octets écrits,
    493 republiés, pour le MÊME abonnement. La comparaison ne pouvait donc jamais réussir, et le
    contrôleur réécrivait la même valeur toutes les dix à vingt secondes, indéfiniment.

    On republie donc le texte demandé — mais JAMAIS sur la foi de l'écriture. Trois conditions,
    toutes rendues par le MATÉRIEL, et toutes exigées :

      · `present` : le récepteur porte un transport file, donc un abonnement existe ;
      · `enabled` : le matériel déclare cet abonnement ACTIF (`subscription.active`) ;
      · même flux : média, groupe, port et source concordent (`ipg_io.sdp_flow_id`).

    Faute d'une seule, on publie ce que dit le matériel. C'est la règle du §5 tenue à l'endroit
    où elle compte : on n'accuse pas réception d'une commutation qu'on n'a pas constatée — un
    accusé optimiste serait exactement le « faux bouton » du §24.1, transposé à la lecture.

    La demande n'est pas oubliée en cas de désaccord : le matériel peut n'avoir pas encore
    rafraîchi son état. Elle est remplacée à l'écriture suivante sur la même feuille."""
    brut = str(blk.get("sdp") or "")
    voulu = _sdp_written.get(tuple(ppath))
    if voulu is None:
        return brut
    # Le refus d'écho se DIT, une fois par motif et par feuille : sans ça, « le contrôleur
    # réécrit toujours » ne se distingue pas de « on n'a jamais mémorisé sa demande ».
    if not (blk.get("present") and blk.get("enabled")):
        _echo_refus(ppath, "le matériel ne déclare pas l'abonnement actif "
                           "(present=%r, enabled=%r)" % (blk.get("present"), blk.get("enabled")))
        return brut
    ida, idb = ipg_io.sdp_flow_id(voulu), ipg_io.sdp_flow_id(brut)
    if ida is None or idb is None or ida != idb:
        _echo_refus(ppath, "le matériel désigne %r, le contrôleur avait demandé %r" % (idb, ida))
        return brut
    return voulu


_echo_dit = {}          # chemin -> dernier motif journalisé, pour ne pas répéter à chaque poussée


def _echo_refus(ppath, motif):
    cle = tuple(ppath)
    if _echo_dit.get(cle) != motif:
        _echo_dit[cle] = motif
        log.info("emberplus: SDP RX %s republié DU MATÉRIEL — %s (§25)", list(ppath), motif)


def _emit_lane_sdp(elements, path_map, slot, lane, pref, dpref, dev):
    """Les SDP de la voie, À PLAT dans sa branche (§17).

    Ils vivaient sous une racine séparée (1100), ce qui obligeait à câbler un même IPG en deux
    endroits : le §12.13 avait justement regroupé l'arbre pour qu'un glisser-déposé emporte
    tout. La raison invoquée alors — « aucun plugin ne publie l'association voie ↔ signal IP » —
    ne tenait déjà plus : elle se lit dans `lanes[].in/out` (cf. `ipg_io.lane_ip_essence`).

    GRILLE PLEINE, comme le reste de la voie : les feuilles existent même sans device, vides.
    C'est le TEXTE d'un SDP qui pèse (1 à 2 ko), pas la feuille — le chemin reste donc
    déterministe et VSM se câble avant que le matériel soit là.

    Écriture : le SDP d'un RÉCEPTEUR est inscriptible, l'écrire EST l'acte de routage. Sur un
    émetteur il décrit ce qu'on produit, donc lecture seule."""
    for bid, direction in ((SDP_RX_BLOCK_ID, ipg_io.SDP_DIR_RX),
                           (SDP_TX_BLOCK_ID, ipg_io.SDP_DIR_TX)):
        tag = "SdpRx" if direction == ipg_io.SDP_DIR_RX else "SdpTx"
        sens = tag[3:].upper()          # « RX » / « TX », côté description
        for essence in ipg_io.ESSENCES:
            blk = ipg_io.lane_ip_essence(dev, lane, essence, direction) or {}
            ref = blk.get("ref")
            eid = ipg_io.ESSENCE_ID[essence] * 10
            ident = "%s%s_%s" % (pref, tag, essence.capitalize())
            # Description COMPLÈTE — « L01 SDP RX Vidéo ». Les feuilles SDP étaient les
            # seules, avec les identités, à n'en porter aucune : le contrôleur y recopiait
            # l'identifiant, et c'est ce qui faisait cohabiter deux nommages dans la même
            # branche (§23). Elles se lisent maintenant comme le reste du catalogue.
            #
            # DEUX libellés, et la frontière est le besoin d'exploitation (§23.3).
            #
            # Un contrôleur trie sa liste sur la description : « SDP » en tête regroupe ce qui
            # le porte. Mais il ne va QUE sur les feuilles qui portent effectivement un texte
            # SDP — huit par voie, quatre essences × deux sens. Les mettre sur les vingt-quatre
            # feuilles noyait ces huit-là au milieu de leurs propres attributs (« SDP RX Audio 1
            # Active » se glisse entre « SDP RX ANC » et « SDP RX Audio 2 »), c'est-à-dire
            # exactement le groupement que l'exploitant demandait, défait par son propre excès.
            #
            # `Present`, `Active` et `Format` décrivent le FLUX, pas le SDP : ils restent au
            # sens (« L01 RX Video Present »). C'est une entorse assumée à la règle « la
            # description trie comme l'identifiant » — ici c'est VSM qui commande, et ce qu'on
            # y attrape ensemble, ce sont les huit champs SDP.
            dlab = "%sSDP %s %s" % (dpref, sens, ipg_io.ESSENCE_LABEL[essence])
            dattr = "%s%s %s" % (dpref, sens, ipg_io.ESSENCE_LABEL[essence])
            base = [IPG_ROOT_ID, slot, lane]
            # TOUJOURS inscriptible en réception (§24), qu'une route existe ou non.
            #
            # Le drapeau valait `ref is not None`, donc il suivait une valeur LIVE — la
            # corrélation d'un flux NMOS au récepteur. Deux conséquences, et la seconde est
            # rédhibitoire. D'abord la forme de l'arbre se mettait à dépendre d'une mesure,
            # ce que le §5 interdit, et un contrôleur qui MÉMORISE garde la feuille grise
            # même après le retour du flux. Ensuite et surtout : au contrôleur, on ne peut
            # pas déposer un SDP sur un paramètre en lecture seule. Le drapeau n'était donc
            # pas un affichage, il INTERDISAIT l'acte de routage — et interdisait du même
            # coup d'armer un abonnement d'avance, qui est précisément l'usage.
            #
            # ⚠ C'est l'inverse de la règle des « faux boutons » de `_emit_canon_param`, et
            # l'inversion est assumée : là-bas le paramètre reste lisible et le faux bouton
            # n'apporte rien ; ici le drapeau EST le seul accès à la feuille. Mieux vaut un
            # levier qui refuse en disant pourquoi qu'un levier absent. Le refus est rendu à
            # `_apply_setvalue` par `_sdp_rx_noref`.
            #
            # SEULE exception : le contributeur DÉCLARE la feuille non inscriptible
            # (`writable: false`, cf. `ipg_io._essence_block`). Ce n'est pas le drapeau live
            # d'autrefois — c'est une configuration, stable, que l'exploitant coche. Elle
            # sert aux matériels qui ne RENDENT PAS le SDP qu'on leur donne : le contrôleur
            # lit alors autre chose que ce qu'il a écrit, conclut à l'échec et recommence
            # sans fin. Mesuré le 2026-09-19 sur l'ANC du Neuron : 1 366 tentatives en une
            # demi-heure sur une seule entrée. Mieux vaut une feuille grise qu'une boucle.
            w_sdp = (direction == ipg_io.SDP_DIR_RX
                     and blk.get("writable") is not False)
            p = base + [bid * 100 + eid + _SDP_FIELD_SDP]
            valeur = _sdp_publie(p, blk) if w_sdp else str(blk.get("sdp") or "")
            elements.append((p, "param", ident, dlab, valeur, glow.PT_STRING, w_sdp))
            if w_sdp and ref is not None:
                path_map[tuple(p)] = (dev["type"], ref, "sdp")
            elif w_sdp:
                _sdp_rx_noref[tuple(p)] = ("aucun matériel sur ce slot" if dev is None else
                                           "aucun récepteur NMOS corrélé à cette voie")
            elements.append((base + [bid * 100 + eid + _SDP_FIELD_PRESENT], "param",
                             ident + "Present", dattr + " Present", bool(blk.get("present")),
                             glow.PT_BOOLEAN, False))
            w_en = ref is not None and blk.get("enabled") is not None
            p = base + [bid * 100 + eid + _SDP_FIELD_ACTIVE]
            elements.append((p, "param", ident + "Active", dattr + " Active",
                             bool(blk.get("enabled")), glow.PT_BOOLEAN, bool(w_en)))
            if w_en:
                path_map[tuple(p)] = (dev["type"], ref, "enabled")
            # « 48 kHz / 24 bits / 8 ch » (§21). Sur les essences AUDIO seulement : la vidéo a
            # déjà ses formats au catalogue, et l'ANC n'a pas de format à dire — leur ajouter
            # une feuille vide coûterait 2 048 éléments pour rien.
            if essence in ipg_io.ESSENCES_AUDIO:
                elements.append((base + [bid * 100 + eid + _SDP_FIELD_FORMAT], "param",
                                 ident + "Format", dattr + " Format",
                                 ipg_io.audio_sdp_summary(blk.get("sdp")),
                                 glow.PT_STRING, False))


_EMPTY_IO = {"devices": {}, "by_slot": {}, "slots": {}, "numbers": {}}

_io_refresh = {"en_cours": False}


def _io_collect_now():
    """Collecte `ember/io` et range le résultat. Ne lève jamais."""
    try:
        state = ipg_io.collect()
    except Exception as e:
        log.warning("emberplus: collecte ember/io échouée : %s", e)
        return None
    with _tree_lock:
        _tree_cache["io"] = state
        _tree_cache["io_ts"] = time.monotonic()
    return state


def _io_state(force=False):
    """État `ember/io` du parc, avec sa péremption propre (IO_TTL_S). Ne lève jamais : un
    échec de collecte rend le dernier état connu plutôt que de vider les grilles — un
    contributeur momentanément muet ne doit pas faire disparaître ses crosspoints du contrôleur.

    La collecte est ASYNCHRONE dès qu'on a déjà un état : elle pèse plusieurs secondes (6,1 s
    mesurées sur un parc de production le 2026-09-17) et elle se faisait dans le fil qui sert le
    contrôleur. Résultat : trois écritures de tally envoyées à 4 s d'intervalle étaient traitées
    D'UN BLOC 13 s plus tard. Un état de grille vieux de quelques secondes ne gêne personne ;
    un tally en retard de 13 s, si. Le premier appel, lui, attend (il n'y a rien à servir)."""
    with _tree_lock:
        state = _tree_cache.get("io")
        fresh = state is not None and (time.monotonic() - (_tree_cache.get("io_ts") or 0)) < IO_TTL_S
        if not fresh and state is not None and not force and not _io_refresh["en_cours"]:
            _io_refresh["en_cours"] = True
            lancer = True
        else:
            lancer = False
    if lancer:
        def _fond():
            try:
                _io_collect_now()
            finally:
                with _tree_lock:
                    _io_refresh["en_cours"] = False
        threading.Thread(target=_fond, name="emberplus-io", daemon=True).start()
    if state is not None and (fresh or not force):
        return state
    return _io_collect_now() or state or dict(_EMPTY_IO)


def _build_tree(seulement=None):
    """Agrège les sous-arbres : (body racine, path_map, matrix_map, contributors).
    Deux voies coexistent : mode LIBRE (ember/tree, monté par plugin) + mode CANONIQUE
    (ember/bindings, monté par slot sous la racine IPG). Le body racine contient
    nœuds/params + matrices en CONTENTS-SEULS (annonce sans le payload, pour éviter la
    déconnexion VSM ; les axes/connexions viennent au GetDirectory)."""
    elements = []
    path_map = {}
    matrix_map = {}
    contributors = []
    _enum_nc.clear()    # garde-fou d'écriture des enums (§22) : même durée de vie que path_map
    _sdp_rx_noref.clear()   # idem, pour le refus motivé d'un SDP RX sans récepteur (§24)
    seen = set()        # nœuds déjà montés, PARTAGÉ : le nœud de service crée la racine du
                        # moule quand la couche IPG est muette, et ne la duplique pas sinon
    global _raw_trees_dirty
    for idx, type_ in _ember_roots():
        # Même règle qu'aux bindings (§26) : un contributeur qui n'a rien signalé n'est pas
        # réinterrogé, son dernier sous-arbre connu est rejoué. C'est EXACTEMENT ce que fait
        # déjà le repli d'un contributeur muet, juste en dessous — on s'en sert à froid.
        if seulement and type_ != seulement and isinstance(_raw_trees.get(type_), dict):
            status, data = 200, _raw_trees[type_]
        else:
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
        # `root` : le numéro de racine est ce que le contrôleur voit, et il est désormais
        # stable — donc il vaut la peine d'être LISIBLE sans ouvrir la base. `movable` le
        # distingue des racines FIXES (moule IPG, service, SDP), qui ne se déplacent pas.
        contributors.append({"type": type_, "label": label, "root": idx, "movable": True})
    # État des entrées/sorties du parc : il sert au moule (résolution des slots), aux six
    # grilles de flux, à l'arbre des SDP et à la grille d'affectation. Une seule collecte,
    # avec sa propre péremption (cf. IO_TTL_S) — c'est de loin le contributeur le plus lourd.
    io_state = _io_state()
    try:
        _append_canonical(elements, path_map, contributors, io_state, seen, seulement)
    except Exception as e:
        log.warning("emberplus: agrégation canonique (IPG) échouée : %s", e)
    # ⚠ Les GRILLES DE FLUX Ember+ ont été retirées le 2026-07-31 : le routage des signaux et
    # l'affectation des slots passent désormais par SW-P-08 (§15). Les garder aurait entretenu
    # deux vérités sur le même crosspoint — et c'est justement pour ne PAS câbler mille
    # paramètres à la main au contrôleur qu'on a choisi un protocole de routeur.
    #
    # `ipg_io.apply_connect` RESTE, et ne doit pas partir avec : c'est elle que le service
    # SW-P-08 appelle pour appliquer un croisement. Seule l'EXPOSITION en matrices disparaît.
    #
    # ⚠ L'ARBRE SDP (racine 1100) a été retiré le 2026-08-12 : les SDP vivent désormais DANS
    # la voie qui les porte (§17), avec le reste de l'IPG. Il n'était resté séparé que parce
    # qu'on croyait l'association voie ↔ signal IP absente du contrat ; elle y était.
    # La racine 1100 reste RÉSERVÉE, comme 1010-1016 : la réattribuer casserait des chemins.
    try:
        _append_service_node(elements, path_map, seen)
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

def _eager_paths():
    """Chemins dont un GetDirectory rend le SOUS-ARBRE ENTIER, et non les seuls enfants directs.

    Réglage `emberplus_eager_paths`, une liste de chemins séparés par des virgules ou des
    espaces (« 5.2 », « 5.2, 3.1 »). VIDE PAR DÉFAUT : sans réglage, rien ne change.

    ── Pourquoi ce réglage existe (2026-09-23) ─────────────────────────────────────────
    VSM se lie à l'IDENTIFIANT, pas au numéro : tant qu'il n'a pas parcouru un nœud, il n'a
    aucun numéro de paramètre à écrire et il envoie `-1`. Le provider rejette alors —
    « inconnu ou lecture seule » — et le tally n'arrive jamais. Mesuré : 104 écritures de
    tally vers le multiviewer C100 entre le 16 et le 19 septembre, TOUTES en `[5, 2, n, -1]`,
    toutes rejetées, sans que rien ne fasse de bruit. L'exploitant devait ouvrir la branche
    dans VSM pour que le tally passe — et ouvrir un objet de monitoring faisait retomber le
    précédent, donc un seul à la fois, ce qui n'est pas tenable pour un multiviewer.

    Le remède global existait déjà (`emberplus_lazy_dir` à faux, l'arbre entier à chaque
    GetDirectory) mais il est hors de prix : la branche IPG porte ~21 600 éléments là où les
    objets de monitoring du C100 en portent 421 — un facteur cinquante. On paierait l'IPG
    pour régler un problème de multiviewer, et on réveillerait ce que le §26 a éteint.

    D'où le grain FIN : on empresse un chemin, pas un arbre.

    Un chemin empressé est servi dès qu'on répond à l'un de ses ANCÊTRES — racine comprise.
    VSM reçoit donc les 42 objets et leurs paramètres à sa première descente, sans que
    personne ait à ouvrir quoi que ce soit."""
    brut = str(settings.get("emberplus_eager_paths") or "")
    out = []
    for morceau in brut.replace(",", " ").split():
        try:
            chemin = tuple(int(x) for x in morceau.split(".") if x != "")
        except ValueError:
            continue                      # un chemin illisible est ignoré, jamais deviné
        if chemin:
            out.append(chemin)
    return out


def _dir_batch():
    """Nombre maximal d'éléments par MESSAGE Ember+ dans une réponse de GetDirectory.

    Réglage `emberplus_dir_batch`. 0 = pas de découpage, c'est-à-dire le comportement
    historique : un seul message, si gros soit-il.

    ── Pourquoi (2026-09-23) ───────────────────────────────────────────────────────────
    Le découpage S101 est correct et l'a toujours été : une réponse de 21,7 ko part en 22
    trames de 1 024 octets, réassemblées par le consommateur en UN message. C'est ce message
    que VSM ne digère pas en entier. Mesuré en tronquant nous-mêmes le message et en le
    redécodant : à 4 096 octets on s'arrête au 8e objet de monitoring — et l'exploitant
    constatait exactement que les objets 1 à 7 fonctionnaient et pas les suivants. Le tampon
    de message est donc de l'ordre de 4 ko chez ce contrôleur.

    On répond donc en PLUSIEURS messages, chacun une collection d'éléments qualifiés
    autonome — le format s'y prête, un chemin absolu ne dépend d'aucun contexte. Le réglage
    est un nombre d'éléments et non d'octets : c'est ce qu'on peut borner avant d'encoder, et
    la taille d'un élément varie peu (~51 octets mesurés)."""
    try:
        n = int(settings.get("emberplus_dir_batch") or 0)
    except (TypeError, ValueError):
        return 0
    return n if n > 0 else 0


def _children_bodies(path):
    """Réponse à un GetDirectory, découpée en messages (cf. `_dir_batch`).

    Renvoie une LISTE de corps encodés. Sans découpage réglé, la liste en contient un seul et
    le comportement est identique à ce qu'il était."""
    lot = _dir_batch()
    if not lot:
        return [_children_body(path)]
    fils, extras = _children_parts(path)
    corps = []
    for i in range(0, len(fils), lot):
        tranche = fils[i:i + lot]
        # Les matrices voyagent avec le PREMIER message : elles sont peu nombreuses et
        # doivent arriver avant qu'on parle de leurs axes.
        corps.append(glow.build_collection(tranche, extra=extras if i == 0 else None))
    if not corps:
        corps = [glow.build_collection([], extra=extras)]
    return corps


def _children_body(path):
    """Corps d'un GetDirectory sur UN nœud : ses enfants DIRECTS, et rien d'autre.

    ⚠ Ce que faisait le provider avant le 2026-07-31 : répondre l'ARBRE ENTIER à tout
    GetDirectory, quelle que soit la profondeur demandée. Mesuré avec notre propre lecteur —
    18 secondes et plusieurs mégaoctets pour obtenir les cinq branches de la racine. Un
    consommateur qui descend nœud par nœud recevait donc tout l'arbre à chaque pas, et un
    contrôleur qui se reconnecte le reprenait en entier.

    Les éléments portent leur chemin COMPLET (arbre plat qualifié), donc une tranche s'encode
    exactement comme le tout : on filtre, on encode, rien d'autre à faire."""
    fils, extras = _children_parts(path)
    return glow.build_collection(fils, extra=extras)


def _children_parts(path):
    """(éléments, matrices) d'un GetDirectory — le calcul, sans l'encodage."""
    with _tree_lock:
        elements = list(_tree_cache.get("elements_list") or [])
        matrix_map = dict(_tree_cache.get("matrix_map") or {})
    n = len(path)
    fils = [e for e in elements if len(e[0]) == n + 1 and list(e[0][:n]) == list(path)]
    # Les matrices s'annoncent en CONTENTS-SEULS, comme dans le corps racine : leurs axes et
    # connexions ne partent qu'au GetDirectory qui les vise, sinon le contrôleur se déconnecte.
    extras = [_encode_matrix(p, m, with_axes=False) for p, m in matrix_map.items()
              if len(p) == n + 1 and list(p[:n]) == list(path)]
    # Sous-arbres EMPRESSÉS (cf. `_eager_paths`) : tout ce qui est sous un chemin déclaré,
    # dès qu'on répond à l'un de ses ancêtres. Dédoublonné par chemin — un enfant direct peut
    # déjà figurer dans `fils` quand le chemin empressé est celui qu'on sert.
    vus = {tuple(e[0]) for e in fils}
    for eag in _eager_paths():
        ne = len(eag)
        if ne < n or tuple(eag[:n]) != tuple(path):
            continue                      # ce chemin n'est pas sous celui qu'on sert
        for e in elements:
            if len(e[0]) > ne and tuple(e[0][:ne]) == eag and tuple(e[0]) not in vus:
                vus.add(tuple(e[0]))
                fils.append(e)
        extras += [_encode_matrix(p, m, with_axes=False) for p, m in matrix_map.items()
                   if len(p) > ne and tuple(p[:ne]) == eag]
    return fils, extras


_agg_refresh = {"en_cours": False}


def _ttl_effectif():
    """Péremption réellement appliquée à l'arbre.

    TREE_TTL_S est un plancher, pas une promesse : sur un parc de production une reconstruction
    coûte 7,4 s pour un TTL de 5 s (mesuré le 2026-09-17). L'arbre était donc périmé avant
    d'être fini, le service reconstruisait sans discontinuer, et tout le reste — connexions,
    poussées, écritures du contrôleur — attendait derrière. On laisse au moins trois fois le
    coût de la dernière reconstruction entre deux : le parc qui coûte cher est relu plus
    rarement, celui qui ne coûte rien garde ses 5 s. La fraîcheur ne vient de toute façon pas
    de ce cycle mais des notifications ciblées (`refresh(seulement=…)`)."""
    return max(TREE_TTL_S, 3.0 * (_tree_cache.get("cout") or 0.0))


def _aggregate_now(seulement=None):
    """Reconstruit l'arbre et range le résultat. Renvoie (path_map, matrix_map)."""
    _t0 = time.monotonic()
    elements, path_map, matrix_map, contributors, el_index, io_state = _build_tree(seulement)
    with _tree_lock:
        _tree_cache.update({"ts": time.monotonic(), "body": None, "path_map": path_map,
                            "matrix_map": matrix_map, "contributors": contributors,
                            "elements": el_index, "elements_list": elements, "io": io_state,
                            "cout": time.monotonic() - _t0})
    with _lock:
        _status["contributors"] = contributors
    return path_map, matrix_map


def _reaggregate(force=False, seulement=None):
    """Ré-agrège si le cache est expiré ou si `force`. N'ENCODE RIEN : le corps n'est produit
    qu'à la demande par `_encoded_body()`. Renvoie (path_map, matrix_map).

    Quand un arbre est déjà en cache, la reconstruction part EN TÂCHE DE FOND et l'appelant
    reçoit l'arbre connu. Reconstruire coûte 7,3 s sur un parc de production (mesuré le 2026-09-17)
    et le cache ne vit que TREE_TTL_S : le contrôleur payait donc ces 7 s au hasard de ses
    requêtes, et les écritures qu'il envoyait pendant ce temps attendaient dans le tampon —
    tallys compris. Un arbre vieux de quelques secondes est sans conséquence ; un tally en
    retard de sept secondes ne l'est pas. `force` (recette, diagnostic) attend toujours."""
    with _tree_lock:
        fresh = (time.monotonic() - _tree_cache["ts"]) < _ttl_effectif()
        connu = _tree_cache.get("elements_list") is not None
        if not force and fresh and connu:
            return _tree_cache["path_map"], _tree_cache["matrix_map"]
        differe = connu and not force and not _agg_refresh["en_cours"]
        if differe:
            _agg_refresh["en_cours"] = True
    if connu and not force:
        if differe:
            def _fond():
                try:
                    _aggregate_now(seulement)
                finally:
                    with _tree_lock:
                        _agg_refresh["en_cours"] = False
            threading.Thread(target=_fond, name="emberplus-agg", daemon=True).start()
        with _tree_lock:
            return _tree_cache["path_map"], _tree_cache["matrix_map"]
    return _aggregate_now(seulement)


def _current_tree(force=False):
    """Renvoie (body, path_map, matrix_map). Le corps est encodé à la demande — n'appeler
    que lorsqu'on en a réellement besoin (GetDirectory, push d'arbre complet)."""
    path_map, matrix_map = _reaggregate(force)
    return _encoded_body(), path_map, matrix_map

def _invalidate():
    with _tree_lock:
        _tree_cache["ts"] = 0.0

def _cible_notify(qui):
    """Contributeur à relire pour une notification venue de `qui`, ou None pour tout relire.

    Deux cas, et le second est celui qui compte en exploitation :

      · `qui` EST un contributeur — une racine du mode libre, ou la couche IPG : on le relit
        lui, et c'est direct ;
      · `qui` est une FAMILLE de matériel (snp, newt, cde1922, neuron). Elle ne contribue pas
        à l'arbre directement : elle alimente la couche IPG, qui l'agrège (§12.11). C'est
        pourtant elle qui notifie — le SNP le fait en quelques millisecondes sur son WebSocket
        (§16), et c'est de loin la notification la plus fréquente. Sans cette traduction, la
        relecture ciblée du §26 ne servait justement PAS le cas le plus fréquent.

    La liste des familles n'est PAS codée en dur et ne coûte aucun appel : l'état `ember/io`
    déjà en cache porte le type de chaque matériel du parc. Une famille absente du parc n'est
    pas reconnue — et retombe donc sur la reconstruction complète, ce qui est le bon défaut."""
    if not qui or qui == "?":
        return None
    if qui in {t for _i, t in _ember_roots()} or qui in _bindings_types():
        return qui
    with _tree_lock:
        io_state = _tree_cache.get("io") or {}
    # ⚠ PAS le champ `type` : depuis le §12.11 il vaut `ipg_generique` pour TOUT le parc —
    # c'est le contributeur, pas la famille. La famille est le préfixe de l'identifiant GLOBAL
    # (« snp:2811962d1076 »), le même que celui publié en `S01_Ident_Family`.
    familles = {str((d or {}).get("device") or "").split(":")[0]
                for d in (io_state.get("devices") or {}).values()}
    familles.discard("")
    if qui in familles:
        # La couche IPG est le contributeur qui porte cette famille dans l'arbre.
        return next(iter(_bindings_types()), None)
    return None


def refresh(seulement=None):
    """Force la ré-agrégation et re-pousse aux abonnés (à appeler après un changement).

    `seulement` = type du contributeur qui a signalé, quand on le sait (§26) : les autres ne
    sont pas réinterrogés, leur dernier état connu est rejoué. C'est une OPTIMISATION, jamais
    une source de vérité — la reconstruction périodique repasse sur tout le monde."""
    if seulement:
        with _lock:
            _notify_qui.add(seulement)
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


def _profile_label():
    """Libellé de la racine du moule. Ne lève jamais : le nœud de service doit pouvoir monter
    la racine même quand la couche IPG est muette — c'est justement là qu'on en a besoin."""
    try:
        return _profile.get_profile().get("label") or "IPG"
    except Exception:
        return "IPG"


def _append_service_node(elements, path_map, seen):
    """Monte le nœud de service SOUS LA RACINE DU MOULE (§19) : cadence de poussée (réglable
    depuis le contrôleur broadcast) et compteurs de diagnostic en lecture seule.

    ⚠ Il vivait sur sa propre racine (1001) jusqu'au 2026-08-12. Ce n'était pas un doublon de
    trop, c'était une SECONDE PLACE : un exploitant qui ouvre la branche IPG doit pouvoir dire
    à qui il parle — quelle instance, quelle version, joignable où — sans aller chercher une
    racine voisine. Même raisonnement qu'au §12.13 pour les grilles et qu'au §17 pour les SDP,
    appliqué cette fois à ce qu'on LIT en exploitation. La racine 1001 reste RÉSERVÉE.

    Les identifiants sont refaits au passage (`IPG_Service_*`, anglais, sans accent ni espace) :
    ceux d'avant — « Voies (affectées / total) » — dataient d'avant la règle du §12.13, et les
    chemins changeant de toute façon, les garder n'aurait servi personne.

    La cadence est AUTO-RÉFÉRENTE — elle règle l'intervalle auquel elle est elle-même
    repoussée. Sans danger grâce aux bornes annoncées (1 s–1 h), qui font refuser une saisie
    absurde par le consumer lui-même plutôt qu'après coup. Ce paramètre n'appartient à aucun
    outil : il n'entre donc PAS dans `path_map` (qui route vers un plugin) et son écriture est
    interceptée en amont par `_apply_service_setvalue`."""
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
    by_slot = io_state.get("by_slot") or {}
    nassigned = sum(1 for s in by_slot if 1 <= s <= nslots) * nper
    nocc = sum(1 for s in by_slot if 1 <= s <= nslots)
    # Compté sur l'agrégation en cours, + 1 : la cadence ci-dessous est le seul paramètre
    # inscriptible du nœud de service. Sert à repérer d'un coup d'œil un `writable` mal posé.
    nwrit = sum(1 for el in elements if el[1] == "param" and len(el) > 6 and el[6]) + 1
    # La racine du moule existe déjà si le catalogue a répondu ; sinon on la crée ici, avec le
    # seul nœud de service — c'est justement quand la couche IPG est muette qu'on a besoin de
    # se diagnostiquer depuis le contrôleur.
    _ensure_node(elements, seen, [IPG_ROOT_ID], _profile_label())
    b = IDENTITY_BLOCK_ID * 100     # 10000+ : hors d'atteinte des numéros de slots (1..99)
    # Les `id` ci-dessous sont des CHEMINS de contrôleur : on n'ajoute qu'EN FIN, jamais au
    # milieu. Identifiant seul, description vide — VÉRIFIÉ, VSM recopie l'identifiant faute de
    # description, donc un champ suffit.
    elements.append(([IPG_ROOT_ID, b], "param", "IPG_Service_UpdateInterval", "",
                     _push_interval(), glow.PT_INTEGER, True, None,
                     PUSH_INTERVAL_MIN_S, PUSH_INTERVAL_MAX_S))
    elements.append(([IPG_ROOT_ID, b + 1], "param", "IPG_Service_Subscribers", "",
                     nsub, glow.PT_INTEGER, False))
    elements.append(([IPG_ROOT_ID, b + 2], "param", "IPG_Service_Clients", "",
                     ncli, glow.PT_INTEGER, False))
    elements.append(([IPG_ROOT_ID, b + 3], "param", "IPG_Service_Contributors", "",
                     contribs, glow.PT_STRING, False))
    elements.append(([IPG_ROOT_ID, b + 4], "param", "IPG_Service_Version", "",
                     _service_version(), glow.PT_STRING, False))
    elements.append(([IPG_ROOT_ID, b + 5], "param", "IPG_Service_Uptime", "",
                     _uptime_str(started), glow.PT_STRING, False))
    elements.append(([IPG_ROOT_ID, b + 6], "param", "IPG_Service_Port", "",
                     port, glow.PT_INTEGER, False))
    elements.append(([IPG_ROOT_ID, b + 7], "param", "IPG_Service_Lanes", "",
                     "%d / %d" % (nassigned, nlanes), glow.PT_STRING, False))
    elements.append(([IPG_ROOT_ID, b + 8], "param", "IPG_Service_LastError", "",
                     str(err) if err else "—", glow.PT_STRING, False))
    elements.append(([IPG_ROOT_ID, b + 9], "param", "IPG_Service_LastPush", "",
                     time.strftime("%H:%M:%S", time.localtime(_last_push_ts))
                     if _last_push_ts else "—", glow.PT_STRING, False))
    elements.append(([IPG_ROOT_ID, b + 10], "param", "IPG_Service_Writable", "",
                     nwrit, glow.PT_INTEGER, False))
    # ─── Les trois qui n'existaient nulle part ───────────────────────────────
    elements.append(([IPG_ROOT_ID, b + 11], "param", "IPG_Service_Ui", "",
                     _ui_url() or "—", glow.PT_STRING, False))
    elements.append(([IPG_ROOT_ID, b + 12], "param", "IPG_Service_Host", "",
                     socket.gethostname(), glow.PT_STRING, False))
    # Quel catalogue a bâti CET arbre. La question s'est déjà posée deux fois en exploitation,
    # et les deux fois une surcharge périmée gelait le moule sans que rien ne le dise. Le
    # numéro seul ne suffit pas : « v3 (défaut) » et « v3 (complétée) » ne décrivent pas la
    # même situation, et « (indisponible) » se lit d'un coup d'œil.
    elements.append(([IPG_ROOT_ID, b + 13], "param", "IPG_Service_Catalog", "",
                     "v%s · %s" % (_profile.get_profile().get("version") or "?",
                                    _profile.origin()), glow.PT_STRING, False))
    elements.append(([IPG_ROOT_ID, b + 14], "param", "IPG_Service_Slots", "",
                     "%d / %d" % (nocc, nslots), glow.PT_STRING, False))


def _apply_service_setvalue(path, value):
    """Intercepte une écriture sur le nœud de service. Renvoie True si le chemin lui
    appartient (traité ou refusé), False pour laisser le routage normal opérer."""
    p = tuple(path)
    if len(p) != 2 or p[0] != IPG_ROOT_ID or p[1] < IDENTITY_BLOCK_ID * 100:
        return False
    if p == (IPG_ROOT_ID, IDENTITY_BLOCK_ID * 100):
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
    # Table des chemins DÉJÀ CONNUE, même au-delà de TREE_TTL_S : un chemin inscriptible bouge
    # rarement, alors que reconstruire l'arbre interroge tous les contributeurs (plusieurs
    # secondes sur un parc chargé, et une reconstruction dure souvent plus que le TTL). Le
    # contrôleur attendait donc cette reconstruction à CHAQUE écriture (mesuré le 2026-09-16 :
    # 7 à 12 s pour un libellé). On ne reconstruit que si le chemin est inconnu du cache.
    with _tree_lock:
        entry = (_tree_cache.get("path_map") or {}).get(tuple(path))
    if not entry:
        path_map, _ = _reaggregate()    # tables seules : pas besoin d'encoder l'arbre
        entry = path_map.get(tuple(path))
    if not entry:
        # Un SDP RX est annoncé inscriptible même sans route (§24) : on doit donc dire ce qui
        # manque, sinon la seule trace d'un routage refusé serait « inconnu ou lecture seule »
        # — le message de la faute de frappe, sur l'action la plus courante du contrôleur.
        motif = _sdp_rx_noref.get(tuple(path))
        if motif:
            log.warning("emberplus: SDP RX %s NON appliqué — %s. La feuille est inscriptible "
                        "pour que le contrôleur puisse l'atteindre, mais il n'y a pas de "
                        "récepteur où écrire.", path, motif)
            return False
        # ⚠ Un chemin dont un élément est NÉGATIF n'est pas une feuille inconnue : c'est un
        # chemin que le contrôleur n'a pas su résoudre. VSM se lie à l'identifiant, pas au
        # numéro (cf. `_eager_paths`) : tant qu'il n'a pas parcouru le nœud, il écrit `-1` à
        # la place du numéro de paramètre. Le confondre avec « inconnu ou lecture seule »
        # nous a coûté cher : 104 écritures de tally vers le multiviewer C100, entre le 16 et
        # le 19 septembre 2026, toutes en `[5, 2, n, -1]`, toutes rejetées — et personne ne
        # l'a vu, parce qu'un tally qui n'arrive pas ressemble à un tally éteint.
        if any(int(x) < 0 for x in path):
            with _lock:
                _rejets_non_resolus["total"] += 1
                _rejets_non_resolus["dernier"] = {"chemin": list(path), "quand": time.time()}
            log.warning("emberplus: setvalue %s REFUSÉ — chemin non résolu par le contrôleur "
                        "(numéro négatif). Il écrit dans un nœud qu'il n'a pas parcouru : ce "
                        "n'est pas un paramètre inconnu, c'est une adresse qu'il n'a pas. "
                        "%d depuis le démarrage.", path, _rejets_non_resolus["total"])
            return False
        log.info("emberplus: setvalue %s ignoré (inconnu ou lecture seule)", path)
        return False
    # `NC` est notre mot pour « ce matériel ne le dit pas » (§22), pas une valeur du device.
    # Ember+ ne sait pas griser une entrée d'énumération : elle apparaît donc dans la liste
    # déroulante d'un paramètre inscriptible, et on la refuse ICI plutôt que de laisser chaque
    # famille inventer sa réponse à un index qu'elle ne sait pas traduire.
    nc = _enum_nc.get(tuple(path))
    if nc is not None:
        try:
            recu = int(value)
        except (TypeError, ValueError):
            recu = None
        if recu == nc:
            log.info("emberplus: setvalue %s = NC refusé (valeur non écrivable)", path)
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
        # Au niveau INFO, seuls les SDP (un abonnement : rare, et précieux pour diagnostiquer le
        # contrôleur). Une écriture d'état — tally, libellé — en produit des milliers par jour en
        # production (mesuré le 2026-09-19 : 30 par seconde sans broncher) : elle passe en DEBUG.
        est_sdp = field == "sdp" or (isinstance(value, str) and value.startswith("v=0"))
        (log.info if est_sdp else log.debug)("emberplus: set %s %s = %r", type_, ref, value)
        # Un SDP de réception déposé par le contrôleur est GARDÉ (§25) : le matériel ne
        # restitue pas ce texte-là, il publie le sien. On le republiera à sa place, mais
        # seulement quand le matériel aura dit être abonné au même flux.
        if field == "sdp":
            _sdp_written[tuple(path)] = value
            # Confirmation IMMÉDIATE, avant toute ré-agrégation : c'est la seule façon de
            # tenir les 3 s du « Parameter Timeout » de VSM (§25.5).
            immediat = _broadcast_param(path, value)
            log.info("emberplus: SDP RX %s mémorisé et republié %s (§25)", list(path),
                     "IMMÉDIATEMENT" if immediat else "au prochain cycle (feuille inconnue)")
        # On sait à QUI on vient d'écrire : la ré-agrégation qui suit n'a aucune raison de
        # réinterroger les autres contributeurs (§26). Sous un flux de commutations, c'est la
        # différence entre relire le parc entier à chaque crosspoint et ne relire que l'outil
        # concerné.
        refresh(seulement=type_)
        return True
    log.warning("emberplus: set %s %s → %s %s", type_, ref, status, data)
    return False

_OP_NAME = {glow.CN_OP_ABSOLUTE: "absolute", glow.CN_OP_CONNECT: "connect",
            glow.CN_OP_DISCONNECT: "disconnect"}

def _reload_matrix(type_, mpath):
    """Recharge UNIQUEMENT le contributeur `type_` et renvoie son entrée matrix_map à jour
    pour `mpath`, sans ré-agréger tout l'arbre (donc sans relire les contributeurs lents en
    I/O réseau comme switch_ports). Renvoie None si introuvable/échec → repli sur refresh()."""
    idx = _ember_root(type_)                    # racine du registre, comme _build_tree
    if idx is None:
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
        _status["unresolved_writes"] = _rejets_non_resolus["total"]
        _status["unresolved_last"] = _rejets_non_resolus["dernier"]
        return dict(_status)

# Trame S101 mémorisée. Encadrer 3,5 Mo coûte 3,5 s (mesuré le 2026-09-17) : c'était payé à
# CHAQUE connexion de client et à CHAQUE poussée vers CHAQUE abonné, alors que la trame est
# rigoureusement la même pour tous. On la garde tant que le corps est le même OBJET — le corps
# encodé étant lui-même en cache, la comparaison d'identité suffit et ne coûte rien.
_wire_cache = {"body": None, "wire": None}


def _wire_for(body):
    w = _wire_cache
    if w["body"] is body and w["wire"] is not None:
        return w["wire"]
    wire = glow.s101_encode_ember(body)
    _wire_cache["body"], _wire_cache["wire"] = body, wire
    return wire


def _send_frame(sock, body):
    try:
        wire = _wire_for(body)
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
        # Qui a signalé depuis la dernière diffusion ? UN SEUL contributeur connu ⇒ on ne
        # réinterroge que lui (§26). Plusieurs, ou un « ? », ⇒ tout, comme avant. On ne devine
        # jamais : dans le doute c'est la reconstruction complète, qui reste la référence.
        with _lock:
            qui = set(_notify_qui)
            _notify_qui.clear()
        seulement = next(iter(qui)) if len(qui) == 1 else None
        _, matrix_map = _reaggregate(force=True, seulement=seulement)   # ré-agrège SANS encoder
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

def _broadcast_param(path, valeur):
    """Repousse IMMÉDIATEMENT une seule feuille, avec sa nouvelle valeur (§25.5).

    MESURÉ dans le journal de VSM le 2026-09-17 : il arme un « Parameter Timeout » de 3,0 s
    sur le paramètre qu'il vient d'écrire, et réécrit s'il ne lui revient pas —

        16:42:20.508  écriture de « L24 SDP RX Video »
        16:42:23.508  {Parameter: L24 SDP RX Video} Parameter Timeout
        §§§§ retry: "IPG_3 In_74 Video" << "C100 Head 10"

    Or une ré-agrégation complète coûte 1,7 à 2,7 s (mesuré le même jour), plus 3,4 Mo à
    transmettre. On passait donc parfois sous les 3 s, souvent non : d'où une confirmation
    intermittente et une boucle de réécriture que rien n'arrêtait. Le débit n'était pas en
    cause, le CHEMIN l'était — on faisait reconstruire tout l'arbre pour une feuille.

    Même geste que `_broadcast_matrix` pour un crosspoint : la collection Ember+ est plate et
    à chemins absolus, un seul élément EST une trame de mise à jour valide.

    ⚠ Ce n'est PAS un accusé optimiste. Depuis `snp` 0.42.0 le plugin relit le récepteur et
    compare le groupe réellement actif avant de rendre `ok` : quand on arrive ici, le matériel
    a déjà confirmé. La règle du §25.3 tient donc toujours — on ne republie que constaté.

    L'index est mis à jour du même coup, sans quoi la diffusion incrémentale suivante
    repousserait cet élément comme s'il venait de changer."""
    cle = tuple(path)
    with _tree_lock:
        el = (_tree_cache.get("elements") or {}).get(cle)
    if not el or el[1] != "param":
        return False
    neuf = tuple(el[:4]) + (valeur,) + tuple(el[5:])
    with _tree_lock:
        idx = _tree_cache.get("elements")
        if idx is not None:
            idx[cle] = neuf
        lst = _tree_cache.get("elements_list")
        if lst is not None:
            for i, e in enumerate(lst):
                if tuple(e[0]) == cle:
                    lst[i] = neuf
                    break
        _tree_cache["body"] = None          # le corps mémoïsé porte l'ancienne valeur
    _send_frames([glow.build_collection([neuf])])
    return True


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
        # PAS de poussée spontanée à la connexion. Un provider Ember+ annonce sa racine sur
        # GetDirectory, et le consommateur descend branche par branche (`_children_body`) :
        # lui jeter l'arbre entier — 3,6 Mo ici — n'est demandé par personne. Ça coûtait
        # ~5 s pendant lesquelles ce client n'était pas lu, donc ses premières écritures
        # attendaient : les premiers tallys après chaque reconnexion du contrôleur arrivaient
        # en retard (mesuré le 2026-09-17). Le réglage rallume l'ancien comportement si un
        # contrôleur s'avérait en dépendre.
        if bool(settings.get("emberplus_push_initial")):
            body, _, _ = _current_tree()
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
                for corps in _children_bodies(list(mp)):
                    _send_frame(sock, corps)
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
    from app.auth import require_login, require_perm, current_user
    from app.database import audit_log as _audit_log

    @bp.route("/api/ember/notify", methods=["POST"])
    def ember_notify():
        """Un outil signale qu'il a du NEUF. Le service ré-agrège et diffuse le diff.

        Pourquoi ça existe : le SNP nous notifie ses changements en quelques millisecondes
        (WebSocket, cf. son plugin), mais le service ne le découvrait qu'à son minuteur de
        `emberplus_push_interval` secondes. Ces secondes-là étaient les dernières du parcours.

        Volontairement SANS granularité : l'appelant dit « quelque chose a bougé chez moi », pas
        quoi. Transmettre les objets touchés obligerait le service à traduire une structure
        propre à une famille de matériel en chemins Ember+ — c'est-à-dire à détenir une seconde
        vérité sur ce que le plugin sait déjà. Le diff de `_diff_elements` fait ce tri sans rien
        savoir de personne, et ne pousse que les éléments réellement modifiés.

        Auth : session connectée OU jeton partagé, pour les outils en conteneur. Même motif que
        `/api/mail/send`."""
        jeton = settings.get("emberplus_notify_token") or ""
        entete = request.headers.get("X-BT-Ember-Token") or ""
        if not current_user():
            if not jeton or entete != jeton:
                return jsonify({"error": "non autorisé"}), 401
        # `silent=True` : un appelant qui ne dit pas qui il est reste servi. Refuser un corps
        # absent transformerait une commodité en source de 400 inexplicables.
        qui = ((request.get_json(silent=True) or {}).get("type")) or "?"
        if not is_running():
            return jsonify({"ok": True, "ignored": "service arrêté"})
        # En FOND : la ré-agrégation prend ~0,6 s et l'appelant n'a aucune raison de l'attendre.
        # Il vient de recevoir une notification de son matériel, il a mieux à faire que patienter.
        # `qui` était reçu puis JETÉ : on déclenchait une reconstruction complète alors que
        # l'appelant avait dit d'où venait le changement. On ne lui demande toujours pas CE qui
        # a changé — ce serait la seconde vérité que refuse le §12.11 — seulement d'où, ce
        # qu'il est seul à savoir. Le diff continue de décider ce qui part sur le fil.
        cible = _cible_notify(qui)
        threading.Thread(target=refresh, kwargs={"seulement": cible},
                         daemon=True).start()
        log.info("emberplus: %s signale un changement → ré-agrégation %s", qui,
                 ("ciblée sur %s" % cible) if cible else "complète (contributeur non reconnu)")
        return jsonify({"ok": True})

    @bp.route("/api/emberplus/status", methods=["GET"])
    @require_login
    def emberplus_status():
        out = status_dict()
        out["enabled_setting"] = bool(settings.get("emberplus_enabled"))
        out["port_setting"] = int(settings.get("emberplus_port") or 9000)
        out["push_interval_setting"] = _push_interval()
        out["slots_count_setting"] = ipg_io.num_slots()
        out["lanes_per_slot_setting"] = ipg_io.lanes_per_slot()
        out["ui_url_setting"] = str(settings.get("emberplus_ui_url") or "")
        out["ui_url"] = _ui_url()       # ce qui est réellement publié, détecté ou forcé
        return jsonify(out)

    @bp.route("/api/emberplus/roots", methods=["POST"])
    @require_perm("settings.edit")
    def emberplus_roots_set():
        """Impose la racine d'un contributeur du mode libre (§16).

        Sert le cas où un contrôleur est DÉJÀ câblé sur un numéro donné : le registre garantit
        qu'une racine ne bouge plus toute seule, il fallait encore pouvoir en choisir une. Le
        refus de collision et le retrait de l'ancien numéro sont dans `set_root` — c'est là que
        vit la règle, pas dans la route."""
        data = request.get_json(silent=True) or {}
        type_ = str(data.get("type") or "")
        try:
            n = int(data.get("root"))
        except (TypeError, ValueError):
            return jsonify({"error": "racine invalide"}), 400
        ok, err = set_root(type_, n)
        if not ok:
            return jsonify({"error": err}), 400
        _audit_log("emberplus", "root_set", "%s → %d" % (type_, n),
                   user_id=None, username=(current_user() or {}).get("username") or "système")
        refresh()               # le sous-arbre change de place : il faut réémettre
        _reaggregate(force=True)   # et RENDRE l'état neuf : `refresh` ne fait qu'invalider,
        return jsonify(status_dict())   # donc sans ça l'écran réafficherait l'ancien numéro

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
        # Adresse de l'UI publiée à la racine du moule (§19). Un schéma manquant est AJOUTÉ
        # plutôt que refusé : « 192.168.1.10:5000 » est ce qu'un exploitant tape naturellement,
        # et le renvoyer en erreur pour un `http://` absent serait de la pédanterie.
        ui = data.get("ui_url")
        if ui is not None:
            ui = str(ui).strip()
            if ui and "://" not in ui:
                ui = "http://" + ui
            if len(ui) > 200:
                return jsonify({"error": "adresse d'interface trop longue"}), 400
            if str(settings.get("emberplus_ui_url") or "") != ui:
                settings.set("emberplus_ui_url", ui)
                _ui_url_cache["ts"] = 0.0      # forcer la relecture, forcée comme détectée
                structure = True               # la valeur publiée change → réémettre
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
