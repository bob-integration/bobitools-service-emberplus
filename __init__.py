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
import socket
import threading
import time

from app import settings, tools
from app import plugins as _plugins
from app.database import audit_log
from . import emberplus_glow as glow
from . import profile as _profile

log = logging.getLogger(__name__)

# Acteur virtuel attribué dans l'audit pour toute modification venant d'Ember+.
EMBER_ACTOR = "Service Ember+"

# Racine FIXE du « moule IPG » (mode canonique, monté par slot). Valeur haute et stable,
# distincte des racines par-plugin (1..k) : garantit un chemin VSM stable quels que soient
# les plugins activés. Structure sous cette racine : [IPG, slot, voie, bloc, param].
IPG_ROOT_ID = 1000

NOTIFY_DEBOUNCE_S = 1.0        # max 1 broadcast / seconde
TREE_TTL_S = 5.0               # ré-agrégation de l'arbre au plus toutes les 5 s
TREE_CALL_TIMEOUT_S = 4        # garde-fou : un contributeur lent à `ember/tree` est sauté
                              # (502 → ignoré) ; ses nœuds reviendront au prochain tour.

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
_tree_cache = {"ts": 0.0, "body": None, "path_map": {}, "matrix_map": {}, "contributors": [],
               "elements": {}}


# ═════════════════════════════════════════════════════════════════════
# Agrégation des contributions d'outils → éléments Glow plats
# ═════════════════════════════════════════════════════════════════════

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

def _append_canonical(elements, path_map, contributors):
    """Voie CANONIQUE (« moule IPG ») : agrège les `ember/bindings` de tous les outils,
    bucketise par slot logique, et monte l'arbre sous la racine IPG avec la numérotation
    FIGÉE du profil → arbre identique quel que soit le device backing un slot.

    Contrat plugin (GET ember/bindings) :
        { "devices": [ { "slot": int, "label"?: str,
                         "bindings": [ { "key": "<bloc>.<param>", "lane"?: int,
                                         "value": <v>, "ref": <opaque> }, ... ] } ] }
    Un outil qui n'implémente pas la route (status != 200) est simplement ignoré."""
    prof = _profile.get_profile()
    index = _profile.build_index(prof)

    # slot -> { "label": str|None, "bindings": [ (type_, binding), ... ] }
    slots = {}
    for type_ in _bindings_types():
        status, data = tools.call(type_, "ember/bindings", "GET", actor=EMBER_ACTOR,
                                  timeout=TREE_CALL_TIMEOUT_S)
        if status != 200 or not isinstance(data, dict):
            continue
        for dev in data.get("devices") or []:
            slot = dev.get("slot")
            if slot is None:
                continue
            entry = slots.setdefault(int(slot), {"label": None, "bindings": []})
            if dev.get("label") and not entry["label"]:
                entry["label"] = str(dev["label"])
            for b in dev.get("bindings") or []:
                entry["bindings"].append((type_, b))
    if not slots:
        return

    seen = set()
    _ensure_node(elements, seen, [IPG_ROOT_ID], prof.get("label") or "IPG")
    for slot in sorted(slots):
        entry = slots[slot]
        _ensure_node(elements, seen, [IPG_ROOT_ID, slot],
                     entry["label"] or ("Slot %d" % slot))
        for type_, b in entry["bindings"]:
            res = index.get(b.get("key"))
            if not res:
                log.info("emberplus: binding %r inconnu au profil (ignoré)", b.get("key"))
                continue
            lane = int(b.get("lane") or 1)
            _ensure_node(elements, seen, [IPG_ROOT_ID, slot, lane], "Voie %d" % lane)
            _ensure_node(elements, seen, [IPG_ROOT_ID, slot, lane, res["block_id"]],
                         res["block_label"])
            ppath = [IPG_ROOT_ID, slot, lane, res["block_id"], res["param_id"]]
            if tuple(ppath) in seen:            # collision (2 devices sur un même slot) → 1er gagne
                log.warning("emberplus: chemin canonique %s en double (binding %r ignoré)",
                            ppath, b.get("key"))
                continue
            seen.add(tuple(ppath))
            ptype = _TYPE_MAP.get(res["type"], glow.PT_STRING)
            value = b.get("value")
            if res["type"] == "enum":
                try:
                    value = int(value or 0)
                except (TypeError, ValueError):
                    value = 0
                el = (ppath, "param", res["param_label"], "", value, ptype, True)
                if res["enum"]:                 # n'émet une énumération que si des libellés existent
                    el = el + (res["enum"],)
            else:
                el = (ppath, "param", res["param_label"], "", value, ptype, True)
            elements.append(el)
            if b.get("ref") is not None:
                path_map[tuple(ppath)] = (type_, b.get("ref"))
    contributors.append({"type": "ipg", "label": "%s (%d slot%s)" % (
        prof.get("label") or "IPG", len(slots), "s" if len(slots) > 1 else "")})

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
    for idx, type_ in enumerate(_ember_types(), start=1):
        status, data = tools.call(type_, "ember/tree", "GET", actor=EMBER_ACTOR,
                                  timeout=TREE_CALL_TIMEOUT_S)
        if status != 200 or not isinstance(data, dict):
            log.warning("emberplus: %s ember/tree → %s (ignoré)", type_, status)
            continue
        label = str(data.get("label") or type_)
        base = [idx]
        try:
            sub = [(base, "node", label, "")]
            for node in data.get("nodes") or []:
                _walk_node(base, node, sub, path_map, matrix_map, type_)
        except Exception as e:
            log.warning("emberplus: conversion arbre %s échouée : %s", type_, e)
            continue
        elements += sub
        contributors.append({"type": type_, "label": label})
    try:
        _append_canonical(elements, path_map, contributors)
    except Exception as e:
        log.warning("emberplus: agrégation canonique (IPG) échouée : %s", e)
    extras = [_encode_matrix(p, m, with_axes=False) for p, m in matrix_map.items()]
    body = glow.build_collection(elements, extra=extras)
    # Index par chemin : base de la comparaison incrémentale. Un élément porte à la fois sa
    # valeur et son libellé, donc comparer les tuples suffit à détecter tout ce qui bouge.
    el_index = {tuple(el[0]): el for el in elements}
    return body, path_map, matrix_map, contributors, el_index

def _current_tree(force=False):
    """Renvoie (body, path_map, matrix_map), ré-agrégeant si cache expiré ou forcé."""
    with _tree_lock:
        fresh = (time.monotonic() - _tree_cache["ts"]) < TREE_TTL_S
        if not force and fresh and _tree_cache["body"] is not None:
            return _tree_cache["body"], _tree_cache["path_map"], _tree_cache["matrix_map"]
    body, path_map, matrix_map, contributors, el_index = _build_tree()
    with _tree_lock:
        _tree_cache.update({"ts": time.monotonic(), "body": body, "path_map": path_map,
                            "matrix_map": matrix_map, "contributors": contributors,
                            "elements": el_index})
    with _lock:
        _status["contributors"] = contributors
    return body, path_map, matrix_map

def _invalidate():
    with _tree_lock:
        _tree_cache["ts"] = 0.0

def refresh():
    """Force la ré-agrégation et re-pousse aux abonnés (à appeler après un changement)."""
    _invalidate()
    notify_change()


# ═════════════════════════════════════════════════════════════════════
# Application d'un SetValue → routage vers l'outil propriétaire
# ═════════════════════════════════════════════════════════════════════

def _apply_setvalue(path, value):
    _, path_map, _ = _current_tree()
    entry = path_map.get(tuple(path))
    if not entry:
        log.info("emberplus: setvalue %s ignoré (inconnu ou lecture seule)", path)
        return False
    type_, ref = entry
    status, data = tools.call(type_, "ember/set", "POST",
                              {"ref": ref, "value": value}, actor=EMBER_ACTOR)
    ok = status == 200 and isinstance(data, dict) and not data.get("error")
    if ok:
        detail = json.dumps({"ref": ref, "value": value}, ensure_ascii=False)[:400]
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
    _, _, matrix_map = _current_tree()
    m = matrix_map.get(tuple(matrix_path))
    if not m:
        log.info("emberplus: connect %s ignoré (matrice inconnue)", matrix_path)
        return False
    op = _OP_NAME.get(operation, "absolute")
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
    return False, [el for p, el in new.items() if old.get(p) != el]

def _broadcast_update():
    """Rediffuse aux abonnés le STRICT nécessaire.

    L'arbre entier n'est réémis que si sa structure a changé (chemin ajouté/retiré : arrivée
    ou départ d'un device, ré-affectation, édition du profil). Sinon on n'émet que les
    éléments dont la valeur a bougé — la collection Ember+ étant plate et à chemins absolus,
    un sous-ensemble EST une trame de mise à jour valide. Et si rien n'a bougé, on n'envoie
    rien : l'ancien comportement rediffusait tout l'arbre même à valeurs identiques."""
    with _tree_lock:
        old = dict(_tree_cache.get("elements") or {})
        primed = _tree_cache.get("body") is not None
    try:
        body, _, matrix_map = _current_tree(force=True)
    except Exception as e:
        log.error("emberplus: build arbre échoué : %s", e)
        return
    with _tree_lock:
        new = dict(_tree_cache.get("elements") or {})
    structural, changed = _diff_elements(old, new) if primed else (True, [])
    if structural:
        # Racine (nœuds/params + matrices contents-seuls) PUIS chaque matrice complète, pour
        # que les tallies de connexions remontent aux abonnés après un crosspoint.
        frames = [body] + [_matrix_body(p, m) for p, m in matrix_map.items()]
    elif changed:
        frames = [glow.build_collection(changed)]
    else:
        return
    if glow.DEBUG:
        log.info("emberplus: broadcast %s (%d élément(s))",
                 "arbre complet" if structural else "incrémental", len(changed))
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
            body, _, matrix_map = _current_tree()
            mp = tuple(a.get("path") or [])
            if mp in matrix_map:                       # GetDirectory SUR une matrice
                _send_frame(sock, _matrix_body(mp, matrix_map[mp]))
            else:                                      # racine / nœud → arbre plat
                _send_frame(sock, body)
        elif a["kind"] == "unsubscribe":
            with _lock:
                _subscribed.discard(sock)
        elif a["kind"] == "setvalue":
            _apply_setvalue(a["path"], a["value"])
        elif a["kind"] == "connect":
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

def start(port):
    """Démarre (ou redémarre) le serveur sur `port`."""
    global _server_thread, _running
    stop()
    _running = True
    _server_thread = threading.Thread(target=_server_loop, args=(int(port),), daemon=True)
    _server_thread.start()

def stop():
    """Arrête le serveur ; ferme les clients."""
    global _running, _server_thread
    if not _running:
        return
    _running = False
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
    with _lock:
        _status["running"] = False

def is_running():
    return _running

def boot():
    """Démarrage au lancement de l'app : démarre le serveur si activé en réglages.
    Appelé par le boot générique des services (main.py) après init_db()."""
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
        return jsonify(out)

    @bp.route("/api/emberplus/apply", methods=["POST"])
    @require_perm("settings.edit")
    def emberplus_apply():
        data = request.json or {}
        enabled = bool(data.get("enabled"))
        port = int(data.get("port") or 9000)
        if not (1 <= port <= 65535):
            return jsonify({"error": "port invalide"}), 400
        settings.set("emberplus_enabled", enabled)
        settings.set("emberplus_port", port)
        if enabled:
            start(port)
        else:
            stop()
        _audit_log("emberplus", "apply", f"enabled={enabled} port={port}",
                   user_id=None, username="système")
        return jsonify(status_dict())

    @bp.route("/api/emberplus/profile", methods=["GET"])
    @require_login
    def emberplus_profile_get():
        """Profil canonique courant + liste des clés (consommé par l'UI d'exposition
        des plugins pour peupler le menu déroulant du « moule IPG »)."""
        prof = _profile.get_profile()
        return jsonify({"profile": prof, "keys": _profile.keys(prof)})

    @bp.route("/api/emberplus/profile", methods=["POST"])
    @require_perm("settings.edit")
    def emberplus_profile_set():
        """Enregistre un profil canonique édité (« liste exposée »)."""
        data = request.json or {}
        if not isinstance(data, dict) or not isinstance(data.get("blocks"), list) \
                or not data["blocks"]:
            return jsonify({"error": "profil invalide (blocks requis)"}), 400
        settings.set("emberplus_profile", json.dumps(data, ensure_ascii=False))
        refresh()
        _audit_log("emberplus", "profile",
                   f"maj profil ({len(data['blocks'])} blocs)",
                   user_id=None, username="système")
        return jsonify({"profile": _profile.get_profile(),
                        "keys": _profile.keys()})
