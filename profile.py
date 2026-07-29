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
                                  "writable"?: bool,
                                  "min"?: number, "max"?: number } ] } ] }
Le drapeau optionnel "writable" (défaut True si absent) déclare si le paramètre est
inscriptible côté VSM (SetValue autorisé) ; à False pour un statut en lecture seule
(verrouillage, présence de signal, PTP, voie affectée…).
Les bornes optionnelles "min"/"max" (nombres) déclarent l'intervalle valide côté device
(ex. retard image 0..226) : transmises à VSM (PC_MINIMUM/PC_MAXIMUM) pour empêcher la
saisie d'une valeur hors plage côté pupitre. Absentes → aucune borne annoncée ; `build_index`
renvoie alors `None` (à distinguer d'une borne valant 0).

Le catalogue par défaut ci-dessous (v2) est le catalogue FIGÉ, établi sur relevés réels de
trois familles : Imagine SNP, yellobrik CDE 1922, Ross Newt. Blocs 1-6 = traitement (rempli
par les processeurs) ; blocs 7-9 = PASSERELLE (source d'entrée, transport IP, statut), commun
aux convertisseurs SDI↔ST2110. La SÉLECTION de source (quel flux/BNC) reste HORS moule (mode
libre) : elle n'est pas interchangeable entre familles (cf. EMBERPLUS-IPG.md §10-11). Le
catalogue reste surchargeable en réglages (setting `emberplus_profile`, JSON) sans toucher au
code. RÈGLE D'OR : bloc.id et param.id, et l'ordre des enums, ne bougent JAMAIS — on n'ajoute
qu'en fin ; un id retiré reste vacant (ex. functions.testpattern, id 4.3).
"""
import json
import logging

log = logging.getLogger(__name__)

_STALE_WARNED = False   # cf. get_profile : l'alerte de profil périmé n'est dite qu'une fois

# ─── Profil par défaut « NAP vidéo » ────────────────────────────────────
# Union des blocs curatés SNP + Neuron ; chaque device remplit ce qu'il expose (ou
# qu'on lui mappe). Le routage de source est HORS profil (géré par la matrice, phase 2).
# Numérotation FIGÉE : bloc.id et param.id ne doivent jamais bouger (chemins VSM stables).
#
# Enum canonique des formats de sortie : liste PROVISOIRE, à recaler sur un relevé device
# réel (`GET /neurons/{id}/object/{oid}` → options, et MetaData SNP). L'ordre = l'index vu
# par VSM ; chaque plugin fournit un enum_map (index canonique → valeur native).
# Câblage d'une voie UHD : un seul lien 12G, ou quatre liens 3G (la voie occupe alors quatre
# connecteurs). Relevé sur le Neuron (`Wire Mode In` / `Wire Mode Out`, valeurs 1 et 4).
# ORDRE = CONTRAT : ajouter en fin, jamais au milieu.
_WIRE_MODES = ["1 lien (12G)", "4 liens (3G)"]

_VIDEO_FORMATS = [
    "1080i50", "1080i5994", "1080i60",
    "1080p50", "1080p5994", "1080p60", "1080p25", "1080p2997", "1080p30", "1080p24", "1080p2398",
    "2160p50", "2160p5994", "2160p60", "2160p25", "2160p2997", "2160p30", "2160p24", "2160p2398",
    "720p50", "720p5994", "720p60",
]

# Enum canonique des motifs de mire : relevé RÉEL sur un SNP (Imagine Selenio Network
# Processor), dans cet ordre exact. L'ORDRE DE LA LISTE EST LE CONTRAT : l'index dans la
# liste est ce que VSM voit et enregistre côté pupitre ; toute valeur future DOIT être
# ajoutée EN FIN de liste — un réordonnancement casserait les configurations pupitre
# existantes (même logique que pour `_VIDEO_FORMATS` ci-dessus).
_TESTPATTERNS = [
    "Black",
    "White",
    "Color Bars 75%",
    "Horizontal Sweep Y-only",
    "Horizontal Sweep",
    "Cross Hatch",
    "Pathological EQ",
    "Pathological PLL",
]

# ─── Enums du CATALOGUE PASSERELLE (blocs 7-9) ──────────────────────────
# Établis sur relevés RÉELS de trois familles : Imagine SNP, yellobrik CDE 1922, Ross Newt.
# Même règle que les autres enums : l'ORDRE EST LE CONTRAT (index vu par VSM), on n'ajoute
# qu'EN FIN, jamais au milieu — un réordonnancement casserait les configs pupitre.

# Source d'entrée d'une voie. La mire (générateur interne) est traitée comme une SOURCE, pas
# comme une fonction : « cette voie est une mire » est un choix de source côté opérateur.
# `Mixte` est un état LU (sur un SNP, une section dont les 4 programmes divergent) — VSM peut
# l'afficher mais l'opérateur ne le sélectionne pas. La SÉLECTION de source (quel BNC, quel
# flux/SDP) N'EST PAS dans le moule : elle reste au mode libre car non interchangeable entre
# familles (adresse multicast côté SNP, crosspoint côté routeur, cf. EMBERPLUS-IPG.md §10-11).
_INPUT_MODES = ["Désactivé", "SDI", "IP", "Mire", "Mixte"]
# Mode de transport IP (SNP VidRxMode ; SDP Newt SSN=ST2110-20).
_TRANSPORT_MODES = ["ST 2110", "ST 2022-6"]
# Mise en forme du trafic ST 2110-21 (CDE PRS ; SDP TP=2110TPN/TPW). Narrow/Wide/Linear.
_TRAFFIC_SHAPE = ["Narrow", "Wide", "Linear"]
# État de synchronisation PTP (CDE PtpLockStatus locked/freerun ; SNP ptpCtlrState).
_PTP_STATES = ["Non synchro", "Synchro", "Holdover"]

DEFAULT_PROFILE = {
    "version": 2,                     # v2 : catalogue passerelle figé (blocs 7-9)
    "label": "IPG",                   # IP Gateway — racine affichée au pupitre
    "lanes": 32,                      # indicatif : la taille réelle du vivier est réglée à part
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
        # ⚠ Bloc 4 : le paramètre `testpattern` (id 3, « Mire » on/off) a été RETIRÉ — la mire
        # est désormais une valeur de `gateway.input_mode` (bloc 7). L'id 3 reste VACANT et ne
        # doit JAMAIS être réattribué : le retrait ne renumérote rien (freeze=1, black=2,
        # testpattern_sel=4 gardent leur id), sinon les chemins VSM casseraient.
        {"key": "functions", "label": "Fonctions", "id": 4, "params": [
            {"key": "freeze", "label": "Gel image", "id": 1, "type": "bool"},
            {"key": "black", "label": "Forçage noir", "id": 2, "type": "bool"},
            {"key": "testpattern_sel", "label": "Motif de mire", "id": 4,
             "type": "enum", "enum": _TESTPATTERNS},
        ]},
        {"key": "output", "label": "Sortie", "id": 5, "params": [
            {"key": "video_format", "label": "Format vidéo", "id": 1,
             "type": "enum", "enum": _VIDEO_FORMATS},
            {"key": "media_type", "label": "Type de média", "id": 2,
             "type": "enum", "enum": ["SDI", "ST 2110", "NMOS"]},
            {"key": "wire_mode", "label": "Câblage sortie", "id": 3, "type": "enum",
             "enum": _WIRE_MODES},
        ]},
        {"key": "audio", "label": "Audio", "id": 6, "params": [
            {"key": "delay", "label": "Retard audio", "id": 1, "type": "int", "unit": "ms"},
        ]},
        # ─── Catalogue PASSERELLE (blocs 7-9) — commun aux convertisseurs SDI↔ST2110 ───
        {"key": "gateway", "label": "Passerelle", "id": 7, "params": [
            {"key": "input_mode", "label": "Source d'entrée", "id": 1,
             "type": "enum", "enum": _INPUT_MODES},
            {"key": "input_wire", "label": "Câblage entrée", "id": 2, "type": "enum",
             "enum": _WIRE_MODES},
        ]},
        # ⚠ `wire_mode` : relevé sur le Neuron le 2026-07-29. Une voie UHD peut sortir sur UN
        # lien 12G ou sur QUATRE liens 3G — dans le second cas elle occupe quatre connecteurs
        # physiques. L'ordre de cet enum est le contrat (index vu par VSM) : ajouter en FIN.
        # Le mode UHD lui-même n'est PAS un paramètre : il est DÉRIVÉ du format (l'écrire
        # directement est refusé par le matériel), et `video_format` le dit déjà.
        {"key": "transport", "label": "Transport IP", "id": 8, "params": [
            {"key": "mode", "label": "Mode", "id": 1, "type": "enum", "enum": _TRANSPORT_MODES},
            {"key": "redundancy", "label": "Redondance ST 2022-7", "id": 2, "type": "bool"},
            {"key": "traffic_shape", "label": "Mise en forme trafic", "id": 3,
             "type": "enum", "enum": _TRAFFIC_SHAPE},
        ]},
        # Statut : LECTURE SEULE (writable:false) — présence signal, verrouillage, PTP.
        {"key": "status", "label": "Statut", "id": 9, "params": [
            {"key": "signal", "label": "Signal présent", "id": 1, "type": "bool", "writable": False},
            {"key": "lock", "label": "Verrouillé", "id": 2, "type": "bool", "writable": False},
            {"key": "ptp", "label": "PTP", "id": 3, "type": "enum", "enum": _PTP_STATES,
             "writable": False},
            # Le matériel compare le SIGNAL RÉEL au mode déclaré et publie son verdict —
            # relevé sur le Neuron (`Input Mode Validity` : « OK », « Warning » quand la
            # source n'est pas UHD alors que la voie l'est). Volontairement une CHAÎNE et non
            # un enum : on n'a aucune liste exhaustive des libellés, et un enum incomplet
            # retomberait silencieusement sur l'index 0 (cf. §5). On relaie le mot du device.
            {"key": "input_valid", "label": "Validité de l'entrée", "id": 4, "type": "string",
             "writable": False},
        ]},
    ],
}


def get_profile():
    """Renvoie le profil courant : le JSON du réglage `emberplus_profile` s'il est valide ET
    PAS PÉRIMÉ, sinon le profil par défaut du code.

    ⚠ GARDE-FOU AJOUTÉ LE 2026-07-29, sur un cas réel. Le réglage masquait un profil **v1**
    (blocs 1-6 seulement) alors que le code portait la v2 depuis emberplus 0.10.0 : le
    « catalogue passerelle figé » (blocs 7-9) n'avait donc JAMAIS tourné sur cette instance,
    silencieusement — exactement le piège de l'image Docker périmée (§1.1), transposé aux
    réglages. Une surcharge posée une fois gelait le catalogue pour toujours.

    Un profil de version INFÉRIEURE à celle du code est donc ignoré, et l'ignorer est SÛR par
    construction : la règle d'or du catalogue veut qu'on n'ajoute qu'en fin et qu'aucun id ne
    bouge, donc une version plus récente est toujours un SURENSEMBLE de l'ancienne — aucun
    chemin VSM existant ne peut casser. À version égale ou supérieure, la surcharge gagne : on
    ne défait pas une personnalisation volontaire."""
    from app import settings
    raw = settings.get("emberplus_profile")
    if raw:
        try:
            p = raw if isinstance(raw, dict) else json.loads(raw)
            if isinstance(p, dict) and isinstance(p.get("blocks"), list) and p["blocks"]:
                try:
                    pv = int(p.get("version") or 0)
                except (TypeError, ValueError):
                    pv = 0
                dv = int(DEFAULT_PROFILE.get("version") or 0)
                if pv < dv:
                    # UNE SEULE FOIS : `get_profile()` est appelé à chaque ré-agrégation, donc
                    # toutes les 5 s. Journalisé sans garde, l'avertissement noyait le journal
                    # et se rendait invisible à force d'être répété.
                    global _STALE_WARNED
                    if not _STALE_WARNED:
                        _STALE_WARNED = True
                        log.warning("emberplus: profil réglage v%s PÉRIMÉ (le code est en v%s) → "
                                    "surcharge ignorée. Les blocs ajoutés depuis reviennent dans "
                                    "l'arbre ; réenregistrer le profil dans Réglages → Ember+ "
                                    "repartira de la v%s.", pv, dv, dv)
                else:
                    return p
        except Exception as e:
            log.warning("emberplus: profil réglage invalide (%s) → défaut", e)
    return DEFAULT_PROFILE


def build_index(prof=None):
    """Indexe le profil : { "<bloc>.<param>" : {block_id, block_label, param_id,
    param_label, type, enum, writable, min, max} }. Utilisé pour résoudre une clé
    canonique en `id`. `min`/`max` valent `None` quand la borne correspondante est
    absente du profil (à distinguer d'une borne valant 0)."""
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
