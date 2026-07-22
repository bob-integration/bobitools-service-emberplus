# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

"""Socle protocole Ember+ — pur, sans dépendance, agnostique du métier.

S101 framing + BER + Glow DTD (Node/Parameter qualifiés). Repris VERBATIM du
provider Ember+ de Bobi.Studio (services/emberplus), élagué de sa couche métier
MXL (containers/routing/matrix) : ce module ne connaît QUE des nœuds et paramètres.

Le service (emberplus.py) fournit une liste plate d'éléments
`(path, kind, *args)` ; ce module l'encode en arbre Glow et décode les requêtes
consumer (GetDirectory / Subscribe / Unsubscribe / SetValue).

Debug : exporter EMBERPLUS_DEBUG=1 pour logger tous les octets échangés.
"""
import logging
import math
import os
import struct

log = logging.getLogger(__name__)
DEBUG = os.environ.get("EMBERPLUS_DEBUG") == "1"

# ─── S101 / Glow constantes (cf. libs101 + libember GlowType.hpp) ─────────

BOF, EOF, CE, XOR = 0xFE, 0xFF, 0xFD, 0x20
S101_INVALID_LO = 0xF8
S101_MSG_EMBER          = 0x0E
S101_CMD_PAYLOAD        = 0x00
S101_CMD_KEEPALIVE_REQ  = 0x01
S101_CMD_KEEPALIVE_RESP = 0x02
S101_VERSION            = 0x01
DTD_GLOW                = 0x01
GLOW_APP_BYTES = bytes([0x1F, 0x02])  # minor=31, major=2 → Glow 2.31

PKG_FIRST = 0x80
PKG_LAST  = 0x40
PKG_EMPTY = 0x20

S101_MAX_PAYLOAD_PER_PACKET = 1024  # cf. libs101

# Glow application tags (constructed)
G_PARAMETER               = 1
G_COMMAND                 = 2
G_NODE                    = 3
G_ELEMENT_COLLECTION      = 4
G_QUAL_PARAMETER          = 9
G_QUAL_NODE               = 10
G_ROOT_ELEMENT_COLLECTION = 11
# Matrix (source : libember GlowType.hpp / glow.asn)
G_MATRIX        = 13
G_TARGET        = 14   # Signal target (number seul)
G_SOURCE        = 15   # Signal source (number seul)
G_CONNECTION    = 16
G_QUAL_MATRIX   = 17
G_LABEL         = 18

# Command numbers
CMD_SUBSCRIBE     = 30
CMD_UNSUBSCRIBE   = 31
CMD_GET_DIRECTORY = 32

# MatrixContents field tags (context)
MC_IDENTIFIER      = 0
MC_DESCRIPTION     = 1
MC_TYPE            = 2
MC_ADDRESSING_MODE = 3
MC_TARGET_COUNT    = 4
MC_SOURCE_COUNT    = 5
MC_MAX_TOTAL       = 6
MC_MAX_PER_TARGET  = 7
MC_PARAMS_LOCATION = 8
MC_GAIN_PARAM      = 9
MC_LABELS          = 10
MC_SCHEMA          = 11
MC_TEMPLATE        = 12

# Matrix element child tags (Matrix / QualifiedMatrix)
MX_NUMBER_OR_PATH = 0
MX_CONTENTS       = 1
MX_CHILDREN       = 2
MX_TARGETS        = 3
MX_SOURCES        = 4
MX_CONNECTIONS    = 5

# Matrix type / addressing mode
MATRIX_ONE_TO_N   = 0
MATRIX_ONE_TO_ONE = 1
MATRIX_N_TO_N     = 2
ADDR_LINEAR       = 0
ADDR_NONLINEAR    = 1

# Connection field tags (context)
CN_TARGET      = 0
CN_SOURCES     = 1   # PackedNumbers ::= RELATIVE-OID
CN_OPERATION   = 2   # ConnectionOperation (consumer → provider)
CN_DISPOSITION = 3   # ConnectionDisposition (provider → consumer)

CN_OP_ABSOLUTE   = 0
CN_OP_CONNECT    = 1
CN_OP_DISCONNECT = 2

CN_DISP_TALLY    = 0
CN_DISP_MODIFIED = 1
CN_DISP_PENDING  = 2
CN_DISP_LOCKED   = 3

# Label field tags
LB_BASEPATH    = 0   # RELATIVE-OID
LB_DESCRIPTION = 1

# ParameterContents field tags (context)
PC_IDENTIFIER  = 0
PC_DESCRIPTION = 1
PC_VALUE       = 2
PC_MINIMUM     = 3
PC_MAXIMUM     = 4
PC_ACCESS      = 5
PC_FORMAT      = 6
PC_ENUMERATION = 7
PC_IS_ONLINE   = 9
PC_TYPE        = 13

# NodeContents field tags (context)
NC_IDENTIFIER  = 0
NC_DESCRIPTION = 1
NC_IS_ROOT     = 2
NC_IS_ONLINE   = 3

ACCESS_READ      = 1
ACCESS_READWRITE = 3

PT_INTEGER = 1
PT_REAL    = 2
PT_STRING  = 3
PT_BOOLEAN = 4

# Universal BER tags (le bit constructed sera ajouté côté SEQUENCE/SET)
U_BOOL    = 1
U_INTEGER = 2
U_UTF8    = 12
U_REAL    = 9


# ═════════════════════════════════════════════════════════════════════
# BER encoder (le strict nécessaire pour Glow)
# ═════════════════════════════════════════════════════════════════════

def _ber_len(n):
    if n < 128:
        return bytes([n])
    out = bytearray()
    while n:
        out.insert(0, n & 0xFF)
        n >>= 8
    return bytes([0x80 | len(out)]) + bytes(out)

def _tlv(tag_byte, content):
    return bytes([tag_byte]) + _ber_len(len(content)) + content

def _universal_primitive(tag_num, content):
    return _tlv(tag_num & 0x1F, content)

def _universal_constructed(tag_num, content):
    return _tlv(0x20 | (tag_num & 0x1F), content)

def _app_constructed(tag_num, content):
    assert tag_num < 31, "tag application > 30 non supporté"
    return _tlv(0x60 | tag_num, content)

def _ctx_constructed(tag_num, content):
    assert tag_num < 31, "tag context > 30 non supporté"
    return _tlv(0xA0 | tag_num, content)

def _ctx_primitive(tag_num, content):
    """IMPLICIT context tag pour un type primitif (gardé pour le parsing tolérant)."""
    assert tag_num < 31, "tag context > 30 non supporté"
    return _tlv(0x80 | tag_num, content)

def _ctx_explicit(tag_num, universal_tlv):
    """EXPLICIT context tag : [Context N Constructed] wrappant un TLV universel complet.
    Format Glow.asn (EXPLICIT TAGS par défaut)."""
    return _ctx_constructed(tag_num, universal_tlv)

# Octets canoniques (sans tag/length) — utilisés pour IMPLICIT context tags
def _int_bytes(value):
    value = int(value)
    if value == 0:
        return b"\x00"
    nbytes = 1
    while True:
        try:
            data = value.to_bytes(nbytes, "big", signed=True); break
        except OverflowError:
            nbytes += 1
    while len(data) > 1 and ((data[0] == 0x00 and not (data[1] & 0x80)) or
                              (data[0] == 0xFF and (data[1] & 0x80))):
        data = data[1:]
    return data

def _bool_bytes(value):
    return b"\xff" if value else b"\x00"

def _utf8_bytes(s):
    return (s or "").encode("utf-8")

def _relative_oid_bytes(path):
    out = bytearray()
    for n in path:
        n = int(n)
        if n < 0:
            raise ValueError("RELATIVE-OID exige des entiers positifs")
        if n == 0:
            out.append(0); continue
        chunk = []
        while n:
            chunk.insert(0, n & 0x7F)
            n >>= 7
        for i in range(len(chunk) - 1):
            chunk[i] |= 0x80
        out.extend(chunk)
    return bytes(out)

# Primitives universelles (avec tag/length pour usage Value EXPLICIT)
def ber_int(value):
    return _universal_primitive(U_INTEGER, _int_bytes(value))

def ber_bool(value):
    return _universal_primitive(U_BOOL, _bool_bytes(value))

def ber_utf8(s):
    return _universal_primitive(U_UTF8, _utf8_bytes(s))

def _real_bytes(value):
    if value == 0.0:
        return b""
    if math.isinf(value):
        return b"\x40" if value > 0 else b"\x41"
    if math.isnan(value):
        return b"\x42"
    bits = struct.unpack(">Q", struct.pack(">d", value))[0]
    sign = (bits >> 63) & 1
    raw_exp = (bits >> 52) & 0x7FF
    raw_mant = bits & ((1 << 52) - 1)
    if raw_exp == 0:
        exponent = -1074; mantissa = raw_mant
    else:
        exponent = raw_exp - 1023 - 52; mantissa = raw_mant | (1 << 52)
    while mantissa and (mantissa & 1) == 0:
        mantissa >>= 1; exponent += 1
    if mantissa == 0:
        return b""
    nb_mant = max(1, (mantissa.bit_length() + 7) // 8)
    mantissa_bytes = mantissa.to_bytes(nb_mant, "big")
    if exponent >= 0:
        nb_exp = max(1, (exponent.bit_length() + 8) // 8)
    else:
        nb_exp = max(1, ((-exponent - 1).bit_length() + 8) // 8)
    exp_bytes = exponent.to_bytes(nb_exp, "big", signed=True)
    cb = 0x80
    if sign: cb |= 0x40
    if nb_exp == 1:   cb |= 0b00
    elif nb_exp == 2: cb |= 0b01
    elif nb_exp == 3: cb |= 0b10
    else:             cb |= 0b11
    if nb_exp <= 3:
        return bytes([cb]) + exp_bytes + mantissa_bytes
    return bytes([cb, nb_exp]) + exp_bytes + mantissa_bytes

def ber_real(value):
    return _universal_primitive(U_REAL, _real_bytes(value))

def ber_relative_oid(path):
    return _universal_primitive(13, _relative_oid_bytes(path))

def ber_sequence(*items):
    return _universal_constructed(16, b"".join(items))

def ber_set(*items):
    return _universal_constructed(17, b"".join(items))


# ═════════════════════════════════════════════════════════════════════
# BER decoder (juste ce qu'il faut pour parser les requêtes consumer)
# ═════════════════════════════════════════════════════════════════════

def _decode_len(buf, i):
    L = buf[i]; i += 1
    if L == 0x80:
        # indefinite — non géré côté entrée (les vrais consumers utilisent surtout définite)
        raise ValueError("BER indefinite length non supporté")
    if L & 0x80:
        n = L & 0x7F
        out = 0
        for _ in range(n):
            out = (out << 8) | buf[i]; i += 1
        return out, i
    return L, i

def _decode_tag(buf, i):
    """Renvoie (klass, constructed, tag_num, new_i). Pas de multi-byte tag (suffit pour Glow)."""
    b = buf[i]; i += 1
    klass = (b >> 6) & 0x3   # 0=univ 1=app 2=ctx 3=priv
    constructed = bool(b & 0x20)
    num = b & 0x1F
    if num == 0x1F:
        raise ValueError("BER tag long form non supporté")
    return klass, constructed, num, i

def ber_iter(buf, start=0, end=None):
    """Itère sur les TLVs successifs ; yield (klass, constructed, num, content_bytes)."""
    if end is None:
        end = len(buf)
    i = start
    while i < end:
        klass, constructed, num, i = _decode_tag(buf, i)
        L, i = _decode_len(buf, i)
        content = bytes(buf[i:i + L])
        i += L
        yield klass, constructed, num, content

def parse_int(content):
    return int.from_bytes(content, "big", signed=True) if content else 0

def parse_utf8(content):
    return content.decode("utf-8", errors="replace")

def parse_bool(content):
    return content != b"" and content[0] != 0

def parse_real(content):
    if not content:
        return 0.0
    cb = content[0]
    if cb == 0x40: return float("inf")
    if cb == 0x41: return -float("inf")
    if cb == 0x42: return float("nan")
    if not (cb & 0x80):
        # encodage décimal — best-effort
        try: return float(content[1:].decode("ascii"))
        except Exception: return 0.0
    sign = -1 if (cb & 0x40) else 1
    base_code = (cb >> 4) & 0x3
    base = {0:2, 1:8, 2:16}.get(base_code, 2)
    exp_len_code = cb & 0x3
    if exp_len_code <= 2:
        exp_len = exp_len_code + 1
        exp_start = 1
    else:
        exp_len = content[1]
        exp_start = 2
    exp_bytes = content[exp_start:exp_start + exp_len]
    exponent = int.from_bytes(exp_bytes, "big", signed=True)
    mant_bytes = content[exp_start + exp_len:]
    mantissa = int.from_bytes(mant_bytes, "big") if mant_bytes else 0
    return sign * mantissa * (base ** exponent)

def parse_relative_oid(content):
    out, n = [], 0
    for b in content:
        n = (n << 7) | (b & 0x7F)
        if not (b & 0x80):
            out.append(n); n = 0
    return out


# ═════════════════════════════════════════════════════════════════════
# S101 framing
# ═════════════════════════════════════════════════════════════════════

def _crc16_ccitt(data):
    """CRC-16/X-25 (poly 0x1021 réfléchi=0x8408, init 0xFFFF, reflected I/O, final XOR 0xFFFF).
    Variante utilisée par libs101 (vérifié empiriquement contre VSM)."""
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ 0x8408 if (crc & 1) else (crc >> 1)
    return (crc ^ 0xFFFF) & 0xFFFF

def _escape(buf):
    out = bytearray()
    for b in buf:
        if b >= S101_INVALID_LO:
            out.append(CE); out.append(b ^ XOR)
        else:
            out.append(b)
    return bytes(out)

def _encode_s101_packet(payload_chunk, flags):
    body = bytes([
        0x00,                # slot
        S101_MSG_EMBER,
        S101_CMD_PAYLOAD,
        S101_VERSION,
        flags,
        DTD_GLOW,
        len(GLOW_APP_BYTES)
    ]) + GLOW_APP_BYTES + payload_chunk
    crc = _crc16_ccitt(body)
    body_with_crc = body + bytes([crc & 0xFF, (crc >> 8) & 0xFF])
    return bytes([BOF]) + _escape(body_with_crc) + bytes([EOF])

def s101_encode_ember(payload):
    """Emballe un payload BER Ember+ dans 1+ frames S101 (multi-packet si > MAX)."""
    if len(payload) <= S101_MAX_PAYLOAD_PER_PACKET:
        return _encode_s101_packet(payload, PKG_FIRST | PKG_LAST)
    out = bytearray()
    pos = 0
    while pos < len(payload):
        chunk = payload[pos:pos + S101_MAX_PAYLOAD_PER_PACKET]
        flags = 0
        if pos == 0:
            flags |= PKG_FIRST
        if pos + len(chunk) >= len(payload):
            flags |= PKG_LAST
        out.extend(_encode_s101_packet(chunk, flags))
        pos += len(chunk)
    return bytes(out)

def s101_encode_keepalive_response():
    body = bytes([0x00, S101_MSG_EMBER, S101_CMD_KEEPALIVE_RESP, S101_VERSION])
    crc = _crc16_ccitt(body)
    body_with_crc = body + bytes([crc & 0xFF, (crc >> 8) & 0xFF])
    return bytes([BOF]) + _escape(body_with_crc) + bytes([EOF])

class S101Reader:
    """Re-assemble les payloads Ember+ multi-packet depuis un stream TCP.
    Yield (kind, data) où kind ∈ {'payload', 'keepalive_req', 'keepalive_resp'}."""
    def __init__(self):
        self._buf = bytearray()
        self._in_frame = False
        self._escape = False
        self._payload_acc = bytearray()
        self._payload_active = False

    def feed(self, data):
        for b in data:
            if not self._in_frame:
                if b == BOF:
                    self._in_frame = True
                    self._buf.clear()
                continue
            if b == BOF:
                # nouveau frame en plein milieu — restart
                self._buf.clear(); self._escape = False
                continue
            if b == EOF:
                yield from self._process_frame_body()
                self._in_frame = False
                self._buf.clear()
                self._escape = False
                continue
            if self._escape:
                self._buf.append(b ^ XOR); self._escape = False
            elif b == CE:
                self._escape = True
            else:
                self._buf.append(b)

    def _process_frame_body(self):
        if len(self._buf) < 4:
            return
        payload_len = len(self._buf) - 2
        body = bytes(self._buf[:payload_len])
        crc_lo, crc_hi = self._buf[payload_len], self._buf[payload_len + 1]
        if (crc_lo | (crc_hi << 8)) != _crc16_ccitt(body):
            log.debug("emberplus: CRC mismatch sur frame entrant")
            return
        if body[1] != S101_MSG_EMBER:
            return
        cmd = body[2]
        if cmd == S101_CMD_KEEPALIVE_REQ:
            yield ("keepalive_req", b""); return
        if cmd == S101_CMD_KEEPALIVE_RESP:
            yield ("keepalive_resp", b""); return
        if cmd != S101_CMD_PAYLOAD or len(body) < 7:
            return
        flags = body[4]
        app_count = body[6]
        if len(body) < 7 + app_count:
            return
        payload = body[7 + app_count:]
        if flags & PKG_FIRST:
            self._payload_acc = bytearray(payload)
            self._payload_active = True
        elif self._payload_active:
            self._payload_acc.extend(payload)
        else:
            return
        if (flags & PKG_LAST) and self._payload_active:
            yield ("payload", bytes(self._payload_acc))
            self._payload_acc.clear()
            self._payload_active = False


# ═════════════════════════════════════════════════════════════════════
# Encodage des éléments Glow (QualifiedNode / QualifiedParameter)
# ═════════════════════════════════════════════════════════════════════

def _encode_value_explicit(ptype, raw):
    """Encode la Value en EXPLICIT [Context PC_VALUE] (CHOICE → exige explicit tagging)."""
    if ptype == PT_INTEGER:
        return ber_int(raw if raw is not None else 0)
    if ptype == PT_REAL:
        try: return ber_real(float(raw) if raw is not None else 0.0)
        except Exception: return ber_real(0.0)
    if ptype == PT_BOOLEAN:
        return ber_bool(bool(raw))
    return ber_utf8(str(raw) if raw is not None else "")

def _parameter_contents_set(identifier, description, raw_value, ptype, access, enumeration=None,
                            minimum=None, maximum=None):
    """ParameterContents = SET universel (tag 0x31) contenant chaque champ EXPLICIT-taggé.
    Convention Glow.asn (module EXPLICIT TAGS). `enumeration` = liste d'étiquettes ;
    si fournie, émet PC_ENUMERATION (entrées séparées par '\\n', l'index = la valeur entière).
    `minimum`/`maximum` (optionnels, défaut None → pas d'appel existant qui change de
    comportement) émettent PC_MINIMUM/PC_MAXIMUM quand fournis, encodés dans le MÊME type que
    la valeur (entier pour PT_INTEGER, réel pour PT_REAL) ; ignorés pour les autres types, où
    une borne n'a pas de sens."""
    fields = (
        _ctx_explicit(PC_IDENTIFIER,  ber_utf8(identifier)) +
        _ctx_explicit(PC_DESCRIPTION, ber_utf8(description)) +
        _ctx_explicit(PC_VALUE,       _encode_value_explicit(ptype, raw_value))
    )
    bound_encoder = {PT_INTEGER: ber_int, PT_REAL: ber_real}.get(ptype)
    if bound_encoder is not None:
        if minimum is not None:
            fields += _ctx_explicit(PC_MINIMUM, bound_encoder(minimum))
        if maximum is not None:
            fields += _ctx_explicit(PC_MAXIMUM, bound_encoder(maximum))
    fields += _ctx_explicit(PC_ACCESS, ber_int(access))
    if enumeration:
        fields += _ctx_explicit(PC_ENUMERATION, ber_utf8("\n".join(enumeration)))
    fields += (
        _ctx_explicit(PC_IS_ONLINE,   ber_bool(True)) +
        _ctx_explicit(PC_TYPE,        ber_int(ptype))
    )
    return _universal_constructed(17, fields)  # SET universel

def _qual_parameter(path, identifier, description, raw_value, ptype, writeable, enumeration=None,
                    minimum=None, maximum=None):
    access = ACCESS_READWRITE if writeable else ACCESS_READ
    contents_set = _parameter_contents_set(identifier, description, raw_value, ptype, access,
                                           enumeration, minimum, maximum)
    return _app_constructed(G_QUAL_PARAMETER,
        _ctx_explicit(0, ber_relative_oid(path)) +
        _ctx_explicit(1, contents_set))

def _node_contents_set(identifier, description=""):
    """NodeContents = SET universel contenant les champs EXPLICIT-taggés."""
    fields = _ctx_explicit(NC_IDENTIFIER, ber_utf8(identifier))
    if description:
        fields += _ctx_explicit(NC_DESCRIPTION, ber_utf8(description))
    fields += _ctx_explicit(NC_IS_ONLINE, ber_bool(True))
    return _universal_constructed(17, fields)  # SET universel

def _qual_node(path, identifier, description="", children_bytes=b""):
    """children_bytes : déjà-encodé séquence de [Context 0] EXPLICIT element wrappers."""
    body = (_ctx_explicit(0, ber_relative_oid(path)) +
            _ctx_explicit(1, _node_contents_set(identifier, description)))
    if children_bytes:
        # children [2] EXPLICIT ElementCollection ([App 4] IMPLICIT SEQUENCE OF [0] Element)
        ec = _app_constructed(G_ELEMENT_COLLECTION, children_bytes)
        body += _ctx_explicit(2, ec)
    return _app_constructed(G_QUAL_NODE, body)

def _encode_element(path, kind, *args):
    if kind == 'node':
        identifier, description = args
        return _qual_node(path, identifier, description)
    # param : (identifier, description, raw_value, ptype, writeable[, enumeration[, minimum, maximum]])
    # `enumeration` doit être présent (même à None) pour que `minimum`/`maximum` soient lus :
    # c'est la forme produite par `_append_canonical` ; le mode libre (`_walk_node`) ne va
    # jamais au-delà de `enumeration` et reste donc inchangé.
    identifier, description, raw_value, ptype, writeable = args[:5]
    enumeration = args[5] if len(args) > 5 else None
    minimum = args[6] if len(args) > 6 else None
    maximum = args[7] if len(args) > 7 else None
    return _qual_parameter(path, identifier, description, raw_value, ptype, writeable,
                           enumeration, minimum, maximum)


def build_collection(elements, extra=None):
    """Arbre complet wrappé en [App 0] Root → [App 11] RootElementCollection.
    `elements` : itérable de tuples (path, kind, *args) — kind ∈ {'node','param'} ;
    chaque élément est un QualifiedNode/QualifiedParameter (chemin absolu RELATIVE-OID),
    structure « plate » que les consumers (VSM, tinyEmber+…) reconstruisent en arbre.
    `extra` : éléments DÉJÀ encodés (bytes) à ajouter (ex. QualifiedMatrix)."""
    chunks = []
    for path, kind, *args in elements:
        chunks.append(_ctx_explicit(0, _encode_element(path, kind, *args)))
    for el in (extra or []):
        chunks.append(_ctx_explicit(0, el))
    wrapped = b"".join(chunks)
    rec = _app_constructed(G_ROOT_ELEMENT_COLLECTION, wrapped)
    return _app_constructed(0, rec)  # [App 0] Root wrapper


# ═════════════════════════════════════════════════════════════════════
# Matrix (encodeurs conformes — corrige les non-conformités de Studio)
# ═════════════════════════════════════════════════════════════════════

def _label(base_path, description):
    """GlowLabel [App 18] : basePath [0] RELATIVE-OID + description [1] UTF8."""
    return _app_constructed(G_LABEL,
        _ctx_explicit(LB_BASEPATH, ber_relative_oid(base_path)) +
        _ctx_explicit(LB_DESCRIPTION, ber_utf8(description)))

def matrix_contents(identifier, description, mtype, n_targets, n_sources, labels=None):
    """MatrixContents = SET universel, champs EXPLICIT-taggés. `labels` (optionnel) =
    [(base_path, description), …] → SEQUENCE OF Label en MC_LABELS."""
    fields = (
        _ctx_explicit(MC_IDENTIFIER,      ber_utf8(identifier)) +
        _ctx_explicit(MC_DESCRIPTION,     ber_utf8(description)) +
        _ctx_explicit(MC_TYPE,            ber_int(mtype)) +
        _ctx_explicit(MC_ADDRESSING_MODE, ber_int(ADDR_LINEAR)) +
        _ctx_explicit(MC_TARGET_COUNT,    ber_int(n_targets)) +
        _ctx_explicit(MC_SOURCE_COUNT,    ber_int(n_sources))
    )
    if labels:
        coll = b"".join(_ctx_explicit(0, _label(bp, d)) for bp, d in labels)
        fields += _ctx_explicit(MC_LABELS, _universal_constructed(16, coll))  # SEQUENCE OF
    return _universal_constructed(17, fields)  # SET

def _signal(app_tag, number):
    """Signal (Target/Source) = number [0] UNIQUEMENT (Studio ajoutait un identifier → rejet)."""
    return _app_constructed(app_tag, _ctx_explicit(0, ber_int(number)))

def connection(target, sources, *, disposition=CN_DISP_TALLY, operation=None):
    """Connection [App 16] : target [0] + sources [1] en PackedNumbers (RELATIVE-OID) +
    disposition [3] (report provider→consumer ; tally par défaut). `operation` réservé au
    sens consumer→provider, normalement absent en report."""
    body = _ctx_explicit(CN_TARGET, ber_int(target))
    body += _ctx_explicit(CN_SOURCES, ber_relative_oid(list(sources or [])))
    if operation is not None:
        body += _ctx_explicit(CN_OPERATION, ber_int(operation))
    if disposition is not None:
        body += _ctx_explicit(CN_DISPOSITION, ber_int(disposition))
    return _app_constructed(G_CONNECTION, body)

def qualified_matrix(path, identifier, description, mtype,
                     targets, sources, connections, *, with_axes=True, labels=None):
    """QualifiedMatrix [App 17]. `targets`/`sources` = listes de numéros ;
    `connections` = [(target_num, [source_nums]), …].
    with_axes=False → contents SEULS (annonce en racine, sans le payload — évite la
    déconnexion VSM observée quand la matrice complète est mise dans la collection racine)."""
    contents = matrix_contents(identifier, description, mtype, len(targets), len(sources), labels)
    body = (_ctx_explicit(MX_NUMBER_OR_PATH, ber_relative_oid(path)) +
            _ctx_explicit(MX_CONTENTS, contents))
    if with_axes:
        tcoll = b"".join(_ctx_explicit(0, _signal(G_TARGET, n)) for n in targets)
        scoll = b"".join(_ctx_explicit(0, _signal(G_SOURCE, n)) for n in sources)
        ccoll = b"".join(_ctx_explicit(0, connection(t, ss)) for t, ss in connections)
        body += (_ctx_explicit(MX_TARGETS,     _app_constructed(G_ELEMENT_COLLECTION, tcoll)) +
                 _ctx_explicit(MX_SOURCES,     _app_constructed(G_ELEMENT_COLLECTION, scoll)) +
                 _ctx_explicit(MX_CONNECTIONS, _app_constructed(G_ELEMENT_COLLECTION, ccoll)))
    return _app_constructed(G_QUAL_MATRIX, body)


# ═════════════════════════════════════════════════════════════════════
# Décodage des requêtes consumer (GetDirectory, Subscribe, SetValue)
# ═════════════════════════════════════════════════════════════════════

def _ctx_unwrap(constructed, content, universal_tag):
    """Récupère le contenu primitif d'un champ context-taggé, IMPLICIT ou EXPLICIT.
    IMPLICIT (primitive) → content = octets bruts.
    EXPLICIT (constructed) → content contient un universal TLV ; on retourne son contenu."""
    if not constructed:
        return content
    for k, _, n, c in ber_iter(content):
        if k == 0 and n == universal_tag:
            return c
    return content

def _walk_elements(content):
    """Itère sur les éléments d'une ElementCollection (libember : wrappés en [Context 0]).
    Tolère aussi une SEQUENCE universelle qui contient les éléments."""
    for k, _, n, c in ber_iter(content):
        if k == 2 and n == 0:
            # [Context 0] : peut contenir directement l'app-tag, ou une SEQUENCE
            inner = list(ber_iter(c))
            if len(inner) == 1 and inner[0][0] == 0 and inner[0][2] == 16:
                # SEQUENCE → re-itérer ses entrées
                for k3, _, n3, c3 in ber_iter(inner[0][3]):
                    yield k3, n3, c3
            else:
                for k2, _, n2, c2 in inner:
                    yield k2, n2, c2
        elif k == 0 and n == 16:
            # SEQUENCE non-tagué (variante)
            for k2, _, n2, c2 in ber_iter(c):
                yield k2, n2, c2
        else:
            # Élément directement présent (sans wrapper)
            yield k, n, c

def parse_root(body):
    """Retourne une liste d'actions extraites du root reçu :
    [{"kind":"getdir","path":[...]},
     {"kind":"subscribe","path":[...]},
     {"kind":"unsubscribe","path":[...]},
     {"kind":"setvalue","path":[...],"value": <python value>, "ptype": int|None}]
    Accepte les messages wrappés en [Application 0] (Root) ou directement [App 11]."""
    actions = []
    for klass, _, num, content in ber_iter(body):
        if klass == 1 and num == 0:
            # Wrapper [App 0] Root : descendre pour trouver la RootElementCollection
            for k, _, n, c in ber_iter(content):
                if k == 1 and n == G_ROOT_ELEMENT_COLLECTION:
                    _parse_root_collection(c, actions)
        elif klass == 1 and num == G_ROOT_ELEMENT_COLLECTION:
            _parse_root_collection(content, actions)
    return actions

def _parse_root_collection(content, actions):
    for k, n, c in _walk_elements(content):
        _handle_root_element(k, n, c, actions)

def _handle_root_element(klass, num, content, actions):
    if klass != 1: return
    if num == G_QUAL_PARAMETER:
        _handle_qual_parameter(content, actions)
    elif num == G_QUAL_NODE:
        _handle_qual_node(content, actions)
    elif num == G_QUAL_MATRIX:
        _handle_qual_matrix(content, actions)
    elif num == G_COMMAND:
        # Command directement à la racine = "act on root" (path vide)
        _scan_for_command(klass, num, content, [], actions)
    elif num in (G_NODE, G_PARAMETER):
        # Format non-qualifié (VSM) : hiérarchie imbriquée Node/Parameter avec integer number
        _handle_nonqual_element(klass, num, content, [], actions)

def _handle_nonqual_element(klass, num, content, path, actions):
    """Parser récursif pour le format non-qualifié (VSM) :
    Node=[App 3] et Parameter=[App 1] avec integer number dans [Ctx 0].
    Reconstruit le chemin absolu en descendant la hiérarchie imbriquée."""
    if klass != 1:
        return
    if num == G_NODE:
        node_num = None
        children_raw = None
        for k, constructed, n, c in ber_iter(content):
            if k == 2 and n == 0:
                node_num = parse_int(_ctx_unwrap(constructed, c, U_INTEGER))
            elif k == 2 and n == 2:
                children_raw = c
        if node_num is None or children_raw is None:
            return
        child_path = path + [node_num]
        for k, _, n, c in ber_iter(children_raw):
            if k == 1 and n == G_ELEMENT_COLLECTION:
                for k2, n2, c2 in _walk_elements(c):
                    _handle_nonqual_element(k2, n2, c2, child_path, actions)
    elif num == G_PARAMETER:
        param_num = None
        new_value = None
        ptype = None
        children_raw = None
        for k, constructed, n, c in ber_iter(content):
            if k == 2 and n == 0:
                param_num = parse_int(_ctx_unwrap(constructed, c, U_INTEGER))
            elif k == 2 and n == 1:
                inner = list(ber_iter(c))
                if len(inner) == 1 and inner[0][0] == 0 and inner[0][2] == 17:
                    fields_iter = ber_iter(inner[0][3])
                else:
                    fields_iter = ber_iter(c)
                for k2, c2_constructed, n2, c2 in fields_iter:
                    if k2 == 2 and n2 == PC_VALUE:
                        for k3, _, n3, c3 in ber_iter(c2):
                            if k3 == 0:
                                if n3 == U_INTEGER:  new_value, ptype = parse_int(c3), PT_INTEGER
                                elif n3 == U_REAL:   new_value, ptype = parse_real(c3), PT_REAL
                                elif n3 == U_UTF8:   new_value, ptype = parse_utf8(c3), PT_STRING
                                elif n3 == U_BOOL:   new_value, ptype = parse_bool(c3), PT_BOOLEAN
                    elif k2 == 2 and n2 == PC_TYPE and ptype is None:
                        ptype = parse_int(_ctx_unwrap(c2_constructed, c2, 2))
            elif k == 2 and n == 2:
                children_raw = c
        if param_num is not None and new_value is not None:
            actions.append({"kind": "setvalue", "path": path + [param_num],
                            "value": new_value, "ptype": ptype})
        if param_num is not None and children_raw is not None:
            param_path = path + [param_num]
            for k, _, n, c in ber_iter(children_raw):
                if k == 1 and n == G_ELEMENT_COLLECTION:
                    for k2, n2, c2 in _walk_elements(c):
                        _handle_nonqual_element(k2, n2, c2, param_path, actions)
    elif num == G_COMMAND:
        _scan_for_command(klass, num, content, path, actions)

def _handle_qual_node(content, actions):
    path = []
    children_content = None
    for k, constructed, n, c in ber_iter(content):
        if k == 2 and n == 0:
            path = parse_relative_oid(_ctx_unwrap(constructed, c, 13))
        elif k == 2 and n == 2:
            children_content = c
    if children_content is not None:
        for k, n, c in _walk_elements(children_content):
            _scan_for_command(k, n, c, path, actions)
            if k == 1 and n == G_ELEMENT_COLLECTION:
                for k2, n2, c2 in _walk_elements(c):
                    _scan_for_command(k2, n2, c2, path, actions)

def _handle_qual_parameter(content, actions):
    path = []
    new_value = None
    ptype = None
    children_content = None
    for k, constructed, n, c in ber_iter(content):
        if k == 2 and n == 0:
            path = parse_relative_oid(_ctx_unwrap(constructed, c, 13))
        elif k == 2 and n == 1:
            # ParameterContents : EXPLICIT (contient universal SET) ou IMPLICIT (champs directs).
            inner = list(ber_iter(c))
            if len(inner) == 1 and inner[0][0] == 0 and inner[0][2] == 17:
                fields_iter = ber_iter(inner[0][3])
            else:
                fields_iter = ber_iter(c)
            for k2, c2_constructed, n2, c2 in fields_iter:
                if k2 == 2 and n2 == PC_VALUE:
                    # VALUE est EXPLICIT (CHOICE) : c2 contient un universal TLV
                    for k3, _, n3, c3 in ber_iter(c2):
                        if k3 == 0:
                            if n3 == U_INTEGER:  new_value, ptype = parse_int(c3), PT_INTEGER
                            elif n3 == U_REAL:   new_value, ptype = parse_real(c3), PT_REAL
                            elif n3 == U_UTF8:   new_value, ptype = parse_utf8(c3), PT_STRING
                            elif n3 == U_BOOL:   new_value, ptype = parse_bool(c3), PT_BOOLEAN
                elif k2 == 2 and n2 == PC_TYPE and ptype is None:
                    ptype = parse_int(_ctx_unwrap(c2_constructed, c2, 2))
        elif k == 2 and n == 2:
            children_content = c
    if path and new_value is not None:
        actions.append({"kind": "setvalue", "path": path, "value": new_value, "ptype": ptype})
    if children_content is not None:
        for k, n, c in _walk_elements(children_content):
            _scan_for_command(k, n, c, path, actions)
            if k == 1 and n == G_ELEMENT_COLLECTION:
                for k2, n2, c2 in _walk_elements(c):
                    _scan_for_command(k2, n2, c2, path, actions)

def _parse_packed_numbers(constructed, c):
    """PackedNumbers ::= RELATIVE-OID. Tolérant : accepte aussi SEQUENCE OF INTEGER
    et INTEGER simple (variantes de consumers). Retourne une liste d'entiers."""
    if constructed:
        for k, _, n, cc in ber_iter(c):
            if k == 0 and n == 13:          # RELATIVE-OID (EXPLICIT)
                return parse_relative_oid(cc)
            if k == 0 and n == 16:          # SEQUENCE OF INTEGER (tolérance)
                return [parse_int(x[3]) for x in ber_iter(cc) if x[0] == 0 and x[2] == U_INTEGER]
            if k == 0 and n == U_INTEGER:   # INTEGER simple
                return [parse_int(cc)]
        return parse_relative_oid(c)        # EXPLICIT non reconnu → contenu brut en rel-oid
    return parse_relative_oid(c)            # IMPLICIT primitive → rel-oid brut

def _parse_connection_element(matrix_path, content, actions):
    """Décode une Connection entrante (consumer→provider) en action 'connect'."""
    target = None
    sources = []
    operation = CN_OP_ABSOLUTE
    for k, constructed, n, c in ber_iter(content):
        if k == 2 and n == CN_TARGET:
            target = parse_int(_ctx_unwrap(constructed, c, U_INTEGER))
        elif k == 2 and n == CN_SOURCES:
            sources = _parse_packed_numbers(constructed, c)
        elif k == 2 and n == CN_OPERATION:
            operation = parse_int(_ctx_unwrap(constructed, c, U_INTEGER))
    if target is not None:
        actions.append({"kind": "connect", "matrix_path": matrix_path,
                        "target": target, "sources": sources, "operation": operation})

def _iter_connections(raw):
    """Yield le contenu de chaque Connection [App 16], quel que soit l'emballage de la
    collection connections[5] : ElementCollection [App 4], SEQUENCE universelle, wrapper
    [Ctx 0], ou Connection directe. Robuste aux variantes de consumers (VSM, tinyEmber+…)."""
    for k, _, n, c in ber_iter(raw):
        if k == 1 and n == G_CONNECTION:
            yield c
        elif k == 1 and n == G_ELEMENT_COLLECTION:
            yield from _iter_connections(c)
        elif k == 0 and n == 16:          # SEQUENCE OF
            yield from _iter_connections(c)
        elif k == 2 and n == 0:           # [Ctx 0] wrapper
            yield from _iter_connections(c)

def _handle_qual_matrix(content, actions):
    """Parse un QualifiedMatrix entrant : soit un GetDirectory (pas de connexions),
    soit une/des Connection(s) (crosspoint demandé par le consumer)."""
    path = []
    connections_raw = None
    for k, constructed, n, c in ber_iter(content):
        if k == 2 and n == MX_NUMBER_OR_PATH:
            path = parse_relative_oid(_ctx_unwrap(constructed, c, 13))
        elif k == 2 and n == MX_CONNECTIONS:
            connections_raw = c
    if connections_raw is None:
        if path:
            actions.append({"kind": "getdir", "path": path})
        return
    for conn_content in _iter_connections(connections_raw):
        _parse_connection_element(path, conn_content, actions)

def _scan_for_command(klass, num, content, path, actions):
    if klass != 1 or num != G_COMMAND: return
    cmd_num = None
    for k, constructed, n, c in ber_iter(content):
        if k == 2 and n == 0:
            cmd_num = parse_int(_ctx_unwrap(constructed, c, 2))
    if cmd_num == CMD_GET_DIRECTORY:
        actions.append({"kind": "getdir", "path": path})
    elif cmd_num == CMD_SUBSCRIBE:
        actions.append({"kind": "subscribe", "path": path})
    elif cmd_num == CMD_UNSUBSCRIBE:
        actions.append({"kind": "unsubscribe", "path": path})
