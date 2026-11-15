#!/usr/bin/env python3
"""Compact incr AOF records (D3-all-g11-binaof), the Python side of src/aof.c "Compact incr AOF".

File: MAGIC (8 bytes) then records, one opcode byte each; lengths are unsigned LEB128 varints:
  0x01 / 0x02      SET / set with exactly key and value: <klen> key <vlen> value
  0x03             any command: <argc> then argc x (<len> bytes)
  0x04             annotation (a RESP "#..." line's text without '#' and CRLF): <len> bytes
  0x80|i / 0xC0|i  command CMDS[i] spelled upper / lower case: <argc - 1> then the args
Every record decodes to the argv the RESP record would hold, byte for byte.

The gate's AOF checkers use parse_any() so they read either format, and resp_to_compact() to turn a
RESP record pattern into the compact record a compact file holds for it.
"""
MAGIC = b"\xfeVKAOFC\x01"
CMDS = [b"SET", b"DEL", b"INCR", b"DECR", b"INCRBY", b"DECRBY", b"INCRBYFLOAT", b"APPEND",
        b"PEXPIREAT", b"PERSIST", b"SELECT", b"MULTI", b"EXEC", b"UNLINK", b"SETRANGE", b"HSET",
        b"HDEL", b"LPUSH", b"RPUSH", b"LPOP", b"RPOP", b"SADD", b"SREM", b"ZADD",
        b"ZREM", b"HINCRBY", b"MSET", b"XADD", b"FLUSHALL", b"FLUSHDB", b"EXPIRE", b"PEXPIRE"]
LCMDS = [c.lower() for c in CMDS]


class Truncated(ValueError):
    """The data ends inside a record (or inside the header)."""


def is_compact(data):
    return data[:len(MAGIC)] == MAGIC


def varint(v):
    out = bytearray()
    while v >= 0x80:
        out.append((v & 0x7f) | 0x80)
        v >>= 7
    out.append(v)
    return bytes(out)


def _arg(a):
    a = a if isinstance(a, bytes) else str(a).encode()
    return varint(len(a)) + a


def encode(args):
    """One command (a list of bytes / str / int) as its compact record, as catAppendOnlyCompactCommand."""
    args = [a if isinstance(a, bytes) else str(a).encode() for a in args]
    name = args[0]
    i, lower = -1, 0
    if name in CMDS:
        i = CMDS.index(name)
    elif name in LCMDS:
        i, lower = LCMDS.index(name), 1
    if i == 0 and len(args) == 3:
        return bytes([0x02 if lower else 0x01]) + _arg(args[1]) + _arg(args[2])
    if i >= 0:
        return bytes([0x80 | (0x40 if lower else 0) | i]) + varint(len(args) - 1) + b"".join(_arg(a) for a in args[1:])
    return b"\x03" + varint(len(args)) + b"".join(_arg(a) for a in args)


def encode_annotation(text):
    text = text if isinstance(text, bytes) else text.encode()
    return b"\x04" + _arg(text)


def _get_varint(data, i):
    x, shift = 0, 0
    while True:
        if i >= len(data):
            raise Truncated("varint at %d" % i)
        c = data[i]
        i += 1
        x |= (c & 0x7f) << shift
        if not c & 0x80:
            return x, i
        shift += 7
        if shift >= 64:
            raise ValueError("varint too long at %d" % i)


def _get_bytes(data, i):
    n, i = _get_varint(data, i)
    if i + n > len(data):
        raise Truncated("bytes at %d" % i)
    return data[i:i + n], i + n


def decode_one(data, i):
    """(kind, payload, next offset) for the record at i: kind 'rec' (argv list) or 'ann' (text)."""
    if i >= len(data):
        raise Truncated("record at %d" % i)
    op = data[i]
    i += 1
    if op == 0x04:
        t, i = _get_bytes(data, i)
        return "ann", t, i
    if op in (0x01, 0x02):
        args, n = [CMDS[0] if op == 0x01 else LCMDS[0]], 2
    elif op & 0x80:
        k = op & 0x3f
        if k >= len(CMDS):
            raise ValueError("bad command index %d at %d" % (k, i - 1))
        n, i = _get_varint(data, i)
        args = [LCMDS[k] if op & 0x40 else CMDS[k]]
    elif op == 0x03:
        n, i = _get_varint(data, i)
        if n == 0:
            raise ValueError("empty argv at %d" % (i - 1))
        args = []
    else:
        raise ValueError("bad opcode 0x%02x at %d" % (op, i - 1))
    for _ in range(n):
        a, i = _get_bytes(data, i)
        args.append(a)
    return "rec", args, i


def decode(data, partial=False):
    """(records, annotations, end): records = argv lists; end = the offset after the last whole record.
    partial: stop at a truncated record instead of raising Truncated."""
    if not is_compact(data):
        if partial and MAGIC.startswith(data):
            return [], 0, 0
        raise ValueError("no compact header")
    recs, ann, i = [], 0, len(MAGIC)
    while i < len(data):
        try:
            kind, p, j = decode_one(data, i)
        except Truncated:
            if partial:
                return recs, ann, i
            raise
        if kind == "ann":
            ann += 1
        else:
            recs.append(p)
        i = j
    return recs, ann, i


def parse_resp(data):
    recs, ann, i = [], 0, 0
    while i < len(data):
        if data[i:i + 1] == b"#":
            i = data.index(b"\r\n", i) + 2
            ann += 1
            continue
        if data[i:i + 1] != b"*":
            raise ValueError("bad record at %d: %r" % (i, data[i:i + 20]))
        j = data.index(b"\r\n", i)
        n, i, args = int(data[i + 1:j]), j + 2, []
        for _ in range(n):
            if data[i:i + 1] != b"$":
                raise ValueError("bad bulk at %d: %r" % (i, data[i:i + 20]))
            j = data.index(b"\r\n", i)
            ln, i = int(data[i + 1:j]), j + 2
            args.append(data[i:i + ln])
            if data[i + ln:i + ln + 2] != b"\r\n":
                raise ValueError("bad bulk end at %d" % (i + ln))
            i += ln + 2
        recs.append(args)
    return recs, ann


def parse_any(data):
    """(records, annotations) of a RESP or compact incr file."""
    if is_compact(data):
        r, a, _ = decode(data)
        return r, a
    return parse_resp(data)


def resp_to_compact(resp):
    """The compact records for RESP record bytes (one or more records)."""
    recs, _ = parse_resp(resp)
    return b"".join(encode(r) for r in recs)


def resp_encode(args):
    return b"*%d\r\n" % len(args) + b"".join(b"$%d\r\n%s\r\n" % (len(a), a) for a in args)


def to_resp(data, partial=True):
    """A compact incr file's whole records (and annotations) as the RESP bytes a RESP file would hold, so a
    checker's RESP byte patterns can search it. partial: drop a truncated last record (a file being written)."""
    out, i = [], len(MAGIC)
    while i < len(data):
        try:
            kind, p, j = decode_one(data, i)
        except Truncated:
            if partial:
                break
            raise
        out.append(b"#" + p + b"\r\n" if kind == "ann" else resp_encode(p))
        i = j
    return b"".join(out)


def manifest_incr(dirpath):
    """[(file name, 'compact'|'resp')] of the incr files in the AOF dir's manifest, in order."""
    import os
    man = [f for f in os.listdir(dirpath) if f.endswith(".manifest")][0]
    out = []
    for l in open(os.path.join(dirpath, man)):
        p = l.split()
        kv = dict(zip(p[0::2], p[1::2]))
        if kv.get("type") == "i":
            out.append((kv["file"], kv.get("format", "resp")))
    return out
