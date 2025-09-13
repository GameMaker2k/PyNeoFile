#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# pyneofile_light.py - lightweight pack/unpack/list with Py2/3 compatibility.

import os, sys, io, json, time, struct

PY2 = (sys.version_info[0] == 2)

try:
    basestring
except NameError:
    basestring = (str,)

try:
    unicode  # Py2
except NameError:
    unicode = str

def to_text(s, encoding="utf-8", errors="strict"):
    if s is None:
        return u""
    if isinstance(s, unicode):
        return s
    if isinstance(s, (bytes, bytearray)):
        return s.decode(encoding, errors)
    return unicode(s)

def to_bytes(s, encoding="utf-8", errors="strict"):
    if s is None:
        return b""
    if isinstance(s, (bytes, bytearray)):
        return bytes(s)
    return to_text(s, encoding, errors).encode(encoding, errors)

def normpath(p):
    s = to_text(p)
    s = s.replace("\\\\", "/")
    if "://" not in s:
        while "//" in s:
            s = s.replace("//", "/")
    return s.rstrip("/") or s

def ensure_dir(path):
    if not path:
        return
    if not os.path.isdir(path):
        os.makedirs(path)

_zlib = None
_gzip = None
_bz2 = None
_lzma = None
HAVE_LZMA = False

def _init_backends():
    global _zlib, _gzip, _bz2, _lzma, HAVE_LZMA
    if _zlib is None:
        import zlib as _z
        _zlib = _z
    if _gzip is None:
        import gzip as _g
        _gzip = _g
    if _bz2 is None:
        import bz2 as _b
        _bz2 = _b
    if _lzma is None:
        try:
            import lzma as _l
            _lzma = _l
            HAVE_LZMA = True
        except Exception:
            _lzma = None
            HAVE_LZMA = False

COMP_NONE = 0
COMP_ZLIB = 1
COMP_GZIP = 2
COMP_BZ2  = 3
COMP_XZ   = 4

def compress(data, algo):
    _init_backends()
    if algo == COMP_NONE:
        return data
    if algo == COMP_ZLIB:
        return _zlib.compress(data, 6)
    if algo == COMP_GZIP:
        buf = io.BytesIO()
        gz = _gzip.GzipFile(fileobj=buf, mode='wb', compresslevel=6)
        try:
            gz.write(data)
        finally:
            gz.close()
        return buf.getvalue()
    if algo == COMP_BZ2:
        return _bz2.compress(data, 6) if hasattr(_bz2, "compress") else _bz2.BZ2Compressor(6).compress(data)
    if algo == COMP_XZ:
        if not HAVE_LZMA:
            raise RuntimeError("xz/lzma not available on this Python")
        return _lzma.compress(data, preset=6)
    raise ValueError("Unknown compression algorithm: %r" % algo)

def decompress(data, algo):
    _init_backends()
    if algo == COMP_NONE:
        return data
    if algo == COMP_ZLIB:
        return _zlib.decompress(data)
    if algo == COMP_GZIP:
        buf = io.BytesIO(data)
        gz = _gzip.GzipFile(fileobj=buf, mode='rb')
        try:
            return gz.read()
        finally:
            gz.close()
    if algo == COMP_BZ2:
        return _bz2.decompress(data)
    if algo == COMP_XZ:
        if not HAVE_LZMA:
            raise RuntimeError("xz/lzma not available on this Python")
        return _lzma.decompress(data)
    raise ValueError("Unknown compression algorithm: %r" % algo)

AUTO_XZ_MIN   = 2 * 1024 * 1024
AUTO_BZ2_MIN  = 256 * 1024
AUTO_ZLIB_MIN = 16 * 1024

def pick_algo(total_size):
    _init_backends()
    if total_size >= AUTO_XZ_MIN and HAVE_LZMA:
        return COMP_XZ
    if total_size >= AUTO_BZ2_MIN:
        return COMP_BZ2
    if total_size >= AUTO_ZLIB_MIN:
        return COMP_ZLIB
    return COMP_NONE

MAGIC = b"PNF1"

def _iter_files(sources):
    for src in sources:
        src = to_text(src)
        if os.path.isdir(src):
            base = src
            for root, _, files in os.walk(src):
                for f in files:
                    p = os.path.join(root, f)
                    st = os.stat(p)
                    name = normpath(os.path.relpath(p, base))
                    yield name, p, st.st_size, int(st.st_mtime)
        else:
            st = os.stat(src)
            name = normpath(os.path.basename(src))
            yield name, src, st.st_size, int(st.st_mtime)

def pack(sources, outfile, compression="auto"):
    assert isinstance(sources, (list, tuple)) and sources, "sources must be a non-empty list"
    algo_map = {
        'none': COMP_NONE, 'zlib': COMP_ZLIB, 'gzip': COMP_GZIP,
        'bz2': COMP_BZ2, 'xz': COMP_XZ, 'auto': 'auto',
    }
    if compression not in algo_map:
        raise ValueError("Unknown compression: %r" % compression)
    entries = list(_iter_files(sources))
    total_size = sum(s for _,__,s,___ in entries)
    algo = pick_algo(total_size) if compression == 'auto' else algo_map[compression]

    manifest = {"files": [{"name": n, "size": s, "mtime": m} for (n, p, s, m) in entries]}
    hdr = to_bytes(json.dumps(manifest, sort_keys=True, separators=(',', ':')))
    flags = 0 if algo == COMP_NONE else 1
    header = MAGIC + struct.pack(">BBI", flags, algo, len(hdr)) + hdr

    if outfile == "-" or outfile is None:
        out = sys.stdout if outfile == "-" else sys.stdout
        stream = getattr(out, "buffer", out)
        stream.write(header)
        raw = io.BytesIO()
        for name, path, size, mtime in entries:
            with open(path, "rb") as f:
                while True:
                    chunk = f.read(1024 * 1024)
                    if not chunk:
                        break
                    raw.write(chunk)
        payload = raw.getvalue()
        stream.write(compress(payload, algo))
        stream.flush()
        return

    ensure_dir(os.path.dirname(outfile) or ".")
    with open(outfile, "wb") as f:
        f.write(header)
        raw = io.BytesIO()
        for name, path, size, mtime in entries:
            with open(path, "rb") as rf:
                while True:
                    chunk = rf.read(1024 * 1024)
                    if not chunk:
                        break
                    raw.write(chunk)
        payload = raw.getvalue()
        f.write(compress(payload, algo))

def _read_header(stream):
    magic = stream.read(4)
    if magic != MAGIC:
        raise ValueError("Not a PNF archive or corrupt magic: %r" % magic)
    flags, algo, hdrln = struct.unpack(">BBI", stream.read(6))
    hdr = stream.read(hdrln)
    man = json.loads(to_text(hdr))
    return flags, algo, man

def list_archive(infile):
    stream = sys.stdin if infile in (None, "-") else open(infile, "rb")
    stream = getattr(stream, "buffer", stream)
    try:
        _, algo, man = _read_header(stream)
        return man
    finally:
        if stream is not sys.stdin and stream is not getattr(sys.stdin, "buffer", sys.stdin):
            stream.close()

def unpack(infile, outdir):
    stream = sys.stdin if infile in (None, "-") else open(infile, "rb")
    stream = getattr(stream, "buffer", stream)
    try:
        flags, algo, man = _read_header(stream)
        payload = stream.read()
        data = decompress(payload, algo)
        cursor = 0
        ensure_dir(outdir or ".")
        for entry in man.get("files", []):
            name = normpath(entry["name"])
            size = int(entry["size"])
            mtime = int(entry.get("mtime", int(time.time())))
            outpath = os.path.join(outdir or ".", name)
            ensure_dir(os.path.dirname(outpath))
            with open(outpath, "wb") as f:
                f.write(data[cursor:cursor+size])
            os.utime(outpath, (mtime, mtime))
            cursor += size
        if cursor != len(data):
            sys.stderr.write("Warning: payload trailing bytes: %d\n" % (len(data)-cursor))
    finally:
        if stream is not sys.stdin and stream is not getattr(sys.stdin, "buffer", sys.stdin):
            stream.close()

def _usage():
    msg = (
        "pyneofile_light.py - lightweight pack/unpack/list\\n\\n"
        "Usage:\\n"
        "  pack   OUTFILE  [--comp auto|none|zlib|gzip|bz2|xz]  SRC [SRC...]\\n"
        "  unpack INFILE   OUTDIR\\n"
        "  list   INFILE\\n"
    )
    sys.stdout.write(msg)

def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] in ("-h", "--help"):
        _usage()
        return 0
    cmd = argv.pop(0)
    if cmd == "pack":
        if not argv:
            _usage(); return 2
        outfile = argv.pop(0)
        comp = "auto"
        if argv and argv[0] == "--comp":
            argv.pop(0)
            if not argv:
                _usage(); return 2
            comp = argv.pop(0)
        sources = argv
        if not sources:
            _usage(); return 2
        pack(sources, outfile, comp)
        return 0
    elif cmd == "unpack":
        if len(argv) < 2:
            _usage(); return 2
        infile, outdir = argv[0], argv[1]
        unpack(infile, outdir)
        return 0
    elif cmd == "list":
        if len(argv) < 1:
            _usage(); return 2
        man = list_archive(argv[0])
        sys.stdout.write(json.dumps(man, indent=2, sort_keys=True) + ("\n" if not PY2 else ""))
        return 0
    else:
        _usage()
        return 2

if __name__ == "__main__":
    sys.exit(main())
