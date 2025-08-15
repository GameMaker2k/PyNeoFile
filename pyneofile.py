
# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals, division, absolute_import

"""
pyneofile.py  —  Alternate ArchiveFile core with Py2/3 compatible logic.

Features:
- Pack / unpack / repack / archive_to_array
- Validation and listing helpers (lowercase names)
- INI-driven format detection (prefers PYNEOFILE_INI / pyneofile.ini)
- Compression: zlib, gzip, bz2 (stdlib), xz/lzma when available (Py3)
- Size-based 'auto' compression policy
- Checksums (header/json/content) using stored bytes (padded CRC-32)
- Optional converters: ZIP/TAR (stdlib), RAR via rarfile, 7z via py7zr
- In-memory mode: bytes input, and bytes output when outfile is None/"-"
"""

import os, sys, io, stat, time, json, binascii, hashlib, re, codecs
try:
    from io import open as _iopen
except Exception:
    _iopen = open  # Py2 fallback

# ---------------- Python 2/3 shims ----------------
try:
    basestring
except NameError:
    basestring = (str,)

try:
    unicode
except NameError:
    unicode = str  # Py3 alias

try:
    from io import BytesIO
except ImportError:
    from cStringIO import StringIO as BytesIO  # Py2 fallback

# INI support (Py2/3)
try:
    import configparser as _cfg
except Exception:
    import ConfigParser as _cfg  # Py2

# --------------- Compression shim (stdlib only) ---------------
import zlib, bz2, gzip
try:
    import lzma as _lzma  # Py3
    _HAVE_LZMA = True
except Exception:
    _lzma = None
    _HAVE_LZMA = False

__program_name__ = "PyNeoFile"
__project__ = __program_name__
__project_url__ = "https://github.com/GameMaker2k/PyNeoFile"
__version_info__ = (0, 19, 8, "RC 1", 1)
__version_date_info__ = (2025, 8, 14, "RC 1", 1)
__version_date__ = str(__version_date_info__[0]) + "." + str(
    __version_date_info__[1]).zfill(2) + "." + str(__version_date_info__[2]).zfill(2)
__revision__ = __version_info__[3]
__revision_id__ = "$Id$"
if(__version_info__[4] is not None):
    __version_date_plusrc__ = __version_date__ + \
        "-" + str(__version_date_info__[4])
if(__version_info__[4] is None):
    __version_date_plusrc__ = __version_date__
if(__version_info__[3] is not None):
    __version__ = str(__version_info__[0]) + "." + str(__version_info__[
        1]) + "." + str(__version_info__[2]) + " " + str(__version_info__[3])
if(__version_info__[3] is None):
    __version__ = str(__version_info__[0]) + "." + str(__version_info__[1]) + "." + str(__version_info__[2])

def _normalize_algo(algo):
    if not algo:
        return 'none'
    a = (algo or 'none').lower()
    if a in ('xz', 'lzma'):
        return 'xz'
    if a in ('gz', 'gzip'):
        return 'gzip'
    if a in ('deflate', 'z'):
        return 'zlib'
    if a in ('bzip2', 'bzip', 'bz'):
        return 'bz2'
    if a == 'auto':
        return 'auto'
    return a

def _compress_bytes(data, algo='none', level=None):
    """Return (stored_bytes, used_algo)."""
    algo = _normalize_algo(algo)
    if algo in ('none', ''):
        return data, 'none'
    if algo == 'zlib':
        lvl = zlib.Z_DEFAULT_COMPRESSION if level is None else int(level)
        return zlib.compress(data, lvl), 'zlib'
    if algo == 'gzip':
        bio = BytesIO()
        gz = gzip.GzipFile(fileobj=bio, mode='wb', compresslevel=(6 if level is None else int(level)))
        try:
            gz.write(data)
        finally:
            gz.close()
        return bio.getvalue(), 'gzip'
    if algo == 'bz2':
        if level is None:
            return bz2.compress(data), 'bz2'
        return bz2.compress(data, int(level)), 'bz2'
    if algo == 'xz':
        if not _HAVE_LZMA:
            raise RuntimeError("xz/lzma compression not available on this Python (needs 3.x lzma)")
        kw = {}
        if level is not None:
            kw['preset'] = int(level)
        return _lzma.compress(data, **kw), 'xz'
    raise ValueError("Unknown compression algorithm: %r" % algo)

def _decompress_bytes(data, algo='none'):
    algo = _normalize_algo(algo)
    if algo in ('none', ''):
        return data
    if algo == 'zlib':
        return zlib.decompress(data)
    if algo == 'gzip':
        bio = BytesIO(data)
        gz = gzip.GzipFile(fileobj=bio, mode='rb')
        try:
            return gz.read()
        finally:
            gz.close()
    if algo == 'bz2':
        return bz2.decompress(data)
    if algo == 'xz':
        if not _HAVE_LZMA:
            raise RuntimeError("xz/lzma decompression not available on this Python (needs 3.x lzma)")
        return _lzma.decompress(data)
    raise ValueError("Unknown compression algorithm: %r" % algo)

# --- Auto compression policy thresholds (bytes) ---
_AUTO_XZ_MIN   = 2 * 1024 * 1024     # >= 2 MiB → prefer xz (Py3 only)
_AUTO_BZ2_MIN  = 256 * 1024          # >= 256 KiB → prefer bz2 (Py2 or Py3)
_AUTO_ZLIB_MIN = 16 * 1024           # >= 16 KiB → zlib; smaller often not worth compressing

def _auto_pick_for_size(size_bytes):
    """Return ('none'|'zlib'|'gzip'|'bz2'|'xz', level_or_None)."""
    if size_bytes < _AUTO_ZLIB_MIN:
        return ('none', None)
    if _HAVE_LZMA and size_bytes >= _AUTO_XZ_MIN:
        return ('xz', 6)
    if size_bytes >= _AUTO_BZ2_MIN:
        return ('bz2', 9)
    return ('zlib', 6)

# -----------------------------------------------------------------------------
# In-memory I/O helpers
# -----------------------------------------------------------------------------

def _wrap_infile(infile):
    """Return (fp, close_me). Accepts path, file-like, or bytes/bytearray."""
    if isinstance(infile, (bytes, bytearray, memoryview)):
        return BytesIO(bytes(infile)), True
    if hasattr(infile, 'read'):
        return infile, False
    return _iopen(infile, 'rb'), True

def _wrap_outfile(outfile):
    """Return (fp, close_me, to_bytes). If outfile is None or '-', buffer to bytes."""
    if outfile in (None, '-', b'-'):
        bio = BytesIO()
        return bio, False, True
    if hasattr(outfile, 'write'):
        return outfile, False, False
    return _iopen(outfile, 'wb'), True, False

def _normalize_pack_inputs(infiles):
    """Normalize in-memory inputs into items for pack_iter_neo.
    Supported forms:
      - dict {name: bytes_or_None} (None => directory if name endswith('/'))
      - list/tuple of (name, bytes) or (name, is_dir, bytes_or_None) or dicts
      - single bytes/bytearray => [('memory.bin', False, bytes)]
      - anything else => None (caller will do filesystem walk)
    """
    if isinstance(infiles, dict):
        items = []
        for k, v in infiles.items():
            name = str(k)
            is_dir = bool(v is None or name.endswith('/'))
            items.append({'name': name, 'is_dir': is_dir,
                          'data': (None if is_dir else (bytes(v) if v is not None else b''))})
        return items
    if isinstance(infiles, (bytes, bytearray, memoryview)):
        return [{'name': 'memory.bin', 'is_dir': False, 'data': bytes(infiles)}]
    if isinstance(infiles, (list, tuple)) and infiles:
        def _as_item(x):
            if isinstance(x, dict):
                return x
            if isinstance(x, (list, tuple)):
                if len(x) == 2:
                    n, b = x
                    return {'name': n, 'is_dir': False, 'data': (bytes(b) if b is not None else b'')}
                if len(x) >= 3:
                    n, is_dir, b = x[0], bool(x[1]), x[2]
                    return {'name': n, 'is_dir': is_dir,
                            'data': (None if is_dir else (bytes(b) if b is not None else b''))}
            return None
        items = []
        for it in infiles:
            conv = _as_item(it)
            if conv is None:
                return None
            items.append(conv)
        return items
    return None

# ---------------- Format helpers ----------------
def _ver_digits(verstr):
    """Keep numeric digits only; preserve '001' style."""
    if not verstr:
        return '001'
    digits = ''.join([c for c in unicode(verstr) if c.isdigit()])
    return digits or '001'

def _default_formatspecs():
    return {
        'format_magic': 'ArchiveFile',
        'format_ver': '001',
        'format_delimiter': '\x00',
        'new_style': True,
    }

__formatspecs_ini_cache__ = None

def _decode_delim_escape(s):
    try:
        return codecs.decode(s, 'unicode_escape')
    except Exception:
        return s

def _load_formatspecs_from_ini(paths=None, prefer_section=None):
    """
    Load format definition from an INI file.
    Search order:
      - explicit 'paths'
      - env PYNEOFILE_INI, then PYARCHIVE_INI
      - ./pyneofile.ini, ./archivefile.ini, ./catfile.ini, ./foxfile.ini
    Section selection:
      - prefer_section
      - [config] default=... if present
      - first non-[config] section
    """
    cands = []
    if paths:
        if isinstance(paths, basestring):
            cands.append(paths)
        else:
            cands.extend(paths)
    envp = os.environ.get('PYNEOFILE_INI') or os.environ.get('PYARCHIVE_INI')
    if envp:
        cands.append(envp)
    cands.extend(['pyneofile.ini'])

    picked = None
    for p in cands:
        if os.path.isfile(p):
            picked = p; break
    if not picked:
        return None

    try:
        cp = _cfg.ConfigParser() if hasattr(_cfg, 'ConfigParser') else _cfg.RawConfigParser()
        if hasattr(cp, 'read_file'):
            with _iopen(picked, 'r') as fh:
                cp.read_file(fh)
        else:
            cp.read(picked)
    except Exception:
        return None

    sec = None
    if prefer_section and cp.has_section(prefer_section):
        sec = prefer_section
    else:
        defname = None
        if cp.has_section('config'):
            try:
                defname = cp.get('config', 'default')
            except Exception:
                defname = None
        if defname and cp.has_section(defname):
            sec = defname
        else:
            for name in cp.sections():
                if name.lower() != 'config':
                    sec = name; break
    if not sec:
        return None

    def _get(name, default=None):
        try:
            return cp.get(sec, name)
        except Exception:
            return default

    magic = _get('magic', 'NeoFile')
    ver   = _get('ver', '001')
    delim = _get('delimiter', '\\x00')
    newst = _get('newstyle', 'true')
    ext   = _get('extension', '.neo')

    delim_real = _decode_delim_escape(delim)
    ver_digits = _ver_digits(ver)

    spec = {
        'format_magic': magic,
        'format_ver': ver_digits,
        'format_delimiter': delim_real,
        'new_style': (str(newst).lower() in ('1','true','yes','on')),
        'format_name': sec,
        'extension': ext,
    }
    return spec

def _ensure_formatspecs(specs):
    global __formatspecs_ini_cache__
    if specs:
        return specs
    if __formatspecs_ini_cache__ is None:
        __formatspecs_ini_cache__ = _load_formatspecs_from_ini()
    return __formatspecs_ini_cache__ or _default_formatspecs()

def _to_bytes(s):
    if isinstance(s, bytes):
        return s
    if isinstance(s, (bytearray, memoryview)):
        return bytes(s)
    if not isinstance(s, basestring):
        s = str(s)
    return s.encode('UTF-8')

def _append_null(b, delim):
    if not isinstance(b, bytes):
        b = _to_bytes(b)
    return b + _to_bytes(delim)

def _append_nulls(seq, delim):
    out = b''
    for x in seq:
        out += _append_null(x, delim)
    return out

def _hex(n):
    return ("%x" % int(n)).lower()

def _crc32(data):
    if not isinstance(data, bytes):
        data = _to_bytes(data)
    return ("%08x" % (binascii.crc32(data) & 0xffffffff)).lower()

def _sha_like(name, data):
    if not isinstance(data, bytes):
        data = _to_bytes(data)
    try:
        h = hashlib.new(name)
    except ValueError:
        raise ValueError("Unsupported checksum: %r" % name)
    h.update(data)
    return h.hexdigest()

def _checksum(data, cstype, text=False):
    if cstype in (None, '', 'none'):
        return '0'
    if text and not isinstance(data, bytes):
        data = _to_bytes(data)
    if (cstype or '').lower() == 'crc32':
        return _crc32(data)
    return _sha_like(cstype.lower(), data)

# ---------------- Header builders ----------------
def _write_global_header(fp, numfiles, encoding, checksumtype, extradata, formatspecs):
    delim = formatspecs['format_delimiter']
    magic = formatspecs['format_magic']
    ver_digits = _ver_digits(formatspecs.get('format_ver','001'))

    # extras blob: count + items
    if isinstance(extradata, dict) and extradata:
        payload = json.dumps(extradata, separators=(',', ':')).encode('UTF-8')
        try:
            import base64
            extradata = [base64.b64encode(payload).decode('UTF-8')]
        except Exception:
            extradata = []
    elif isinstance(extradata, dict):
        extradata = []

    extrafields = _hex(len(extradata))
    extras_blob = _append_null(extrafields, delim)
    if extradata:
        extras_blob += _append_nulls(extradata, delim)
    extras_size_hex = _hex(len(extras_blob))

    platform_name = os.name if os.name in ('nt', 'posix') else sys.platform
    fnumfiles_hex = _hex(int(numfiles))

    tmpoutlist = [encoding, platform_name, fnumfiles_hex, extras_size_hex, extrafields]
    tmpoutlen = 3 + len(tmpoutlist) + len(extradata) + 1  # compatibility
    tmpoutlen_hex = _hex(tmpoutlen)

    body = _append_nulls([tmpoutlen_hex, encoding, platform_name, fnumfiles_hex, extras_size_hex, extrafields], delim)
    if extradata:
        body += _append_nulls(extradata, delim)
    body += _append_null(checksumtype, delim)

    prefix = _append_null(magic + ver_digits, delim)
    tmpfileoutstr = body + _append_null('', delim)
    headersize_hex = _hex(len(tmpfileoutstr) - len(_to_bytes(delim)))
    out = prefix + _append_null(headersize_hex, delim) + body
    header_cs = _checksum(out, checksumtype, text=True)
    out += _append_null(header_cs, delim)
    fp.write(out)

def _build_file_header_bytes(filemeta, jsondata, content_bytes_stored, checksumtypes, extradata, formatspecs):
    """Return full bytes for a record (header+json+NUL+content+NUL)."""
    delim = formatspecs['format_delimiter']
    def H(x): return _hex(int(x))

    fname = filemeta['fname']
    if not re.match(r'^[\./]', fname):
        fname = './' + fname

    fields = [
        H(filemeta.get('ftype', 0)),
        filemeta.get('fencoding', 'UTF-8'),
        filemeta.get('fcencoding', 'UTF-8'),
        fname,
        filemeta.get('flinkname', ''),
        H(filemeta.get('fsize', 0)),
        H(filemeta.get('fatime', int(time.time()))),
        H(filemeta.get('fmtime', int(time.time()))),
        H(filemeta.get('fctime', int(time.time()))),
        H(filemeta.get('fbtime', int(time.time()))),
        H(filemeta.get('fmode', stat.S_IFREG | 0o666)),
        H(filemeta.get('fwinattributes', 0)),
        filemeta.get('fcompression', ''),
        H(filemeta.get('fcsize', 0)),
        H(filemeta.get('fuid', 0)),
        filemeta.get('funame', ''),
        H(filemeta.get('fgid', 0)),
        filemeta.get('fgname', ''),
        H(filemeta.get('fid', filemeta.get('index', 0))),
        H(filemeta.get('finode', filemeta.get('index', 0))),
        H(filemeta.get('flinkcount', 1)),
        H(filemeta.get('fdev', 0)),
        H(filemeta.get('fdev_minor', 0)),
        H(filemeta.get('fdev_major', 0)),
        "+" + str(len(delim)),
    ]

    # JSON payload
    fjsontype = 'json' if jsondata else 'none'
    if jsondata:
        raw_json = json.dumps(jsondata, separators=(',', ':')).encode('UTF-8')
        json_cs_type = checksumtypes[2]
        fjsonlen_hex = _hex(len(jsondata) if hasattr(jsondata, '__len__') else 0)
        fjsonsize_hex = _hex(len(raw_json))
        fjsoncs = _checksum(raw_json, json_cs_type, text=True)
    else:
        raw_json = b''
        json_cs_type = 'none'
        fjsonlen_hex = '0'
        fjsonsize_hex = '0'
        fjsoncs = '0'

    # extras (mirrors global)
    if isinstance(extradata, dict) and extradata:
        payload = json.dumps(extradata, separators=(',', ':')).encode('UTF-8')
        try:
            import base64
            extradata = [base64.b64encode(payload).decode('UTF-8')]
        except Exception:
            extradata = []
    elif isinstance(extradata, dict):
        extradata = []

    extrafields = _hex(len(extradata))
    extras_blob = _append_null(extrafields, delim)
    if extradata:
        extras_blob += _append_nulls(extradata, delim)
    extras_size_hex = _hex(len(extras_blob))

    rec_fields = []
    rec_fields.extend(fields)
    rec_fields.extend([fjsontype, fjsonlen_hex, fjsonsize_hex, json_cs_type, fjsoncs])
    rec_fields.extend([extras_size_hex, extrafields])
    if extradata:
        rec_fields.extend(extradata)

    header_cs_type  = checksumtypes[0]
    content_cs_type = checksumtypes[1] if len(content_bytes_stored) > 0 else 'none'
    rec_fields.extend([header_cs_type, content_cs_type])

    record_fields_len_hex = _hex(len(rec_fields) + 2)  # include two checksum VALUE fields
    header_no_cs = _append_nulls(rec_fields, delim)

    tmp_with_placeholders = _append_null(record_fields_len_hex, delim) + header_no_cs
    tmp_with_placeholders += _append_null('', delim) + _append_null('', delim)
    headersize_hex = _hex(len(tmp_with_placeholders) - len(_to_bytes(delim)))

    header_with_sizes = _append_null(headersize_hex, delim) + _append_null(record_fields_len_hex, delim) + header_no_cs

    header_checksum = _checksum(header_with_sizes, header_cs_type, text=True)
    content_checksum = _checksum(content_bytes_stored, content_cs_type, text=False)

    header_full = header_with_sizes + _append_nulls([header_checksum, content_checksum], delim)

    out = header_full + raw_json + _to_bytes(delim) + content_bytes_stored + _to_bytes(delim)
    return out

# --------------- Reader helpers ---------------
def _read_cstring(fp, delim):
    d = _to_bytes(delim)
    out = []
    while True:
        b = fp.read(1)
        if not b:
            break
        out.append(b)
        if len(out) >= len(d) and b''.join(out[-len(d):]) == d:
            return b''.join(out[:-len(d)])
    return b''

def _read_fields(fp, n, delim):
    fields = []
    for _ in range(int(n)):
        fields.append(_read_cstring(fp, delim).decode('UTF-8'))
    return fields

def _parse_global_header(fp, formatspecs, skipchecksum=False):
    delim = formatspecs['format_delimiter']
    magicver = _read_cstring(fp, delim).decode('UTF-8')
    _ = _read_cstring(fp, delim)  # headersize_hex

    tmpoutlenhex = _read_cstring(fp, delim).decode('UTF-8')
    fencoding = _read_cstring(fp, delim).decode('UTF-8')
    fostype = _read_cstring(fp, delim).decode('UTF-8')
    fnumfiles = int(_read_cstring(fp, delim).decode('UTF-8') or '0', 16)
    _ = _read_cstring(fp, delim)  # extras_size
    extrafields = int(_read_cstring(fp, delim).decode('UTF-8') or '0', 16)
    extras = []
    for _i in range(extrafields):
        extras.append(_read_cstring(fp, delim).decode('UTF-8'))
    checksumtype = _read_cstring(fp, delim).decode('UTF-8')
    _header_cs = _read_cstring(fp, delim).decode('UTF-8')
    return {'fencoding': fencoding, 'fnumfiles': fnumfiles, 'fostype': fostype,
            'fextradata': extras, 'fchecksumtype': checksumtype,
            'ffilelist': [], 'fformatspecs': formatspecs}

def _index_json_and_checks(vals):
    """Index JSON meta and checksum positions for a header field list `vals`."""
    def _is_hex(s):
        return bool(s) and all(c in '0123456789abcdefABCDEF' for c in s)

    if len(vals) < 25:
        raise ValueError("Record too short to index JSON/checksum meta; got %d fields" % len(vals))

    idx = 25
    fjsontype = vals[idx]; idx += 1

    v2 = vals[idx]     if idx < len(vals) else ''
    v3 = vals[idx + 1] if idx + 1 < len(vals) else ''
    v4 = vals[idx + 2] if idx + 2 < len(vals) else ''

    cs_candidates = set(['none','crc32','md5','sha1','sha224','sha256','sha384','sha512','blake2b','blake2s'])

    if _is_hex(v2) and _is_hex(v3) and v4.lower() in cs_candidates:
        idx_json_type = idx - 1
        idx_json_len  = idx
        idx_json_size = idx + 1
        idx_json_cst  = idx + 2
        idx_json_cs   = idx + 3
        idx += 4
    else:
        idx_json_type = idx - 1
        idx_json_len  = None
        idx_json_size = idx
        idx_json_cst  = idx + 1
        idx_json_cs   = idx + 2
        idx += 3

    if idx + 2 > len(vals):
        raise ValueError("Missing extras header fields")

    idx_extras_size  = idx
    idx_extras_count = idx + 1
    try:
        count_int = int(vals[idx_extras_count] or '0', 16)
    except Exception:
        raise ValueError("Extras count not hex; got %r" % vals[idx_extras_count])
    idx = idx + 2 + count_int

    if idx + 4 > len(vals):
        raise ValueError("Missing checksum types/values in header")

    idx_header_cs_type   = idx
    idx_content_cs_type  = idx + 1
    idx_header_cs        = idx + 2
    idx_content_cs       = idx + 3

    return {
        'json': (idx_json_type, idx_json_len, idx_json_size, idx_json_cst, idx_json_cs),
        'cstypes': (idx_header_cs_type, idx_content_cs_type),
        'csvals': (idx_header_cs, idx_content_cs),
    }

def _parse_record(fp, formatspecs, listonly=False, skipchecksum=False, uncompress=True):
    delim = formatspecs['format_delimiter']
    dbytes = _to_bytes(delim)

    first = _read_cstring(fp, delim)
    if first == b'0':
        second = _read_cstring(fp, delim)
        if second == b'0':
            return None
        headersize_hex = first.decode('UTF-8')
        fields_len_hex = second.decode('UTF-8')
    else:
        headersize_hex = first.decode('UTF-8')
        fields_len_hex = _read_cstring(fp, delim).decode('UTF-8')

    try:
        n_fields = int(fields_len_hex, 16)
    except Exception:
        raise ValueError("Bad record field-count hex: %r" % fields_len_hex)

    vals = _read_fields(fp, n_fields, delim)
    if len(vals) < 25:
        raise ValueError("Record too short: expected >=25 header fields, got %d" % len(vals))

    (ftypehex, fencoding, fcencoding, fname, flinkname,
     fsize_hex, fatime_hex, fmtime_hex, fctime_hex, fbtime_hex,
     fmode_hex, fwinattrs_hex, fcompression, fcsize_hex,
     fuid_hex, funame, fgid_hex, fgname, fid_hex, finode_hex,
     flinkcount_hex, fdev_hex, fdev_minor_hex, fdev_major_hex,
     fseeknextfile) = vals[:25]

    idx = _index_json_and_checks(vals)
    (idx_json_type, idx_json_len, idx_json_size, idx_json_cst, idx_json_cs) = idx['json']
    (idx_header_cs_type, idx_content_cs_type) = idx['cstypes']
    (idx_header_cs, idx_content_cs) = idx['csvals']

    fjsonsize_hex = vals[idx_json_size] or '0'
    try:
        fjsonsize = int(fjsonsize_hex, 16)
    except Exception:
        raise ValueError("Bad JSON size hex: %r" % fjsonsize_hex)

    json_bytes = fp.read(fjsonsize)
    fp.read(len(dbytes))

    # Read content (stored bytes)
    fsize  = int(fsize_hex, 16)
    fcsize = int(fcsize_hex, 16)
    read_size = fcsize if (fcompression not in ('', 'none', 'auto') and fcsize > 0) else fsize

    content_stored = b''
    if read_size:
        if listonly:
            fp.seek(read_size, io.SEEK_CUR)
        else:
            content_stored = fp.read(read_size)
    fp.read(len(dbytes))

    # Verify checksums (header json/content)
    header_cs_type  = vals[idx_header_cs_type]
    content_cs_type = vals[idx_content_cs_type]
    header_cs_val   = vals[idx_header_cs]
    content_cs_val  = vals[idx_content_cs]
    json_cs_type    = vals[idx_json_cst]
    json_cs_val     = vals[idx_json_cs]

    if fjsonsize and not skipchecksum:
        if _checksum(json_bytes, json_cs_type, text=True) != json_cs_val:
            raise ValueError("JSON checksum mismatch for %s" % fname)

    if not skipchecksum and read_size and not listonly:
        if _checksum(content_stored, content_cs_type, text=False) != content_cs_val:
            raise ValueError("Content checksum mismatch for %s" % fname)

    # Optionally decompress for returned content
    content_ret = content_stored
    if not listonly and uncompress and fcompression not in ('', 'none', 'auto'):
        try:
            content_ret = _decompress_bytes(content_stored, fcompression)
        except RuntimeError:
            content_ret = content_stored

    if not re.match(r'^[\./]', fname):
        fname = './' + fname

    return {
        'fid': int(fid_hex, 16),
        'finode': int(finode_hex, 16),
        'fname': fname,
        'flinkname': flinkname,
        'ftype': int(ftypehex, 16),
        'fsize': fsize,
        'fcsize': fcsize,
        'fatime': int(fatime_hex, 16),
        'fmtime': int(fmtime_hex, 16),
        'fctime': int(fctime_hex, 16),
        'fbtime': int(fbtime_hex, 16),
        'fmode': int(fmode_hex, 16),
        'fwinattributes': int(fwinattrs_hex, 16),
        'fuid': int(fuid_hex, 16),
        'funame': funame,
        'fgid': int(fgid_hex, 16),
        'fgname': fgname,
        'fcompression': fcompression,
        'fseeknext': fseeknextfile,
        'fjson': (json.loads(json_bytes.decode('UTF-8') or 'null') if fjsonsize else {}),
        'fcontent': (None if listonly else content_ret),
    }

# ---------------- Public API ----------------
def pack_neo(infiles, outfile=None, formatspecs=None,
             checksumtypes=("crc32","crc32","crc32"),
             encoding="UTF-8",
             compression="auto",
             compression_level=None):
    """Pack files/dirs to an archive file or return bytes when outfile is None/'-'."""
    fs = _ensure_formatspecs(formatspecs)
    delim = fs['format_delimiter']

    # In-memory sources?
    items = _normalize_pack_inputs(infiles)
    if items is not None:
        return pack_iter_neo(items, outfile, formatspecs=fs,
                             checksumtypes=checksumtypes, encoding=encoding,
                             compression=compression, compression_level=compression_level)

    if isinstance(infiles, basestring):
        paths = [infiles]
    else:
        paths = list(infiles)

    # Build file list (dirs recursively)
    filelist = []
    base_dir = None
    if len(paths) == 1 and os.path.isdir(paths[0]):
        base_dir = os.path.abspath(paths[0])
    for p in paths:
        if os.path.isdir(p):
            for root, dirs, files in os.walk(p):
                filelist.append((os.path.join(root, ''), True))
                for name in files:
                    filelist.append((os.path.join(root, name), False))
        else:
            filelist.append((p, False))

    # open destination
    fp, close_me, to_bytes = _wrap_outfile(outfile)

    try:
        _write_global_header(fp, len(filelist), encoding, checksumtypes[0], extradata=[], formatspecs=fs)

        fid = 0
        for apath, is_dir in filelist:
            st = os.lstat(apath)
            mode = st.st_mode
            if is_dir or stat.S_ISDIR(mode):
                raw = b''
                ftype = 5
            else:
                with _iopen(apath, 'rb') as f:
                    raw = f.read()
                ftype = 0

            # Decide compression
            algo = _normalize_algo(compression)
            if algo == 'auto':
                algo, auto_level = _auto_pick_for_size(len(raw))
                level = compression_level if compression_level is not None else auto_level
            else:
                level = compression_level

            try:
                stored_bytes, used_algo = _compress_bytes(raw, algo, level=level)
            except RuntimeError:
                stored_bytes, used_algo = _compress_bytes(raw, 'zlib', level=(6 if level is None else level))

            meta = {
                'ftype': ftype,
                'fencoding': encoding,
                'fcencoding': encoding,
                'fname': './' + os.path.relpath(apath).replace('\\', '/') if not re.match(r'^[\./]', apath) else apath,
                'flinkname': '',
                'fsize': len(raw),
                'fatime': int(getattr(st, 'st_atime', time.time())),
                'fmtime': int(getattr(st, 'st_mtime', time.time())),
                'fctime': int(getattr(st, 'st_ctime', time.time())),
                'fbtime': int(getattr(st, 'st_mtime', time.time())),
                'fmode': int(mode),
                'fwinattributes': 0,
                'fcompression': used_algo,
                'fcsize': len(stored_bytes),
                'fuid': int(getattr(st, 'st_uid', 0)),
                'funame': '',
                'fgid': int(getattr(st, 'st_gid', 0)),
                'fgname': '',
                'fid': fid,
                'finode': int(getattr(st, 'st_ino', fid)),
                'flinkcount': int(getattr(st, 'st_nlink', 1)),
                'fdev': int(getattr(st, 'st_dev', 0)),
                'fdev_minor': 0,
                'fdev_major': 0,
                'index': fid,
            }
            fid += 1

            rec = _build_file_header_bytes(meta, jsondata={}, content_bytes_stored=stored_bytes,
                                           checksumtypes=checksumtypes, extradata=[], formatspecs=fs)
            fp.write(rec)

        # end marker
        fp.write(_append_nulls(['0','0'], fs['format_delimiter']))
        if to_bytes:
            return fp.getvalue()
    finally:
        if close_me:
            fp.close()

def archive_to_array_neo(infile, formatspecs=None,
                         listonly=False, skipchecksum=False, uncompress=True):
    fs = _ensure_formatspecs(formatspecs)
    fp, close_me = _wrap_infile(infile)
    try:
        top = _parse_global_header(fp, fs, skipchecksum=skipchecksum)
        while True:
            rec = _parse_record(fp, fs, listonly=listonly, skipchecksum=skipchecksum, uncompress=uncompress)
            if rec is None:
                break
            top['ffilelist'].append(rec)
        return top
    finally:
        if close_me:
            fp.close()

def unpack_neo(infile, outdir='.', formatspecs=None, skipchecksum=False, uncompress=True):
    arr = archive_to_array_neo(infile, formatspecs=formatspecs, listonly=False, skipchecksum=skipchecksum, uncompress=uncompress)
    if not arr:
        return False

    # In-memory extraction
    if outdir in (None, '-', b'-'):
        result = {}
        for ent in arr['ffilelist']:
            if ent['ftype'] == 5:
                result[ent['fname']] = None
            else:
                result[ent['fname']] = ent.get('fcontent') or b''
        return result

    if not os.path.isdir(outdir):
        if os.path.exists(outdir):
            raise IOError("not a directory: %r" % outdir)
        os.makedirs(outdir)
    for ent in arr['ffilelist']:
        path = os.path.join(outdir, ent['fname'].lstrip('./'))
        if ent['ftype'] == 5:  # directory
            if not os.path.isdir(path):
                os.makedirs(path)
            continue
        d = os.path.dirname(path)
        if d and not os.path.isdir(d):
            os.makedirs(d)
        with _iopen(path, 'wb') as f:
            f.write(ent.get('fcontent') or b'')
        try:
            os.chmod(path, ent.get('fmode', 0o666))
        except Exception:
            pass
    return True

def repack_neo(infile, outfile=None, formatspecs=None,
               checksumtypes=("crc32","crc32","crc32"),
               compression="auto",
               compression_level=None):
    arr = archive_to_array_neo(infile, formatspecs=formatspecs, listonly=False, skipchecksum=False, uncompress=False)
    fs = _ensure_formatspecs(formatspecs)
    fp, close_me, to_bytes = _wrap_outfile(outfile)
    try:
        _write_global_header(fp, len(arr['ffilelist']), arr.get('fencoding', 'UTF-8'), checksumtypes[0],
                             extradata=arr.get('fextradata', []), formatspecs=fs)
        for i, ent in enumerate(arr['ffilelist']):
            src_algo = _normalize_algo(ent.get('fcompression', 'none'))
            dst_algo = _normalize_algo(compression)

            stored_src = ent.get('fcontent') or b''  # we requested uncompress=False, so this is stored bytes

            if dst_algo == 'auto':
                try:
                    raw = _decompress_bytes(stored_src, src_algo) if src_algo != 'none' else stored_src
                except RuntimeError:
                    raw = stored_src
                dst_algo, dst_level = _auto_pick_for_size(len(raw))
            else:
                if src_algo != 'none':
                    try:
                        raw = _decompress_bytes(stored_src, src_algo)
                    except RuntimeError:
                        raw = stored_src
                else:
                    raw = stored_src
                dst_level = compression_level

            if dst_algo == src_algo or (dst_algo == 'none' and src_algo == 'none'):
                stored_bytes = stored_src
                used_algo = src_algo
                try:
                    raw_len = len(_decompress_bytes(stored_src, src_algo)) if src_algo != 'none' else len(stored_src)
                except RuntimeError:
                    raw_len = len(stored_src)
            else:
                stored_bytes, used_algo = _compress_bytes(raw, dst_algo, level=dst_level)
                raw_len = len(raw)

            meta = {
                'ftype': ent['ftype'],
                'fencoding': arr.get('fencoding', 'UTF-8'),
                'fcencoding': arr.get('fencoding', 'UTF-8'),
                'fname': ent['fname'],
                'flinkname': ent.get('flinkname',''),
                'fsize': raw_len,
                'fatime': ent.get('fatime', int(time.time())),
                'fmtime': ent.get('fmtime', int(time.time())),
                'fctime': ent.get('fctime', int(time.time())),
                'fbtime': ent.get('fbtime', int(time.time())),
                'fmode': ent.get('fmode', stat.S_IFREG | 0o666),
                'fwinattributes': ent.get('fwinattributes', 0),
                'fcompression': used_algo,
                'fcsize': len(stored_bytes),
                'fuid': ent.get('fuid', 0),
                'funame': ent.get('funame', ''),
                'fgid': ent.get('fgid', 0),
                'fgname': ent.get('fgname', ''),
                'fid': ent.get('fid', i),
                'finode': ent.get('finode', i),
                'flinkcount': ent.get('flinkcount', 1),
                'fdev': ent.get('fdev', 0),
                'fdev_minor': ent.get('fdev_minor', 0),
                'fdev_major': ent.get('fdev_major', 0),
                'index': i,
            }
            rec = _build_file_header_bytes(meta, jsondata=ent.get('fjson', {}), content_bytes_stored=stored_bytes,
                                           checksumtypes=checksumtypes, extradata=[], formatspecs=fs)
            fp.write(rec)
        fp.write(_append_nulls(['0','0'], fs['format_delimiter']))
        if to_bytes:
            return fp.getvalue()
    finally:
        if close_me:
            fp.close()

# -----------------------------------------------------------------------------
# Alt validation and listing helpers (lowercase names for consistency)
# -----------------------------------------------------------------------------

def _read_record_raw(fp, formatspecs):
    """Low-level read of a single record returning header fields and stored blobs."""
    delim = formatspecs['format_delimiter']
    dbytes = _to_bytes(delim)

    first = _read_cstring(fp, delim)
    if first == b'0':
        second = _read_cstring(fp, delim)
        if second == b'0':
            return None
        headersize_hex = first.decode('UTF-8')
        fields_len_hex = second.decode('UTF-8')
    else:
        headersize_hex = first.decode('UTF-8')
        fields_len_hex = _read_cstring(fp, delim).decode('UTF-8')

    try:
        n_fields = int(fields_len_hex, 16)
    except Exception:
        raise ValueError("Bad record field-count hex: %r" % fields_len_hex)

    vals = _read_fields(fp, n_fields, delim)
    idxs = _index_json_and_checks(vals)
    (idx_json_type, idx_json_len, idx_json_size, idx_json_cst, idx_json_cs) = idxs['json']

    fjsonsize_hex = vals[idx_json_size] or '0'
    try:
        fjsonsize = int(fjsonsize_hex, 16)
    except Exception:
        raise ValueError("Bad JSON size hex: %r" % fjsonsize_hex)

    json_bytes = fp.read(fjsonsize)
    fp.read(len(dbytes))

    fcompression = vals[12]
    fsize_hex = vals[5]
    fcsize_hex = vals[13]
    fsize  = int(fsize_hex, 16)
    fcsize = int(fcsize_hex, 16)
    read_size = fcsize if (fcompression not in ('', 'none', 'auto') and fcsize > 0) else fsize
    content_stored = b''
    if read_size:
        content_stored = fp.read(read_size)
    fp.read(len(dbytes))

    return headersize_hex, fields_len_hex, vals, json_bytes, content_stored

def archivefilevalidate_neo(infile, formatspecs=None, verbose=False, return_details=False):
    """Validate an ArchiveFile using the alt parser."""
    fs = _ensure_formatspecs(formatspecs)
    details = []
    ok_all = True

    fp, close_me = _wrap_infile(infile)
    try:
        _ = _parse_global_header(fp, fs, skipchecksum=False)
        idx = 0
        while True:
            raw = _read_record_raw(fp, fs)
            if raw is None:
                break
            headersize_hex, fields_len_hex, vals, json_bytes, content_stored = raw

            idxs = _index_json_and_checks(vals)
            (idx_json_type, idx_json_len, idx_json_size, idx_json_cst, idx_json_cs) = idxs['json']
            (idx_header_cs_type, idx_content_cs_type) = idxs['cstypes']
            (idx_header_cs, idx_content_cs) = idxs['csvals']

            fname = vals[3]
            header_cs_type  = vals[idx_header_cs_type]
            content_cs_type = vals[idx_content_cs_type]
            header_cs_val   = vals[idx_header_cs]
            content_cs_val  = vals[idx_content_cs]
            json_cs_type    = vals[idx_json_cst]
            json_cs_val     = vals[idx_json_cs]

            delim = fs['format_delimiter']
            header_bytes = _append_null(headersize_hex, delim) + _append_null(fields_len_hex, delim) + _append_nulls(vals[:-2], delim)
            computed_hcs = _checksum(header_bytes, header_cs_type, text=True)
            h_ok = (computed_hcs == header_cs_val)

            j_ok = True
            try:
                fjsonsize_hex = vals[idx_json_size] or '0'
                fjsonsize = int(fjsonsize_hex, 16) if fjsonsize_hex else 0
            except Exception:
                fjsonsize = 0
            if fjsonsize:
                computed_jcs = _checksum(json_bytes, json_cs_type, text=True)
                j_ok = (computed_jcs == json_cs_val)

            c_ok = True
            if content_stored:
                computed_ccs = _checksum(content_stored, content_cs_type, text=False)
                c_ok = (computed_ccs == content_cs_val)

            entry_ok = h_ok and j_ok and c_ok
            ok_all = ok_all and entry_ok
            if verbose or return_details:
                details.append({
                    'index': idx,
                    'name': fname,
                    'header_ok': h_ok,
                    'json_ok': j_ok,
                    'content_ok': c_ok,
                    'fcompression': vals[12],
                    'fsize_hex': vals[5],
                    'fcsize_hex': vals[13],
                })
            idx += 1
    finally:
        if close_me:
            fp.close()

    if return_details:
        return ok_all, details
    return ok_all

def archivefilelistfiles_neo(infile, formatspecs=None, advanced=False, include_dirs=True):
    """List entries in an archive without extracting."""
    fs = _ensure_formatspecs(formatspecs)
    out = []

    fp, close_me = _wrap_infile(infile)
    try:
        _ = _parse_global_header(fp, fs, skipchecksum=True)
        while True:
            raw = _read_record_raw(fp, fs)
            if raw is None:
                break
            headersize_hex, fields_len_hex, vals, json_bytes, content_stored = raw

            ftypehex = vals[0]
            fname = vals[3]
            fcompression = vals[12]
            fsize_hex = vals[5]
            fcsize_hex = vals[13]
            fatime_hex = vals[6]
            fmtime_hex = vals[7]
            fmode_hex  = vals[10]

            ftype = int(ftypehex, 16)
            is_dir = (ftype == 5)

            if not include_dirs and is_dir:
                continue

            if not re.match(r'^[\./]', fname):
                fname = './' + fname

            if not advanced:
                out.append(fname)
            else:
                out.append({
                    'name': fname,
                    'type': 'dir' if is_dir else 'file',
                    'compression': fcompression or 'none',
                    'size': int(fsize_hex, 16),
                    'stored_size': int(fcsize_hex, 16),
                    'mtime': int(fmtime_hex, 16),
                    'atime': int(fatime_hex, 16),
                    'mode': int(fmode_hex, 16),
                })
    finally:
        if close_me:
            fp.close()
    return out

# -----------------------------------------------------------------------------
# Pack from iterator + foreign-archive conversion (stdlib + optional deps)
# -----------------------------------------------------------------------------

def pack_iter_neo(items, outfile, formatspecs=None,
                  checksumtypes=("crc32","crc32","crc32"),
                  encoding="UTF-8",
                  compression="auto",
                  compression_level=None):
    """
    Pack directly from an iterable of entries without touching the filesystem.
    Each item may be either a tuple (name, is_dir, data_bytes_or_None)
    or a dict with keys:
        name (str), is_dir (bool), data (bytes or None),
        mode (int, optional), mtime (int, optional),
        uid (int), gid (int), uname (str), gname (str)
    """
    fs = _ensure_formatspecs(formatspecs)
    fp, close_me, to_bytes = _wrap_outfile(outfile)

    try:
        # Count items first (may be a generator -> materialize)
        if not hasattr(items, '__len__'):
            items = list(items)
        _write_global_header(fp, len(items), encoding, checksumtypes[0], extradata=[], formatspecs=fs)

        fid = 0
        for it in items:
            if isinstance(it, dict):
                name = it.get('name')
                is_dir = bool(it.get('is_dir', False))
                data = it.get('data', None)
                mode = int(it.get('mode', stat.S_IFDIR | 0o755 if is_dir else stat.S_IFREG | 0o666))
                mtime = int(it.get('mtime', time.time()))
                uid = int(it.get('uid', 0)); gid = int(it.get('gid', 0))
                uname = it.get('uname', ''); gname = it.get('gname', '')
            else:
                name, is_dir, data = it
                mode = stat.S_IFDIR | 0o755 if is_dir or (name.endswith('/') and data is None) else stat.S_IFREG | 0o666
                mtime = int(time.time())
                uid = gid = 0; uname = gname = ''

            # Normalize name
            name = name.replace('\\', '/')
            if not re.match(r'^[\./]', name):
                name = './' + name

            if is_dir or name.endswith('/'):
                raw = b''
                ftype = 5
            else:
                raw = data or b''
                ftype = 0

            # Decide compression
            algo = _normalize_algo(compression)
            if algo == 'auto':
                algo, auto_level = _auto_pick_for_size(len(raw))
                level = compression_level if compression_level is not None else auto_level
            else:
                level = compression_level

            try:
                stored_bytes, used_algo = _compress_bytes(raw, algo, level=level)
            except RuntimeError:
                stored_bytes, used_algo = _compress_bytes(raw, 'zlib', level=(6 if level is None else level))

            meta = {
                'ftype': ftype,
                'fencoding': encoding,
                'fcencoding': encoding,
                'fname': name,
                'flinkname': '',
                'fsize': len(raw),
                'fatime': mtime,
                'fmtime': mtime,
                'fctime': mtime,
                'fbtime': mtime,
                'fmode': int(mode),
                'fwinattributes': 0,
                'fcompression': used_algo,
                'fcsize': len(stored_bytes),
                'fuid': uid,
                'funame': uname,
                'fgid': gid,
                'fgname': gname,
                'fid': fid,
                'finode': fid,
                'flinkcount': 1,
                'fdev': 0,
                'fdev_minor': 0,
                'fdev_major': 0,
                'index': fid,
            }
            fid += 1

            rec = _build_file_header_bytes(meta, jsondata={}, content_bytes_stored=stored_bytes,
                                           checksumtypes=checksumtypes, extradata=[], formatspecs=fs)
            fp.write(rec)

        # end marker
        fp.write(_append_nulls(['0','0'], fs['format_delimiter']))
        if to_bytes:
            return fp.getvalue()
    finally:
        if close_me:
            fp.close()

def _sniff_foreign_type(path):
    lower = os.path.basename(path).lower() if isinstance(path, (str, bytes)) else ''
    # Extension first
    if lower.endswith('.zip'):
        return 'zip'
    if lower.endswith(('.tar', '.tar.gz', '.tgz', '.tar.bz2', '.tbz2', '.tar.xz', '.txz')):
        return 'tar'
    if lower.endswith('.rar'):
        return 'rar'
    if lower.endswith('.7z'):
        return '7z'
    # Fallback: stdlib probes for zip/tar only
    try:
        import zipfile
        if isinstance(path, basestring) and zipfile.is_zipfile(path):
            return 'zip'
    except Exception:
        pass
    try:
        import tarfile
        if isinstance(path, basestring) and hasattr(tarfile, 'is_tarfile') and tarfile.is_tarfile(path):
            return 'tar'
    except Exception:
        pass
    return None

def _iter_tar_members(tarf):
    for m in tarf.getmembers():
        name = m.name
        if m.isdir():
            yield {'name': name.rstrip('/') + '/', 'is_dir': True, 'data': None,
                   'mode': (stat.S_IFDIR | (m.mode or 0o755)), 'mtime': int(getattr(m, 'mtime', time.time())),
                   'uid': int(getattr(m, 'uid', 0)), 'gid': int(getattr(m, 'gid', 0)),
                   'uname': getattr(m, 'uname', ''), 'gname': getattr(m, 'gname', '')}
        else:
            try:
                fh = tarf.extractfile(m)
                data = fh.read() if fh is not None else b''
            except Exception:
                data = b''
            yield {'name': name, 'is_dir': False, 'data': data,
                   'mode': (stat.S_IFREG | (m.mode or 0o644)), 'mtime': int(getattr(m, 'mtime', time.time())),
                   'uid': int(getattr(m, 'uid', 0)), 'gid': int(getattr(m, 'gid', 0)),
                   'uname': getattr(m, 'uname', ''), 'gname': getattr(m, 'gname', '')}

def _iter_zip_members(zipf):
    for zi in zipf.infolist():
        name = zi.filename
        mode = (zi.external_attr >> 16) & 0o777 if hasattr(zi, 'external_attr') else 0o644
        mtime = int(time.mktime(getattr(zi, 'date_time', (1980,1,1,0,0,0)) + (0,0,-1)))
        if name.endswith('/'):
            yield {'name': name, 'is_dir': True, 'data': None,
                   'mode': (stat.S_IFDIR | (mode or 0o755)), 'mtime': mtime}
        else:
            try:
                data = zipf.read(zi)
            except Exception:
                data = b''
            yield {'name': name, 'is_dir': False, 'data': data,
                   'mode': (stat.S_IFREG | (mode or 0o644)), 'mtime': mtime}

def _iter_rar_members(rarf):
    for ri in rarf.infolist():
        name = getattr(ri, 'filename', None) or getattr(ri, 'arcname', None)
        if name is None:
            continue
        try:
            is_dir = ri.is_dir()
        except Exception:
            is_dir = name.endswith('/') or name.endswith('\\')
        try:
            dt = getattr(ri, 'date_time', None)
            if dt:
                mtime = int(time.mktime(tuple(dt) + (0,0,-1)))
            else:
                mtime = int(time.time())
        except Exception:
            mtime = int(time.time())
        if is_dir:
            yield {'name': name, 'is_dir': True, 'data': None,
                   'mode': (stat.S_IFDIR | 0o755), 'mtime': mtime}
        else:
            try:
                data = rarf.read(ri)
            except Exception:
                data = b''
            yield {'name': name, 'is_dir': False, 'data': data,
                   'mode': (stat.S_IFREG | 0o644), 'mtime': mtime}

def _iter_7z_members(z7):
    names = []
    try:
        entries = z7.list()
        for e in entries:
            name = getattr(e, 'filename', None) or getattr(e, 'name', None)
            if name is None:
                continue
            is_dir = bool(getattr(e, 'is_directory', False)) or name.endswith('/') or name.endswith('\\')
            names.append((name, is_dir))
    except Exception:
        try:
            for n in z7.getnames():
                is_dir = n.endswith('/') or n.endswith('\\')
                names.append((n, is_dir))
        except Exception:
            names = []
    try:
        data_map = z7.readall()
    except Exception:
        data_map = {}

    for name, is_dir in names:
        if is_dir:
            yield {'name': name, 'is_dir': True, 'data': None,
                   'mode': (stat.S_IFDIR | 0o755), 'mtime': int(time.time())}
        else:
            try:
                blob = data_map.get(name, b'')
                if not isinstance(blob, (bytes, bytearray)):
                    try:
                        blob = b''.join(blob) if isinstance(blob, list) else bytes(blob)
                    except Exception:
                        blob = b''
            except Exception:
                blob = b''
            yield {'name': name, 'is_dir': False, 'data': blob,
                   'mode': (stat.S_IFREG | 0o644), 'mtime': int(time.time())}

def convert_foreign_to_neo(infile, outfile=None, formatspecs=None,
                           checksumtypes=("crc32","crc32","crc32"),
                           compression="auto",
                           compression_level=None):
    """
    Convert a foreign archive (zip/tar/rar/7z) into the alt ArchiveFile format.
    Uses stdlib for zip/tar; requires 'rarfile' for RAR and 'py7zr' for 7z.
    Returns bytes when outfile is None/'-'; otherwise writes a file.
    """
    kind = _sniff_foreign_type(infile) if isinstance(infile, basestring) else None

    if kind == 'zip' or (not kind and not isinstance(infile, basestring)):
        import zipfile
        from io import BytesIO
        zsrc = BytesIO(infile) if isinstance(infile, (bytes, bytearray, memoryview)) else (infile if isinstance(infile, basestring) else _wrap_infile(infile)[0])
        try:
            with zipfile.ZipFile(zsrc, 'r') as zf:
                return pack_iter_neo(_iter_zip_members(zf), outfile, formatspecs=formatspecs,
                                     checksumtypes=checksumtypes, compression=compression,
                                     compression_level=compression_level)
        except zipfile.BadZipfile:
            pass  # maybe not a zip; try others

    if kind == 'tar' or (not kind and isinstance(infile, basestring) and os.path.splitext(infile)[1].startswith('.tar')):
        import tarfile
        from io import BytesIO
        src = BytesIO(infile) if isinstance(infile, (bytes, bytearray, memoryview)) else (infile if isinstance(infile, basestring) else _wrap_infile(infile)[0])
        with tarfile.open(src, 'r:*') as tf:
            return pack_iter_neo(_iter_tar_members(tf), outfile, formatspecs=formatspecs,
                                 checksumtypes=checksumtypes, compression=compression,
                                 compression_level=compression_level)

    if kind == 'rar':
        try:
            import rarfile
        except Exception as e:
            raise RuntimeError("RAR support requires 'rarfile' package: %s" % e)
        from io import BytesIO
        rsrc = BytesIO(infile) if isinstance(infile, (bytes, bytearray, memoryview)) else (infile if isinstance(infile, basestring) else _wrap_infile(infile)[0])
        with rarfile.RarFile(rsrc) as rf:
            return pack_iter_neo(_iter_rar_members(rf), outfile, formatspecs=formatspecs,
                                 checksumtypes=checksumtypes, compression=compression,
                                 compression_level=compression_level)

    if kind == '7z':
        try:
            import py7zr
        except Exception as e:
            raise RuntimeError("7z support requires 'py7zr' package: %s" % e)
        from io import BytesIO
        zsrc = BytesIO(infile) if isinstance(infile, (bytes, bytearray, memoryview)) else (infile if isinstance(infile, basestring) else _wrap_infile(infile)[0])
        with py7zr.SevenZipFile(zsrc, 'r') as z7:
            return pack_iter_neo(_iter_7z_members(z7), outfile, formatspecs=formatspecs,
                                 checksumtypes=checksumtypes, compression=compression,
                                 compression_level=compression_level)

    raise ValueError("Unsupported foreign archive (zip/tar/rar/7z only): %r" % (infile,))
