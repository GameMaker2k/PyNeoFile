# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

"""
PyNeoFile core (_neo variants)
- archive_to_array_neo(..., skipjson=False)
- pack_neo, pack_iter_neo, unpack_neo, repack_neo
- archivefilelistfiles_neo, archivefilevalidate_neo
- convert_foreign_to_neo (zip/tar)
- make_empty_file_pointer_neo / make_empty_file_neo (and *_archive_* aliases)
Python 2/3 compatible.
"""
import os
import io
import sys
import json
import time

# py2/py3 helpers
PY2 = (sys.version_info[0] == 2)
if PY2:
    import zlib as _zlib
    try:
        import backports.lzma as _lzma
    except Exception:
        _lzma = None
    import bz2 as _bz2
    import hashlib as _hashlib
    def b(s):
        return s if isinstance(s, bytes) else s.encode('utf-8')
    def u(s):
        return s.decode('utf-8') if isinstance(s, bytes) else s
else:
    import zlib as _zlib
    try:
        import lzma as _lzma
    except Exception:
        _lzma = None
    import bz2 as _bz2
    import hashlib as _hashlib
    def b(s):
        return s if isinstance(s, (bytes, bytearray)) else str(s).encode('utf-8')
    def u(s):
        return s.decode('utf-8') if isinstance(s, (bytes, bytearray)) else str(s)

# Octal helper (avoid legacy 0-prefixed literals in Python 3 parsing)
def _O(s):
    try:
        return int(s, 8)
    except Exception:
        return int(str(s), 8)

S_IFREG = _O('0100000')
S_IFDIR = _O('0040000')
MODE_FILE_DEFAULT = _O('0666')
MODE_DIR_DEFAULT  = _O('0755')

try:
    import configparser as _configparser
except Exception:
    import ConfigParser as _configparser

__program_name__ = "PyNeoFile"
__project__ = __program_name__
__project_url__ = "https://github.com/GameMaker2k/PyNeoFile"
__version_info__ = (0, 19, 10, "RC 1", 1)
__version_date_info__ = (2025, 8, 15, "RC 1", 1)
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

# ------------- checksum utils -------------

def _hex(n):
    return format(int(n), 'x')

def _crc32_hex(data):
    if not isinstance(data, (bytes, bytearray)):
        data = b(data)
    return format(_zlib.crc32(data) & 0xffffffff, '08x')

def _checksum(data, cstype, text=False):
    t = (cstype or 'none').lower()
    if t in ('', 'none'):
        return '0'
    if not isinstance(data, (bytes, bytearray)):
        data = b(data)
    if t == 'crc32':
        return _crc32_hex(data)
    import hashlib as _hashlib_local
    if t in ('md5','sha1','sha224','sha256','sha384','sha512'):
        h = getattr(_hashlib_local, t)()
        h.update(data)
        return h.hexdigest()
    raise ValueError('Unsupported checksum: %r' % cstype)

# ------------- compression utils -------------

def _norm_algo(a):
    a = (a or 'none').lower()
    if a == 'gz': a = 'gzip'
    if a in ('bz','bzip','bzip2'): a = 'bz2'
    if a == 'z': a = 'zlib'
    if a == 'xz': a = 'lzma'
    return a

def _compress_bytes(data, algo='none', level=None):
    algo = _norm_algo(algo)
    if not isinstance(data, (bytes, bytearray)):
        data = b(data)
    if algo == 'none':
        return data, 'none'
    if algo == 'zlib':
        lvl = -1 if level is None else int(level)
        return _zlib.compress(data, lvl), 'zlib'
    if algo == 'gzip':
        import gzip
        out = io.BytesIO()
        with gzip.GzipFile(fileobj=out, mode='wb', compresslevel=(level if level is not None else 9)) as gf:
            gf.write(data)
        return out.getvalue(), 'gzip'
    if algo == 'bz2':
        import bz2 as _bz2_local
        lvl = 9 if level is None else int(level)
        return _bz2_local.compress(data, lvl), 'bz2'
    if algo == 'lzma':
        if _lzma is None:
            raise ValueError('lzma not available')
        if level is None:
            return _lzma.compress(data), 'lzma'
        else:
            return _lzma.compress(data, preset=int(level)), 'lzma'
    raise ValueError('Unknown compression: %r' % algo)

def _decompress_bytes(data, algo='none'):
    algo = _norm_algo(algo)
    if not isinstance(data, (bytes, bytearray)):
        data = b(data)
    if algo == 'none':
        return data
    if algo == 'zlib':
        return _zlib.decompress(data)
    if algo == 'gzip':
        import gzip
        return gzip.decompress(data) if hasattr(gzip, 'decompress') else _zlib.decompress(data, 16 + _zlib.MAX_WBITS)
    if algo == 'bz2':
        import bz2 as _bz2_local2
        return _bz2_local2.decompress(data)
    if algo == 'lzma':
        if _lzma is None:
            raise ValueError('lzma not available')
        return _lzma.decompress(data)
    raise ValueError('Unknown compression: %r' % algo)

def _auto_pick_for_size(n):
    if n < 16384: return 'none', None
    if n >= 262144: return 'bz2', 9
    return 'zlib', 6

# ------------- formatspecs / INI -------------

def _decode_escape(s):
    s = u(s)
    # Handle \xNN and common escapes
    try:
        return s.encode('utf-8').decode('unicode_escape').encode('latin1').decode('latin1')
    except Exception:
        return s

def _default_formatspecs():
    return {
        'format_magic': 'NeoFile',
        'format_ver': '001',
        'format_delimiter': b('\x00'),
        'new_style': True,
    }

def _ver_digits(ver):
    if not ver: return '001'
    digits = ''.join(ch for ch in u(ver) if ch.isdigit())
    return digits or '001'

def _load_formatspecs_from_ini(paths=None, prefer_section=None):
    cands = []
    if paths:
        if isinstance(paths, (list, tuple)):
            cands.extend(paths)
        else:
            cands.append(paths)
    for env in ('PYNEOFILE_INI','PYARCHIVE_INI'):
        p = os.environ.get(env)
        if p: cands.append(p)
    cands.extend(['neofile.ini'])
    picked = None
    for p in cands:
        if os.path.isfile(p):
            picked = p
            break
    if not picked:
        return None
    cp = _configparser.ConfigParser()
    with io.open(picked, 'r', encoding='utf-8', errors='ignore') as f:
        if hasattr(cp, 'read_file'):
            cp.read_file(f)
        else:
            cp.readfp(f)
    if prefer_section and cp.has_section(prefer_section):
        sec = prefer_section
    elif cp.has_section('config') and cp.has_option('config','default') and cp.has_section(cp.get('config','default')):
        sec = cp.get('config','default')
    else:
        sec = next((s for s in cp.sections() if s.lower() != 'config'), None)
    if not sec:
        return None
    magic = cp.get(sec, 'magic') if cp.has_option(sec, 'magic') else 'ArchiveFile'
    ver   = cp.get(sec, 'ver') if cp.has_option(sec, 'ver') else '001'
    delim = cp.get(sec, 'delimiter') if cp.has_option(sec, 'delimiter') else r'\x00'
    newst = cp.get(sec, 'newstyle') if cp.has_option(sec, 'newstyle') else 'true'
    return {
        'format_magic': magic,
        'format_ver'  : _ver_digits(ver),
        'format_delimiter': b(_decode_escape(delim)),
        'new_style': str(newst).lower() in ('true','1','yes'),
        'format_name': sec
    }

def _ensure_formatspecs(specs=None):
    if specs and isinstance(specs, dict):
        fs = dict(_default_formatspecs())
        fs.update(specs)
        return fs
    env = _load_formatspecs_from_ini()
    return env or _default_formatspecs()

# ------------- I/O helpers -------------

def _open_in(infile):
    """Return (fp, should_close). Accepts path, bytes/bytearray, or file-like."""
    if hasattr(infile, 'read'):
        return infile, False
    if isinstance(infile, (bytes, bytearray)):
        return io.BytesIO(infile), True
    if infile is None:
        raise ValueError('infile is None')
    return io.open(u(infile), 'rb'), True

def _open_out(outfile):
    """Return (buffer_mode, fp, buf). If buffer_mode True, collect into bytes."""
    if outfile in (None, '-', b'-'):
        return True, None, bytearray()
    if hasattr(outfile, 'write'):
        return False, outfile, None
    fp = io.open(u(outfile), 'wb')
    return False, fp, None

def _write(bufmode, fp, buf, data):
    if not isinstance(data, (bytes, bytearray)):
        data = b(data)
    if bufmode:
        buf.extend(data)
    else:
        fp.write(data)

# ------------- low-level reading -------------

def _read_cstring(fp, delim):
    """Chunked scan for delimiter. Leaves fp after the delimiter. Returns bytes without delimiter."""
    d = delim
    dl = len(d)
    read = fp.read
    if dl == 1:
        out = bytearray()
        while True:
            chunk = read(4096)
            if not chunk:
                return bytes(out)
            idx = chunk.find(d)
            if idx != -1:
                out.extend(chunk[:idx])
                tail = chunk[idx+1:]
                if tail:
                    fp.seek(-len(tail), os.SEEK_CUR)
                return bytes(out)
            out.extend(chunk)
    # general case
    buf = bytearray()
    while True:
        chunk = read(4096)
        if not chunk:
            return bytes(buf)
        buf.extend(chunk)
        i = buf.find(d)
        if i != -1:
            out = bytes(buf[:i])
            tail_len = len(buf) - (i + dl)
            if tail_len:
                fp.seek(-tail_len, os.SEEK_CUR)
            return out
        keep = dl - 1
        if keep > 0 and len(buf) >= keep:
            tail = bytes(buf[-keep:])
            fp.seek(-keep, os.SEEK_CUR)
            buf = bytearray(tail)

def _read_fields(fp, n, delim):
    return [ _read_cstring(fp, delim) for _ in range(int(n)) ]

# ------------- global header -------------

def _write_global_header(dst, fs, numfiles, encoding, checksumtype):
    d = fs['format_delimiter']
    magic = fs['format_magic']
    ver = _ver_digits(fs['format_ver'])
    platform_name = os.name

    extras = []
    extrafields = _hex(len(extras))
    extras_blob = _append_null(extrafields, d) + (b'' if not extras else _append_nulls(extras, d))
    extras_size_hex = _hex(len(extras_blob))
    fnumfiles_hex = _hex(int(numfiles))

    body = _append_nulls([
        _hex(3 + 5 + len(extras) + 1),
        encoding,
        platform_name,
        fnumfiles_hex,
        _hex(len(extras_blob)),
        extrafields
    ], d)
    if extras:
        body += _append_nulls(extras, d)
    body += _append_null(checksumtype, d)

    prefix = _append_null(magic + ver, d)
    tmpfileoutstr = body + _append_null('', d)
    headersize_hex = _hex(len(tmpfileoutstr) - len(d))

    out = prefix + _append_null(headersize_hex, d) + body
    header_cs = _checksum(out, checksumtype, True)
    out += _append_null(header_cs, d)
    dst.write(out)

def _parse_global_header(fp, fs):
    d = fs['format_delimiter']
    magicver = _read_cstring(fp, d)  # magic+ver (unused here)
    _ = _read_cstring(fp, d)         # headersize
    _ = _read_cstring(fp, d)         # tmpoutlen
    fencoding = u(_read_cstring(fp, d))
    fostype   = u(_read_cstring(fp, d))
    fnumfiles = int(_read_cstring(fp, d) or b'0', 16)
    _ = _read_cstring(fp, d)         # extras size
    extrafields = int(_read_cstring(fp, d) or b'0', 16)
    extras = [ u(_read_cstring(fp, d)) for _ in range(extrafields) ]
    checksumtype = u(_read_cstring(fp, d))
    _ = _read_cstring(fp, d)         # header checksum
    return {
        'fencoding': fencoding or 'UTF-8',
        'fnumfiles': fnumfiles,
        'fostype'  : fostype,
        'fextradata': extras,
        'fchecksumtype': checksumtype,
        'ffilelist': [],
        'fformatspecs': fs,
    }

def _append_null(x, d):
    return (x if isinstance(x, (bytes, bytearray)) else b(x)) + d

def _append_nulls(arr, d):
    out = bytearray()
    for it in arr:
        out.extend(_append_null(it, d))
    return bytes(out)

# ------------- record parsing -------------

def _index_json_and_checks(vals):
    if len(vals) < 25:
        raise ValueError("Record too short: got %d" % len(vals))
    idx = 25
    fjsontype = vals[idx]; idx += 1
    v2 = vals[idx] if idx < len(vals) else b''
    v3 = vals[idx+1] if idx+1 < len(vals) else b''
    v4 = vals[idx+2] if idx+2 < len(vals) else b''
    def ishex(s):
        try:
            s = u(s)
            return s and all(c in '0123456789abcdefABCDEF' for c in s)
        except Exception:
            return False
    csnames = set(['none','crc32','md5','sha1','sha224','sha256','sha384','sha512','blake2b','blake2s'])
    if ishex(v2) and ishex(v3) and u(v4).lower() in csnames:
        idx_json_type = 25
        idx_json_len  = 26
        idx_json_size = 27
        idx_json_cst  = 28
        idx_json_cs   = 29
        idx_extras_size = 30
    else:
        idx_json_type = 25
        idx_json_len  = None
        idx_json_size = 26
        idx_json_cst  = 27
        idx_json_cs   = 28
        idx_extras_size = 29
    idx_extras_count = idx_extras_size + 1
    count = int((vals[idx_extras_count] or b'0'), 16)
    idx = idx_extras_count + 1 + count
    idx_header_cs_type  = idx
    idx_content_cs_type = idx + 1
    idx_header_cs       = idx + 2
    idx_content_cs      = idx + 3
    return (idx_json_type, idx_json_len, idx_json_size, idx_json_cst, idx_json_cs,
            idx_header_cs_type, idx_content_cs_type, idx_header_cs, idx_content_cs)

def _parse_record(fp, fs, listonly=False, skipchecksum=False, uncompress=True, skipjson=False):
    d = fs['format_delimiter']
    first = _read_cstring(fp, d)
    if first == b'0':
        second = _read_cstring(fp, d)
        if second == b'0':
            return None  # end marker
        headersize_hex = first
        fields_len_hex = second
    else:
        headersize_hex = first
        fields_len_hex = _read_cstring(fp, d)
    n_fields = int(fields_len_hex or b'0', 16)
    vals = _read_fields(fp, n_fields, d)
    if len(vals) < 25:
        raise ValueError("Record too short: expected >=25 header fields, got %d" % len(vals))

    (idx_json_type, idx_json_len, idx_json_size, idx_json_cst, idx_json_cs,
     idx_header_cs_type, idx_content_cs_type, idx_header_cs, idx_content_cs) = _index_json_and_checks(vals)

    # unpack first 25 meta fields
    (ftypehex, fencoding, fcencoding, fname, flinkname,
     fsize_hex, fatime_hex, fmtime_hex, fctime_hex, fbtime_hex,
     fmode_hex, fwinattrs_hex, fcompression, fcsize_hex,
     fuid_hex, funame, fgid_hex, fgname, fid_hex, finode_hex,
     flinkcount_hex, fdev_hex, fdev_minor_hex, fdev_major_hex,
     fseeknextfile) = vals[:25]

    # JSON sizes
    fjsonsize_hex = vals[idx_json_size] or b'0'
    fjsonsize = int(fjsonsize_hex, 16)

    # JSON payload
    if fjsonsize > 0:
        if listonly or skipjson:
            fp.seek(fjsonsize + len(d), os.SEEK_CUR)  # skip payload + its delimiter
            json_bytes = b''
        else:
            json_bytes = fp.read(fjsonsize)
            fp.read(len(d))  # trailing delim
    else:
        json_bytes = b''
        fp.read(len(d))

    # Content
    fsize  = int(fsize_hex or b'0', 16)
    fcsize = int(fcsize_hex or b'0', 16)
    stored_len = (fcsize if u(fcompression) not in ('', 'none') and fcsize > 0 else fsize)

    content_stored = b''
    if stored_len:
        if listonly:
            fp.seek(stored_len, os.SEEK_CUR)
        else:
            content_stored = fp.read(stored_len)
    fp.read(len(d))  # trailing delim

    header_cs_type  = u(vals[idx_header_cs_type])
    content_cs_type = u(vals[idx_content_cs_type])
    header_cs_val   = u(vals[idx_header_cs])
    content_cs_val  = u(vals[idx_content_cs])
    json_cs_type    = u(vals[idx_json_cst])
    json_cs_val     = u(vals[idx_json_cs])

    # Verify checksums when requested
    if fjsonsize and (not skipchecksum) and (not (listonly or skipjson)):
        if _checksum(json_bytes, json_cs_type, True) != json_cs_val:
            raise ValueError("JSON checksum mismatch for %s" % u(fname))
    if (not skipchecksum) and stored_len and (not listonly):
        if _checksum(content_stored, content_cs_type, False) != content_cs_val:
            raise ValueError("Content checksum mismatch for %s" % u(fname))

    # Optional decompression for returned content
    content_ret = content_stored
    if (not listonly) and uncompress and u(fcompression) not in ('', 'none') and content_stored:
        try:
            content_ret = _decompress_bytes(content_stored, u(fcompression))
        except Exception:
            content_ret = content_stored

    name = u(fname)
    if not name.startswith(('./','/')):
        name = './' + name

    # Decode JSON object (if we loaded it)
    fjson = {}
    if json_bytes:
        try:
            fjson = json.loads(u(json_bytes))
        except Exception:
            fjson = {}

    to_int = lambda x: int((x or b'0'), 16)
    return {
        'fid': to_int(fid_hex),
        'finode': to_int(finode_hex),
        'fname': name,
        'flinkname': u(flinkname),
        'ftype': to_int(ftypehex),
        'fsize': to_int(fsize_hex),
        'fcsize': to_int(fcsize_hex),
        'fatime': to_int(fatime_hex),
        'fmtime': to_int(fmtime_hex),
        'fctime': to_int(fctime_hex),
        'fbtime': to_int(fbtime_hex),
        'fmode': to_int(fmode_hex),
        'fwinattributes': to_int(fwinattrs_hex),
        'fuid': to_int(fuid_hex),
        'funame': u(funame),
        'fgid': to_int(fgid_hex),
        'fgname': u(fgname),
        'fcompression': u(fcompression) or 'none',
        'fseeknext': u(fseeknextfile),
        'fjson': fjson,
        'fcontent': (None if listonly else content_ret),
    }

# ------------- public parse -------------

def archive_to_array_neo(infile, formatspecs=None, listonly=False, skipchecksum=False, uncompress=True, skipjson=False):
    fs = _ensure_formatspecs(formatspecs)
    fp, need_close = _open_in(infile)
    try:
        top = _parse_global_header(fp, fs)
        while True:
            rec = _parse_record(fp, fs, listonly=listonly, skipchecksum=skipchecksum, uncompress=uncompress, skipjson=skipjson)
            if rec is None:
                break
            top['ffilelist'].append(rec)
        return top
    finally:
        if need_close:
            try: fp.close()
            except Exception: pass

# ------------- builders (packing) -------------

def _build_record(fs, meta, jsondata, content_stored, checksumtypes):
    d = fs['format_delimiter']
    H = lambda x: _hex(int(x))
    fname = meta['fname']
    if not fname.startswith(('./','/')):
        fname = './' + fname

    fields = [
        H(meta.get('ftype', 0)),
        meta.get('fencoding','UTF-8'),
        meta.get('fcencoding','UTF-8'),
        fname,
        meta.get('flinkname',''),
        H(meta.get('fsize',0)),
        H(meta.get('fatime', int(time.time()))),
        H(meta.get('fmtime', int(time.time()))),
        H(meta.get('fctime', int(time.time()))),
        H(meta.get('fbtime', int(time.time()))),
        H(meta.get('fmode', S_IFREG | MODE_FILE_DEFAULT)),
        H(meta.get('fwinattributes',0)),
        meta.get('fcompression','none'),
        H(meta.get('fcsize', 0)),
        H(meta.get('fuid', 0)),
        meta.get('funame',''),
        H(meta.get('fgid', 0)),
        meta.get('fgname',''),
        H(meta.get('fid', meta.get('index', 0))),
        H(meta.get('finode', meta.get('index', 0))),
        H(meta.get('flinkcount', 1)),
        H(meta.get('fdev', 0)),
        H(meta.get('fdev_minor', 0)),
        H(meta.get('fdev_major', 0)),
        H(len(d)),
    ]

    # JSON section
    if jsondata:
        raw_json = b(json.dumps(jsondata, separators=(',',':')))
        fjsontype = 'json'
        fjsonlen_hex = H(len(jsondata) if hasattr(jsondata,'__len__') else 0)
        fjsonsize_hex = H(len(raw_json))
        fjson_cs_type = checksumtypes[2]
        fjson_cs = _checksum(raw_json, fjson_cs_type, True)
    else:
        raw_json = b''
        fjsontype = 'none'
        fjsonlen_hex = '0'
        fjsonsize_hex = '0'
        fjson_cs_type = 'none'
        fjson_cs = '0'

    extras_size_hex = H(len(_append_null('0', d)))
    extrafields = '0'

    rec_fields = fields + [fjsontype, fjsonlen_hex, fjsonsize_hex, fjson_cs_type, fjson_cs, extras_size_hex, extrafields]
    header_cs_type  = checksumtypes[0]
    content_cs_type = (checksumtypes[1] if content_stored else 'none')
    rec_fields += [header_cs_type, content_cs_type]

    record_fields_len_hex = H(len(rec_fields) + 2)
    header_no_cs = _append_nulls(rec_fields, d)
    tmp_with_placeholders = _append_null(record_fields_len_hex, d) + header_no_cs + _append_null('', d) + _append_null('', d)
    headersize_hex = H(len(tmp_with_placeholders) - len(d))
    header_with_sizes = _append_null(headersize_hex, d) + _append_null(record_fields_len_hex, d) + header_no_cs

    header_checksum = _checksum(header_with_sizes, header_cs_type, True)
    content_checksum = _checksum(content_stored, content_cs_type, False)
    header_full = header_with_sizes + _append_null(header_checksum, d) + _append_null(content_checksum, d)
    return header_full + raw_json + d + content_stored + d

def pack_iter_neo(items, outfile=None, formatspecs=None,
                  checksumtypes=('crc32','crc32','crc32'),
                  encoding='UTF-8', compression='auto', compression_level=None):
    fs = _ensure_formatspecs(formatspecs)
    bufmode, fp, buf = _open_out(outfile)

    class _Dst(object):
        def write(self, data):
            _write(bufmode, fp, buf, data)
    dst = _Dst()
    # snapshot items (iterator safety)
    items_list = list(items)
    _write_global_header(dst, fs, len(items_list), encoding, checksumtypes[0])

    fid = 0
    for it in items_list:
        if isinstance(it, dict) and 'name' in it:
            name = it['name']
            is_dir = bool(it.get('is_dir'))
            data = it.get('data')
            mode = int(it.get('mode', S_IFDIR | MODE_DIR_DEFAULT if is_dir else S_IFREG | MODE_FILE_DEFAULT))
            mtime = int(it.get('mtime', time.time()))
        elif isinstance(it, (list, tuple)) and len(it) >= 3:
            name, is_dir, data = it[0], bool(it[1]), it[2]
            mode = S_IFDIR | MODE_DIR_DEFAULT if is_dir else S_IFREG | MODE_FILE_DEFAULT
            mtime = int(time.time())
        else:
            raise ValueError("Bad item: %r" % (it,))

        name = u(name).replace('\\','/')
        if not name.startswith(('./','/')):
            name = './' + name

        if is_dir or name.endswith('/'):
            raw = b''; ftype = 5
        else:
            raw = b'' if data is None else (data if isinstance(data, (bytes, bytearray)) else b(data))
            ftype = 0

        algo = (compression or 'none').lower()
        if algo == 'auto':
            algo, lvl = _auto_pick_for_size(len(raw))
            level = compression_level if compression_level is not None else lvl
        else:
            level = compression_level

        try:
            stored, used_algo = _compress_bytes(raw, algo, level)
        except Exception:
            stored, used_algo = _compress_bytes(raw, 'zlib', 6 if level is None else level)

        meta = {
            'ftype': ftype, 'fencoding': encoding, 'fcencoding': encoding, 'fname': name,
            'flinkname':'', 'fsize': len(raw),
            'fatime': mtime, 'fmtime': mtime, 'fctime': mtime, 'fbtime': mtime,
            'fmode': int(mode), 'fwinattributes': 0,
            'fcompression': used_algo, 'fcsize': len(stored),
            'fuid':0,'funame':'','fgid':0,'fgname':'',
            'fid': fid, 'finode': fid, 'flinkcount':1, 'fdev':0, 'fdev_minor':0, 'fdev_major':0, 'index':fid,
        }
        rec = _build_record(fs, meta, {}, stored, checksumtypes)
        _write(bufmode, fp, buf, rec)
        fid += 1

    _write(bufmode, fp, buf, _append_nulls(['0','0'], fs['format_delimiter']))
    if bufmode:
        return bytes(buf)
    return True

def pack_neo(infiles, outfile=None, formatspecs=None,
             checksumtypes=('crc32','crc32','crc32'),
             encoding='UTF-8', compression='auto', compression_level=None):
    # dict path->bytes or list of paths
    if isinstance(infiles, dict):
        items = []
        for name, data in infiles.items():
            is_dir = (data is None) or u(name).endswith('/')
            items.append({'name': name, 'is_dir': is_dir, 'data': (None if is_dir else data)})
        return pack_iter_neo(items, outfile, formatspecs, checksumtypes, encoding, compression, compression_level)

    # else treat as paths
    paths = infiles if isinstance(infiles, (list, tuple)) else [infiles]
    filelist = []
    for p in paths:
        p = u(p)
        if os.path.isdir(p):
            for root, dirs, files in os.walk(p):
                relroot = root[len(p.rstrip(os.sep))+1:] if root != p else ''
                for dname in dirs:
                    rel = os.path.join(relroot, dname).replace('\\','/')
                    if rel and not rel.endswith('/'):
                        rel += '/'
                    filelist.append((os.path.join(root, dname), True, rel))
                for fname in files:
                    rel = os.path.join(relroot, fname).replace('\\','/')
                    filelist.append((os.path.join(root, fname), False, rel))
        else:
            filelist.append((p, False, os.path.basename(p).replace('\\','/')))

    fs = _ensure_formatspecs(formatspecs)
    bufmode, fp, buf = _open_out(outfile)
    class _Dst(object):
        def write(self, data):
            _write(bufmode, fp, buf, data)
    dst = _Dst()
    _write_global_header(dst, fs, len(filelist), encoding, checksumtypes[0])

    fid = 0
    for apath, is_dir, relname in filelist:
        if is_dir:
            raw = b''; ftype = 5
        else:
            with io.open(apath, 'rb') as f:
                raw = f.read()
            ftype = 0
        if not relname.startswith(('./','/')):
            relname = './' + relname

        algo = (compression or 'none').lower()
        if algo == 'auto':
            algo, lvl = _auto_pick_for_size(len(raw))
            level = compression_level if compression_level is not None else lvl
        else:
            level = compression_level
        try:
            stored, used_algo = _compress_bytes(raw, algo, level)
        except Exception:
            stored, used_algo = _compress_bytes(raw, 'zlib', 6 if level is None else level)
        meta = {
            'ftype': ftype, 'fencoding': encoding, 'fcencoding': encoding, 'fname': relname,
            'flinkname':'', 'fsize': len(raw),
            'fatime': int(time.time()), 'fmtime': int(time.time()), 'fctime': int(time.time()), 'fbtime': int(time.time()),
            'fmode': S_IFDIR | MODE_DIR_DEFAULT if is_dir else S_IFREG | MODE_FILE_DEFAULT,
            'fwinattributes': 0,
            'fcompression': used_algo, 'fcsize': len(stored),
            'fuid':0,'funame':'','fgid':0,'fgname':'',
            'fid': fid, 'finode': fid, 'flinkcount':1, 'fdev':0, 'fdev_minor':0, 'fdev_major':0, 'index':fid,
        }
        rec = _build_record(fs, meta, {}, stored, checksumtypes)
        _write(bufmode, fp, buf, rec)
        fid += 1

    _write(bufmode, fp, buf, _append_nulls(['0','0'], fs['format_delimiter']))
    if bufmode:
        return bytes(buf)
    return True

def archivefilelistfiles_neo(infile, formatspecs=None, advanced=False, include_dirs=True, skipjson=True):
    fs = _ensure_formatspecs(formatspecs)
    fp, need_close = _open_in(infile)
    try:
        _ = _parse_global_header(fp, fs)
        out = []
        while True:
            rec = _parse_record(fp, fs, listonly=True, skipchecksum=True, uncompress=False, skipjson=skipjson)
            if rec is None: break
            is_dir = (rec['ftype'] == 5)
            if not advanced:
                if is_dir and not include_dirs:
                    continue
                out.append(rec['fname'])
            else:
                out.append({
                    'name': rec['fname'],
                    'type': 'dir' if is_dir else 'file',
                    'compression': rec['fcompression'] or 'none',
                    'size': rec['fsize'],
                    'stored_size': rec['fcsize'],
                    'mtime': rec['fmtime'],
                    'atime': rec['fatime'],
                    'mode': rec['fmode'],
                })
        return out
    finally:
        if need_close:
            try: fp.close()
            except Exception: pass

def archivefilevalidate_neo(infile, formatspecs=None, verbose=False, return_details=False, skipjson=False):
    arr = archive_to_array_neo(infile, formatspecs=formatspecs, listonly=False, skipchecksum=False, uncompress=False, skipjson=skipjson)
    ok = True
    details = []
    for i, e in enumerate(arr.get('ffilelist', [])):
        details.append({'index': i, 'name': e['fname'], 'header_ok': True, 'json_ok': True, 'content_ok': True})
    if return_details:
        return ok, details
    return ok

def unpack_neo(infile, outdir='.', formatspecs=None, skipchecksum=False, uncompress=True):
    arr = archive_to_array_neo(infile, formatspecs=formatspecs, listonly=False, skipchecksum=skipchecksum, uncompress=uncompress, skipjson=False)
    if outdir in (None, '-', b'-'):
        result = {}
        for ent in arr.get('ffilelist', []):
            if ent['ftype'] == 5:
                result[ent['fname']] = None
            else:
                result[ent['fname']] = ent.get('fcontent') or b''
        return result
    if not os.path.isdir(outdir):
        if os.path.exists(outdir):
            raise ValueError('not a directory: %r' % outdir)
        os.makedirs(outdir)
    for ent in arr.get('ffilelist', []):
        path = os.path.join(outdir, ent['fname'].lstrip('./'))
        if ent['ftype'] == 5:
            if not os.path.isdir(path):
                os.makedirs(path)
        else:
            d = os.path.dirname(path)
            if d and not os.path.isdir(d):
                os.makedirs(d)
            with io.open(path, 'wb') as f:
                f.write(ent.get('fcontent') or b'')
            try:
                os.chmod(path, ent.get('fmode', 0) & 0o777)
            except Exception:
                pass
    return True

def repack_neo(infile, outfile=None, formatspecs=None, checksumtypes=('crc32','crc32','crc32'),
               compression='auto', compression_level=None):
    arr = archive_to_array_neo(infile, formatspecs=formatspecs, listonly=False, skipchecksum=False, uncompress=False, skipjson=True)
    fs = _ensure_formatspecs(formatspecs)
    bufmode, fp, buf = _open_out(outfile)

    class _Dst(object):
        def write(self, data):
            _write(bufmode, fp, buf, data)
    dst = _Dst()
    _write_global_header(dst, fs, len(arr.get('ffilelist', [])), arr.get('fencoding','UTF-8'), checksumtypes[0])

    out_count = 0
    for ent in arr.get('ffilelist', []):
        src_algo = (ent.get('fcompression') or 'none').lower()
        stored_src = ent.get('fcontent') or b''
        try:
            raw = _decompress_bytes(stored_src, src_algo) if src_algo != 'none' else stored_src
        except Exception:
            raw = stored_src

        dst_algo = (compression or 'auto').lower()
        if dst_algo == 'auto':
            dst_algo, dst_level = _auto_pick_for_size(len(raw))
        else:
            dst_level = compression_level

        if dst_algo == src_algo or (dst_algo=='none' and src_algo=='none'):
            stored, used_algo = stored_src, src_algo
            try:
                raw_len = len(_decompress_bytes(stored_src, src_algo)) if src_algo != 'none' else len(stored_src)
            except Exception:
                raw_len = len(stored_src)
        else:
            stored, used_algo = _compress_bytes(raw, dst_algo, dst_level)
            raw_len = len(raw)

        meta = {
            'ftype': ent['ftype'],
            'fencoding': arr.get('fencoding','UTF-8'),
            'fcencoding': arr.get('fencoding','UTF-8'),
            'fname': ent['fname'],
            'flinkname': ent.get('flinkname',''),
            'fsize': raw_len,
            'fatime': ent.get('fatime', int(time.time())),
            'fmtime': ent.get('fmtime', int(time.time())),
            'fctime': ent.get('fctime', int(time.time())),
            'fbtime': ent.get('fbtime', int(time.time())),
            'fmode': ent.get('fmode', S_IFREG | MODE_FILE_DEFAULT),
            'fwinattributes': ent.get('fwinattributes', 0),
            'fcompression': used_algo, 'fcsize': len(stored),
            'fuid': ent.get('fuid', 0),'funame': ent.get('funame',''),
            'fgid': ent.get('fgid', 0),'fgname': ent.get('fgname',''),
            'fid': ent.get('fid', out_count), 'finode': ent.get('finode', out_count),
            'flinkcount': ent.get('flinkcount', 1),
            'fdev': ent.get('fdev', 0), 'fdev_minor': ent.get('fdev_minor', 0), 'fdev_major': ent.get('fdev_major', 0),
            'index': out_count,
        }
        rec = _build_record(fs, meta, ent.get('fjson', {}), stored, checksumtypes)
        _write(bufmode, fp, buf, rec)
        out_count += 1

    _write(bufmode, fp, buf, _append_nulls(['0','0'], fs['format_delimiter']))
    if bufmode:
        return bytes(buf)
    return True

# ------------- convert (zip/tar) -------------

def convert_foreign_to_neo(infile, outfile=None, formatspecs=None,
                           checksumtypes=('crc32','crc32','crc32'),
                           compression='auto', compression_level=None):
    if isinstance(infile, (bytes, bytearray)):
        raise ValueError("convert expects a file path")
    p = u(infile)
    lp = p.lower()
    if lp.endswith('.zip'):
        import zipfile
        z = zipfile.ZipFile(p, 'r')
        items = []
        for zi in z.infolist():
            name = zi.filename
            is_dir = name.endswith('/')
            data = None if is_dir else z.read(zi)
            items.append({'name': name, 'is_dir': is_dir, 'data': data})
        z.close()
        return pack_iter_neo(items, outfile, formatspecs, checksumtypes, 'UTF-8', compression, compression_level)

    if lp.endswith('.tar') or any(lp.endswith(ext) for ext in ('.tar.gz','.tgz','.tar.bz2','.tbz2','.tar.xz','.txz')):
        import tarfile
        mode = 'r:*'
        with tarfile.open(p, mode) as tf:
            items = []
            for m in tf.getmembers():
                name = m.name
                is_dir = m.isdir()
                data = None if is_dir else (tf.extractfile(m).read() if m.isfile() else b'')
                items.append({'name': name, 'is_dir': is_dir, 'data': data})
        return pack_iter_neo(items, outfile, formatspecs, checksumtypes, 'UTF-8', compression, compression_level)

    raise ValueError("Unsupported foreign archive (zip/tar.* only)")

# ------------- empty archive helpers (_neo) -------------

def _select_formatspecs_neo(formatspecs=None, fmttype=None, outfile=None):
    if isinstance(formatspecs, dict) and 'format_magic' in formatspecs:
        return formatspecs
    if isinstance(formatspecs, dict):
        if fmttype and fmttype not in ('auto', None) and fmttype in formatspecs:
            cand = formatspecs.get(fmttype)
            if isinstance(cand, dict) and 'format_magic' in cand:
                return cand
        if (fmttype == 'auto' or fmttype is None) and outfile and not hasattr(outfile, 'write'):
            try:
                _, ext = os.path.splitext(u(outfile))
                ext = (ext or '').lstrip('.').lower()
                if ext and ext in formatspecs:
                    cand = formatspecs.get(ext)
                    if isinstance(cand, dict) and 'format_magic' in cand:
                        return cand
            except Exception:
                pass
        for v in formatspecs.values():
            if isinstance(v, dict) and 'format_magic' in v:
                return v
    return _ensure_formatspecs(formatspecs)

def make_empty_file_pointer_neo(fp, fmttype=None, checksumtype='crc32', formatspecs=None, encoding='UTF-8'):
    fs = _select_formatspecs_neo(formatspecs, fmttype, None)
    d  = fs['format_delimiter']
    class _Dst(object):
        def __init__(self, fp): self.fp = fp
        def write(self, data): self.fp.write(data if isinstance(data, (bytes, bytearray)) else b(data))
    dst = _Dst(fp)
    _write_global_header(dst, fs, 0, encoding, checksumtype)
    fp.write(_append_nulls(['0', '0'], d))
    try:
        fp.flush()
        if hasattr(os, 'fsync'): os.fsync(fp.fileno())
    except Exception:
        pass
    return fp

def make_empty_archive_file_pointer_neo(fp, fmttype=None, checksumtype='crc32', formatspecs=None, encoding='UTF-8'):
    return make_empty_file_pointer_neo(fp, fmttype, checksumtype, formatspecs, encoding)

def make_empty_file_neo(outfile=None, fmttype=None, checksumtype='crc32', formatspecs=None,
                        encoding='UTF-8', returnfp=False):
    fs = _select_formatspecs_neo(formatspecs, fmttype, outfile)
    d  = fs['format_delimiter']
    bufmode, fp, buf = _open_out(outfile)
    class _Dst(object):
        def write(self, data): _write(bufmode, fp, buf, data)
    dst = _Dst()
    _write_global_header(dst, fs, 0, encoding, checksumtype)
    _write(bufmode, fp, buf, _append_nulls(['0', '0'], d))
    if bufmode:
        return bytes(buf)
    if returnfp:
        try:
            if hasattr(fp, 'seek'): fp.seek(0, os.SEEK_SET)
        except Exception:
            pass
        return fp
    try:
        fp.flush()
        if hasattr(os, 'fsync'): os.fsync(fp.fileno())
    except Exception:
        pass
    return True

def make_empty_archive_file_neo(outfile=None, fmttype=None, checksumtype='crc32', formatspecs=None,
                                encoding='UTF-8', returnfp=False):
    return make_empty_file_neo(outfile, fmttype, checksumtype, formatspecs, encoding, returnfp)
