#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

"""PyNeoFile CLI (core-only)
- Uses only the `pyneofile` core module.
- Supports --no-json fast path for scans.
- Adds verbose (-d) printing during CREATE (-c), mirroring original output.
"""

import sys, os, io, argparse
import pyneofile as P

def _read_input_bytes(path):
    if path in (None, '-', b'-'):
        return getattr(sys.stdin, 'buffer', sys.stdin).read()
    with io.open(path, 'rb') as fp: return fp.read()

def _write_output_bytes(path, data):
    if isinstance(data, str): data = data.encode('utf-8')
    if path in (None, '-', b'-'):
        getattr(sys.stdout, 'buffer', sys.stdout).write(data); return
    d = os.path.dirname(path);  (os.makedirs(d) if d and not os.path.isdir(d) else None)
    with io.open(path, 'wb') as fp: fp.write(data)

def main(argv=None):
    p = argparse.ArgumentParser(prog="neofile", description="PyNeoFile CLI (core-only)")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument('-l', '--list', action='store_true')
    g.add_argument('-e', '--extract', action='store_true')
    g.add_argument('-c', '--create', action='store_true')
    g.add_argument('-r', '--repack', action='store_true')
    g.add_argument('--validate', action='store_true')
    g.add_argument('-t', '--convert', action='store_true')
    p.add_argument('-i','--input'); p.add_argument('-o','--output')
    p.add_argument('-P','--compression', default='auto')
    p.add_argument('-L','--level', default=None, type=int)
    p.add_argument('--skipchecksum', action='store_true')
    p.add_argument('-d','--verbose', action='store_true')
    p.add_argument('--no-json', action='store_true')
    a = p.parse_args(argv)

    if a.list:
        src = a.input
        if src in (None, '-', b'-'):
            data = _read_input_bytes(src)
            entries = P.archivefilelistfiles_neo(data, advanced=a.verbose, include_dirs=True, skipjson=True if a.no_json else True)
        else:
            entries = P.archivefilelistfiles_neo(src, advanced=a.verbose, include_dirs=True, skipjson=a.no_json)
        if a.verbose:
            for e in entries:
                if isinstance(e, dict): print("{type}\t{compression}\t{size}\t{name}".format(**e))
                else: print(e)
        else:
            for e in entries: print(e['name'] if isinstance(e, dict) else e)
        return 0

    if a.validate:
        src = a.input
        if src in (None, '-', b'-'):
            data = _read_input_bytes(src)
            ok, details = P.archivefilevalidate_neo(data, verbose=a.verbose, return_details=True, skipjson=a.no_json)
        else:
            ok, details = P.archivefilevalidate_neo(src, verbose=a.verbose, return_details=True, skipjson=a.no_json)
        print("OK" if ok else "BAD")
        if a.verbose:
            for d in details: print("{index}\t{name}\t{header_ok}\t{json_ok}\t{content_ok}".format(**d))
        return 0 if ok else 2

    if a.extract:
        src = a.input; outdir = a.output or '.'
        if src in (None, '-', b'-'):
            data = _read_input_bytes(src); ok = P.unpack_neo(data, outdir, skipchecksum=a.skipchecksum, uncompress=True)
        else:
            ok = P.unpack_neo(src, outdir, skipchecksum=a.skipchecksum, uncompress=True)
        return 0 if ok else 1

    if a.create:
        dst = a.output or '-'; src_path = a.input
        if src_path in (None, '-', b'-'):
            data = _read_input_bytes(src_path); payload = {"stdin.bin": data}
            blob = P.pack_neo(payload, outfile=None, checksumtypes=('crc32','crc32','crc32'),
                              encoding='UTF-8', compression=a.compression, compression_level=a.level)
            _write_output_bytes(dst, blob); return 0
        if a.verbose:
            norm = os.path.normpath(src_path)
            if os.path.isfile(norm):
                base = os.path.basename(norm).replace('\\','/'); print('./' + base)
            else:
                base = os.path.basename(norm).replace('\\','/')
                for root, dirs, files in os.walk(norm, topdown=True):
                    rel = base if root == norm else base + '/' + os.path.relpath(root, norm).replace('\\','/')
                    print('./' + rel)
                    for fname in sorted(files): print('./' + rel + '/' + fname)
        res = P.pack_neo(src_path, outfile=dst, checksumtypes=('crc32','crc32','crc32'),
                         encoding='UTF-8', compression=a.compression, compression_level=a.level)
        if isinstance(res, (bytes, bytearray)): _write_output_bytes(dst, res)
        return 0

    if a.repack:
        src = a.input; dst = a.output or '-'
        data_or_path = src if src not in (None, '-', b'-') else _read_input_bytes(src)
        res = P.repack_neo(data_or_path, outfile=dst, checksumtypes=('crc32','crc32','crc32'),
                           compression=a.compression, compression_level=a.level)
        if isinstance(res, (bytes, bytearray)): _write_output_bytes(dst, res)
        return 0

    if a.convert:
        src = a.input; dst = a.output or '-'
        if src in (None, '-', b'-'): raise SystemExit("convert requires a path input (zip/tar)")
        res = P.convert_foreign_to_neo(src, outfile=dst, checksumtypes=('crc32','crc32','crc32'),
                                       compression=a.compression, compression_level=a.level)
        if isinstance(res, (bytes, bytearray)): _write_output_bytes(dst, res)
        return 0

if __name__ == "__main__":
    sys.exit(main())
