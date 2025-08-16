#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
import sys, os, io, argparse, json

__program_name__ = "PyNeoFile"

try:
    import pyneofile as P
except Exception as e:
    raise SystemExit("Failed to import core module 'pyneofile': %s" % (e,))

def _read_input_bytes(path):
    if path in (None, '-', b'-'):
        data = sys.stdin.buffer.read()
        return data
    with io.open(path, 'rb') as fp:
        return fp.read()

def _write_output_bytes(path, data):
    if path in (None, '-', b'-'):
        sys.stdout.buffer.write(data)
        return
    d = os.path.dirname(path)
    if d and not os.path.isdir(d):
        os.makedirs(d)
    with io.open(path, 'wb') as fp:
        fp.write(data)

def main(argv=None):
    p = argparse.ArgumentParser(prog=__program_name__, description="PyNeoFile CLI (core-only)")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument('-l', '--list', action='store_true', help='List archive entries')
    g.add_argument('-e', '--extract', action='store_true', help='Extract files')
    g.add_argument('-c', '--create', action='store_true', help='Create archive from path or stdin')
    g.add_argument('-r', '--repack', action='store_true', help='Repack archive, optionally changing compression')
    g.add_argument('--validate', action='store_true', help='Validate checksums/structure')
    g.add_argument('-t', '--convert', action='store_true', help='Convert foreign (zip/tar) -> neo')

    p.add_argument('-i', '--input', required=False, help='Input path (use - for stdin)')
    p.add_argument('-o', '--output', required=False, help='Output path (use - for stdout)')
    p.add_argument('-P', '--compression', default='auto', help='Compression: auto|none|zlib|gzip|bz2|lzma')
    p.add_argument('-L', '--level', default=None, type=int, help='Compression level')
    p.add_argument('--skipchecksum', action='store_true', help='Skip content checksum verification')
    p.add_argument('-d', '--verbose', action='store_true', help='Verbose listing')
    p.add_argument('--no-json', action='store_true', help='Skip reading per-file JSON blocks (faster)')

    args = p.parse_args(argv)

    if args.list:
        src = args.input
        if src in (None, '-', b'-'):
            data = _read_input_bytes(src)
            entries = P.archivefilelistfiles_neo(data, advanced=args.verbose, include_dirs=True, skipjson=True if args.no_json else True)
        else:
            entries = P.archivefilelistfiles_neo(src, advanced=args.verbose, include_dirs=True, skipjson=args.no_json)
        if args.verbose:
            for e in entries:
                if isinstance(e, dict):
                    print("{type}	{compression}	{size}	{name}".format(**e))
                else:
                    print(e)
        else:
            for e in entries:
                print(e['name'] if isinstance(e, dict) else e)
        return 0

    if args.validate:
        src = args.input
        if src in (None, '-', b'-'):
            data = _read_input_bytes(src)
            ok, details = P.archivefilevalidate_neo(data, verbose=args.verbose, return_details=True, skipjson=args.no_json)
        else:
            ok, details = P.archivefilevalidate_neo(src, verbose=args.verbose, return_details=True, skipjson=args.no_json)
        print("OK" if ok else "BAD")
        if args.verbose:
            for d in details:
                print("{index}	{name}	{header_ok}	{json_ok}	{content_ok}".format(**d))
        return 0 if ok else 2

    if args.extract:
        src = args.input
        outdir = args.output or '.'
        if src in (None, '-', b'-'):
            data = _read_input_bytes(src)
            ok = P.unpack_neo(data, outdir, skipchecksum=args.skipchecksum, uncompress=True)
        else:
            ok = P.unpack_neo(src, outdir, skipchecksum=args.skipchecksum, uncompress=True)
        return 0 if ok else 1

    if args.create:
        dst = args.output or '-'
        if args.input in (None, '-', b'-'):
            data = _read_input_bytes(args.input)
            payload = {"stdin.bin": data}
            blob = P.pack_neo(payload, outfile=None, checksumtypes=('crc32','crc32','crc32'),
                              encoding='UTF-8', compression=args.compression, compression_level=args.level)
            _write_output_bytes(dst, blob)
        else:
            res = P.pack_neo(args.input, outfile=dst, checksumtypes=('crc32','crc32','crc32'),
                             encoding='UTF-8', compression=args.compression, compression_level=args.level)
            if isinstance(res, (bytes, bytearray)):
                _write_output_bytes(dst, res)
        return 0

    if args.repack:
        src = args.input
        dst = args.output or '-'
        res = P.repack_neo(src if src not in (None, '-', b'-') else _read_input_bytes(src),
                           outfile=dst, checksumtypes=('crc32','crc32','crc32'),
                           compression=args.compression, compression_level=args.level)
        if isinstance(res, (bytes, bytearray)):
            _write_output_bytes(dst, res)
        return 0

    if args.convert:
        src = args.input
        dst = args.output or '-'
        if src in (None, '-', b'-'):
            raise SystemExit("convert requires a path input (zip/tar). Use -i <file>")
        res = P.convert_foreign_to_neo(src, outfile=dst, checksumtypes=('crc32','crc32','crc32'),
                                       compression=args.compression, compression_level=args.level)
        if isinstance(res, (bytes, bytearray)):
            _write_output_bytes(dst, res)
        return 0

if __name__ == "__main__":
    sys.exit(main())
