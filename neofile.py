
#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function, unicode_literals

"""
neofile.py — CLI for the PyNeoFile format (.neo).
Uses the pyneofile wrappers (which call into pyneoarc_alt) and prefers pyneofile.ini.
Supports converting from zip/tar (stdlib), and rar/7z when optional libs are installed.
"""

import os, sys, argparse, tempfile
import pyneofile as N

__program_name__ = "pyneofile"
__version__ = "0.1.0"

def _build_formatspecs_from_args(args):
    # Allow explicit override; otherwise let wrappers auto-load pyneofile.ini
    if args.format is None or args.format.lower() == "auto":
        return None
    return {
        "format_name": args.format,
        "format_magic": args.format,
        "format_ver": (args.formatver or "001"),
        "format_delimiter": (args.delimiter or "\x00"),
        "new_style": True,
    }

def _convert_or_fail(infile, outpath, formatspecs, checksum, compression, level):
    try:
        N.convert_foreign_to_neo(infile, outpath, formatspecs=formatspecs,
                                 checksumtypes=(checksum, checksum, checksum),
                                 compression=compression, compression_level=level)
        return True
    except RuntimeError as e:
        msg = str(e)
        if "rarfile" in msg.lower():
            sys.stderr.write("error: RAR support requires 'rarfile'. Install via: pip install rarfile\n")
        elif "py7zr" in msg.lower():
            sys.stderr.write("error: 7z support requires 'py7zr'. Install via: pip install py7zr\n")
        else:
            sys.stderr.write("convert error: %s\n" % msg)
        return False
    except Exception as e:
        sys.stderr.write("convert error: %s\n" % e)
        return False

def main(argv=None):
    p = argparse.ArgumentParser(description="PyNeoFile (.neo) archiver", add_help=True)
    p.add_argument("-V","--version", action="version", version=__program_name__ + " " + __version__)

    p.add_argument("-i","--input", nargs="+", required=True, help="Input files/dirs or archive file")
    p.add_argument("-o","--output", default=None, help="Output file or directory")

    p.add_argument("-c","--create", action="store_true", help="Create a .neo archive from inputs")
    p.add_argument("-e","--extract", action="store_true", help="Extract an archive to --output")
    p.add_argument("-r","--repack", action="store_true", help="Repack an archive (change compression)")
    p.add_argument("-l","--list", action="store_true", help="List entries")
    p.add_argument("-v","--validate", action="store_true", help="Validate checksums")
    p.add_argument("-t","--convert", action="store_true", help="Convert zip/tar/rar/7z → .neo first")

    p.add_argument("-F","--format", default="auto", help="Format magic (default 'auto' via pyneofile.ini)")
    p.add_argument("-D","--delimiter", default=None, help="Delimiter (when not using 'auto')")
    p.add_argument("-m","--formatver", default=None, help="Version digits (e.g. 001)")

    p.add_argument("-P","--compression", default="auto", help="Compression: none|zlib|gzip|bz2|xz|auto")
    p.add_argument("-L","--level", default=None, help="Compression level/preset")
    p.add_argument("-C","--checksum", default="crc32", help="Checksum algorithm")
    p.add_argument("-s","--skipchecksum", action="store_true", help="Skip checks while reading")
    p.add_argument("-d","--verbose", action="store_true", help="Verbose listing")

    args = p.parse_args(argv)

    formatspecs = _build_formatspecs_from_args(args)
    inputs = args.input
    infile0 = inputs[0]
    compression = args.compression
    level = None if args.level in (None, "",) else int(args.level)
    checksum = args.checksum

    if args.create:
        if args.convert:
            if not args.output: p.error("--output is required")
            ok = _convert_or_fail(infile0, args.output, formatspecs, checksum, compression, level)
            return 0 if ok else 1
        if not args.output: p.error("--output is required")
        N.pack_neo(inputs, args.output, formatspecs=formatspecs,
                   checksumtypes=(checksum, checksum, checksum),
                   compression=compression, compression_level=level)
        if args.verbose: sys.stderr.write("created: %s\n" % args.output)
        return 0

    if args.repack:
        if args.convert:
            if not args.output: p.error("--output is required")
            ok = _convert_or_fail(infile0, args.output, formatspecs, checksum, compression, level)
            return 0 if ok else 1
        if not args.output: p.error("--output is required")
        N.repack_neo(infile0, args.output, formatspecs=formatspecs,
                     checksumtypes=(checksum, checksum, checksum),
                     compression=compression, compression_level=level)
        if args.verbose: sys.stderr.write("repacked: %s\n" % args.output)
        return 0

    if args.extract:
        outdir = args.output or "."
        if args.convert:
            tmp_arc = os.path.join(tempfile.gettempdir(), "pyneofile_convert.neo")
            ok = _convert_or_fail(infile0, tmp_arc, formatspecs, checksum, compression, level)
            if not ok: return 1
            use = tmp_arc
        else:
            use = infile0
        N.unpack_neo(use, outdir, formatspecs=formatspecs, skipchecksum=args.skipchecksum, uncompress=True)
        if args.verbose: sys.stderr.write("extracted → %s\n" % outdir)
        return 0

    if args.list:
        if args.convert:
            tmp_arc = os.path.join(tempfile.gettempdir(), "pyneofile_convert.neo")
            ok = _convert_or_fail(infile0, tmp_arc, formatspecs, checksum, compression, level)
            if not ok: return 1
            use = tmp_arc
        else:
            use = infile0
        names = N.archivefilelistfiles_neo(use, formatspecs=formatspecs, advanced=args.verbose, include_dirs=True)
        if not args.verbose:
            for n in names: sys.stdout.write(n + "\n")
        else:
            for ent in names:
                sys.stdout.write("%s\t%s\t%s\t%s\n" % (
                    ent['type'], ent['compression'], ent['size'], ent['name']
                ))
        return 0

    if args.validate:
        if args.convert:
            tmp_arc = os.path.join(tempfile.gettempdir(), "pyneofile_convert.neo")
            ok = _convert_or_fail(infile0, tmp_arc, formatspecs, checksum, compression, level)
            if not ok: return 1
            use = tmp_arc
        else:
            use = infile0
        ok, details = N.archivefilevalidate_neo(use, formatspecs=formatspecs, verbose=args.verbose, return_details=True)
        if not args.verbose:
            sys.stdout.write("valid: %s\n" % ("yes" if ok else "no"))
        else:
            sys.stdout.write("valid: %s (entries: %d)\n" % ("yes" if ok else "no", len(details)))
            for d in details:
                sys.stdout.write("%4d %s h:%s j:%s c:%s\n" % (
                    d['index'], d['name'], d['header_ok'], d['json_ok'], d['content_ok']
                ))
        return 0

    p.error("one of --create/--extract/--repack/--list/--validate is required")

if __name__ == "__main__":
    sys.exit(main())
