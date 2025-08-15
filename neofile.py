
#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function, unicode_literals

"""
neofile.py
A lightweight CLI re-creation of archivefile.py that uses the *alt* implementation
(pyneofile) you asked for, including:
 - INI-based format defaults (auto-fallback)
 - stdlib compression with size-based "auto" (zlib/gzip/bz2, xz on Py3 when available)
 - robust checksums (CRC-32 padded), JSON/header/content verification
 - Python 2 and 3 compatibility
 - Optional conversion from ZIP/TAR (stdlib), and **RAR/7z** when extra libs are installed:
      * RAR:   pip install rarfile
      * 7z:    pip install py7zr

Operations:
  -c / --create    pack files/dirs into an archive
  -e / --extract   extract an archive
  -r / --repack    repack (optionally change compression)
  -l / --list      list entries (fast, header-only)
  -v / --validate  validate checksums

Convert support:
  Use -t/--convert with --create/--repack/--list/--validate to convert a foreign archive
  (zip/tar are stdlib; rar/7z need optional libs) into the alt ArchiveFile format first.
"""

import os
import sys
import argparse
import tempfile

# Graceful SIGPIPE on non-Windows
if os.name != 'nt':
    try:
        import signal
        if hasattr(signal, 'SIGPIPE'):
            signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    except Exception:
        pass

# Import alt core
try:
    import pyneofile as A
except Exception as e:
    sys.stderr.write("Failed to import pyneofile: %s\n" % (e,))
    sys.exit(2)

__program_name__ = "neofile"
__version__ = "0.2.0"

def _build_formatspecs_from_args(args):
    """Create a formatspecs dict or return None to use INI auto-fallback."""
    if args.format is None or args.format.lower() == "auto":
        return None  # let alt core load INI or defaults
    magic = args.format
    ver   = args.formatver if args.formatver is not None else "001"
    delim = args.delimiter if args.delimiter is not None else "\x00"
    return {
        "format_name": magic,
        "format_magic": magic,
        "format_ver": ver,                 # alt core keeps digits as-is
        "format_delimiter": delim,
        "new_style": True,
    }

def _read_listfile(path):
    items = []
    with open(path, 'r') as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith('#'):
                continue
            items.append(s)
    return items

def _convert_or_fail(infile, outpath, formatspecs, checksum, compression, level):
    """Call core convert function and show friendly messages for missing deps."""
    try:
        A.convert_foreign_to_alt(infile, outpath, formatspecs=formatspecs,
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
    except ValueError as e:
        # Unsupported format (not zip/tar/rar/7z)
        sys.stderr.write("convert error: %s\n" % e)
        return False
    except Exception as e:
        sys.stderr.write("unexpected convert error: %s\n" % e)
        return False

def main(argv=None):
    p = argparse.ArgumentParser(description="Manipulate ArchiveFile (alt) archives.", conflict_handler="resolve", add_help=True)

    p.add_argument("-V", "--version", action="version", version=__program_name__ + " " + __version__)

    # IO
    p.add_argument("-i", "--input", nargs="+", required=True, help="Input file(s) or archive file.")
    p.add_argument("-o", "--output", default=None, help="Output file or directory.")

    # Ops
    p.add_argument("-c", "--create", action="store_true", help="Create an archive from input files/dirs.")
    p.add_argument("-e", "--extract", action="store_true", help="Extract an archive to --output (directory).")
    p.add_argument("-r", "--repack", action="store_true", help="Repack an existing archive (can change compression).")
    p.add_argument("-l", "--list", action="store_true", help="List archive entries.")
    p.add_argument("-v", "--validate", action="store_true", help="Validate archive checksums.")
    p.add_argument("-t", "--convert", action="store_true", help="Treat input as foreign (zip/tar/rar/7z) and convert to ArchiveFile first.")

    # Format & delimiter
    p.add_argument("-F", "--format", default="auto", help="Format magic to use (or 'auto' to read INI).")
    p.add_argument("-D", "--delimiter", default=None, help="Delimiter to use when --format is not 'auto'.")
    p.add_argument("-m", "--formatver", default=None, help="Format version digits (e.g. 001).")

    # Compression
    p.add_argument("-P", "--compression", default="auto", help="Compression: none|zlib|gzip|bz2|xz|auto")
    p.add_argument("-L", "--level", default=None, help="Compression level/preset (int).")
    p.add_argument("-W", "--wholefile", action="store_true", help="(Ignored; CLI compatibility).")

    # Validation & extraction behavior
    p.add_argument("-C", "--checksum", default="crc32", help="Checksum algorithm (header/content/json).")
    p.add_argument("-s", "--skipchecksum", action="store_true", help="Skip checksum verification while reading.")
    p.add_argument("-p", "--preserve", action="store_false", help="Do not preserve permissions/times (kept for compatibility).")

    # Misc
    p.add_argument("-d", "--verbose", action="store_true", help="Verbose logging.")
    p.add_argument("-T", "--text", action="store_true", help="Treat the first input argument as a text file containing paths (one per line).")

    args = p.parse_args(argv)

    # Choose primary action
    actions = ['create', 'extract', 'repack', 'list', 'validate']
    active = next((name for name in actions if getattr(args, name)), None)
    if not active:
        p.error("one of --create/--extract/--repack/--list/--validate is required")

    # formatspecs (None => INI auto-fallback inside the alt core)
    formatspecs = _build_formatspecs_from_args(args)

    # Inputs
    inputs = args.input
    infile0 = inputs[0]

    # Compression/level
    compression = args.compression
    level = None if args.level in (None, "",) else int(args.level)

    # Checksum triple
    checksum = args.checksum
    checks = (checksum, checksum, checksum)

    if active == 'create':
        if args.text:
            inputs = _read_listfile(infile0)
        if args.convert:
            if not args.output:
                p.error("--output is required when using --convert")
            ok = _convert_or_fail(infile0, args.output, formatspecs, checksum, compression, level)
            return 0 if ok else 1
        if not args.output:
            p.error("--output is required for --create")
        A.pack_alt(inputs if not args.text else inputs, args.output, formatspecs=formatspecs,
                   checksumtypes=checks, compression=compression, compression_level=level)
        if args.verbose:
            sys.stderr.write("created: %s\n" % args.output)
        return 0

    if active == 'repack':
        if args.convert:
            if not args.output:
                p.error("--output is required when using --repack --convert")
            ok = _convert_or_fail(infile0, args.output, formatspecs, checksum, compression, level)
            return 0 if ok else 1
        if not args.output:
            p.error("--output is required for --repack")
        A.repack_alt(infile0, args.output, formatspecs=formatspecs,
                     checksumtypes=checks, compression=compression, compression_level=level)
        if args.verbose:
            sys.stderr.write("repacked: %s -> %s\n" % (infile0, args.output))
        return 0

    if active == 'extract':
        outdir = args.output or "."
        if args.convert:
            tmp_arc = os.path.join(tempfile.gettempdir(), "af_alt_convert.arc")
            ok = _convert_or_fail(infile0, tmp_arc, formatspecs, checksum, compression, level)
            if not ok:
                return 1
            infile0_use = tmp_arc
        else:
            infile0_use = infile0
        A.unpack_alt(infile0_use, outdir, formatspecs=formatspecs, skipchecksum=args.skipchecksum, uncompress=True)
        if args.verbose:
            sys.stderr.write("extracted to: %s\n" % outdir)
        return 0

    if active == 'list':
        if args.convert:
            tmp_arc = os.path.join(tempfile.gettempdir(), "af_alt_convert.arc")
            ok = _convert_or_fail(infile0, tmp_arc, formatspecs, checksum, compression, level)
            if not ok:
                return 1
            use = tmp_arc
        else:
            use = infile0
        names = A.archivefilelistfiles_alt(use, formatspecs=formatspecs, advanced=args.verbose, include_dirs=True)
        if not args.verbose:
            for n in names:
                sys.stdout.write(n + "\n")
        else:
            for ent in names:
                sys.stdout.write("%s\t%s\t%s\t%s\n" % (
                    ent['type'], ent['compression'], ent['size'], ent['name']
                ))
        return 0

    if active == 'validate':
        if args.convert:
            tmp_arc = os.path.join(tempfile.gettempdir(), "af_alt_convert.arc")
            ok = _convert_or_fail(infile0, tmp_arc, formatspecs, checksum, compression, level)
            if not ok:
                return 1
            use = tmp_arc
        else:
            use = infile0
        ok, details = A.archivefilevalidate_alt(use, formatspecs=formatspecs, verbose=args.verbose, return_details=True)
        if not args.verbose:
            sys.stdout.write("valid: %s\n" % ("yes" if ok else "no"))
        else:
            sys.stdout.write("valid: %s (entries: %d)\n" % ("yes" if ok else "no", len(details)))
            for d in details:
                sys.stdout.write("%4d %s h:%s j:%s c:%s\n" % (
                    d['index'], d['name'], d['header_ok'], d['json_ok'], d['content_ok']
                ))
        return 0

    return 0

if __name__ == "__main__":
    sys.exit(main())
