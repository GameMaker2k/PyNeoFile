
#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function, unicode_literals

"""
neofile.py — CLI for the PyNeoFile format (.neo).

New:
- Input '-' for list/validate/extract/repack/convert reads archive bytes from stdin.
- Output '-' for create/repack/convert writes archive bytes to stdout.
- Extract with '-o -' streams a TAR archive to stdout (use: `... -e -i in.neo -o - > out.tar`).
"""

import os, sys, argparse, tempfile, tarfile, io, base64
import pyneofile as N

__project__ = pyneofile.__project__
__program_name__ = pyneofile.__program_name__
__file_format_name__ = pyneofile.__file_format_name__
__file_format_magic__ = pyneofile.__file_format_magic__
__file_format_len__ = pyneofile.__file_format_len__
__file_format_hex__ = pyneofile.__file_format_hex__
__file_format_delimiter__ = pyneofile.__file_format_delimiter__
__file_format_dict__ = pyneofile.__file_format_dict__
__file_format_default__ = pyneofile.__file_format_default__
__file_format_multi_dict__ = pyneofile.__file_format_multi_dict__
__use_new_style__ = pyneofile.__use_new_style__
__use_advanced_list__ = pyneofile.__use_advanced_list__
__use_alt_inode__ = pyneofile.__use_alt_inode__
__project_url__ = pyneofile.__project_url__
__version_info__ = pyneofile.__version_info__
__version_date_info__ = pyneofile.__version_date_info__
__version_date__ = pyneofile.__version_date__
__version_date_plusrc__ = pyneofile.__version_date_plusrc__
__version__ = pyneofile.__version__

def _stdout_bin():
    return getattr(sys.stdout, "buffer", sys.stdout)

def _stdin_bin():
    return getattr(sys.stdin, "buffer", sys.stdin)

def _build_formatspecs_from_args(args):
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
        return N.convert_foreign_to_neo(infile, outpath, formatspecs=formatspecs,
                                        checksumtypes=(checksum, checksum, checksum),
                                        compression=compression, compression_level=level)
    except RuntimeError as e:
        msg = str(e)
        if "rarfile" in msg.lower():
            sys.stderr.write("error: RAR support requires 'rarfile'. Install via: pip install rarfile\n")
        elif "py7zr" in msg.lower():
            sys.stderr.write("error: 7z support requires 'py7zr'. Install via: pip install py7zr\n")
        else:
            sys.stderr.write("convert error: %s\n" % msg)
        return None
    except Exception as e:
        sys.stderr.write("convert error: %s\n" % e)
        return None

def _emit_tar_stream_from_array(arr, outfp):
    """Write a tar stream to outfp from the parsed archive array (no re-compress)."""
    tf = tarfile.open(fileobj=outfp, mode='w|')  # stream mode
    try:
        for ent in arr['ffilelist']:
            name = ent['fname'].lstrip('./')
            if ent['ftype'] == 5:
                ti = tarfile.TarInfo(name=name.rstrip('/') + '/')
                ti.type = tarfile.DIRTYPE
                ti.mode = ent.get('fmode', 0o755) & 0o777
                ti.mtime = ent.get('fmtime', 0)
                ti.size = 0
                tf.addfile(ti)
            else:
                data = ent.get('fcontent') or b''
                bio = io.BytesIO(data)
                ti = tarfile.TarInfo(name=name)
                ti.type = tarfile.REGTYPE
                ti.mode = ent.get('fmode', 0o644) & 0o777
                ti.mtime = ent.get('fmtime', 0)
                ti.size = len(data)
                tf.addfile(ti, fileobj=bio)
    finally:
        tf.close()

def main(argv=None):
    p = argparse.ArgumentParser(description="PyNeoFile (.neo) archiver", add_help=True)
    p.add_argument("-V","--version", action="version", version=__program_name__ + " " + __version__)

    p.add_argument("-i","--input", nargs="+", required=True, help="Input files/dirs or archive file ('-' = stdin for archive bytes or newline-separated paths with -c)")
    p.add_argument("-o","--output", default=None, help="Output file or directory ('-'=stdout for archive bytes; on -e streams a TAR)")

    p.add_argument("-c","--create", action="store_true", help="Create a .neo archive from inputs")
    p.add_argument("-e","--extract", action="store_true", help="Extract an archive to --output (or stream TAR to stdout with -o -)")
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
    p.add_argument("-T","--text", action="store_true", help="Treat -i - as newline-separated path list when used with -c/--create")

    args = p.parse_args(argv)

    formatspecs = _build_formatspecs_from_args(args)
    inputs = args.input
    infile0 = inputs[0]
    compression = args.compression
    level = None if args.level in (None, "",) else int(args.level)
    checksum = args.checksum

    # Determine active action
    actions = ["create","extract","repack","list","validate"]
    active = next((a for a in actions if getattr(args, a)), None)
    if not active:
        p.error("one of --create/--extract/--repack/--list/--validate is required")

    # Helper: read archive bytes from stdin for non-create ops
    def _maybe_archive_bytes():
        if infile0 == '-':
            return _stdin_bin().read()
        return None

    if args.create:
        if infile0 == '-' and not args.text:
            # read newline-separated paths from stdin
            items = [line.strip() for line in sys.stdin if line.strip() and not line.startswith('#')]
        else:
            items = inputs

        if args.convert:
            if not args.output:
                p.error("--output is required (use '-' to stream to stdout)")
            data = _convert_or_fail(infile0, (None if args.output == '-' else args.output),
                                    formatspecs, checksum, compression, level)
            if data is None:
                return 1
            if args.output == '-':
                _stdout_bin().write(data)
            return 0

        if not args.output:
            p.error("--output is required for --create (use '-' to stream)")
        out_bytes = (args.output == '-')
        if out_bytes:
            data = N.pack_neo(items, None, formatspecs=formatspecs,
                              checksumtypes=(checksum, checksum, checksum),
                              compression=compression, compression_level=level)
            _stdout_bin().write(data)
        else:
            N.pack_neo(items, args.output, formatspecs=formatspecs,
                       checksumtypes=(checksum, checksum, checksum),
                       compression=compression, compression_level=level)
            if args.verbose: sys.stderr.write("created: %s\n" % args.output)
        return 0

    if args.repack:
        src = _maybe_archive_bytes() or infile0
        if args.convert:
            if not args.output:
                p.error("--output is required (use '-' to stream)")
            data = _convert_or_fail(src, (None if args.output == '-' else args.output),
                                    formatspecs, checksum, compression, level)
            if data is None:
                return 1
            if args.output == '-':
                _stdout_bin().write(data)
            return 0

        if not args.output:
            p.error("--output is required for --repack (use '-' to stream)")
        if args.output == '-':
            data = N.repack_neo(src, None, formatspecs=formatspecs,
                                checksumtypes=(checksum, checksum, checksum),
                                compression=compression, compression_level=level)
            _stdout_bin().write(data)
        else:
            N.repack_neo(src, args.output, formatspecs=formatspecs,
                         checksumtypes=(checksum, checksum, checksum),
                         compression=compression, compression_level=level)
            if args.verbose: sys.stderr.write("repacked: %s -> %s\n" % (('<stdin>' if infile0 == '-' else infile0), args.output))
        return 0

    if args.extract:
        src = _maybe_archive_bytes() or infile0
        if args.output in (None, '.') and infile0 == '-':
            # default would attempt to mkdir '.'; fine
            pass
        if args.output == '-':
            # stream TAR to stdout
            arr = N.archive_to_array_neo(src, formatspecs=formatspecs, listonly=False,
                                         skipchecksum=args.skipchecksum, uncompress=True)
            _emit_tar_stream_from_array(arr, _stdout_bin())
            return 0
        outdir = args.output or "."
        N.unpack_neo(src, outdir, formatspecs=formatspecs, skipchecksum=args.skipchecksum, uncompress=True)
        if args.verbose: sys.stderr.write("extracted → %s\n" % outdir)
        return 0

    if args.list:
        src = _maybe_archive_bytes() or infile0
        names = N.archivefilelistfiles_neo(src, formatspecs=formatspecs, advanced=args.verbose, include_dirs=True)
        if not args.verbose:
            for n in names: sys.stdout.write(n + "\n")
        else:
            for ent in names:
                sys.stdout.write("%s\t%s\t%s\t%s\n" % (
                    ent['type'], ent['compression'], ent['size'], ent['name']
                ))
        return 0

    if args.validate:
        src = _maybe_archive_bytes() or infile0
        ok, details = N.archivefilevalidate_neo(src, formatspecs=formatspecs, verbose=args.verbose, return_details=True)
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
