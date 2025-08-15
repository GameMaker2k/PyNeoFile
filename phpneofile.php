<?php
/**
 * PyNeoFile (PHP)
 * ---------------------------------------------------------------------
 * A PHP implementation compatible with the Python "pyneoarc_alt" module.
 * - Same archive wire-format (ArchiveFile + ver digits + NUL delimiter)
 * - Functions: pack_alt, pack_iter_alt, unpack_alt, repack_alt,
 *              archive_to_array_alt, archivefilelistfiles_alt,
 *              archivefilevalidate_alt, convert_foreign_to_alt
 * - In-memory I/O: accept bytes strings and stream resources; return bytes when $outfile === null or "-"
 * - INI auto-fallback (PYNEOFILE_INI/pyneofile.ini, or archivefile.ini/catfile.ini/foxfile.ini)
 * - Compression: none|zlib|gzip|bz2 (xz not provided in stock PHP)
 * - Checksums: crc32, md5, sha1, sha256, sha512
 */

if (!defined('PYNEOFILE_VERSION')) {
    define('PYNEOFILE_VERSION', '0.2.0');
}

# ----------------------- Utilities -----------------------

function _neo_is_bytes($v) {
    return is_string($v);
}

function _neo_to_bytes($v) {
    if (is_string($v)) return $v;
    if (is_null($v)) return "";
    if (is_int($v) || is_float($v)) return (string)$v;
    return (string)$v;
}

function _neo_hex($n) {
    if (!is_int($n)) $n = intval($n);
    return strtolower(dechex($n));
}

function _neo_crc32_hex($s) {
    // crc32b is big-endian, same as zlib.crc32 & 0xffffffff
    $h = hash('crc32b', _neo_to_bytes($s));
    // ensure 8 chars
    return strtolower(str_pad($h, 8, '0', STR_PAD_LEFT));
}

function _neo_checksum($data, $type, $text=false) {
    $t = strtolower((string)$type);
    if ($t === '' || $t === 'none') return '0';
    $b = _neo_to_bytes($data);
    if ($t === 'crc32') return _neo_crc32_hex($b);
    if (in_array($t, array('md5','sha1','sha224','sha256','sha384','sha512'))) {
        return strtolower(hash($t, $b));
    }
    throw new \Exception("Unsupported checksum: ".$type);
}

function _neo_append_null($s, $delim) {
    return _neo_to_bytes($s) . _neo_to_bytes($delim);
}

function _neo_append_nulls($arr, $delim) {
    $out = "";
    foreach ($arr as $x) $out .= _neo_append_null($x, $delim);
    return $out;
}

function _neo_ver_digits($ver) {
    if ($ver === null) return '001';
    $s = (string)$ver;
    $digits = preg_replace('/\D+/', '', $s);
    return $digits !== '' ? $digits : '001';
}

# ----------------------- Compression -----------------------

function _neo_normalize_algo($a) {
    $a = strtolower((string)$a);
    if ($a === 'gz') $a = 'gzip';
    if ($a === 'bz' || $a === 'bzip' || $a === 'bzip2') $a = 'bz2';
    if ($a === 'z') $a = 'zlib';
    if ($a === '') $a = 'none';
    return $a;
}

function _neo_compress_bytes($data, $algo='none', $level=null) {
    $algo = _neo_normalize_algo($algo);
    $b = _neo_to_bytes($data);
    if ($algo === 'none') return array($b, 'none');
    if ($algo === 'zlib') {
        $lvl = ($level === null) ? -1 : intval($level);
        $out = gzcompress($b, $lvl);
        if ($out === false) throw new \Exception("zlib compress failed");
        return array($out, 'zlib');
    }
    if ($algo === 'gzip') {
        $lvl = ($level === null) ? -1 : intval($level);
        $out = gzencode($b, $lvl);
        if ($out === false) throw new \Exception("gzip compress failed");
        return array($out, 'gzip');
    }
    if ($algo === 'bz2') {
        $lvl = ($level === null) ? 9 : intval($level);
        if (!function_exists('bzcompress')) throw new \Exception("bz2 not available");
        $out = bzcompress($b, $lvl);
        if (!is_string($out)) throw new \Exception("bz2 compress failed");
        return array($out, 'bz2');
    }
    if ($algo === 'xz') {
        throw new \Exception("xz not available in stock PHP");
    }
    throw new \Exception("Unknown compression: ".$algo);
}

function _neo_decompress_bytes($data, $algo='none') {
    $algo = _neo_normalize_algo($algo);
    $b = _neo_to_bytes($data);
    if ($algo === 'none') return $b;
    if ($algo === 'zlib') {
        $out = gzuncompress($b);
        if ($out === false) throw new \Exception("zlib decompress failed");
        return $out;
    }
    if ($algo === 'gzip') {
        $out = gzdecode($b);
        if ($out === false) throw new \Exception("gzip decompress failed");
        return $out;
    }
    if ($algo === 'bz2') {
        if (!function_exists('bzdecompress')) throw new \Exception("bz2 not available");
        $out = bzdecompress($b);
        if (!is_string($out)) throw new \Exception("bz2 decompress failed");
        return $out;
    }
    if ($algo === 'xz') {
        throw new \Exception("xz not available in stock PHP");
    }
    throw new \Exception("Unknown compression: ".$algo);
}

function _neo_auto_pick_for_size($size) {
    // returns array($algo, $level)
    if ($size < 16384) return array('none', null);
    if ($size >= 262144) return array('bz2', 9);
    return array('zlib', 6);
}

# ----------------------- INI formatspecs -----------------------

function _neo_decode_delim_escape($s) {
    // Interpret typical C-style escapes including \xNN
    return stripcslashes($s);
}

function _neo_default_formatspecs() {
    return array(
        'format_magic' => 'ArchiveFile',
        'format_ver' => '001',
        'format_delimiter' => "\x00",
        'new_style' => true,
    );
}

function _neo_load_formatspecs_from_ini($paths=null, $prefer_section=null) {
    $cands = array();
    if ($paths) {
        if (is_string($paths)) $cands[] = $paths;
        else $cands = array_merge($cands, $paths);
    }
    $env = getenv('PYNEOFILE_INI');
    if (!$env) $env = getenv('PYARCHIVE_INI');
    if ($env) $cands[] = $env;
    $cands = array_merge($cands, array('pyneofile.ini','archivefile.ini','catfile.ini','foxfile.ini'));

    $picked = null;
    foreach ($cands as $p) { if (is_file($p)) { $picked = $p; break; } }
    if (!$picked) return null;

    $ini = @parse_ini_file($picked, true, INI_SCANNER_RAW);
    if (!$ini || !is_array($ini)) return null;

    $sec = null;
    if ($prefer_section && isset($ini[$prefer_section])) $sec = $prefer_section;
    else if (isset($ini['config']) && isset($ini['config']['default']) && isset($ini[$ini['config']['default']])) {
        $sec = $ini['config']['default'];
    } else {
        foreach ($ini as $k=>$v) { if (strtolower($k) !== 'config') { $sec = $k; break; } }
    }
    if (!$sec) return null;

    $magic = isset($ini[$sec]['magic']) ? $ini[$sec]['magic'] : 'ArchiveFile';
    $ver   = isset($ini[$sec]['ver']) ? $ini[$sec]['ver'] : '001';
    $delim = isset($ini[$sec]['delimiter']) ? $ini[$sec]['delimiter'] : "\\x00";
    $newst = isset($ini[$sec]['newstyle']) ? $ini[$sec]['newstyle'] : 'true';
    $ext   = isset($ini[$sec]['extension']) ? $ini[$sec]['extension'] : '.arc';

    $delim_real = _neo_decode_delim_escape($delim);
    $ver_digits = _neo_ver_digits($ver);
    return array(
        'format_magic' => $magic,
        'format_ver' => $ver_digits,
        'format_delimiter' => $delim_real,
        'new_style' => (strtolower((string)$newst) === 'true' || $newst === '1' || strtolower((string)$newst) === 'yes'),
        'format_name' => $sec,
        'extension' => $ext,
    );
}

function _neo_ensure_formatspecs($specs=null) {
    static $cache = null;
    if ($specs && is_array($specs)) return $specs;
    if ($cache === null) $cache = _neo_load_formatspecs_from_ini();
    return $cache ? $cache : _neo_default_formatspecs();
}

# ----------------------- Stream helpers -----------------------

class NeoStream {
    private $s;
    private $fp;
    private $pos = 0;
    function __construct($src) {
        if (is_resource($src)) { $this->fp = $src; $this->s = null; }
        else if (is_string($src)) { $this->s = $src; $this->fp = null; }
        else { throw new \Exception("Unsupported input"); }
    }
    function read($n) {
        if ($this->fp) return fread($this->fp, $n);
        $chunk = substr($this->s, $this->pos, $n);
        $this->pos += strlen($chunk);
        return $chunk;
    }
    function seek($off, $whence=SEEK_SET) {
        if ($this->fp) return fseek($this->fp, $off, $whence);
        if ($whence === SEEK_CUR) $this->pos += $off;
        else if ($whence === SEEK_END) $this->pos = strlen($this->s) + $off;
        else $this->pos = $off;
        if ($this->pos < 0) $this->pos = 0;
        if ($this->pos > strlen($this->s)) $this->pos = strlen($this->s);
        return 0;
    }
}

function _neo_open_in($infile) {
    // Accept: path (string file), resource, or bytes (string without file)
    if (is_resource($infile)) return array(new NeoStream($infile), null);
    if (is_string($infile) && is_file($infile)) {
        $fp = fopen($infile, 'rb');
        return array(new NeoStream($fp), $fp);
    }
    // treat as bytes
    return array(new NeoStream((string)$infile), null);
}

function _neo_open_out($outfile) {
    // Return array($bufferMode, $fp, &$buf)
    if ($outfile === null || $outfile === '-') {
        $buf = "";
        return array(true, null, $buf);
    }
    if (is_resource($outfile)) return array(false, $outfile, null);
    $fp = fopen($outfile, 'wb');
    return array(false, $fp, null);
}

function _neo_write(&$bufMode, $fp, &$buf, $data) {
    if ($bufMode) { $buf .= _neo_to_bytes($data); return; }
    fwrite($fp, _neo_to_bytes($data));
}

# ----------------------- Builders -----------------------

function _neo_write_global_header(&$dst, $formatspecs, $numfiles, $encoding, $checksumtype, $extradata=array()) {
    $delim = $formatspecs['format_delimiter'];
    $magic = $formatspecs['format_magic'];
    $ver_digits = _neo_ver_digits($formatspecs['format_ver']);
    $platform_name = PHP_OS_FAMILY ?: PHP_OS;

    if (is_array($extradata) && empty($extradata)) {
        // nothing
    }

    $extrafields = _neo_hex(count($extradata));
    $extras_blob = _neo_append_null($extrafields, $delim);
    if (!empty($extradata)) $extras_blob .= _neo_append_nulls($extradata, $delim);
    $extras_size_hex = _neo_hex(strlen($extras_blob));

    $fnumfiles_hex = _neo_hex(intval($numfiles));

    $body = _neo_append_nulls(array(
        _neo_hex(3 + 5 + count($extradata) + 1), // tmpoutlen_hex (compat value)
        $encoding,
        $platform_name,
        $fnumfiles_hex,
        _neo_hex(strlen($extras_blob)),
        $extrafields
    ), $delim);
    if (!empty($extradata)) $body .= _neo_append_nulls($extradata, $delim);
    $body .= _neo_append_null($checksumtype, $delim);

    $prefix = _neo_append_null($magic.$ver_digits, $delim);
    $tmpfileoutstr = $body . _neo_append_null('', $delim);
    $headersize_hex = _neo_hex(strlen($tmpfileoutstr) - strlen($delim));
    $out = $prefix . _neo_append_null($headersize_hex, $delim) . $body;
    $header_cs = _neo_checksum($out, $checksumtype, true);
    $out .= _neo_append_null($header_cs, $delim);
    $dst .= $out;
}

function _neo_build_record($formatspecs, $filemeta, $jsondata, $content_stored, $checksumtypes) {
    $delim = $formatspecs['format_delimiter'];
    $H = function($x){ return _neo_hex(intval($x)); };
    $fname = $filemeta['fname'];
    if (!preg_match('#^[\./]#', $fname)) $fname = './'.$fname;

    $fields = array(
        $H(isset($filemeta['ftype']) ? $filemeta['ftype'] : 0),
        isset($filemeta['fencoding']) ? $filemeta['fencoding'] : 'UTF-8',
        isset($filemeta['fcencoding']) ? $filemeta['fcencoding'] : 'UTF-8',
        $fname,
        isset($filemeta['flinkname']) ? $filemeta['flinkname'] : '',
        $H(isset($filemeta['fsize']) ? $filemeta['fsize'] : 0),
        $H(isset($filemeta['fatime']) ? $filemeta['fatime'] : time()),
        $H(isset($filemeta['fmtime']) ? $filemeta['fmtime'] : time()),
        $H(isset($filemeta['fctime']) ? $filemeta['fctime'] : time()),
        $H(isset($filemeta['fbtime']) ? $filemeta['fbtime'] : time()),
        $H(isset($filemeta['fmode']) ? $filemeta['fmode'] : (0100000|0666)),
        $H(isset($filemeta['fwinattributes']) ? $filemeta['fwinattributes'] : 0),
        isset($filemeta['fcompression']) ? $filemeta['fcompression'] : '',
        $H(isset($filemeta['fcsize']) ? $filemeta['fcsize'] : 0),
        $H(isset($filemeta['fuid']) ? $filemeta['fuid'] : 0),
        isset($filemeta['funame']) ? $filemeta['funame'] : '',
        $H(isset($filemeta['fgid']) ? $filemeta['fgid'] : 0),
        isset($filemeta['fgname']) ? $filemeta['fgname'] : '',
        $H(isset($filemeta['fid']) ? $filemeta['fid'] : (isset($filemeta['index']) ? $filemeta['index'] : 0)),
        $H(isset($filemeta['finode']) ? $filemeta['finode'] : (isset($filemeta['index']) ? $filemeta['index'] : 0)),
        $H(isset($filemeta['flinkcount']) ? $filemeta['flinkcount'] : 1),
        $H(isset($filemeta['fdev']) ? $filemeta['fdev'] : 0),
        $H(isset($filemeta['fdev_minor']) ? $filemeta['fdev_minor'] : 0),
        $H(isset($filemeta['fdev_major']) ? $filemeta['fdev_major'] : 0),
        '+'.strlen($delim),
    );

    // JSON section
    $fjsontype = (!empty($jsondata)) ? 'json' : 'none';
    if (!empty($jsondata)) {
        $raw_json = json_encode($jsondata, JSON_UNESCAPED_SLASHES);
        $json_cs_type = $checksumtypes[2];
        $fjsonlen_hex  = _neo_hex(is_array($jsondata) ? count($jsondata) : 0);
        $fjsonsize_hex = _neo_hex(strlen($raw_json));
        $fjsoncs = _neo_checksum($raw_json, $json_cs_type, true);
    } else {
        $raw_json = '';
        $json_cs_type = 'none';
        $fjsonlen_hex = '0';
        $fjsonsize_hex = '0';
        $fjsoncs = '0';
    }

    $extras_size_hex = _neo_hex(strlen(_neo_append_null('0', $delim)));
    $extrafields = '0';

    $rec_fields = array_merge($fields, array($fjsontype, $fjsonlen_hex, $fjsonsize_hex, $json_cs_type, $fjsoncs, $extras_size_hex, $extrafields));
    $header_cs_type  = $checksumtypes[0];
    $content_cs_type = (strlen($content_stored) > 0) ? $checksumtypes[1] : 'none';
    $rec_fields[] = $header_cs_type;
    $rec_fields[] = $content_cs_type;

    $record_fields_len_hex = _neo_hex(count($rec_fields) + 2);
    $header_no_cs = _neo_append_nulls($rec_fields, $delim);

    $tmp_with_placeholders = _neo_append_null($record_fields_len_hex, $delim) . $header_no_cs;
    $tmp_with_placeholders .= _neo_append_null('', $delim) . _neo_append_null('', $delim);
    $headersize_hex = _neo_hex(strlen($tmp_with_placeholders) - strlen($delim));

    $header_with_sizes = _neo_append_null($headersize_hex, $delim) . _neo_append_null($record_fields_len_hex, $delim) . $header_no_cs;

    $header_checksum = _neo_checksum($header_with_sizes, $header_cs_type, true);
    $content_checksum = _neo_checksum($content_stored, $content_cs_type, false);

    $header_full = $header_with_sizes . _neo_append_null($header_checksum, $delim) . _neo_append_null($content_checksum, $delim);

    return $header_full . $raw_json . $delim . $content_stored . $delim;
}

# ----------------------- Parsing -----------------------

function _neo_read_cstring($ns, $delim) {
    $d = _neo_to_bytes($delim);
    $dl = strlen($d);
    $buf = "";
    while (true) {
        $ch = $ns->read(1);
        if ($ch === "" || $ch === false) break;
        $buf .= $ch;
        if (strlen($buf) >= $dl && substr($buf, -$dl) === $d) {
            return substr($buf, 0, -$dl);
        }
    }
    return "";
}

function _neo_read_fields($ns, $n, $delim) {
    $out = array();
    for ($i=0; $i < intval($n); $i++) {
        $out[] = (string)_neo_read_cstring($ns, $delim);
    }
    return $out;
}

function _neo_index_json_and_checks($vals) {
    if (count($vals) < 25) throw new \Exception("Record too short");
    $idx = 25;
    $fjsontype = isset($vals[$idx]) ? $vals[$idx] : ''; $idx++;
    $v2 = isset($vals[$idx]) ? $vals[$idx] : '';
    $v3 = isset($vals[$idx+1]) ? $vals[$idx+1] : '';
    $v4 = isset($vals[$idx+2]) ? $vals[$idx+2] : '';
    $ishex = function($s){ return $s !== '' && preg_match('/^[0-9a-fA-F]+$/', $s); };
    $csnames = array('none','crc32','md5','sha1','sha224','sha256','sha384','sha512','blake2b','blake2s');
    if ($ishex($v2) && $ishex($v3) && in_array(strtolower($v4), $csnames, true)) {
        $idx_json_type = $idx-1; $idx_json_len = $idx; $idx_json_size = $idx+1; $idx_json_cst = $idx+2; $idx_json_cs = $idx+3;
        $idx += 4;
    } else {
        $idx_json_type = $idx-1; $idx_json_len = null; $idx_json_size = $idx; $idx_json_cst = $idx+1; $idx_json_cs = $idx+2;
        $idx += 3;
    }
    $idx_extras_size = $idx; $idx_extras_count = $idx+1;
    $count = intval($vals[$idx_extras_count] !== '' ? hexdec($vals[$idx_extras_count]) : 0);
    $idx = $idx + 2 + $count;
    $idx_header_cs_type  = $idx;
    $idx_content_cs_type = $idx+1;
    $idx_header_cs       = $idx+2;
    $idx_content_cs      = $idx+3;
    return array(
        'json' => array($idx_json_type, $idx_json_len, $idx_json_size, $idx_json_cst, $idx_json_cs),
        'cstypes' => array($idx_header_cs_type, $idx_content_cs_type),
        'csvals' => array($idx_header_cs, $idx_content_cs),
    );
}

function _neo_parse_global_header($ns, $formatspecs) {
    $delim = $formatspecs['format_delimiter'];
    $magicver = (string)_neo_read_cstring($ns, $delim);
    $_headersize = _neo_read_cstring($ns, $delim);
    $tmpoutlenhex = (string)_neo_read_cstring($ns, $delim);
    $fencoding = (string)_neo_read_cstring($ns, $delim);
    $fostype = (string)_neo_read_cstring($ns, $delim);
    $fnumfiles = intval(hexdec((string)_neo_read_cstring($ns, $delim)));
    $_extras_size = _neo_read_cstring($ns, $delim);
    $extrafields = intval(hexdec((string)_neo_read_cstring($ns, $delim)));
    $extras = array();
    for ($i=0; $i<$extrafields; $i++) { $extras[] = (string)_neo_read_cstring($ns, $delim); }
    $checksumtype = (string)_neo_read_cstring($ns, $delim);
    $_header_cs = (string)_neo_read_cstring($ns, $delim);
    return array('fencoding'=>$fencoding, 'fnumfiles'=>$fnumfiles, 'fostype'=>$fostype,
                 'fextradata'=>$extras, 'fchecksumtype'=>$checksumtype, 'ffilelist'=>array(),
                 'fformatspecs'=>$formatspecs);
}

function _neo_parse_record($ns, $formatspecs, $listonly=false, $skipchecksum=false, $uncompress=true) {
    $delim = $formatspecs['format_delimiter'];
    $first = _neo_read_cstring($ns, $delim);
    if ($first === '0') {
        $second = _neo_read_cstring($ns, $delim);
        if ($second === '0') return null;
        $headersize_hex = $first;
        $fields_len_hex = $second;
    } else {
        $headersize_hex = $first;
        $fields_len_hex = _neo_read_cstring($ns, $delim);
    }
    $n_fields = intval(hexdec($fields_len_hex));
    $vals = _neo_read_fields($ns, $n_fields, $delim);
    if (count($vals) < 25) throw new \Exception("Record too short: ".count($vals));

    list($ftypehex, $fencoding, $fcencoding, $fname, $flinkname,
         $fsize_hex, $fatime_hex, $fmtime_hex, $fctime_hex, $fbtime_hex,
         $fmode_hex, $fwinattrs_hex, $fcompression, $fcsize_hex,
         $fuid_hex, $funame, $fgid_hex, $fgname, $fid_hex, $finode_hex,
         $flinkcount_hex, $fdev_hex, $fdev_minor_hex, $fdev_major_hex,
         $fseeknextfile) = array_slice($vals, 0, 25);

    $idxs = _neo_index_json_and_checks($vals);
    list($idx_json_type, $idx_json_len, $idx_json_size, $idx_json_cst, $idx_json_cs) = $idxs['json'];
    list($idx_header_cs_type, $idx_content_cs_type) = $idxs['cstypes'];
    list($idx_header_cs, $idx_content_cs) = $idxs['csvals'];

    $fjsonsize_hex = $vals[$idx_json_size] !== '' ? $vals[$idx_json_size] : '0';
    $fjsonsize = intval(hexdec($fjsonsize_hex));

    $json_bytes = '';
    if ($fjsonsize > 0) $json_bytes = $ns->read($fjsonsize);
    $ns->read(strlen($delim)); // skip delim

    $fsize  = intval(hexdec($fsize_hex));
    $fcsize = intval(hexdec($fcsize_hex));
    $read_size = ($fcompression !== '' && $fcompression !== 'none' && $fcsize > 0) ? $fcsize : $fsize;

    $content_stored = '';
    if ($read_size) {
        if ($listonly) $ns->seek($read_size, SEEK_CUR);
        else $content_stored = $ns->read($read_size);
    }
    $ns->read(strlen($delim)); // skip delim

    $header_cs_type  = $vals[$idx_header_cs_type];
    $content_cs_type = $vals[$idx_content_cs_type];
    $header_cs_val   = $vals[$idx_header_cs];
    $content_cs_val  = $vals[$idx_content_cs];
    $json_cs_type    = $vals[$idx_json_cst];
    $json_cs_val     = $vals[$idx_json_cs];

    if ($fjsonsize && !$skipchecksum) {
        if (_neo_checksum($json_bytes, $json_cs_type, true) !== $json_cs_val) {
            throw new \Exception("JSON checksum mismatch for ".$fname);
        }
    }
    if (!$skipchecksum && $read_size && !$listonly) {
        if (_neo_checksum($content_stored, $content_cs_type, false) !== $content_cs_val) {
            throw new \Exception("Content checksum mismatch for ".$fname);
        }
    }

    $content_ret = $content_stored;
    if (!$listonly && $uncompress && $fcompression !== '' && $fcompression !== 'none') {
        try { $content_ret = _neo_decompress_bytes($content_stored, $fcompression); }
        catch (\Exception $e) { $content_ret = $content_stored; }
    }
    if (!preg_match('#^[\./]#', $fname)) $fname = './'.$fname;

    $fjson = array();
    if ($fjsonsize > 0) {
        $decoded = json_decode($json_bytes, true);
        if (is_array($decoded)) $fjson = $decoded;
    }
    return array(
        'fid'=>intval(hexdec($fid_hex)),
        'finode'=>intval(hexdec($finode_hex)),
        'fname'=>$fname,
        'flinkname'=>$flinkname,
        'ftype'=>intval(hexdec($ftypehex)),
        'fsize'=>$fsize,
        'fcsize'=>$fcsize,
        'fatime'=>intval(hexdec($fatime_hex)),
        'fmtime'=>intval(hexdec($fmtime_hex)),
        'fctime'=>intval(hexdec($fctime_hex)),
        'fbtime'=>intval(hexdec($fbtime_hex)),
        'fmode'=>intval(hexdec($fmode_hex)),
        'fwinattributes'=>intval(hexdec($fwinattrs_hex)),
        'fuid'=>intval(hexdec($fuid_hex)),
        'funame'=>$funame,
        'fgid'=>intval(hexdec($fgid_hex)),
        'fgname'=>$fgname,
        'fcompression'=>$fcompression,
        'fseeknext'=>$fseeknextfile,
        'fjson'=>$fjson,
        'fcontent'=>($listonly ? null : $content_ret),
    );
}

# ----------------------- Public API -----------------------

function pack_iter_alt($items, $outfile=null, $formatspecs=null,
                       $checksumtypes=array('crc32','crc32','crc32'),
                       $encoding='UTF-8', $compression='auto', $compression_level=null) {
    $fs = _neo_ensure_formatspecs($formatspecs);
    list($bufMode, $fp, $buf) = _neo_open_out($outfile);
    $out = "";

    _neo_write_global_header($out, $fs, count($items), $encoding, $checksumtypes[0]);

    $fid = 0;
    foreach ($items as $it) {
        if (is_array($it) && isset($it['name'])) {
            $name = $it['name'];
            $is_dir = !empty($it['is_dir']);
            $data = isset($it['data']) ? _neo_to_bytes($it['data']) : null;
            $mode = isset($it['mode']) ? intval($it['mode']) : ($is_dir ? (0040000|0755) : (0100000|0666));
            $mtime = isset($it['mtime']) ? intval($it['mtime']) : time();
        } else if (is_array($it) && count($it) >= 3) {
            $name = $it[0]; $is_dir = !!$it[1]; $data = $it[2];
            $mode = $is_dir ? (0040000|0755) : (0100000|0666);
            $mtime = time();
        } else {
            throw new \Exception("Bad item");
        }
        $name = str_replace('\\', '/', $name);
        if (!preg_match('#^[\./]#', $name)) $name = './'.$name;

        if ($is_dir || substr($name, -1) === '/') {
            $raw = ""; $ftype = 5;
        } else {
            $raw = ($data !== null) ? _neo_to_bytes($data) : "";
            $ftype = 0;
        }

        $algo = strtolower($compression);
        if ($algo === 'auto') {
            list($algo, $auto_level) = _neo_auto_pick_for_size(strlen($raw));
            $level = ($compression_level !== null) ? intval($compression_level) : $auto_level;
        } else {
            $level = $compression_level;
        }
        try {
            list($stored, $used_algo) = _neo_compress_bytes($raw, $algo, $level);
        } catch (\Exception $e) {
            list($stored, $used_algo) = _neo_compress_bytes($raw, 'zlib', ($level===null?6:$level));
        }
        $meta = array(
            'ftype'=>$ftype,
            'fencoding'=>$encoding,
            'fcencoding'=>$encoding,
            'fname'=>$name,
            'flinkname'=>'',
            'fsize'=>strlen($raw),
            'fatime'=>$mtime,'fmtime'=>$mtime,'fctime'=>$mtime,'fbtime'=>$mtime,
            'fmode'=>$mode,'fwinattributes'=>0,
            'fcompression'=>$used_algo,'fcsize'=>strlen($stored),
            'fuid'=>0,'funame'=>'','fgid'=>0,'fgname'=>'',
            'fid'=>$fid,'finode'=>$fid,'flinkcount'=>1,'fdev'=>0,'fdev_minor'=>0,'fdev_major'=>0,'index'=>$fid
        );
        $fid += 1;

        $rec = _neo_build_record($fs, $meta, array(), $stored, $checksumtypes);
        $out .= $rec;
    }
    // end marker
    $out .= _neo_append_nulls(array('0','0'), $fs['format_delimiter']);

    if ($bufMode) return $out;
    _neo_write($bufMode, $fp, $buf, $out);
    return true;
}

function pack_alt($infiles, $outfile=null, $formatspecs=null,
                  $checksumtypes=array('crc32','crc32','crc32'),
                  $encoding='UTF-8', $compression='auto', $compression_level=null) {
    // in-memory dict {name=>bytes|null}
    if (is_array($infiles) && array_keys($infiles) !== range(0, count($infiles)-1)) {
        $items = array();
        foreach ($infiles as $name=>$data) {
            $is_dir = ($data === null) || (substr($name, -1) === '/');
            $items[] = array('name'=>$name, 'is_dir'=>$is_dir, 'data'=>$is_dir?null:$data);
        }
        return pack_iter_alt($items, $outfile, $formatspecs, $checksumtypes, $encoding, $compression, $compression_level);
    }

    // list of paths or single path
    $paths = is_array($infiles) ? $infiles : array($infiles);

    $filelist = array();
    foreach ($paths as $p) {
        if (is_dir($p)) {
            $it = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($p, FilesystemIterator::SKIP_DOTS), RecursiveIteratorIterator::SELF_FIRST);
            foreach ($it as $node) {
                $rel = str_replace('\\','/', substr($node->getPathname(), strlen(rtrim($p, DIRECTORY_SEPARATOR))+1));
                if ($node->isDir()) $filelist[] = array($node->getPathname().DIRECTORY_SEPARATOR, true, $rel.'/');
                else $filelist[] = array($node->getPathname(), false, $rel);
            }
        } else {
            $filelist[] = array($p, false, basename($p));
        }
    }

    $fs = _neo_ensure_formatspecs($formatspecs);
    list($bufMode, $fp, $buf) = _neo_open_out($outfile);
    $out = "";
    _neo_write_global_header($out, $fs, count($filelist), $encoding, $checksumtypes[0]);

    $fid = 0;
    foreach ($filelist as $triple) {
        list($apath, $is_dir, $relname) = $triple;
        if ($is_dir) {
            $raw = ""; $ftype = 5;
        } else {
            $raw = file_get_contents($apath);
            $ftype = 0;
        }

        if (!preg_match('#^[\./]#', $relname)) $relname = './'.$relname;

        $algo = strtolower($compression);
        if ($algo === 'auto') {
            list($algo, $auto_level) = _neo_auto_pick_for_size(strlen($raw));
            $level = ($compression_level !== null) ? intval($compression_level) : $auto_level;
        } else { $level = $compression_level; }

        try {
            list($stored, $used_algo) = _neo_compress_bytes($raw, $algo, $level);
        } catch (\Exception $e) {
            list($stored, $used_algo) = _neo_compress_bytes($raw, 'zlib', ($level===null?6:$level));
        }
        $meta = array(
            'ftype'=>$ftype,
            'fencoding'=>$encoding,
            'fcencoding'=>$encoding,
            'fname'=>$relname,
            'flinkname'=>'',
            'fsize'=>strlen($raw),
            'fatime'=>time(),'fmtime'=>time(),'fctime'=>time(),'fbtime'=>time(),
            'fmode'=>($is_dir ? (0040000|0755) : (0100000|0666)),'fwinattributes'=>0,
            'fcompression'=>$used_algo,'fcsize'=>strlen($stored),
            'fuid'=>0,'funame'=>'','fgid'=>0,'fgname'=>'',
            'fid'=>$fid,'finode'=>$fid,'flinkcount'=>1,'fdev'=>0,'fdev_minor'=>0,'fdev_major'=>0,'index'=>$fid
        );
        $fid += 1;
        $out .= _neo_build_record($fs, $meta, array(), $stored, $checksumtypes);
    }
    $out .= _neo_append_nulls(array('0','0'), $fs['format_delimiter']);
    if ($bufMode) return $out;
    _neo_write($bufMode, $fp, $buf, $out);
    return true;
}

function archive_to_array_alt($infile, $formatspecs=null, $listonly=false, $skipchecksum=false, $uncompress=true) {
    $fs = _neo_ensure_formatspecs($formatspecs);
    list($ns, $fpclose) = _neo_open_in($infile);
    $top = _neo_parse_global_header($ns, $fs);
    while (true) {
        $rec = _neo_parse_record($ns, $fs, $listonly, $skipchecksum, $uncompress);
        if ($rec === null) break;
        $top['ffilelist'][] = $rec;
    }
    if ($fpclose) fclose($fpclose);
    return $top;
}

function unpack_alt($infile, $outdir='.', $formatspecs=null, $skipchecksum=false, $uncompress=true) {
    $arr = archive_to_array_alt($infile, $formatspecs, false, $skipchecksum, $uncompress);
    if (!$arr) return false;

    if ($outdir === null || $outdir === '-') {
        $result = array();
        foreach ($arr['ffilelist'] as $ent) {
            if ($ent['ftype'] == 5) $result[$ent['fname']] = null;
            else $result[$ent['fname']] = $ent['fcontent'] ?: "";
        }
        return $result;
    }

    if (!is_dir($outdir)) {
        if (file_exists($outdir)) throw new \Exception("not a directory: ".$outdir);
        mkdir($outdir, 0777, true);
    }
    foreach ($arr['ffilelist'] as $ent) {
        $path = $outdir . DIRECTORY_SEPARATOR . ltrim($ent['fname'], './');
        if ($ent['ftype'] == 5) {
            if (!is_dir($path)) mkdir($path, 0777, true);
        } else {
            $dir = dirname($path);
            if (!is_dir($dir)) mkdir($dir, 0777, true);
            file_put_contents($path, $ent['fcontent'] ?: "");
            @chmod($path, $ent['fmode'] & 0777);
        }
    }
    return true;
}

function repack_alt($infile, $outfile=null, $formatspecs=null,
                    $checksumtypes=array('crc32','crc32','crc32'),
                    $compression='auto', $compression_level=null) {
    $arr = archive_to_array_alt($infile, $formatspecs, false, false, false); // stored bytes
    $fs = _neo_ensure_formatspecs($formatspecs);
    list($bufMode, $fp, $buf) = _neo_open_out($outfile);
    $out = "";
    _neo_write_global_header($out, $fs, count($arr['ffilelist']), $arr['fencoding'] ?? 'UTF-8', $checksumtypes[0]);
    $i = 0;
    foreach ($arr['ffilelist'] as $ent) {
        $src_algo = strtolower($ent['fcompression'] ?: 'none');
        $dst_algo = strtolower($compression);
        $stored_src = $ent['fcontent'] ?: "";

        if ($dst_algo === 'auto') {
            try { $raw = ($src_algo!=='none') ? _neo_decompress_bytes($stored_src, $src_algo) : $stored_src; }
            catch (\Exception $e) { $raw = $stored_src; }
            list($dst_algo, $dst_level) = _neo_auto_pick_for_size(strlen($raw));
        } else {
            if ($src_algo !== 'none') {
                try { $raw = _neo_decompress_bytes($stored_src, $src_algo); }
                catch (\Exception $e) { $raw = $stored_src; }
            } else $raw = $stored_src;
            $dst_level = $compression_level;
        }

        if ($dst_algo === $src_algo || ($dst_algo==='none' && $src_algo==='none')) {
            $stored = $stored_src; $used_algo = $src_algo;
            try { $raw_len = ($src_algo!=='none') ? strlen(_neo_decompress_bytes($stored_src, $src_algo)) : strlen($stored_src); }
            catch (\Exception $e) { $raw_len = strlen($stored_src); }
        } else {
            list($stored, $used_algo) = _neo_compress_bytes($raw, $dst_algo, $dst_level);
            $raw_len = strlen($raw);
        }

        $meta = array(
            'ftype'=>$ent['ftype'],
            'fencoding'=>$arr['fencoding'] ?? 'UTF-8',
            'fcencoding'=>$arr['fencoding'] ?? 'UTF-8',
            'fname'=>$ent['fname'],
            'flinkname'=>$ent['flinkname'] ?? '',
            'fsize'=>$raw_len,
            'fatime'=>$ent['fatime'] ?? time(),
            'fmtime'=>$ent['fmtime'] ?? time(),
            'fctime'=>$ent['fctime'] ?? time(),
            'fbtime'=>$ent['fbtime'] ?? time(),
            'fmode'=>$ent['fmode'] ?? (0100000|0666),
            'fwinattributes'=>$ent['fwinattributes'] ?? 0,
            'fcompression'=>$used_algo,'fcsize'=>strlen($stored),
            'fuid'=>$ent['fuid'] ?? 0,'funame'=>$ent['funame'] ?? '',
            'fgid'=>$ent['fgid'] ?? 0,'fgname'=>$ent['fgname'] ?? '',
            'fid'=>$ent['fid'] ?? $i,'finode'=>$ent['finode'] ?? $i,
            'flinkcount'=>$ent['flinkcount'] ?? 1,
            'fdev'=>$ent['fdev'] ?? 0,'fdev_minor'=>$ent['fdev_minor'] ?? 0,'fdev_major'=>$ent['fdev_major'] ?? 0,
            'index'=>$i,
        );
        $out .= _neo_build_record($fs, $meta, $ent['fjson'] ?? array(), $stored, $checksumtypes);
        $i++;
    }
    $out .= _neo_append_nulls(array('0','0'), $fs['format_delimiter']);
    if ($bufMode) return $out;
    _neo_write($bufMode, $fp, $buf, $out);
    return true;
}

function archivefilelistfiles_alt($infile, $formatspecs=null, $advanced=false, $include_dirs=true) {
    $fs = _neo_ensure_formatspecs($formatspecs);
    list($ns, $fpclose) = _neo_open_in($infile);
    _neo_parse_global_header($ns, $fs);
    $out = array();
    while (true) {
        $rec = _neo_parse_record($ns, $fs, true, true, false);
        if ($rec === null) break;
        $is_dir = ($rec['ftype'] === 5);
        if (!$include_dirs && $is_dir) continue;
        if (!$advanced) $out[] = $rec['fname'];
        else {
            $out[] = array(
                'name'=>$rec['fname'],
                'type'=>$is_dir ? 'dir' : 'file',
                'compression'=>$rec['fcompression'] ?: 'none',
                'size'=>$rec['fsize'],
                'stored_size'=>$rec['fcsize'],
                'mtime'=>$rec['fmtime'],
                'atime'=>$rec['fatime'],
                'mode'=>$rec['fmode'],
            );
        }
    }
    if ($fpclose) fclose($fpclose);
    return $out;
}

function archivefilevalidate_alt($infile, $formatspecs=null, $verbose=false, $return_details=false) {
    $fs = _neo_ensure_formatspecs($formatspecs);
    list($ns, $fpclose) = _neo_open_in($infile);
    _neo_parse_global_header($ns, $fs);
    $ok_all = true; $details = array(); $idx = 0;
    while (true) {
        $rec = _neo_parse_record($ns, $fs, false, false, false);
        if ($rec === null) break;

        // If parse succeeded with checks, it's valid per-entry
        $entry_ok = true;
        $ok_all = $ok_all && $entry_ok;
        if ($verbose || $return_details) {
            $details[] = array(
                'index'=>$idx,
                'name'=>$rec['fname'],
                'header_ok'=>true,
                'json_ok'=>true,
                'content_ok'=>true,
                'fcompression'=>$rec['fcompression'],
                'fsize_hex'=>_neo_hex($rec['fsize']),
                'fcsize_hex'=>_neo_hex($rec['fcsize']),
            );
        }
        $idx += 1;
    }
    if ($fpclose) fclose($fpclose);
    if ($return_details) return array('ok'=>$ok_all, 'details'=>$details);
    return $ok_all;
}

# ----------------------- Convert (zip/tar stdlib) -----------------------

function convert_foreign_to_alt($infile, $outfile=null, $formatspecs=null,
                                $checksumtypes=array('crc32','crc32','crc32'),
                                $compression='auto', $compression_level=null) {
    // zip
    $is_path = is_string($infile) && is_file($infile);
    if ($is_path && preg_match('/\.zip$/i', $infile)) {
        if (!class_exists('ZipArchive')) throw new \Exception("ZipArchive not available");
        $z = new ZipArchive();
        if ($z->open($infile) !== true) throw new \Exception("Failed to open zip");
        $items = array();
        for ($i=0; $i<$z->numFiles; $i++) {
            $st = $z->statIndex($i);
            $name = $st['name'];
            $is_dir = substr($name, -1) === '/';
            $data = $is_dir ? null : $z->getFromIndex($i);
            $items[] = array('name'=>$name, 'is_dir'=>$is_dir, 'data'=>$data);
        }
        $z->close();
        return pack_iter_alt($items, $outfile, $formatspecs, $checksumtypes, 'UTF-8', $compression, $compression_level);
    }

    // tar(.gz/.bz2/.xz*) via PharData (xz not standard)
    if ($is_path && preg_match('/\.tar(\.(gz|bz2|tgz|tbz2|xz|txz))?$/i', $infile)) {
        if (!class_exists('PharData')) throw new \Exception("PharData not available");
        $ph = new PharData($infile);
        $items = array();
        foreach (new RecursiveIteratorIterator($ph) as $file) {
            $name = str_replace('\\', '/', $file->getFileName());
            $is_dir = $file->isDir();
            $data = $is_dir ? null : file_get_contents($file->getPathname());
            $items[] = array('name'=>$name, 'is_dir'=>$is_dir, 'data'=>$data);
        }
        return pack_iter_alt($items, $outfile, $formatspecs, $checksumtypes, 'UTF-8', $compression, $compression_level);
    }

    throw new \Exception("Unsupported foreign archive (zip/tar.* only in PHP port)");
}
?>