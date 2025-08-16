<?php
/**
 * PyNeoFile PHP - alt parser (core subset)
 * - archive_to_array_neo($infile, $formatspecs=null, $listonly=false, $skipchecksum=false, $uncompress=true, $skipjson=false)
 * - archivefilelistfiles_neo($infile, $formatspecs=null, $advanced=false, $include_dirs=true, $skipjson=true)
 * - archivefilevalidate_neo($infile, $formatspecs=null, $verbose=false, $return_details=false, $skipjson=false)
 * - unpack_neo($infile, $outdir='.', $formatspecs=null, $skipchecksum=false, $uncompress=true, $skipjson=false)
 *
 * Notes:
 * - Focused on reading/listing/validating/unpacking. (Packing/repacking can be added later.)
 * - Compression: supports 'none','zlib','gzip','bz2'. 'lzma' not supported in stock PHP (skips or throws).
 * - Checksums: 'none','crc32','md5','sha1','sha224','sha256','sha384','sha512' via hash().
 */

// -------------- Formatspecs & helpers --------------

function _neo_b($s) { return (string)$s; }

function _neo_default_formatspecs() {
    return array(
        'format_magic' => 'NeoFile',
        'format_ver' => '001',
        'format_delimiter' => "\x00",
        'new_style' => true,
    );
}

function _neo_decode_escape($s) {
    // Handle \xHH sequences
    return preg_replace_callback('/\\\\x([0-9A-Fa-f]{2})/', function($m){
        return chr(hexdec($m[1]));
    }, (string)$s);
}

function _neo_load_ini_formatspecs($paths=null) {
    $cands = array();
    if ($paths) {
        if (is_array($paths)) { $cands = array_merge($cands, $paths); }
        else { $cands[] = $paths; }
    }
    foreach (array('PYNEOFILE_INI','PYARCHIVE_INI') as $env) {
        $p = getenv($env);
        if ($p) $cands[] = $p;
    }
    foreach (array('neofile.ini') as $p) {
        $cands[] = $p;
    }
    $picked = null;
    foreach ($cands as $p) {
        if ($p && is_file($p)) { $picked = $p; break; }
    }
    if (!$picked) return null;
    $ini = parse_ini_file($picked, true, INI_SCANNER_RAW);
    if (!$ini) return null;
    $sec = null;
    if (isset($ini['config']['default']) && isset($ini[$ini['config']['default']])) {
        $sec = $ini['config']['default'];
    } else {
        foreach ($ini as $k=>$v) { if (strtolower($k) !== 'config') { $sec = $k; break; } }
    }
    if (!$sec || !isset($ini[$sec])) return null;
    $S = $ini[$sec];
    $magic = isset($S['magic']) ? $S['magic'] : 'ArchiveFile';
    $ver   = isset($S['ver']) ? $S['ver'] : '001';
    $delim = isset($S['delimiter']) ? _neo_decode_escape($S['delimiter']) : "\x00";
    $newst = isset($S['newstyle']) ? strtolower($S['newstyle']) : 'true';
    return array(
        'format_magic' => $magic,
        'format_ver' => preg_replace('/\D+/', '', $ver) ?: '001',
        'format_delimiter' => $delim,
        'new_style' => in_array($newst, array('true','1','yes')),
        'format_name' => $sec
    );
}

function _neo_formatspecs($specs=null) {
    if (is_array($specs)) {
        return array_merge(_neo_default_formatspecs(), $specs);
    }
    $env = _neo_load_ini_formatspecs(null);
    return $env ? $env : _neo_default_formatspecs();
}

// -------------- Stream --------------

class _NeoStream {
    private $fp = null;
    private $buf = "";
    private $pos = 0;
    private $len = 0;
    private $from_file = false;

    function __construct($src) {
        if (is_resource($src)) {
            $meta = stream_get_meta_data($src);
            if (isset($meta['mode']) && strpos($meta['mode'], 'r') !== false) {
                $this->fp = $src;
                $this->from_file = true;
                return;
            }
        }
        if (is_string($src) && file_exists($src)) {
            $this->fp = fopen($src, 'rb');
            if (!$this->fp) throw new \Exception("Cannot open file: $src");
            $this->from_file = true;
            return;
        }
        // treat as bytes string
        $this->buf = (string)$src;
        $this->len = strlen($this->buf);
        $this->pos = 0;
        $this->from_file = false;
    }

    function read($n) {
        if ($this->from_file) {
            if ($n <= 0) return "";
            $s = fread($this->fp, $n);
            return ($s === false) ? "" : $s;
        } else {
            if ($n <= 0) return "";
            $remain = $this->len - $this->pos;
            if ($remain <= 0) return "";
            $to = ($n > $remain) ? $remain : $n;
            $out = substr($this->buf, $this->pos, $to);
            $this->pos += $to;
            return $out;
        }
    }

    function seek($offset, $whence=SEEK_SET) {
        if ($this->from_file) {
            fseek($this->fp, $offset, $whence);
            return;
        } else {
            if ($whence === SEEK_CUR) $this->pos += $offset;
            elseif ($whence === SEEK_END) $this->pos = $this->len + $offset;
            else $this->pos = $offset;
            if ($this->pos < 0) $this->pos = 0;
            if ($this->pos > $this->len) $this->pos = $this->len;
        }
    }

    function tell() {
        if ($this->from_file) return ftell($this->fp);
        return $this->pos;
    }

    function close() {
        if ($this->from_file && $this->fp) { fclose($this->fp); $this->fp = null; }
    }
}

// -------------- Low-level read --------------

function _neo_read_cstring($ns, $delim) {
    // Chunked scan; leaves stream after delimiter; returns string w/o delimiter.
    $d = (string)$delim;
    $dl = strlen($d);
    // Fast path: single-byte delimiter
    if ($dl === 1) {
        $out = "";
        while (true) {
            $chunk = $ns->read(4096);
            if ($chunk === "" || $chunk === false) return $out;
            $idx = strpos($chunk, $d);
            if ($idx !== false) {
                $out .= substr($chunk, 0, $idx);
                $tail = substr($chunk, $idx + 1);
                if ($tail !== "") $ns->seek(-strlen($tail), SEEK_CUR);
                return $out;
            }
            $out .= $chunk;
        }
    }
    // General path
    $buf = "";
    while (true) {
        $chunk = $ns->read(4096);
        if ($chunk === "" || $chunk === false) return $buf;
        $buf .= $chunk;
        $i = strpos($buf, $d);
        if ($i !== false) {
            $out = substr($buf, 0, $i);
            $tail_len = strlen($buf) - ($i + $dl);
            if ($tail_len > 0) $ns->seek(-$tail_len, SEEK_CUR);
            return $out;
        }
        $keep = $dl - 1;
        if ($keep > 0 && strlen($buf) >= $keep) {
            $tail = substr($buf, -$keep);
            $ns->seek(-$keep, SEEK_CUR);
            $buf = $tail;
        }
    }
}

function _neo_read_fields($ns, $n, $delim) {
    $out = array();
    for ($i=0; $i<(int)$n; $i++) {
        $out[] = _neo_read_cstring($ns, $delim);
    }
    return $out;
}

// -------------- Checksums & compression --------------

function _neo_checksum($data, $type, $text=false) {
    $t = strtolower((string)$type);
    if ($t === '' || $t === 'none') return '0';
    if ($t === 'crc32') {
        $v = crc32($data);
        if ($v < 0) $v += 4294967296;
        return sprintf("%08x", $v);
    }
    // hash()-based
    $ok = in_array($t, array('md5','sha1','sha224','sha256','sha384','sha512'), true);
    if ($ok) return hash($t, $data);
    throw new \Exception("Unsupported checksum: $type");
}

function _neo_decompress($data, $algo) {
    $a = strtolower((string)$algo);
    if ($a === '' || $a === 'none') return $data;
    if ($a === 'zlib') {
        $res = @gzuncompress($data);
        if ($res === false) throw new \Exception("zlib decompress failed");
        return $res;
    }
    if ($a === 'gzip') {
        if (function_exists('gzdecode')) {
            $res = @gzdecode($data);
            if ($res === false) throw new \Exception("gzip decompress failed");
            return $res;
        }
        // poor man's fallback: try zlib
        $res = @gzuncompress($data);
        if ($res === false) throw new \Exception("gzip decompress not available");
        return $res;
    }
    if ($a === 'bz2') {
        if (!function_exists('bzdecompress')) throw new \Exception("bz2 extension not available");
        $res = @bzdecompress($data);
        if ($res === false || is_int($res)) throw new \Exception("bz2 decompress failed");
        return $res;
    }
    if ($a === 'lzma') {
        throw new \Exception("lzma not available in PHP");
    }
    throw new \Exception("Unknown compression: $algo");
}

// -------------- Global header --------------

function _neo_parse_global_header($ns, $fs) {
    $d = $fs['format_delimiter'];
    $magicver = _neo_read_cstring($ns, $d);
    $headersize = _neo_read_cstring($ns, $d);
    $tmpout = _neo_read_cstring($ns, $d);
    $fencoding = _neo_read_cstring($ns, $d);
    $fostype = _neo_read_cstring($ns, $d);
    $fnumfiles = hexdec(_neo_read_cstring($ns, $d) ?: '0');
    $extras_size = hexdec(_neo_read_cstring($ns, $d) ?: '0');
    $extrafields = hexdec(_neo_read_cstring($ns, $d) ?: '0');
    $extras = array();
    for ($i=0; $i<$extrafields; $i++) { $extras[] = _neo_read_cstring($ns, $d); }
    $checksumtype = _neo_read_cstring($ns, $d);
    $header_cs = _neo_read_cstring($ns, $d); // ignored
    return array(
        'fencoding' => $fencoding ?: 'UTF-8',
        'fnumfiles' => (int)$fnumfiles,
        'fostype' => $fostype,
        'fextradata' => $extras,
        'fchecksumtype' => $checksumtype,
        'ffilelist' => array(),
        'fformatspecs' => $fs,
    );
}

function _neo_index_json_and_checks($vals) {
    $n = count($vals);
    if ($n < 25) throw new \Exception("Record too short: got $n");
    $idx = 25;
    $fjsontype = $vals[$idx];
    $v2 = isset($vals[$idx+1]) ? $vals[$idx+1] : '';
    $v3 = isset($vals[$idx+2]) ? $vals[$idx+2] : '';
    $v4 = isset($vals[$idx+3]) ? $vals[$idx+3] : '';
    $ishex = function($s) { return $s !== '' && preg_match('/^[0-9A-Fa-f]+$/', $s); };
    $csnames = array('none','crc32','md5','sha1','sha224','sha256','sha384','sha512','blake2b','blake2s');
    if ($ishex($v2) && $ishex($v3) && in_array(strtolower($v4), $csnames, true)) {
        // 5-field JSON header
        return array(25, 26, 27, 28, 29, 30);
    } else {
        // 4-field JSON header (no jsonlen)
        return array(25, null, 26, 27, 28, 29);
    }
    // returns: [idx_json_type, idx_json_len, idx_json_size, idx_json_cst, idx_json_cs, idx_extras_size]
}

function _neo_parse_record($ns, $fs, $listonly=false, $skipchecksum=false, $uncompress=true, $skipjson=false) {
    $d = $fs['format_delimiter'];
    $first = _neo_read_cstring($ns, $d);
    if ($first === '0') {
        $second = _neo_read_cstring($ns, $d);
        if ($second === '0') return null; // end
        $headersize_hex = $first;
        $fields_len_hex = $second;
    } else {
        $headersize_hex = $first;
        $fields_len_hex = _neo_read_cstring($ns, $d);
    }
    $n_fields = hexdec($fields_len_hex ?: '0');
    $vals = _neo_read_fields($ns, $n_fields, $d);
    if (count($vals) < 25) throw new \Exception("Record too short: expected >=25 header fields, got ".count($vals));

    list($idx_json_type, $idx_json_len, $idx_json_size, $idx_json_cst, $idx_json_cs, $idx_extras_size) = _neo_index_json_and_checks($vals);

    list($ftypehex, $fencoding, $fcencoding, $fname, $flinkname,
         $fsize_hex, $fatime_hex, $fmtime_hex, $fctime_hex, $fbtime_hex,
         $fmode_hex, $fwinattrs_hex, $fcompression, $fcsize_hex,
         $fuid_hex, $funame, $fgid_hex, $fgname, $fid_hex, $finode_hex,
         $flinkcount_hex, $fdev_hex, $fdev_minor_hex, $fdev_major_hex,
         $fseeknextfile) = array_slice($vals, 0, 25);

    $fjsonsize_hex = $vals[$idx_json_size] !== '' ? $vals[$idx_json_size] : '0';
    $fjsonsize = hexdec($fjsonsize_hex);

    // JSON payload
    $json_bytes = '';
    if ($fjsonsize > 0) {
        if ($listonly || $skipjson) {
            $ns->seek($fjsonsize + strlen($d), SEEK_CUR);
        } else {
            $json_bytes = $ns->read($fjsonsize);
            $ns->read(strlen($d)); // delimiter
        }
    } else {
        $ns->read(strlen($d)); // empty JSON delimiter
    }

    // Content
    $fsize  = hexdec($fsize_hex !== '' ? $fsize_hex : '0');
    $fcsize = hexdec($fcsize_hex !== '' ? $fcsize_hex : '0');
    $cmp = strtolower($fcompression);
    $stored_len = ($cmp !== '' && $cmp !== 'none' && $fcsize > 0) ? $fcsize : $fsize;
    $content_stored = '';
    if ($stored_len) {
        if ($listonly) {
            $ns->seek($stored_len, SEEK_CUR);
        } else {
            $content_stored = $ns->read($stored_len);
        }
    }
    $ns->read(strlen($d)); // trailing delimiter after content

    // Checksums
    $header_cs_type  = isset($vals[$idx_extras_size + 1]) ? $vals[$idx_extras_size + 1] : 'none';
    $content_cs_type = isset($vals[$idx_extras_size + 2]) ? $vals[$idx_extras_size + 2] : 'none';
    $header_cs_val   = isset($vals[$idx_extras_size + 3]) ? $vals[$idx_extras_size + 3] : '0';
    $content_cs_val  = isset($vals[$idx_extras_size + 4]) ? $vals[$idx_extras_size + 4] : '0';
    $json_cs_type    = isset($vals[$idx_json_cst]) ? $vals[$idx_json_cst] : 'none';
    $json_cs_val     = isset($vals[$idx_json_cs]) ? $vals[$idx_json_cs] : '0';

    if ($fjsonsize && !$skipchecksum && !($listonly || $skipjson)) {
        $calc = _neo_checksum($json_bytes, $json_cs_type, true);
        if (strtolower($calc) !== strtolower($json_cs_val)) {
            throw new \Exception("JSON checksum mismatch for " . $fname);
        }
    }
    if (!$skipchecksum && $stored_len && !$listonly) {
        $calc = _neo_checksum($content_stored, $content_cs_type, false);
        if (strtolower($calc) !== strtolower($content_cs_val)) {
            throw new \Exception("Content checksum mismatch for " . $fname);
        }
    }

    // Optional decompression
    $content_ret = $content_stored;
    if (!$listonly && $uncompress && $stored_len && $cmp !== '' && $cmp !== 'none') {
        try {
            $content_ret = _neo_decompress($content_stored, $cmp);
        } catch (\Exception $e) {
            $content_ret = $content_stored;
        }
    }

    $name = (string)$fname;
    if (!(strpos($name, './') === 0 || strpos($name, '/') === 0)) {
        $name = './' . $name;
    }

    $to_int = function($hex) { $hex = ($hex === '' ? '0' : $hex); return hexdec($hex); };
    return array(
        'fid' => $to_int($fid_hex),
        'finode' => $to_int($finode_hex),
        'fname' => $name,
        'flinkname' => (string)$flinkname,
        'ftype' => $to_int($ftypehex),
        'fsize' => $to_int($fsize_hex),
        'fcsize' => $to_int($fcsize_hex),
        'fatime' => $to_int($fatime_hex),
        'fmtime' => $to_int($fmtime_hex),
        'fctime' => $to_int($fctime_hex),
        'fbtime' => $to_int($fbtime_hex),
        'fmode' => $to_int($fmode_hex),
        'fwinattributes' => $to_int($fwinattrs_hex),
        'fuid' => $to_int($fuid_hex),
        'funame' => (string)$funame,
        'fgid' => $to_int($fgid_hex),
        'fgname' => (string)$fgname,
        'fcompression' => $cmp ?: 'none',
        'fseeknext' => (string)$fseeknextfile,
        'fjson' => ($json_bytes !== '' ? json_decode($json_bytes, true) : array()),
        'fcontent' => ($listonly ? null : $content_ret),
    );
}

// -------------- Public API --------------

function archive_to_array_neo($infile, $formatspecs=null, $listonly=false, $skipchecksum=false, $uncompress=true, $skipjson=false) {
    $fs = _neo_formatspecs($formatspecs);
    $ns = new _NeoStream($infile);
    try {
        $top = _neo_parse_global_header($ns, $fs);
        while (true) {
            $rec = _neo_parse_record($ns, $fs, $listonly, $skipchecksum, $uncompress, $skipjson);
            if ($rec === null) break;
            $top['ffilelist'][] = $rec;
        }
        return $top;
    } finally {
        $ns->close();
    }
}

function archivefilelistfiles_neo($infile, $formatspecs=null, $advanced=false, $include_dirs=true, $skipjson=true) {
    $fs = _neo_formatspecs($formatspecs);
    $ns = new _NeoStream($infile);
    $out = array();
    try {
        $top = _neo_parse_global_header($ns, $fs);
        while (true) {
            $rec = _neo_parse_record($ns, $fs, true, true, false, $skipjson);
            if ($rec === null) break;
            $is_dir = ($rec['ftype'] == 5);
            if (!$advanced) {
                if ($is_dir && !$include_dirs) continue;
                $out[] = $rec['fname'];
            } else {
                $out[] = array(
                    'name' => $rec['fname'],
                    'type' => $is_dir ? 'dir' : 'file',
                    'compression' => $rec['fcompression'],
                    'size' => $rec['fsize'],
                    'stored_size' => $rec['fcsize'],
                    'mtime' => $rec['fmtime'],
                    'atime' => $rec['fatime'],
                    'mode' => $rec['fmode'],
                );
            }
        }
        return $out;
    } finally {
        $ns->close();
    }
}

function archivefilevalidate_neo($infile, $formatspecs=null, $verbose=false, $return_details=false, $skipjson=false) {
    $arr = archive_to_array_neo($infile, $formatspecs, false, false, false, $skipjson);
    $ok = true;
    $details = array();
    $i = 0;
    foreach ($arr['ffilelist'] as $e) {
        $details[] = array('index'=>$i, 'name'=>$e['fname'], 'header_ok'=>true, 'json_ok'=>true, 'content_ok'=>true);
        $i++;
    }
    if ($return_details) return array($ok, $details);
    return $ok;
}

function unpack_neo($infile, $outdir='.', $formatspecs=null, $skipchecksum=false, $uncompress=true, $skipjson=false) {
    $arr = archive_to_array_neo($infile, $formatspecs, false, $skipchecksum, $uncompress, $skipjson);
    if ($outdir === null || $outdir === '-' ) {
        // return map of name => bytes (directories => null)
        $res = array();
        foreach ($arr['ffilelist'] as $e) {
            if ($e['ftype'] == 5) $res[$e['fname']] = null;
            else $res[$e['fname']] = ($e['fcontent'] !== null ? $e['fcontent'] : '');
        }
        return $res;
    }
    if (!is_dir($outdir)) {
        if (file_exists($outdir)) throw new \Exception("Not a directory: $outdir");
        if (!@mkdir($outdir, 0777, true)) throw new \Exception("Failed to mkdir: $outdir");
    }
    foreach ($arr['ffilelist'] as $e) {
        $name = ltrim($e['fname'], './');
        $path = rtrim($outdir, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . $name;
        if ($e['ftype'] == 5) {
            if (!is_dir($path) && !@mkdir($path, 0777, true)) throw new \Exception("Failed to mkdir: $path");
        } else {
            $dir = dirname($path);
            if (!is_dir($dir) && !@mkdir($dir, 0777, true)) throw new \Exception("Failed to mkdir: $dir");
            $bytes = ($e['fcontent'] !== null ? $e['fcontent'] : '');
            $fh = fopen($path, 'wb');
            if (!$fh) throw new \Exception("Failed to open for write: $path");
            fwrite($fh, $bytes);
            fclose($fh);
        }
    }
    return true;
}
?>