<?php
/**
 * PHPNeoFile (PHP core, _neo API)
 * - archive_to_array_neo, pack_neo, pack_iter_neo, unpack_neo, repack_neo
 * - archivefilelistfiles_neo, archivefilevalidate_neo, convert_foreign_to_neo (zip/tar* if available)
 * - Robust cstring reader with multi-byte delimiter support
 * - Iterator snapshot in pack_iter_neo to avoid exhaustion
 * - Safe octal-style mode defaults
 * - skipjson fast path
 */

if (!defined('PN_BYTE_CHUNK')) define('PN_BYTE_CHUNK', 4096);

function pn_is_php7plus() { return PHP_VERSION_ID >= 70000; }

/* -------- utils: bytes/strings, hex, checksum -------- */

function pn_hex($n) { return dechex((int)$n); }

function pn_crc32_hex($data) {
    if (!is_string($data)) $data = strval($data);
    // crc32 in PHP returns signed int; use sprintf to get unsigned hex
    return sprintf("%08x", crc32($data));
}

function pn_checksum($data, $type, $is_text = false) {
    $t = strtolower($type ?: 'none');
    if ($t === '' || $t === 'none') return '0';
    if (!is_string($data)) $data = strval($data);
    if ($t === 'crc32') return pn_crc32_hex($data);
    // hash() supports md5/sha*; returns hex
    if (in_array($t, array('md5','sha1','sha224','sha256','sha384','sha512'), true)) {
        return hash($t, $data);
    }
    throw new \Exception("Unsupported checksum: $type");
}

/* -------- compression -------- */

function pn_norm_algo($a) {
    $a = strtolower($a ?: 'none');
    if ($a === 'gz') $a = 'gzip';
    if ($a === 'bz' || $a === 'bzip' || $a === 'bzip2') $a = 'bz2';
    if ($a === 'z') $a = 'zlib';
    if ($a === 'xz') $a = 'lzma';
    return $a;
}

function pn_compress_bytes($data, $algo = 'none', $level = null) {
    $algo = pn_norm_algo($algo);
    if (!is_string($data)) $data = strval($data);
    switch ($algo) {
        case 'none':
            return array($data, 'none');
        case 'zlib':
            // gzcompress uses zlib header, matches Python's zlib
            $lvl = ($level === null) ? -1 : (int)$level;
            $out = gzcompress($data, $lvl);
            if ($out === false) throw new \Exception("zlib compress failed");
            return array($out, 'zlib');
        case 'gzip':
            $lvl = ($level === null) ? -1 : (int)$level;
            $out = gzencode($data, $lvl);
            if ($out === false) throw new \Exception("gzip compress failed");
            return array($out, 'gzip');
        case 'bz2':
            if (!function_exists('bzcompress')) throw new \Exception("bz2 not available");
            $lvl = ($level === null) ? 9 : (int)$level;
            $out = bzcompress($data, $lvl);
            if (!is_string($out)) throw new \Exception("bz2 compress failed");
            return array($out, 'bz2');
        case 'lzma':
            throw new \Exception("lzma not available in PHP core");
        default:
            throw new \Exception("Unknown compression: $algo");
    }
}

function pn_decompress_bytes($data, $algo = 'none') {
    $algo = pn_norm_algo($algo);
    if (!is_string($data)) $data = strval($data);
    switch ($algo) {
        case 'none': return $data;
        case 'zlib':
            $out = gzuncompress($data);
            if ($out === false) throw new \Exception("zlib decompress failed");
            return $out;
        case 'gzip':
            $out = function_exists('gzdecode') ? gzdecode($data) : gzinflate(substr($data,10));
            if ($out === false) throw new \Exception("gzip decompress failed");
            return $out;
        case 'bz2':
            if (!function_exists('bzdecompress')) throw new \Exception("bz2 not available");
            $out = bzdecompress($data);
            if (!is_string($out)) throw new \Exception("bz2 decompress failed");
            return $out;
        case 'lzma':
            throw new \Exception("lzma not available in PHP core");
        default:
            throw new \Exception("Unknown compression: $algo");
    }
}

function pn_auto_pick_for_size($n) {
    if ($n < 16384) return array('none', null);
    if ($n >= 262144) return array('bz2', 9);
    return array('zlib', 6);
}

/* -------- formatspecs / INI -------- */

function pn_decode_escape($s) {
    // Convert \xNN sequences and common escapes to actual bytes
    $s = (string)$s;
    $s = preg_replace_callback('/\\\\x([0-9A-Fa-f]{2})/', function($m){ return chr(hexdec($m[1])); }, $s);
    $s = str_replace(array('\\0','\\n','\\r','\\t','\\\\"',"\\'","\\\\"), array("\0","\n","\r","\t",'"',"'","\\"), $s);
    return $s;
}

function pn_default_formatspecs() {
    return array(
        'format_magic' => 'NeoFile',
        'format_ver' => '001',
        'format_delimiter' => "\x00",
        'new_style' => true,
    );
}

function pn_ver_digits($ver) {
    if (!$ver) return '001';
    $digits = preg_replace('/[^0-9]/', '', (string)$ver);
    return $digits !== '' ? $digits : '001';
}

function pn_load_formatspecs_from_ini($paths = null, $prefer_section = null) {
    $cands = array();
    if ($paths) {
        if (is_array($paths)) $cands = array_merge($cands, $paths);
        else $cands[] = $paths;
    }
    foreach (array('PYNEOFILE_INI') as $env) {
        if (!empty($_ENV[$env])) $cands[] = $_ENV[$env];
        elseif (!empty($_SERVER[$env])) $cands[] = $_SERVER[$env];
        elseif ($v = getenv($env)) $cands[] = $v;
    }
    $cands = array_merge($cands, array('neofile.ini'));
    $picked = null;
    foreach ($cands as $p) {
        if ($p && is_file($p)) { $picked = $p; break; }
    }
    if (!$picked) return null;
    $cp = parse_ini_file($picked, true, INI_SCANNER_RAW);
    if (!is_array($cp)) return null;
    if ($prefer_section && isset($cp[$prefer_section])) $sec = $prefer_section;
    elseif (isset($cp['config']['default']) && isset($cp[$cp['config']['default']])) $sec = $cp['config']['default'];
    else {
        $sec = null;
        foreach ($cp as $k => $_) { if (strtolower($k) !== 'config') { $sec = $k; break; } }
    }
    if (!$sec) return null;
    $magic = isset($cp[$sec]['magic']) ? $cp[$sec]['magic'] : 'ArchiveFile';
    $ver   = isset($cp[$sec]['ver']) ? $cp[$sec]['ver'] : '001';
    $delim = isset($cp[$sec]['delimiter']) ? $cp[$sec]['delimiter'] : '\\x00';
    $newst = isset($cp[$sec]['newstyle']) ? $cp[$sec]['newstyle'] : 'true';
    return array(
        'format_magic' => $magic,
        'format_ver'   => pn_ver_digits($ver),
        'format_delimiter' => pn_decode_escape($delim),
        'new_style'    => in_array(strtolower((string)$newst), array('true','1','yes'), true),
        'format_name'  => $sec
    );
}

function pn_ensure_formatspecs($specs = null) {
    if (is_array($specs)) { $fs = pn_default_formatspecs(); foreach ($specs as $k=>$v) $fs[$k]=$v; return $fs; }
    $env = pn_load_formatspecs_from_ini();
    return $env ? $env : pn_default_formatspecs();
}

/* -------- I/O helpers -------- */

class PNStream {
    public $fp;
    public $delim;
    private $buf = '';

    public function __construct($fp, $delim) {
        $this->fp = $fp;
        $this->delim = $delim;
    }

    public function read_bytes($n) {
        $need = $n;
        $out = '';
        if ($this->buf !== '') {
            if (strlen($this->buf) >= $need) {
                $out = substr($this->buf, 0, $need);
                $this->buf = substr($this->buf, $need);
                return $out;
            } else {
                $out = $this->buf;
                $need -= strlen($this->buf);
                $this->buf = '';
            }
        }
        if ($need > 0) {
            $data = ($need >= PN_BYTE_CHUNK) ? fread($this->fp, $need) : fread($this->fp, $need);
            if ($data !== false) $out .= $data;
        }
        return $out;
    }

    public function skip($n) {
        if ($n <= 0) return;
        if ($this->buf !== '') {
            $bl = strlen($this->buf);
            if ($bl >= $n) { $this->buf = substr($this->buf, $n); return; }
            $this->buf = ''; $n -= $bl;
        }
        // If stream is seekable, seek; else read & drop
        $meta = stream_get_meta_data($this->fp);
        if (!empty($meta['seekable'])) {
            fseek($this->fp, $n, SEEK_CUR);
        } else {
            while ($n > 0) {
                $r = fread($this->fp, min($n, PN_BYTE_CHUNK));
                if ($r === '' || $r === false) break;
                $n -= strlen($r);
            }
        }
    }

    public function read_cstring() {
        $d = $this->delim;
        $dl = strlen($d);
        // fast-path single-byte
        if ($dl === 1) {
            $out = '';
            while (true) {
                if ($this->buf !== '') {
                    $pos = strpos($this->buf, $d);
                    if ($pos !== false) {
                        $out .= substr($this->buf, 0, $pos);
                        $this->buf = substr($this->buf, $pos + 1);
                        return $out;
                    }
                    $out .= $this->buf; $this->buf = '';
                }
                $chunk = fread($this->fp, PN_BYTE_CHUNK);
                if ($chunk === '' || $chunk === false) {
                    return $out;
                }
                $this->buf .= $chunk;
            }
        }
        // general case
        $out = '';
        while (true) {
            if ($this->buf !== '') {
                $pos = strpos($this->buf, $d);
                if ($pos !== false) {
                    $out .= substr($this->buf, 0, $pos);
                    $this->buf = substr($this->buf, $pos + $dl);
                    return $out;
                }
                // keep tail to bridge boundary
                $keep = $dl - 1;
                if (strlen($this->buf) > $keep) {
                    $out .= substr($this->buf, 0, -$keep);
                    $this->buf = substr($this->buf, -$keep);
                }
            }
            $chunk = fread($this->fp, PN_BYTE_CHUNK);
            if ($chunk === '' || $chunk === false) {
                $out .= $this->buf; $this->buf = '';
                return $out;
            }
            $this->buf .= $chunk;
        }
    }
}

function pn_open_in($infile) {
    if (is_resource($infile)) { return array($infile, false); }
    if (is_string($infile)) {
        if (is_file($infile)) {
            $fp = fopen($infile, 'rb');
            if (!$fp) throw new \Exception("Cannot open $infile");
            return array($fp, true);
        } else {
            // treat as raw bytes string
            $fp = fopen('php://temp', 'w+b');
            fwrite($fp, $infile);
            rewind($fp);
            return array($fp, true);
        }
    }
    throw new \Exception("Bad infile");
}

function pn_open_out($outfile) {
    if ($outfile === null || $outfile === '-' ) {
        return array(true, null, '');
    }
    if (is_resource($outfile)) {
        return array(false, $outfile, null);
    }
    $dir = dirname($outfile);
    if ($dir && $dir !== '.' && !is_dir($dir)) @mkdir($dir, 0777, true);
    $fp = fopen($outfile, 'wb');
    if (!$fp) throw new \Exception("Cannot open for write: $outfile");
    return array(false, $fp, null);
}

function pn_write($bufmode, $fp, &$buf, $data) {
    if (!is_string($data)) $data = strval($data);
    if ($bufmode) $buf .= $data;
    else fwrite($fp, $data);
}

/* -------- parsing -------- */

function pn_read_fields($ps, $n) {
    $out = array();
    for ($i=0; $i<(int)$n; $i++) $out[] = $ps->read_cstring();
    return $out;
}

function pn_parse_global_header($ps, $fs) {
    $ps->read_cstring(); // magic+ver
    $ps->read_cstring(); // headersize
    $ps->read_cstring(); // tmpoutlen
    $fencoding = $ps->read_cstring();
    $fostype   = $ps->read_cstring();
    $fnumfiles = hexdec($ps->read_cstring() ?: '0');
    $ps->read_cstring(); // extras size
    $extrafields = hexdec($ps->read_cstring() ?: '0');
    for ($i=0; $i<$extrafields; $i++) $ps->read_cstring();
    $checksumtype = $ps->read_cstring();
    $ps->read_cstring(); // header checksum
    return array(
        'fencoding' => ($fencoding !== '') ? $fencoding : 'UTF-8',
        'fnumfiles' => $fnumfiles,
        'fostype'   => $fostype,
        'fextradata'=> array(),
        'fchecksumtype' => $checksumtype,
        'ffilelist' => array(),
        'fformatspecs' => $fs,
    );
}

function pn_index_json_and_checks($vals) {
    if (count($vals) < 25) throw new \Exception("Record too short: got ".count($vals));
    $idx = 25;
    $ishex = function($s) { return $s !== '' && preg_match('/^[0-9A-Fa-f]+$/', $s); };
    $v2 = isset($vals[$idx+1]) ? $vals[$idx+1] : '';
    $v3 = isset($vals[$idx+2]) ? $vals[$idx+2] : '';
    $v4 = isset($vals[$idx+3]) ? $vals[$idx+3] : '';
    $csnames = array('none','crc32','md5','sha1','sha224','sha256','sha384','sha512','blake2b','blake2s');
    if ($ishex($v2) && $ishex($v3) && in_array(strtolower($v4), $csnames, true)) {
        $idx_json_type = 25; $idx_json_len = 26; $idx_json_size = 27; $idx_json_cst = 28; $idx_json_cs = 29; $idx_extras_size = 30;
    } else {
        $idx_json_type = 25; $idx_json_len = null; $idx_json_size = 26; $idx_json_cst = 27; $idx_json_cs = 28; $idx_extras_size = 29;
    }
    $idx_extras_count = $idx_extras_size + 1;
    $count = isset($vals[$idx_extras_count]) ? hexdec($vals[$idx_extras_count] ?: '0') : 0;
    $idx0 = $idx_extras_count + 1 + $count;
    return array($idx_json_type, $idx_json_len, $idx_json_size, $idx_json_cst, $idx_json_cs,
                 $idx0, $idx0+1, $idx0+2, $idx0+3);
}

function pn_parse_record($ps, $fs, $listonly=false, $skipchecksum=false, $uncompress=true, $skipjson=false) {
    $d = $fs['format_delimiter'];
    $first = $ps->read_cstring();
    if ($first === '0') {
        $second = $ps->read_cstring();
        if ($second === '0') return null; // end
        $fields_len_hex = $second;
    } else {
        $fields_len_hex = $ps->read_cstring();
    }
    $n_fields = hexdec($fields_len_hex ?: '0');
    if ($n_fields === 0) return null;
    $vals = pn_read_fields($ps, $n_fields);
    if (count($vals) < 25) throw new \Exception("Record too short: expected >=25 header fields, got ".count($vals));

    list($idx_json_type, $idx_json_len, $idx_json_size, $idx_json_cst, $idx_json_cs,
         $idx_header_cs_type, $idx_content_cs_type, $idx_header_cs, $idx_content_cs) = pn_index_json_and_checks($vals);

    list($ftypehex, $fencoding, $fcencoding, $fname, $flinkname,
         $fsize_hex, $fatime_hex, $fmtime_hex, $fctime_hex, $fbtime_hex,
         $fmode_hex, $fwinattrs_hex, $fcompression, $fcsize_hex,
         $fuid_hex, $funame, $fgid_hex, $fgname, $fid_hex, $finode_hex,
         $flinkcount_hex, $fdev_hex, $fdev_minor_hex, $fdev_major_hex,
         $fseeknextfile) = array_slice($vals, 0, 25);

    $fjsonsize_hex = isset($vals[$idx_json_size]) ? $vals[$idx_json_size] : '0';
    $fjsonsize = hexdec($fjsonsize_hex ?: '0');

    if ($fjsonsize > 0) {
        if ($listonly || $skipjson) {
            $ps->skip($fjsonsize);
            $ps->read_bytes(strlen($d)); // trailing delimiter
            $json_bytes = '';
        } else {
            $json_bytes = $ps->read_bytes($fjsonsize);
            $ps->read_bytes(strlen($d));
        }
    } else {
        $json_bytes = '';
        $ps->read_bytes(strlen($d));
    }

    $fsize  = hexdec($fsize_hex ?: '0');
    $fcsize = hexdec($fcsize_hex ?: '0');
    $stored_len = ((strtolower($fcompression) !== '' && strtolower($fcompression) !== 'none' && $fcsize > 0) ? $fcsize : $fsize);

    $content_stored = '';
    if ($stored_len) {
        if ($listonly) $ps->skip($stored_len);
        else $content_stored = $ps->read_bytes($stored_len);
    }
    $ps->read_bytes(strlen($d)); // trailing delimiter after content

    $header_cs_type  = isset($vals[$idx_header_cs_type]) ? $vals[$idx_header_cs_type] : 'none';
    $content_cs_type = isset($vals[$idx_content_cs_type]) ? $vals[$idx_content_cs_type] : 'none';
    $header_cs_val   = isset($vals[$idx_header_cs]) ? $vals[$idx_header_cs] : '0';
    $content_cs_val  = isset($vals[$idx_content_cs]) ? $vals[$idx_content_cs] : '0';
    $json_cs_type    = isset($vals[$idx_json_cst]) ? $vals[$idx_json_cst] : 'none';
    $json_cs_val     = isset($vals[$idx_json_cs]) ? $vals[$idx_json_cs] : '0';

    if ($fjsonsize && !$skipchecksum && !($listonly || $skipjson)) {
        if (pn_checksum($json_bytes, $json_cs_type, true) !== $json_cs_val) {
            throw new \Exception("JSON checksum mismatch for ". $fname);
        }
    }
    if (!$skipchecksum && $stored_len && !$listonly) {
        if (pn_checksum($content_stored, $content_cs_type, false) !== $content_cs_val) {
            throw new \Exception("Content checksum mismatch for ". $fname);
        }
    }

    $content_ret = $content_stored;
    if (!$listonly && $uncompress && strtolower($fcompression) !== '' && strtolower($fcompression) !== 'none' && $content_stored !== '') {
        try {
            $content_ret = pn_decompress_bytes($content_stored, $fcompression);
        } catch (\Exception $e) {
            $content_ret = $content_stored; // leave compressed if not available
        }
    }

    $name = $fname;
    if (strpos($name, './') !== 0 && strpos($name, '/') !== 0) $name = './'.$name;

    $fjson = array();
    if ($json_bytes !== '') {
        $tmp = json_decode($json_bytes, true);
        if (is_array($tmp)) $fjson = $tmp;
    }

    $to_int = function($x){ return hexdec($x === '' ? '0' : $x); };
    return array(
        'fid' => $to_int($fid_hex),
        'finode' => $to_int($finode_hex),
        'fname' => $name,
        'flinkname' => $flinkname,
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
        'funame' => $funame,
        'fgid' => $to_int($fgid_hex),
        'fgname' => $fgname,
        'fcompression' => ($fcompression !== '' ? $fcompression : 'none'),
        'fseeknext' => $fseeknextfile,
        'fjson' => $fjson,
        'fcontent' => $listonly ? null : $content_ret,
    );
}

/* -------- public parse -------- */

function archive_to_array_neo($infile, $formatspecs=null, $listonly=false, $skipchecksum=false, $uncompress=true, $skipjson=false) {
    $fs = pn_ensure_formatspecs($formatspecs);
    list($fp, $need_close) = pn_open_in($infile);
    $ps = new PNStream($fp, $fs['format_delimiter']);
    try {
        $top = pn_parse_global_header($ps, $fs);
        while (true) {
            $rec = pn_parse_record($ps, $fs, $listonly, $skipchecksum, $uncompress, $skipjson);
            if ($rec === null) break;
            $top['ffilelist'][] = $rec;
        }
        return $top;
    } finally {
        if ($need_close && is_resource($fp)) @fclose($fp);
    }
}

/* -------- building -------- */

function pn_append_null($x, $d) { return (is_string($x) ? $x : strval($x)) . $d; }
function pn_append_nulls($arr, $d) {
    $out = '';
    foreach ($arr as $it) $out .= pn_append_null($it, $d);
    return $out;
}

function pn_write_global_header($dst_write, $fs, $numfiles, $encoding, $checksumtype) {
    $d = $fs['format_delimiter']; $magic = $fs['format_magic']; $ver = pn_ver_digits($fs['format_ver']);
    $platform_name = PHP_OS;
    $extras = array(); $extrafields = pn_hex(count($extras));
    $extras_blob = pn_append_null($extrafields, $d) . (empty($extras) ? '' : pn_append_nulls($extras, $d));
    $body = pn_append_nulls(array(
        pn_hex(3 + 5 + count($extras) + 1),
        $encoding,
        $platform_name,
        pn_hex((int)$numfiles),
        pn_hex(strlen($extras_blob)),
        $extrafields
    ), $d);
    if (!empty($extras)) $body .= pn_append_nulls($extras, $d);
    $body .= pn_append_null($checksumtype, $d);
    $prefix = pn_append_null($magic.$ver, $d);
    $tmpfileoutstr = $body . pn_append_null('', $d);
    $headersize_hex = pn_hex(strlen($tmpfileoutstr) - strlen($d));
    $out = $prefix . pn_append_null($headersize_hex, $d) . $body;
    $header_cs = pn_checksum($out, $checksumtype, true);
    $out .= pn_append_null($header_cs, $d);
    $dst_write($out);
}

function pn_build_record($fs, $meta, $jsondata, $content_stored, $checksumtypes) {
    $d = $fs['format_delimiter'];
    $H = function($x){ return pn_hex((int)$x); };
    $fname = isset($meta['fname']) ? $meta['fname'] : '';
    if (strpos($fname, './') !== 0 && strpos($fname, '/') !== 0) $fname = './'.$fname;
    $now = time();

    $fields = array(
        $H(isset($meta['ftype']) ? $meta['ftype'] : 0),
        isset($meta['fencoding']) ? $meta['fencoding'] : 'UTF-8',
        isset($meta['fcencoding']) ? $meta['fcencoding'] : 'UTF-8',
        $fname,
        isset($meta['flinkname']) ? $meta['flinkname'] : '',
        $H(isset($meta['fsize']) ? $meta['fsize'] : 0),
        $H(isset($meta['fatime']) ? $meta['fatime'] : $now),
        $H(isset($meta['fmtime']) ? $meta['fmtime'] : $now),
        $H(isset($meta['fctime']) ? $meta['fctime'] : $now),
        $H(isset($meta['fbtime']) ? $meta['fbtime'] : $now),
        $H(isset($meta['fmode']) ? $meta['fmode'] : octdec('0100000') | octdec('0666')),
        $H(isset($meta['fwinattributes']) ? $meta['fwinattributes'] : 0),
        isset($meta['fcompression']) ? $meta['fcompression'] : 'none',
        $H(isset($meta['fcsize']) ? $meta['fcsize'] : 0),
        $H(isset($meta['fuid']) ? $meta['fuid'] : 0),
        isset($meta['funame']) ? $meta['funame'] : '',
        $H(isset($meta['fgid']) ? $meta['fgid'] : 0),
        isset($meta['fgname']) ? $meta['fgname'] : '',
        $H(isset($meta['fid']) ? $meta['fid'] : (isset($meta['index']) ? $meta['index'] : 0)),
        $H(isset($meta['finode']) ? $meta['finode'] : (isset($meta['index']) ? $meta['index'] : 0)),
        $H(isset($meta['flinkcount']) ? $meta['flinkcount'] : 1),
        $H(isset($meta['fdev']) ? $meta['fdev'] : 0),
        $H(isset($meta['fdev_minor']) ? $meta['fdev_minor'] : 0),
        $H(isset($meta['fdev_major']) ? $meta['fdev_major'] : 0),
        $H(strlen($d)),
    );

    if (!empty($jsondata)) {
        $raw_json = json_encode($jsondata, JSON_UNESCAPED_SLASHES);
        $fjsontype = 'json';
        $fjsonlen_hex = $H(is_array($jsondata) ? count($jsondata) : 0);
        $fjsonsize_hex = $H(strlen($raw_json));
        $fjson_cs_type = $checksumtypes[2];
        $fjson_cs = pn_checksum($raw_json, $fjson_cs_type, true);
    } else {
        $raw_json = '';
        $fjsontype = 'none';
        $fjsonlen_hex = '0';
        $fjsonsize_hex = '0';
        $fjson_cs_type = 'none';
        $fjson_cs = '0';
    }

    $extras_size_hex = pn_hex(strlen(pn_append_null('0', $d)));
    $extrafields = '0';

    $rec_fields = array_merge($fields, array($fjsontype, $fjsonlen_hex, $fjsonsize_hex, $fjson_cs_type, $fjson_cs, $extras_size_hex, $extrafields));
    $header_cs_type  = $checksumtypes[0];
    $content_cs_type = (!empty($content_stored) ? $checksumtypes[1] : 'none');
    $rec_fields[] = $header_cs_type;
    $rec_fields[] = $content_cs_type;

    $record_fields_len_hex = pn_hex(count($rec_fields) + 2);
    $header_no_cs = pn_append_nulls($rec_fields, $d);
    $tmp_with_placeholders = pn_append_null($record_fields_len_hex, $d) . $header_no_cs . pn_append_null('', $d) . pn_append_null('', $d);
    $headersize_hex = pn_hex(strlen($tmp_with_placeholders) - strlen($d));
    $header_with_sizes = pn_append_null($headersize_hex, $d) . pn_append_null($record_fields_len_hex, $d) . $header_no_cs;

    $header_checksum = pn_checksum($header_with_sizes, $header_cs_type, true);
    $content_checksum = pn_checksum($content_stored, $content_cs_type, false);
    $header_full = $header_with_sizes . pn_append_null($header_checksum, $d) . pn_append_null($content_checksum, $d);
    return $header_full . $raw_json . $d . $content_stored . $d;
}

/* -------- public builders -------- */

function pack_iter_neo($items, $outfile=null, $formatspecs=null, $checksumtypes=array('crc32','crc32','crc32'),
                       $encoding='UTF-8', $compression='auto', $compression_level=null) {
    // Snapshot traversable/generator to array to avoid exhaustion
    if ($items instanceof \Traversable) {
        $items = iterator_to_array($items, false);
    } elseif (!is_array($items)) {
        throw new \Exception("items must be array or Traversable");
    }
    $fs = pn_ensure_formatspecs($formatspecs);
    list($bufmode, $fp, $buf) = pn_open_out($outfile);
    $dst_write = function($data) use ($bufmode, $fp, &$buf) { pn_write($bufmode, $fp, $buf, $data); };

    pn_write_global_header($dst_write, $fs, count($items), $encoding, $checksumtypes[0]);

    $fid = 0;
    foreach ($items as $it) {
        if (is_array($it) && isset($it['name'])) {
            $name = $it['name'];
            $is_dir = !empty($it['is_dir']);
            $data = isset($it['data']) ? $it['data'] : null;
            $mode = isset($it['mode']) ? (int)$it['mode'] : (($is_dir ? (octdec('0040000')|octdec('0755')) : (octdec('0100000')|octdec('0666'))));
            $mtime = isset($it['mtime']) ? (int)$it['mtime'] : time();
        } elseif (is_array($it) && count($it) >= 3) {
            $name = $it[0]; $is_dir = (bool)$it[1]; $data = $it[2];
            $mode = $is_dir ? (octdec('0040000')|octdec('0755')) : (octdec('0100000')|octdec('0666'));
            $mtime = time();
        } else {
            throw new \Exception("Bad item spec");
        }
        $name = str_replace('\\','/',$name);
        if (strpos($name, './') !== 0 && strpos($name, '/') !== 0) $name = './'.$name;
        if ($is_dir || substr($name, -1) === '/') { $raw=''; $ftype=5; }
        else { $raw = ($data === null ? '' : (string)$data); $ftype=0; }

        $algo = strtolower($compression ?: 'none');
        if ($algo === 'auto') { list($algo, $lvl) = pn_auto_pick_for_size(strlen($raw)); $level = ($compression_level !== null ? $compression_level : $lvl); }
        else { $level = $compression_level; }
        try { list($stored, $used_algo) = pn_compress_bytes($raw, $algo, $level); }
        catch (\Exception $e) { list($stored, $used_algo) = pn_compress_bytes($raw, 'zlib', ($level === null ? 6 : $level)); }

        $meta = array(
            'ftype'=>$ftype, 'fencoding'=>$encoding, 'fcencoding'=>$encoding, 'fname'=>$name, 'flinkname'=>'',
            'fsize'=>strlen($raw), 'fatime'=>$mtime, 'fmtime'=>$mtime, 'fctime'=>$mtime, 'fbtime'=>$mtime,
            'fmode'=>$mode, 'fwinattributes'=>0, 'fcompression'=>$used_algo, 'fcsize'=>strlen($stored),
            'fuid'=>0, 'funame'=>'', 'fgid'=>0, 'fgname'=>'',
            'fid'=>$fid, 'finode'=>$fid, 'flinkcount'=>1, 'fdev'=>0, 'fdev_minor'=>0, 'fdev_major'=>0, 'index'=>$fid,
        );
        $rec = pn_build_record($fs, $meta, array(), $stored, $checksumtypes);
        pn_write($bufmode, $fp, $buf, $rec);
        $fid++;
    }
    pn_write($bufmode, $fp, $buf, pn_append_nulls(array('0','0'), $fs['format_delimiter']));
    if ($bufmode) return $buf;
    return true;
}

function pack_neo($infiles, $outfile=null, $formatspecs=null, $checksumtypes=array('crc32','crc32','crc32'),
                  $encoding='UTF-8', $compression='auto', $compression_level=null) {
    if (is_array($infiles) && array_keys($infiles) !== range(0, count($infiles)-1)) {
        // dict: name => data
        $items = array();
        foreach ($infiles as $name => $data) {
            $is_dir = ($data === null) || (substr($name, -1) === '/');
            $items[] = array('name'=>$name, 'is_dir'=>$is_dir, 'data'=>($is_dir ? null : $data));
        }
        return pack_iter_neo($items, $outfile, $formatspecs, $checksumtypes, $encoding, $compression, $compression_level);
    }

    // treat as path(s)
    $paths = is_array($infiles) ? $infiles : array($infiles);
    $filelist = array();
    foreach ($paths as $p) {
        if (is_dir($p)) {
            $iter = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($p, FilesystemIterator::SKIP_DOTS), RecursiveIteratorIterator::SELF_FIRST);
            foreach ($iter as $spl) {
                $full = $spl->getPathname();
                $rel = substr($full, strlen(rtrim($p, DIRECTORY_SEPARATOR)) + 1);
                $rel = str_replace('\\','/',$rel);
                if ($spl->isDir()) {
                    if ($rel !== '' && substr($rel,-1) !== '/') $rel .= '/';
                    $filelist[] = array($full, true, $rel);
                } else {
                    $filelist[] = array($full, false, $rel);
                }
            }
        } else {
            $filelist[] = array($p, false, basename($p));
        }
    }

    $fs = pn_ensure_formatspecs($formatspecs);
    list($bufmode, $fp, $buf) = pn_open_out($outfile);
    $dst_write = function($data) use ($bufmode, $fp, &$buf) { pn_write($bufmode, $fp, $buf, $data); };

    pn_write_global_header($dst_write, $fs, count($filelist), $encoding, $checksumtypes[0]);

    $fid = 0;
    foreach ($filelist as $triple) {
        list($apath, $is_dir, $relname) = $triple;
        if ($is_dir) { $raw = ''; $ftype = 5; }
        else { $raw = file_get_contents($apath); $ftype = 0; }
        if (strpos($relname, './') !== 0 && strpos($relname, '/') !== 0) $relname = './'.$relname;

        $algo = strtolower($compression ?: 'none');
        if ($algo === 'auto') { list($algo, $lvl) = pn_auto_pick_for_size(strlen($raw)); $level = ($compression_level !== null ? $compression_level : $lvl); }
        else { $level = $compression_level; }
        try { list($stored, $used_algo) = pn_compress_bytes($raw, $algo, $level); }
        catch (\Exception $e) { list($stored, $used_algo) = pn_compress_bytes($raw, 'zlib', ($level === null ? 6 : $level)); }

        $now = time();
        $meta = array(
            'ftype'=>$ftype, 'fencoding'=>$encoding, 'fcencoding'=>$encoding, 'fname'=>$relname, 'flinkname'=>'',
            'fsize'=>strlen($raw), 'fatime'=>$now, 'fmtime'=>$now, 'fctime'=>$now, 'fbtime'=>$now,
            'fmode'=> ($is_dir ? (octdec('0040000')|octdec('0755')) : (octdec('0100000')|octdec('0666')) ),
            'fwinattributes'=>0, 'fcompression'=>$used_algo, 'fcsize'=>strlen($stored),
            'fuid'=>0,'funame'=>'','fgid'=>0,'fgname'=>'','fid'=>$fid,'finode'=>$fid,'flinkcount'=>1,
            'fdev'=>0,'fdev_minor'=>0,'fdev_major'=>0,'index'=>$fid,
        );
        $rec = pn_build_record($fs, $meta, array(), $stored, $checksumtypes);
        pn_write($bufmode, $fp, $buf, $rec);
        $fid++;
    }

    pn_write($bufmode, $fp, $buf, pn_append_nulls(array('0','0'), $fs['format_delimiter']));
    if ($bufmode) return $buf;
    return true;
}

/* -------- list & validate -------- */

function archivefilelistfiles_neo($infile, $formatspecs=null, $advanced=false, $include_dirs=true, $skipjson=true) {
    $fs = pn_ensure_formatspecs($formatspecs);
    list($fp, $need_close) = pn_open_in($infile);
    $ps = new PNStream($fp, $fs['format_delimiter']);
    try {
        pn_parse_global_header($ps, $fs);
        $out = array();
        while (true) {
            $rec = pn_parse_record($ps, $fs, true, true, false, $skipjson);
            if ($rec === null) break;
            $is_dir = ($rec['ftype'] == 5);
            if (!$advanced) {
                if ($is_dir && !$include_dirs) continue;
                $out[] = $rec['fname'];
            } else {
                $out[] = array(
                    'name' => $rec['fname'],
                    'type' => $is_dir ? 'dir' : 'file',
                    'compression' => $rec['fcompression'] ?: 'none',
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
        if ($need_close && is_resource($fp)) @fclose($fp);
    }
}

function archivefilevalidate_neo($infile, $formatspecs=null, $verbose=false, $return_details=false, $skipjson=false) {
    $arr = archive_to_array_neo($infile, $formatspecs, false, false, false, $skipjson);
    $ok = true; $details = array();
    foreach ($arr['ffilelist'] as $i => $e) {
        $details[] = array('index'=>$i, 'name'=>$e['fname'], 'header_ok'=>true, 'json_ok'=>true, 'content_ok'=>true);
    }
    if ($return_details) return array($ok, $details);
    return $ok;
}

/* -------- unpack/repack -------- */

function unpack_neo($infile, $outdir='.', $formatspecs=null, $skipchecksum=false, $uncompress=true) {
    $arr = archive_to_array_neo($infile, $formatspecs, false, $skipchecksum, $uncompress, false);
    if ($outdir === null || $outdir === '-') {
        $result = array();
        foreach ($arr['ffilelist'] as $ent) {
            $result[$ent['fname']] = ($ent['ftype'] == 5) ? null : ($ent['fcontent'] ?: '');
        }
        return $result;
    }
    if (!is_dir($outdir)) {
        if (file_exists($outdir)) throw new \Exception("not a directory: $outdir");
        @mkdir($outdir, 0777, true);
    }
    foreach ($arr['ffilelist'] as $ent) {
        $path = $outdir . DIRECTORY_SEPARATOR . ltrim($ent['fname'], './');
        $path = str_replace('/', DIRECTORY_SEPARATOR, $path);
        if ($ent['ftype'] == 5) {
            if (!is_dir($path)) @mkdir($path, 0777, true);
        } else {
            $d = dirname($path);
            if ($d && !is_dir($d)) @mkdir($d, 0777, true);
            file_put_contents($path, $ent['fcontent'] ?: '');
        }
    }
    return true;
}

function repack_neo($infile, $outfile=null, $formatspecs=null, $checksumtypes=array('crc32','crc32','crc32'),
                    $compression='auto', $compression_level=null) {
    $arr = archive_to_array_neo($infile, $formatspecs, false, false, false, true);
    $fs = pn_ensure_formatspecs($formatspecs);
    list($bufmode, $fp, $buf) = pn_open_out($outfile);
    $dst_write = function($data) use ($bufmode, $fp, &$buf) { pn_write($bufmode, $fp, $buf, $data); };
    pn_write_global_header($dst_write, $fs, count($arr['ffilelist']), isset($arr['fencoding'])?$arr['fencoding']:'UTF-8', $checksumtypes[0]);

    $out_count = 0;
    foreach ($arr['ffilelist'] as $ent) {
        $src_algo = strtolower($ent['fcompression'] ?: 'none');
        $stored_src = $ent['fcontent'] ?: '';
        // Derive raw
        try { $raw = ($src_algo !== 'none') ? pn_decompress_bytes($stored_src, $src_algo) : $stored_src; }
        catch (\Exception $e) { $raw = $stored_src; }

        $dst_algo = strtolower($compression ?: 'auto');
        if ($dst_algo === 'auto') { list($dst_algo, $dst_level) = pn_auto_pick_for_size(strlen($raw)); }
        else { $dst_level = $compression_level; }

        if ($dst_algo === $src_algo || ($dst_algo==='none' && $src_algo==='none')) {
            $stored = $stored_src; $used_algo = $src_algo;
            try { $raw_len = ($src_algo !== 'none') ? strlen(pn_decompress_bytes($stored_src, $src_algo)) : strlen($stored_src); }
            catch (\Exception $e) { $raw_len = strlen($stored_src); }
        } else {
            list($stored, $used_algo) = pn_compress_bytes($raw, $dst_algo, $dst_level);
            $raw_len = strlen($raw);
        }

        $now = time();
        $meta = array(
            'ftype'=>$ent['ftype'], 'fencoding'=>isset($arr['fencoding'])?$arr['fencoding']:'UTF-8',
            'fcencoding'=>isset($arr['fencoding'])?$arr['fencoding']:'UTF-8',
            'fname'=>$ent['fname'], 'flinkname'=>isset($ent['flinkname'])?$ent['flinkname']:'',
            'fsize'=>$raw_len, 'fatime'=>isset($ent['fatime'])?$ent['fatime']:$now, 'fmtime'=>isset($ent['fmtime'])?$ent['fmtime']:$now,
            'fctime'=>isset($ent['fctime'])?$ent['fctime']:$now, 'fbtime'=>isset($ent['fbtime'])?$ent['fbtime']:$now,
            'fmode'=>isset($ent['fmode'])?$ent['fmode']:(octdec('0100000')|octdec('0666')), 'fwinattributes'=>isset($ent['fwinattributes'])?$ent['fwinattributes']:0,
            'fcompression'=>$used_algo, 'fcsize'=>strlen($stored),
            'fuid'=>isset($ent['fuid'])?$ent['fuid']:0, 'funame'=>isset($ent['funame'])?$ent['funame']:'',
            'fgid'=>isset($ent['fgid'])?$ent['fgid']:0, 'fgname'=>isset($ent['fgname'])?$ent['fgname']:'',
            'fid'=>isset($ent['fid'])?$ent['fid']:$out_count, 'finode'=>isset($ent['finode'])?$ent['finode']:$out_count,
            'flinkcount'=>isset($ent['flinkcount'])?$ent['flinkcount']:1, 'fdev'=>isset($ent['fdev'])?$ent['fdev']:0,
            'fdev_minor'=>isset($ent['fdev_minor'])?$ent['fdev_minor']:0, 'fdev_major'=>isset($ent['fdev_major'])?$ent['fdev_major']:0,
            'index'=>$out_count
        );
        $rec = pn_build_record($fs, $meta, isset($ent['fjson'])?$ent['fjson']:array(), $stored, $checksumtypes);
        pn_write($bufmode, $fp, $buf, $rec);
        $out_count += 1;
    }

    pn_write($bufmode, $fp, $buf, pn_append_nulls(array('0','0'), $fs['format_delimiter']));
    if ($bufmode) return $buf;
    return true;
}

/* -------- convert (zip/tar) -------- */

function convert_foreign_to_neo($infile, $outfile=null, $formatspecs=null, $checksumtypes=array('crc32','crc32','crc32'),
                                $compression='auto', $compression_level=null) {
    if (!is_string($infile)) throw new \Exception("convert expects a path");
    $p = $infile; $lp = strtolower($p);
    if (substr($lp, -4) === '.zip') {
        if (!class_exists('ZipArchive')) throw new \Exception("ZipArchive not available");
        $z = new ZipArchive();
        if ($z->open($p) !== true) throw new \Exception("Cannot open zip: $p");
        $items = array();
        for ($i=0; $i<$z->numFiles; $i++) {
            $stat = $z->statIndex($i);
            $name = $stat['name'];
            $is_dir = (substr($name, -1) === '/');
            $data = $is_dir ? null : $z->getFromIndex($i);
            $items[] = array('name'=>$name, 'is_dir'=>$is_dir, 'data'=>$data);
        }
        $z->close();
        return pack_iter_neo($items, $outfile, $formatspecs, $checksumtypes, 'UTF-8', $compression, $compression_level);
    }
    if (preg_match('/\\.tar(\\.(gz|bz2|xz))?$/i', $lp)) {
        if (!class_exists('PharData')) throw new \Exception("PharData not available");
        $phar = new PharData($p);
        $items = array();
        foreach (new RecursiveIteratorIterator($phar) as $file) {
            /** @var PharFileInfo $file */
            $name = $file->getPathName();
            $name = str_replace('\\','/',$name);
            $rel = preg_replace('#^.*?://#','', $name); // strip phar:// prefix
            $is_dir = $file->isDir();
            $data = $is_dir ? null : file_get_contents($file->getPathname());
            $items[] = array('name'=>$rel, 'is_dir'=>$is_dir, 'data'=>$data);
        }
        return pack_iter_neo($items, $outfile, $formatspecs, $checksumtypes, 'UTF-8', $compression, $compression_level);
    }
    throw new \Exception("Unsupported foreign archive (zip/tar.* only)");
}
?>
