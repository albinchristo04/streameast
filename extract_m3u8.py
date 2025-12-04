#!/usr/bin/env python3
"""
extract_m3u8.py

Read normalized matches (matches_clean.json) and extract playable m3u8 playlist URLs
for each resolved stream. Output is written to matcheswithm3u8.json.

Usage:
  pip install requests tqdm urllib3
  python extract_m3u8.py --in matches_clean.json --out matcheswithm3u8.json --workers 12 --timeout 10 --verify

Options:
  --in            Input normalized JSON (matches_clean.json)
  --out           Output JSON (matcheswithm3u8.json)
  --workers       Concurrent workers (default 8)
  --timeout       Per-request timeout seconds (default 10)
  --retries       Retry attempts for transient errors (default 2)
  --backoff       Backoff factor for retries (default 0.3)
  --verify        Perform lightweight verification of each candidate m3u8 (GET header + small body)
  --resume        Resume from existing output file if present (will skip matches already processed)
Notes:
 - Script performs only GET/HEAD requests to discover playlists. Do not use for heavy scraping.
 - Resolved m3u8 URLs may be short-lived tokens.
 - Respect terms of service and copyright when using retrieved streams.
"""

import argparse
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urljoin, urlparse
from datetime import datetime, timezone

try:
    import requests
    from requests.exceptions import RequestException
    from urllib3.util import Retry
    from requests.adapters import HTTPAdapter
except Exception:
    print("Install dependencies: pip install requests urllib3 tqdm", file=sys.stderr)
    raise

try:
    from tqdm import tqdm
except Exception:
    def tqdm(x, **k): return x

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stderr)
logger = logging.getLogger("extract_m3u8")

USER_AGENT = "m3u8-extractor/1.0 (+https://github.com/)"

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def create_session(retries=2, backoff=0.3):
    sess = requests.Session()
    sess.headers.update({"User-Agent": USER_AGENT})
    retry = Retry(total=retries, read=retries, connect=retries, backoff_factor=backoff,
                  status_forcelist=(429, 500, 502, 503, 504), allowed_methods=frozenset(["GET","HEAD","OPTIONS"]))
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess

def head(session, url, timeout=10):
    try:
        resp = session.head(url, timeout=timeout, allow_redirects=True)
        return {"status": resp.status_code, "effective_url": resp.url, "headers": dict(resp.headers)}
    except RequestException as e:
        return {"error": str(e)}

def get_text(session, url, timeout=10, max_bytes=100*1024):
    """
    GET small portion of the URL (up to max_bytes) and return (status, effective_url, text, headers).
    """
    try:
        resp = session.get(url, timeout=timeout, stream=True, allow_redirects=True)
        status = resp.status_code
        eff = resp.url
        headers = dict(resp.headers)
        # read up to max_bytes
        body = []
        total = 0
        for chunk in resp.iter_content(chunk_size=1024):
            if not chunk:
                break
            body.append(chunk)
            total += len(chunk)
            if total >= max_bytes:
                break
        text = b"".join(body).decode(errors="replace")
        resp.close()
        return {"status": status, "effective_url": eff, "text": text, "headers": headers}
    except RequestException as e:
        return {"error": str(e)}

def is_hls_playlist(text):
    if not text:
        return False
    return "#EXTM3U" in text

def parse_master_playlist(text, base_url):
    """
    Parse master playlist text and return list of variants:
    each variant: {"bandwidth": int or None, "resolution": str or None, "label": str or None, "url": absolute_url}
    """
    lines = [l.strip() for l in text.splitlines() if l.strip() != ""]
    variants = []
    last_inf = None
    for line in lines:
        if line.startswith("#EXT-X-STREAM-INF:"):
            last_inf = line[len("#EXT-X-STREAM-INF:"):].strip()
            # parse attributes
            attrs = {}
            for part in last_inf.split(","):
                if "=" in part:
                    k, v = part.split("=", 1)
                    attrs[k.strip()] = v.strip().strip('"')
            bw = None
            res = None
            label = None
            if "BANDWIDTH" in attrs:
                try:
                    bw = int(attrs["BANDWIDTH"])
                except Exception:
                    bw = None
            if "RESOLUTION" in attrs:
                res = attrs["RESOLUTION"]
            if "NAME" in attrs:
                label = attrs["NAME"]
            # next non-comment line is URL
        elif not line.startswith("#"):
            # line is a URI
            uri = line
            if last_inf is None:
                # variant without preceding EXT-X-STREAM-INF - treat as simple
                variants.append({"bandwidth": None, "resolution": None, "label": None, "url": urljoin(base_url, uri)})
            else:
                variants.append({"bandwidth": bw, "resolution": res, "label": label, "url": urljoin(base_url, uri)})
                last_inf = None
    return variants

def parse_media_playlist_get_segments(text):
    """
    Optionally parse media playlist for segment URIs (not used now, but available if needed).
    """
    lines = [l.strip() for l in text.splitlines() if l.strip() != ""]
    segments = []
    for line in lines:
        if line.startswith("#"):
            continue
        # relative or absolute URI
        segments.append(line)
    return segments

def resolve_candidate(session, candidate_url, timeout=10, verify=False):
    """
    Try to resolve candidate URL to m3u8(s).
    Returns dict with:
      original_url, probes: {head, get}, is_hls(bool), is_master(bool), m3u8_variants(list), media_playlist(bool)
    """
    result = {"original_url": candidate_url, "probes": {}, "is_hls": False, "is_master": False, "variants": [], "media_playlist": False, "notes": None}
    # HEAD first
    h = head(session, candidate_url, timeout=timeout)
    result["probes"]["head"] = h
    # If head indicates content-type m3u8, do GET small
    need_get = True
    ctype = ""
    if isinstance(h, dict) and "headers" in h and h.get("status") and h["status"] < 400:
        headers = h.get("headers") or {}
        ctype = headers.get("Content-Type","").lower()
        if "mpegurl" in ctype or "vnd.apple.mpegurl" in ctype or candidate_url.lower().endswith(".m3u8"):
            need_get = True
    # GET small content
    g = get_text(session, candidate_url, timeout=timeout)
    result["probes"]["get"] = g
    if "error" in g:
        result["notes"] = "GET-error"
        return result
    text = g.get("text","")
    if is_hls_playlist(text):
        result["is_hls"] = True
        # determine master vs media
        if "#EXT-X-STREAM-INF" in text:
            result["is_master"] = True
            variants = parse_master_playlist(text, g.get("effective_url") or candidate_url)
            result["variants"] = variants
            result["media_playlist"] = False
        else:
            result["is_master"] = False
            result["media_playlist"] = True
            # the playlist itself is media - record as single variant
            result["variants"] = [{"bandwidth": None, "resolution": None, "label": None, "url": g.get("effective_url") or candidate_url}]
    else:
        result["notes"] = "not_hls"
    # If verify requested, optionally try GET each variant to ensure playable
    if verify and result["variants"]:
        verified = []
        for v in result["variants"]:
            vv = v.copy()
            try:
                probe = get_text(session, v["url"], timeout=timeout)
                vv["probe_status"] = probe.get("status")
                vv["probe_effective_url"] = probe.get("effective_url")
                vv["probe_hls"] = is_hls_playlist(probe.get("text",""))
            except Exception as e:
                vv["probe_error"] = str(e)
            verified.append(vv)
        result["variants"] = verified
    return result

def process_match_item(session, match_obj, timeout=10, verify=False):
    """
    Given a normalized match object (from matches_clean.json), inspect its resolved_streams and return:
    {
      matchId, title, resolved_m3u8: [ { original_url, probes..., is_hls, is_master, variants: [...] } ]
    }
    """
    matchId = match_obj.get("matchId")
    title = match_obj.get("title")
    streams = match_obj.get("resolved_streams") or []
    out = {"matchId": matchId, "title": title, "checked_at": now_iso(), "resolved_m3u8": []}
    for s in streams:
        url = s.get("url") if isinstance(s, dict) else s
        label = s.get("label") if isinstance(s, dict) else None
        if not url:
            continue
        try:
            res = resolve_candidate(session, url, timeout=timeout, verify=verify)
            res["source_label"] = label
            out["resolved_m3u8"].append(res)
            # If this candidate is master with variants, also expand each variant into top-level list
            # (we keep variants nested; normalizer can pick best)
        except Exception as e:
            out["resolved_m3u8"].append({"original_url": url, "error": str(e)})
    return out

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def write_atomic(path, obj):
    tmp = Path(path).with_suffix(".partial.json")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
    tmp.replace(path)

def main():
    p = argparse.ArgumentParser(prog="extract_m3u8", description="Extract m3u8 playlists from matches_clean.json")
    p.add_argument("--in", dest="input", required=True, help="Input matches_clean.json")
    p.add_argument("--out", dest="output", required=True, help="Output matcheswithm3u8.json")
    p.add_argument("--workers", dest="workers", type=int, default=8, help="Concurrency workers")
    p.add_argument("--timeout", dest="timeout", type=float, default=10, help="Per-request timeout (seconds)")
    p.add_argument("--retries", dest="retries", type=int, default=2, help="Retry attempts")
    p.add_argument("--backoff", dest="backoff", type=float, default=0.3, help="Retry backoff")
    p.add_argument("--verify", dest="verify", action="store_true", help="Verify candidate m3u8 by fetching small body")
    p.add_argument("--resume", dest="resume", action="store_true", help="Resume from existing output file")
    args = p.parse_args()

    session = create_session(retries=args.retries, backoff=args.backoff)
    session.headers.update({"User-Agent": USER_AGENT})

    data = load_json(args.input)
    matches = data.get("matches") or []

    out_path = Path(args.output)
    existing = {}
    if args.resume and out_path.exists():
        try:
            existing = load_json(out_path)["matches_map"]
            logger.info("Resuming, loaded %d already-processed matches", len(existing))
        except Exception:
            existing = {}

    results = {"generated_at": now_iso(), "input": str(args.input), "matches_map": {}, "matches_list": []}

    # Build list of work items
    work_items = []
    for m in matches:
        mid = m.get("matchId")
        if mid and str(mid) in existing:
            continue
        work_items.append(m)

    logger.info("Processing %d matches with %d workers (verify=%s)", len(work_items), args.workers, args.verify)

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        future_map = {ex.submit(process_match_item, session, m, args.timeout, args.verify): m for m in work_items}
        for fut in tqdm(as_completed(future_map), total=len(future_map), desc="matches"):
            m = future_map[fut]
            try:
                item_res = fut.result()
            except Exception as e:
                logger.exception("Error processing match %s: %s", m.get("matchId"), e)
                item_res = {"matchId": m.get("matchId"), "error": str(e)}
            # store
            results["matches_list"].append(item_res)
            key = str(item_res.get("matchId") or m.get("matchId") or m.get("title"))
            results["matches_map"][key] = item_res
            # write incremental
            try:
                write_atomic(args.output, results)
            except Exception as e:
                logger.exception("Failed to write partial output: %s", e)

    logger.info("Completed. Writing final output to %s", args.output)
    write_atomic(args.output, results)
    print("Done. Output:", args.output)

if __name__ == "__main__":
    main()
