#!/usr/bin/env python3
"""
scraper.py

Concurrent, robust WatchFooty + SpiderEmbed extractor.

Usage:
  pip install requests tqdm urllib3
  python scraper.py --base https://api.watchfooty.st --embed https://spiderembed.top \
      --out watchfooty_full.json --referer "https://www.watchfooty.st" --rate 0.4 \
      --timeout 15 --retries 2 --backoff 0.3 --workers 12 --check-images --resume

Notes:
 - READ ONLY: script performs GET/HEAD requests only.
 - Resume support: use --resume to load existing output.
"""
import argparse
import json
import time
import sys
import logging
from urllib.parse import urljoin, urlparse, parse_qs
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

try:
    import requests
    from requests.exceptions import RequestException
    from urllib3.util import Retry
    from requests.adapters import HTTPAdapter
except Exception:
    print("Please install required libraries: pip install requests tqdm urllib3", file=sys.stderr)
    raise

try:
    from tqdm import tqdm
except Exception:
    tqdm = lambda x, **k: x

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stderr)
logger = logging.getLogger("scraper")

USER_AGENT = "watchfooty-extractor/1.2 (+https://github.com/)"

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def build_url(base, path):
    if not base:
        return path
    if path.startswith("http://") or path.startswith("https://"):
        return path
    return urljoin(base.rstrip("/") + "/", path.lstrip("/"))

def create_session(retries=2, backoff=0.3):
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        status_forcelist=(429, 500, 502, 503, 504),
        backoff_factor=backoff,
        allowed_methods=frozenset(["GET","HEAD","OPTIONS"])
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

def safe_get(session, url, headers=None, params=None, timeout=15, allow_redirects=True):
    out = {"requested_url": url, "timestamp": now_iso(), "status_code": None, "error": None, "effective_url": None,
           "response_json": None, "response_text": None, "response_headers": None}
    try:
        resp = session.get(url, headers=headers or {}, params=params or {}, timeout=timeout, allow_redirects=allow_redirects)
        out["status_code"] = resp.status_code
        out["effective_url"] = resp.url
        out["response_headers"] = dict(resp.headers)
        ctype = resp.headers.get("Content-Type","")
        text = resp.text
        if resp.status_code == 200 and ("application/json" in ctype or text.strip().startswith("{") or text.strip().startswith("[")):
            try:
                out["response_json"] = resp.json()
            except Exception as je:
                out["error"] = "json_decode_error: " + str(je)
                out["response_text"] = text[:20000]
        else:
            out["response_text"] = text[:20000]
    except RequestException as e:
        out["error"] = str(e)
    return out

def safe_head(session, url, headers=None, timeout=6):
    out = {"requested_url": url, "timestamp": now_iso(), "status_code": None, "error": None, "effective_url": None, "response_headers": None}
    try:
        resp = session.head(url, headers=headers or {}, timeout=timeout, allow_redirects=True)
        out["status_code"] = resp.status_code
        out["effective_url"] = resp.url
        out["response_headers"] = dict(resp.headers)
    except RequestException as e:
        out["error"] = str(e)
    return out

def resolve_spider_embed(session, embed_host, stream_url, referer=None, timeout=12):
    """
    Try to resolve spiderembed tokens using /api/get?id= or /api/get?url=
    Return a dict with original_url, resolved(bool), api_probe, discovered_streams(list)
    """
    res = {"original_url": stream_url, "resolved": False, "api_probe": None, "discovered_streams": []}
    if not stream_url:
        return res
    low = stream_url.lower()
    if any(ext in low for ext in [".m3u8", ".mp4", ".m3u", "videoplayback"]):
        res["resolved"] = True
        res["discovered_streams"] = [{"label":"direct","url":stream_url}]
        return res

    parsed = urlparse(stream_url)
    q = parse_qs(parsed.query)
    headers = {"User-Agent": USER_AGENT}
    if referer:
        headers["Referer"] = referer

    try_hosts = []
    if parsed.netloc and ("spiderembed" in parsed.netloc or "spider" in parsed.netloc):
        try_hosts.append(f"{parsed.scheme}://{parsed.netloc}")
    if embed_host:
        try_hosts.append(embed_host.rstrip("/"))

    candidate_ids = []
    if "id" in q:
        candidate_ids += q.get("id",[])
    if "token" in q:
        candidate_ids += q.get("token",[])
    path_parts = [p for p in parsed.path.split("/") if p]
    if path_parts:
        candidate_ids.append(path_parts[-1])

    for host in try_hosts:
        api_get = build_url(host, "/api/get")
        # try id-based
        for cid in candidate_ids:
            probe_url = api_get + "?id=" + cid
            probe = safe_get(session, probe_url, headers=headers, timeout=timeout)
            res["api_probe"] = probe
            if probe.get("status_code")==200 and probe.get("response_json"):
                j = probe["response_json"]
                streams = []
                if isinstance(j, dict):
                    if "streams" in j and isinstance(j["streams"], list):
                        streams = j["streams"]
                    elif "resolvedUrl" in j:
                        streams = [{"label":"resolved","url":j.get("resolvedUrl")}]
                    elif "url" in j:
                        streams = [{"label":"resolved","url":j.get("url")}]
                if streams:
                    res["resolved"] = True
                    res["discovered_streams"] = streams
                    return res
        # try url param
        probe = safe_get(session, api_get, headers=headers, params={"url": stream_url}, timeout=timeout)
        res["api_probe"] = probe
        if probe.get("status_code")==200 and probe.get("response_json"):
            j = probe["response_json"]
            streams = []
            if isinstance(j, dict):
                if "streams" in j and isinstance(j["streams"], list):
                    streams = j["streams"]
                elif "resolvedUrl" in j:
                    streams = [{"label":"resolved","url":j.get("resolvedUrl")}]
                elif "url" in j:
                    streams = [{"label":"resolved","url":j.get("url")}]
            if streams:
                res["resolved"] = True
                res["discovered_streams"] = streams
                return res
    return res

def fetch_matches_for_sport(session, base_api, sport, timeout, rate_delay):
    """
    More tolerant fetching: try multiple common endpoint shapes.
    Returns the safe_get probe object.
    """
    time.sleep(rate_delay)
    candidates = [
        f"/api/v1/matches/{sport}",                    # existing assumption
        f"/api/v1/matches?sport={sport}",              # query param style
        f"/api/v1/sports/{sport}/matches",             # alternate nested style
        f"/api/v1/matches?league={sport}",             # sometimes 'league' used
    ]
    last_probe = None
    for path in candidates:
        url = build_url(base_api, path)
        logger.debug("Trying matches URL: %s", url)
        probe = safe_get(session, url, timeout=timeout)
        last_probe = probe
        if probe.get("status_code") == 200 and (probe.get("response_json") or probe.get("response_text")):
            probe["attempted_path"] = path
            return probe
    return last_probe or {"requested_url": build_url(base_api, f"/api/v1/matches/{sport}"), "error": "no_probe_made"}

def fetch_match_detail(session, base_api, match_id, timeout, rate_delay):
    time.sleep(rate_delay)
    url = build_url(base_api, f"/api/v1/match/{match_id}")
    return safe_get(session, url, timeout=timeout)

def process_match(session, base_api, embed_host, m_basic, timeout, rate_delay, referer_base, check_images):
    """
    Fetch match details, collect poster/logo endpoints, optionally check images (HEAD),
    resolve streams (SpiderEmbed) with a short timeout.
    Returns enriched match object.
    """
    match_id = None
    if isinstance(m_basic, dict):
        match_id = m_basic.get("matchId") or m_basic.get("id") or m_basic.get("match_id")
    out = {"basic": m_basic, "match_probe": None, "poster_probe": None, "team_logos": {}, "league_logo": None, "embed_resolutions": [], "match_id": match_id}
    if not match_id:
        out["error"] = "missing_match_id"
        return out

    # match detail
    match_url = build_url(base_api, f"/api/v1/match/{match_id}")
    r = safe_get(session, match_url, timeout=timeout)
    out["match_probe"] = r

    # poster id
    poster_id = None
    if isinstance(m_basic, dict):
        poster_id = m_basic.get("poster") or m_basic.get("posterId") or m_basic.get("poster_id")
    match_json = r.get("response_json") or {}
    if not poster_id and isinstance(match_json, dict):
        poster_id = match_json.get("poster") or match_json.get("posterId") or match_json.get("poster_id")
    if poster_id:
        poster_url = build_url(base_api, f"/api/v1/poster/{poster_id}")
        out["poster_probe"] = {"url": poster_url}
        if check_images:
            out["poster_probe"]["head"] = safe_head(session, poster_url, timeout=min(6, timeout))

    # team logos
    teams = m_basic.get("teams") if isinstance(m_basic, dict) else None
    if isinstance(match_json, dict) and not teams:
        teams = match_json.get("teams")
    if teams and isinstance(teams, dict):
        for side in ("home","away"):
            t = teams.get(side)
            if t and isinstance(t, dict):
                lid = t.get("logoId") or t.get("logo_id") or t.get("logo")
                if lid:
                    logo_url = build_url(base_api, f"/api/v1/team-logo/{lid}")
                    out["team_logos"][side] = {"url": logo_url}
                    if check_images:
                        out["team_logos"][side]["head"] = safe_head(session, logo_url, timeout=min(6, timeout))

    # league logo
    league_logo_id = None
    if isinstance(m_basic, dict):
        league_logo_id = m_basic.get("leagueLogoId") or m_basic.get("league_logo_id") or m_basic.get("leagueLogo")
    if not league_logo_id and isinstance(match_json, dict):
        league_logo_id = match_json.get("leagueLogoId") or match_json.get("league_logo_id") or match_json.get("leagueLogo")
    if league_logo_id:
        ll_url = build_url(base_api, f"/api/v1/league-logo/{league_logo_id}")
        out["league_logo"] = {"url": ll_url}
        if check_images:
            out["league_logo"]["head"] = safe_head(session, ll_url, timeout=min(6, timeout))

    # streams
    streams = []
    if isinstance(match_json, dict):
        streams = match_json.get("streams") or match_json.get("sources") or (m_basic.get("streams") if isinstance(m_basic, dict) else [])
    else:
        streams = (m_basic.get("streams") if isinstance(m_basic, dict) else [])

    for s in streams:
        stream_url = None
        if isinstance(s, str):
            stream_url = s
        elif isinstance(s, dict):
            stream_url = s.get("url") or s.get("resolvedUrl") or s.get("source")
        if not stream_url:
            continue
        resolved = resolve_spider_embed(session, embed_host, stream_url, referer=referer_base, timeout=min(12, timeout))
        out["embed_resolutions"].append({"stream_meta": s, "resolution": resolved})

    return out

def load_existing_output(path):
    if not Path(path).exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning("Failed to load existing output file: %s", e)
        return None

def write_output_atomic(path, data):
    tmp = Path(path).with_suffix(".partial.json")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    tmp.replace(path)

def main():
    p = argparse.ArgumentParser(prog="scraper", description="Concurrent WatchFooty + SpiderEmbed extractor")
    p.add_argument("--base", dest="base_api", default="https://api.watchfooty.st", help="Base API host")
    p.add_argument("--embed", dest="embed_host", default="https://spiderembed.top", help="Embed provider host")
    p.add_argument("--out", dest="out_file", default="watchfooty_full.json", help="Output JSON file")
    p.add_argument("--sport", dest="sports", action="append", help="Specific sport slug to scan (e.g., football)")
    p.add_argument("--rate", "--rate-delay", dest="rate_delay", type=float, default=0.4, help="Delay seconds between requests (alias --rate-delay)")
    p.add_argument("--referer", dest="referer", help="Referer header to use for embed resolution (match page URL)")
    p.add_argument("--timeout", dest="timeout", type=float, default=15, help="Per-request timeout in seconds")
    p.add_argument("--retries", dest="retries", type=int, default=2, help="Number of retries for transient errors")
    p.add_argument("--backoff", dest="backoff", type=float, default=0.3, help="Backoff factor for retries")
    p.add_argument("--workers", dest="workers", type=int, default=8, help="Number of concurrent worker threads for match detail fetches")
    p.add_argument("--check-images", dest="check_images", action="store_true", help="Perform HEAD checks for poster/team/league images (fast). Off by default.")
    p.add_argument("--resume", dest="resume", action="store_true", help="Resume from existing output file if present.")
    args = p.parse_args()

    session = create_session(retries=args.retries, backoff=args.backoff)
    session.headers.update({"User-Agent": USER_AGENT})
    out_file = args.out_file
    existing = load_existing_output(out_file) if args.resume else None

    # base skeleton
    output = {
        "scanned_at": now_iso(),
        "base_api": args.base_api,
        "embed_host": args.embed_host,
        "sports": [],
        "matches": {},
        "errors": []
    }
    processed_match_ids = set()
    if existing:
        logger.info("Resuming from existing output: %s", out_file)
        # keep existing full blob, but ensure we still update scanned_at etc.
        output = existing
        # gather processed match ids
        for sport_block in output.get("matches", {}).values():
            items = sport_block.get("items", [])
            for it in items:
                if isinstance(it, dict) and it.get("match_id"):
                    processed_match_ids.add(str(it.get("match_id")))
                else:
                    basic = it.get("basic") if isinstance(it, dict) else None
                    if isinstance(basic, dict):
                        mid = basic.get("matchId") or basic.get("id") or basic.get("match_id")
                        if mid:
                            processed_match_ids.add(str(mid))

    try:
        # 1) fetch sports
        logger.info("Fetching sports list...")
        sports_url = build_url(args.base_api, "/api/v1/sports")
        sres = safe_get(session, sports_url, timeout=args.timeout)
        output["sports_probe"] = sres
        sports = []
        if sres.get("response_json"):
            sports = sres["response_json"]
            output["sports"] = sports
        else:
            logger.warning("Sports endpoint returned no JSON. response_text head: %s", (sres.get("response_text") or "")[:500])
            output.setdefault("errors", []).append({"stage":"sports","detail":sres})

        # derive candidate slugs robustly: prefer slug -> id -> name -> key
        slugs = []
        if args.sports:
            slugs = args.sports
        else:
            if isinstance(sports, list):
                for s in sports:
                    if not isinstance(s, dict):
                        continue
                    candidate = s.get("slug") or s.get("id") or s.get("name") or s.get("key")
                    if candidate:
                        slugs.append(str(candidate))
            else:
                logger.warning("Unexpected sports format: %s", type(sports))

        logger.info("Resolved %d sport slugs to query: %s", len(slugs), slugs)

        # 2) fetch matches per sport (sequential; each returned block will be processed concurrently)
        logger.info("Fetching matches for %d sports...", len(slugs))
        for sport in slugs:
            logger.info("Fetching matches for sport: %s", sport)
            mres = fetch_matches_for_sport(session, args.base_api, sport, timeout=args.timeout, rate_delay=args.rate_delay)
            js = mres.get("response_json")
            sample = None
            if isinstance(js, list):
                sample = {"type":"list","len":len(js), "head": js[:3]}
                items = js
            elif isinstance(js, dict):
                if "items" in js and isinstance(js["items"], list):
                    items = js["items"]
                    sample = {"type":"dict_with_items","len":len(items), "head": items[:3]}
                elif "matches" in js and isinstance(js["matches"], list):
                    items = js["matches"]
                    sample = {"type":"dict_with_matches","len":len(items), "head": items[:3]}
                else:
                    # treat the dict itself as a single match item
                    items = [js]
                    sample = {"type":"single_dict_wrapped","len":1, "keys": list(js.keys())[:10]}
            else:
                items = []
                sample = {"type":"unknown","repr": (mres.get("response_text") or "")[:300]}

            logger.info("Matches probe sample for %s: %s", sport, sample)

            # Merge-on-resume: if we have existing output and it already contains items for this sport, keep them.
            existing_items = None
            if existing:
                existing_items = output.get("matches", {}).get(sport, {}).get("items")
                if existing_items and isinstance(existing_items, list) and len(existing_items) > 0:
                    # Keep existing items, but log counts and still store the fresh probe for diagnostics
                    logger.info("Keeping %d existing items for sport %s (probe returned %d)", len(existing_items), sport, len(items))
                    output.setdefault("matches", {})[sport] = {"probe": mres, "items": existing_items}
                else:
                    # No existing items -> use fresh items (even if empty)
                    output.setdefault("matches", {})[sport] = {"probe": mres, "items": items}
            else:
                output.setdefault("matches", {})[sport] = {"probe": mres, "items": items}

        # 3) prepare list of all basic match objects to process (skip already processed)
        to_process = []
        for sport, block in output.get("matches", {}).items():
            items = block.get("items") or []
            for m in items:
                mid = None
                if isinstance(m, dict):
                    mid = m.get("matchId") or m.get("id") or m.get("match_id")
                # if no id but dict-like, still process (process_match will mark missing)
                if mid and str(mid) in processed_match_ids:
                    continue
                to_process.append((sport, m))

        logger.info("Total matches to process: %d (workers=%d)", len(to_process), args.workers)

        # 4) concurrent processing of matches
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            future_to_match = {}
            for sport, m in to_process:
                future = ex.submit(process_match, session, args.base_api, args.embed_host, m, args.timeout, args.rate_delay, args.referer, args.check_images)
                future_to_match[future] = (sport, m)

            for fut in tqdm(as_completed(future_to_match), total=len(future_to_match), desc="processing matches"):
                sport, basic = future_to_match[fut]
                try:
                    enriched = fut.result()
                except Exception as e:
                    logger.exception("Exception processing match: %s", e)
                    enriched = {"basic": basic, "error": str(e)}
                output.setdefault("matches", {})
                output.setdefault("matches", {}).setdefault(sport, {"probe": output["matches"].get(sport, {}).get("probe"), "items": output["matches"].get(sport, {}).get("items", [])})
                output["matches"][sport]["items"].append(enriched)
                # write incrementally
                write_output_atomic(out_file, output)

    except KeyboardInterrupt:
        logger.warning("Interrupted by user. Saving partial results to %s", out_file)
    except Exception as e:
        logger.exception("Unhandled exception: %s", e)
        output.setdefault("errors", []).append({"stage":"exception","detail":str(e)})
    finally:
        output["finished_at"] = now_iso()
        try:
            write_output_atomic(out_file, output)
            logger.info("Final output written to %s", out_file)
        except Exception as e:
            logger.exception("Failed to write final output: %s", e)

if __name__ == "__main__":
    main()
