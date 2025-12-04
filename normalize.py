#!/usr/bin/env python3
"""
normalize.py

Read a full extractor output (watchfooty_full.json) and produce a compact matches_clean.json.

Usage:
  python normalize.py --in watchfooty_full.json --out matches_clean.json

Options:
  --only-live       Keep only matches that appear to be live (heuristic).
  --only-upcoming   Keep only upcoming matches (heuristic).
  --verbose         Print progress.

Heuristics for live/upcoming:
 - live: `currentMinute` present in match JSON or `status` contains 'live' or `isEvent` True
 - upcoming: match timestamp in future (unix) or `date`/`start` exists and is after now
"""

import argparse
import json
import sys
from datetime import datetime, timezone

def to_iso(ts):
    """
    Convert a timestamp value (int seconds, int ms, or ISO string) to ISO-8601 UTC string.
    Returns None if conversion fails.
    """
    if ts is None:
        return None
    # If already ISO-like
    if isinstance(ts, str):
        try:
            # attempt to parse many ISO formats by letting datetime.fromisoformat handle it (Python 3.11+)
            # fallback: return as-is if it already contains timezone marker
            if "T" in ts:
                try:
                    dt = datetime.fromisoformat(ts)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt.astimezone(timezone.utc).isoformat()
                except Exception:
                    return ts
        except Exception:
            return ts
    # If numeric: seconds or milliseconds
    try:
        n = int(ts)
        # heuristic: if > 10^12 treat as ms
        if n > 10**12:
            n = n / 1000.0
        dt = datetime.fromtimestamp(n, tz=timezone.utc)
        return dt.isoformat()
    except Exception:
        return None

def pick(d, keys, default=None):
    """Pick first existing key from dict d among keys (list)."""
    if not isinstance(d, dict):
        return default
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default

def extract_logo_url(logo_probe_or_record):
    """
    Extract a sensible URL for image/logo/poster from probe objects or plain strings.
    The extractor typically stores either:
     - a dict with 'url'
     - a probe dict with response_json/response_text or effective_url
    """
    if not logo_probe_or_record:
        return None
    if isinstance(logo_probe_or_record, str):
        return logo_probe_or_record
    if isinstance(logo_probe_or_record, dict):
        # common shapes:
        # {"url": "..."}
        if "url" in logo_probe_or_record and isinstance(logo_probe_or_record["url"], str):
            return logo_probe_or_record["url"]
        # {"requested_url": "..."}
        if "requested_url" in logo_probe_or_record:
            return logo_probe_or_record["requested_url"]
        # probe might include 'effective_url' or 'response_headers' with Location; prefer effective_url
        if "response_json" in logo_probe_or_record and isinstance(logo_probe_or_record["response_json"], dict):
            # sometimes the API returns direct image URL inside JSON
            # try to find likely keys
            for k in ("url","image","src","link"):
                if k in logo_probe_or_record["response_json"] and isinstance(logo_probe_or_record["response_json"][k], str):
                    return logo_probe_or_record["response_json"][k]
        if logo_probe_or_record.get("effective_url"):
            return logo_probe_or_record.get("effective_url")
        # fallback to requested_url
        if logo_probe_or_record.get("requested_url"):
            return logo_probe_or_record.get("requested_url")
    return None

def collect_streams_from_enriched(enriched):
    """
    Given an enriched match object (as produced by the extractor), return list of {label, url}
    - Check embed_resolutions[*].resolution.discovered_streams
    - Check match_probe.response_json streams/sources
    - Check basic['streams']
    """
    streams = []
    # 1) embed_resolutions
    for er in (enriched.get("embed_resolutions") or []):
        res = er.get("resolution") or {}
        # if resolved, check discovered_streams list
        for ds in (res.get("discovered_streams") or []):
            if isinstance(ds, dict):
                url = ds.get("url") or ds.get("resolvedUrl") or ds.get("src")
                label = ds.get("label") or ds.get("quality") or ds.get("resolution")
                if url:
                    streams.append({"label": label or "resolved", "url": url})
    # 2) match_probe payload
    mp = enriched.get("match_probe") or {}
    mj = mp.get("response_json") if isinstance(mp, dict) else None
    if isinstance(mj, dict):
        for key in ("streams","sources","videos"):
            val = mj.get(key)
            if isinstance(val, list):
                for s in val:
                    if isinstance(s, str):
                        streams.append({"label": None, "url": s})
                    elif isinstance(s, dict):
                        url = s.get("url") or s.get("resolvedUrl") or s.get("src")
                        label = s.get("label") or s.get("quality") or s.get("language")
                        if url:
                            streams.append({"label": label, "url": url})
    # 3) basic object streams
    basic = enriched.get("basic") or {}
    bstreams = basic.get("streams") or basic.get("sources")
    if isinstance(bstreams, list):
        for s in bstreams:
            if isinstance(s, str):
                streams.append({"label": None, "url": s})
            elif isinstance(s, dict):
                url = s.get("url") or s.get("resolvedUrl") or s.get("source")
                label = s.get("label") or s.get("quality")
                if url:
                    streams.append({"label": label, "url": url})
    # normalize: keep only unique final URLs and prefer known video extensions
    seen = set()
    normalized = []
    for s in streams:
        u = s.get("url")
        if not u:
            continue
        if u in seen:
            continue
        seen.add(u)
        normalized.append({"label": s.get("label"), "url": u})
    return normalized

def is_match_live(enriched):
    """
    Heuristic: look for currentMinute, status='live', isEvent true, or match_probe shows minute info
    """
    basic = enriched.get("basic") or {}
    mp = enriched.get("match_probe") or {}
    mj = mp.get("response_json") if isinstance(mp, dict) else None
    # check keys in match data
    for candidate in (basic, mj):
        if isinstance(candidate, dict):
            if candidate.get("currentMinute") or candidate.get("minute") or candidate.get("isEvent") is True:
                return True
            status = candidate.get("status") or candidate.get("state")
            if isinstance(status, str) and "live" in status.lower():
                return True
    return False

def is_match_upcoming(enriched):
    """
    Heuristic: examine timestamps and date fields; if start time in future -> upcoming
    """
    now = datetime.now(timezone.utc)
    basic = enriched.get("basic") or {}
    mp = enriched.get("match_probe") or {}
    mj = mp.get("response_json") if isinstance(mp, dict) else None

    # candidate timestamp fields
    for candidate in (basic, mj):
        if not isinstance(candidate, dict):
            continue
        # common keys: 'timestamp', 'date', 'start', 'startTime'
        for k in ("timestamp","startTimestamp","start","date","time","start_time"):
            if k in candidate and candidate[k]:
                iso = to_iso(candidate[k])
                if iso:
                    try:
                        dt = datetime.fromisoformat(iso)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        if dt > now:
                            return True
                        else:
                            return False
                    except Exception:
                        pass
    return False

def normalize_one(enriched):
    basic = enriched.get("basic") or {}
    mp = enriched.get("match_probe") or {}
    mj = mp.get("response_json") if isinstance(mp, dict) else None

    match_id = basic.get("matchId") or basic.get("id") or basic.get("match_id") or enriched.get("match_id")
    title = pick(basic, ["title","name","matchTitle"]) or (mj.get("title") if isinstance(mj, dict) else None)
    league = pick(basic, ["league","competition","tournament"]) or (mj.get("league") if isinstance(mj, dict) else None)

    # start time / timestamp
    start = pick(basic, ["timestamp","date","start","startTimestamp","start_time"]) or (mj.get("timestamp") if isinstance(mj, dict) else None)
    start_iso = to_iso(start)

    # teams
    teams_obj = {}
    teams = basic.get("teams") or (mj.get("teams") if isinstance(mj, dict) else None)
    if isinstance(teams, dict):
        for side in ("home","away"):
            t = teams.get(side) or {}
            name = pick(t, ["name","title","teamName"]) or None
            logo = None
            # if t contains logoUrl or logo info
            if isinstance(t, dict):
                logo = t.get("logoUrl") or t.get("logo") or t.get("logo_url") or t.get("logoUrl")
            teams_obj[side] = {"name": name, "logo_url": logo}
    else:
        # try alternative fields
        home = pick(basic, ["homeTeam","team_home","home"])
        away = pick(basic, ["awayTeam","team_away","away"])
        teams_obj["home"] = {"name": home and (home.get("name") if isinstance(home, dict) else home), "logo_url": None}
        teams_obj["away"] = {"name": away and (away.get("name") if isinstance(away, dict) else away), "logo_url": None}

    # score
    score = {}
    if isinstance(mj, dict):
        # try common keys
        home_score = pick(mj, ["homeScore","home_score","scoreHome","score_home"])
        away_score = pick(mj, ["awayScore","away_score","scoreAway","score_away"])
        if home_score is None and isinstance(mj.get("score"), dict):
            home_score = mj["score"].get("home")
            away_score = mj["score"].get("away")
        if home_score is not None or away_score is not None:
            score = {"home": home_score, "away": away_score}

    # poster
    poster = None
    # try poster_probe first
    pprobe = enriched.get("poster_probe")
    poster = extract_logo_url(pprobe) or pick(basic, ["poster","posterUrl","poster_url","thumbnail"]) or (mj.get("poster") if isinstance(mj, dict) else None)

    # resolved streams
    streams = collect_streams_from_enriched(enriched)

    normalized = {
        "matchId": str(match_id) if match_id is not None else None,
        "title": title,
        "league": league,
        "start_time": start_iso,
        "teams": teams_obj,
        "score": score or None,
        "poster_url": poster,
        "resolved_streams": streams,
        # minimal raw for reference (can be omitted)
        "raw": {
            "basic_keys": list(basic.keys()) if isinstance(basic, dict) else None,
            "match_probe_status": mp.get("status_code") if isinstance(mp, dict) else None
        }
    }
    return normalized

def normalize_all(input_path, output_path, only_live=False, only_upcoming=False, verbose=False):
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    matches_clean = []
    matches_block = data.get("matches") or {}
    total = sum(len(v.get("items") or []) for v in matches_block.values())
    if verbose:
        print(f"Processing {total} total matches from {len(matches_block)} sports...")

    processed = 0
    for sport, block in matches_block.items():
        items = block.get("items") or []
        for enriched in items:
            processed += 1
            if verbose and processed % 50 == 0:
                print(f"  processed {processed}/{total} ...")
            # ensure enriched is the enriched object: sometimes entries may be raw basic objects
            # if it's already enriched, it will have 'match_probe' etc; if not wrap
            if not isinstance(enriched, dict) or ("match_probe" not in enriched and "basic" not in enriched):
                # legacy: item is basic only
                enriched_wrapped = {"basic": enriched}
            else:
                enriched_wrapped = enriched

            # apply filters
            if only_live and not is_match_live(enriched_wrapped):
                continue
            if only_upcoming and not is_match_upcoming(enriched_wrapped):
                continue

            norm = normalize_one(enriched_wrapped)
            matches_clean.append(norm)

    # write output
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({"generated_at": datetime.now(timezone.utc).isoformat(), "count": len(matches_clean), "matches": matches_clean}, f, indent=2, ensure_ascii=False)
    if verbose:
        print(f"Wrote {len(matches_clean)} normalized matches to {output_path}")

def main():
    p = argparse.ArgumentParser(prog="normalize", description="Normalize watchfooty_full.json to compact matches_clean.json")
    p.add_argument("--in", dest="input_path", required=True, help="Input JSON (watchfooty_full.json)")
    p.add_argument("--out", dest="output_path", required=True, help="Output JSON (matches_clean.json)")
    p.add_argument("--only-live", dest="only_live", action="store_true", help="Keep only live matches (heuristic)")
    p.add_argument("--only-upcoming", dest="only_upcoming", action="store_true", help="Keep only upcoming matches (heuristic)")
    p.add_argument("--verbose", dest="verbose", action="store_true", help="Verbose")
    args = p.parse_args()

    normalize_all(args.input_path, args.output_path, only_live=args.only_live, only_upcoming=args.only_upcoming, verbose=args.verbose)

if __name__ == "__main__":
    main()
