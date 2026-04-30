# streamlit_app.py
# Curtley James — BMI / Central
# Nexus Trading Address Finder — Render + Google Drive Edition
# Run with: streamlit run streamlit_app.py

import streamlit as st
import pandas as pd
import urllib.request
import urllib.error
import urllib.parse
import re
import time
import csv
import os
import io
import json
import threading
import queue

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
    HAS_GDRIVE = True
except ImportError:
    HAS_GDRIVE = False

# ---------------------------------------------------------------------------
# Company-name-keyed disk cache
# ---------------------------------------------------------------------------
CACHE_PATH = "address_cache.json"
_cache_lock = threading.Lock()


def _normalise_key(name: str) -> str:
    """Lowercase, strip punctuation/whitespace — stable cache key."""
    return re.sub(r"[^a-z0-9]", "", name.lower().strip())


def cache_load() -> dict:
    try:
        if os.path.exists(CACHE_PATH):
            with open(CACHE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def cache_save(cache: dict) -> None:
    try:
        tmp = CACHE_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
        os.replace(tmp, CACHE_PATH)
    except Exception:
        pass


def cache_get(cache: dict, name: str):
    """Return cached address dict for a company name, or None."""
    return cache.get(_normalise_key(name))


def cache_set(cache: dict, name: str, addr: dict) -> None:
    """Write one entry and flush to disk (thread-safe)."""
    key = _normalise_key(name)
    with _cache_lock:
        cache[key] = addr
        cache_save(cache)


# ---------------------------------------------------------------------------
# UK & Ireland geographic reference data
# ---------------------------------------------------------------------------
UK_CITIES = {
    "london","manchester","birmingham","leeds","glasgow","sheffield","bradford",
    "edinburgh","liverpool","bristol","cardiff","coventry","nottingham","leicester",
    "sunderland","belfast","newcastle","brighton","hull","plymouth","stoke",
    "wolverhampton","derby","swansea","southampton","salford","aberdeen","westminster",
    "portsmouth","york","peterborough","dundee","lancaster","oxford","cambridge",
    "norwich","bath","exeter","gloucester","lincoln","chester","worcester","hereford",
    "truro","chichester","winchester","salisbury","ely","ripon","wakefield",
    "inverness","stirling","perth","paisley","hamilton","east kilbride",
    "livingston","cumbernauld","dunfermline","kirkcaldy","ayr","motherwell",
    "londonderry","derry","lisburn","newtownabbey","bangor","craigavon","armagh",
    "newry","antrim","omagh",
    "dublin","cork","limerick","galway","waterford","drogheda","dundalk","swords",
    "bray","navan","ennis","tralee","kilkenny","carlow","sligo","athlone","clonmel",
    "wexford","mullingar","letterkenny","celbridge","leixlip","naas","portlaoise",
    "dun laoghaire","dunlaoghaire","tallaght","blanchardstown",
}
IE_COUNTIES = {
    "carlow","cavan","clare","cork","donegal","dublin","galway","kerry","kildare",
    "kilkenny","laois","leitrim","limerick","longford","louth","mayo","meath",
    "monaghan","offaly","roscommon","sligo","tipperary","waterford","westmeath",
    "wexford","wicklow","antrim","armagh","down","fermanagh","londonderry","tyrone",
}
UK_COUNTIES = {
    "bedfordshire","berkshire","bristol","buckinghamshire","cambridgeshire","cheshire",
    "city of london","cornwall","cumbria","derbyshire","devon","dorset","durham",
    "east riding of yorkshire","east sussex","essex","gloucestershire","greater london",
    "greater manchester","hampshire","herefordshire","hertfordshire","isle of wight",
    "kent","lancashire","leicestershire","lincolnshire","merseyside","norfolk",
    "north yorkshire","northamptonshire","northumberland","nottinghamshire","oxfordshire",
    "rutland","shropshire","somerset","south yorkshire","staffordshire","suffolk",
    "surrey","tyne and wear","warwickshire","west midlands","west sussex",
    "west yorkshire","wiltshire","worcestershire",
    "aberdeenshire","angus","argyll and bute","clackmannanshire","dumfries and galloway",
    "east ayrshire","east dunbartonshire","east lothian","east renfrewshire",
    "falkirk","fife","highland","inverclyde","midlothian","moray",
    "north ayrshire","north lanarkshire","orkney islands","perth and kinross",
    "renfrewshire","scottish borders","shetland islands","south ayrshire",
    "south lanarkshire","stirling","west dunbartonshire","west lothian",
    "blaenau gwent","bridgend","caerphilly","carmarthenshire","ceredigion",
    "conwy","denbighshire","flintshire","gwynedd","isle of anglesey","merthyr tydfil",
    "monmouthshire","neath port talbot","newport","pembrokeshire","powys",
    "rhondda cynon taf","torfaen","vale of glamorgan","wrexham",
    "antrim and newtownabbey","ards and north down","armagh city banbridge and craigavon",
    "causeway coast and glens","derry city and strabane","fermanagh and omagh",
    "lisburn and castlereagh","mid and east antrim","mid ulster","newry mourne and down",
}
ALL_COUNTIES = UK_COUNTIES | IE_COUNTIES

UK_POSTCODE_RE = re.compile(r'\b([A-Z]{1,2}[0-9][0-9A-Z]?\s*[0-9][A-Z]{2})\b', re.I)
IE_EIRCODE_RE  = re.compile(r'\b([AC-FHKNPRTV][0-9]{2}\s*[0-9AC-FHKNPRTV]{4})\b', re.I)
STREET_NUM_RE  = re.compile(
    r'(\d{1,4}[A-Z]?\s*[-–]?\s*\d{0,4}[A-Z]?\s+'
    r'[A-Z][a-zA-Z\'\-]{2,}'
    r'(?:\s+[A-Z][a-zA-Z\'\-]{2,}){0,4}'
    r'(?:\s+(?:Street|St|Road|Rd|Lane|Ln|Avenue|Ave|Way|Drive|Dr|Close|Cl|'
    r'Court|Ct|Place|Pl|Terrace|Terr|Row|Gardens|Gdns|Grove|Gr|Crescent|Cres|'
    r'Square|Sq|Park|Industrial|Estate|Business|Boulevard|Blvd|Quay|Parade|'
    r'Rise|Hill|View|Walk|Mews|Yard|Wharf|Gate|Green|Cross|'
    r'House|Centre|Center|Floor|Unit|Suite))?)',
    re.I
)
TRADING_SIGNAL_RE = re.compile(
    r'(?:trading\s+(?:address|from)|visit\s+us|find\s+us|'
    r'our\s+(?:office|location|address|premises|store|shop|showroom|warehouse|depot)|'
    r'head\s*quarters|hq\s*:|main\s+office|contact\s+us|get\s+in\s+touch|'
    r'where\s+(?:to\s+)?find\s+us|how\s+to\s+find\s+us)',
    re.I
)
EXCLUDED_TERMS = re.compile(
    r'(?:p\.?\s*o\.?\s*box|po\s+box|virtual\s+office|mail(?:ing)?\s+address|'
    r'registered\s+address|companies\s+house|agent\s*:|c/o\b)',
    re.I
)
FETCH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}

OUTPUT_COLS = ["Company Name", "Website", "Country",
               "Street", "City", "State/County", "Post Code", "Found Country"]

HEADER_PATTERNS = {
    "name":    re.compile(r'company|business|organisation|organization|name', re.I),
    "website": re.compile(r'website|url|web|domain|site', re.I),
    "country": re.compile(r'country|nation|market|territory', re.I),
}

# ---------------------------------------------------------------------------
# Google Drive helpers
# ---------------------------------------------------------------------------
def _drive_service():
    if not HAS_GDRIVE:
        return None
    try:
        creds_dict = dict(st.secrets["gcp_service_account"])
        creds = service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=["https://www.googleapis.com/auth/drive"]
        )
        return build("drive", "v3", credentials=creds, cache_discovery=False)
    except Exception:
        return None


def gdrive_find_file(service, name, folder_id):
    try:
        q = f"name='{name}' and '{folder_id}' in parents and trashed=false"
        res = service.files().list(q=q, fields="files(id,name)").execute()
        files = res.get("files", [])
        return files[0]["id"] if files else None
    except Exception:
        return None


def gdrive_upload_csv(service, csv_bytes, name, folder_id, file_id=None):
    try:
        media = MediaIoBaseUpload(io.BytesIO(csv_bytes), mimetype="text/csv", resumable=False)
        if file_id:
            service.files().update(fileId=file_id, media_body=media).execute()
            return file_id
        else:
            meta = {"name": name, "parents": [folder_id]}
            f = service.files().create(body=meta, media_body=media, fields="id").execute()
            return f["id"]
    except Exception:
        return None


def gdrive_download_csv(service, file_id):
    try:
        buf = io.BytesIO()
        req = service.files().get_media(fileId=file_id)
        dl  = MediaIoBaseDownload(buf, req)
        done = False
        while not done:
            _, done = dl.next_chunk()
        return buf.getvalue()
    except Exception:
        return None

# ---------------------------------------------------------------------------
# Scraping engine
# ---------------------------------------------------------------------------
def fetch_html(url):
    req = urllib.request.Request(url, headers=FETCH_HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=12) as resp:
            return resp.read().decode("utf-8", errors="ignore"), resp.geturl()
    except Exception:
        return None, url


def clean_html(html):
    html = re.sub(r'<(script|style|nav|footer|header|noscript)[^>]*>.*?</\1>',
                  ' ', html, flags=re.I | re.S)
    html = re.sub(r'<(?:br|p|div|li|tr|h[1-6]|section|article|address)[^>]*>',
                  '\n', html, flags=re.I)
    html = re.sub(r'<[^>]+>', ' ', html)
    html = (html.replace('&amp;', '&').replace('&nbsp;', ' ')
                .replace('&#39;', "'").replace('&quot;', '"')
                .replace('&lt;', '<').replace('&gt;', '>')
                .replace('&#8211;', '–').replace('&#8212;', '—'))
    html = re.sub(r'&[a-z]{2,6};', ' ', html)
    return html


def segment_blocks(text):
    raw = re.split(r'\n{2,}', text)
    return [b.strip() for b in raw if 15 < len(b.strip()) < 400 and re.search(r'\d', b)]


def find_postcode(text):
    m = UK_POSTCODE_RE.search(text)
    if m:
        return m.group(1).upper()
    m = IE_EIRCODE_RE.search(text)
    if m:
        return m.group(1).upper()
    return None


def find_link(html, base_url, pattern):
    m = re.search(r'href=["\']([^"\']*' + pattern + r'[^"\']*)["\']', html, re.I)
    if not m:
        return None
    path = m.group(1)
    if path.startswith("http"):
        return path
    parsed = urllib.parse.urlparse(base_url)
    return urllib.parse.urljoin(f"{parsed.scheme}://{parsed.netloc}", path)


def normalise_country(raw):
    r = raw.strip().lower()
    return "Ireland" if r in {"ireland", "republic of ireland", "eire"} else "United Kingdom"


def parse_address_block(block, postcode):
    block = re.sub(r'[,|•·]\s*', '\n', block)
    block = re.sub(r'\s{2,}', '\n', block)
    lines = [l.strip() for l in block.splitlines() if l.strip()]
    pc_norm = re.sub(r'\s+', ' ', postcode).upper().strip()
    lines = [l for l in lines if re.sub(r'\s+', ' ', l).upper().strip() != pc_norm]
    country = ""
    country_re = re.compile(
        r'^(united kingdom|uk|great britain|england|scotland|wales|'
        r'northern ireland|ireland|republic of ireland|eire)$', re.I
    )
    lines_keep = []
    for l in lines:
        if country_re.match(l.strip()):
            if not country:
                country = normalise_country(l.strip())
        else:
            lines_keep.append(l)
    lines = lines_keep
    if not country:
        country = "Ireland" if IE_EIRCODE_RE.match(postcode.strip()) else "United Kingdom"
    county = ""
    lines_keep = []
    for l in lines:
        test = re.sub(r'^(?:co\.?\s*|county\s+)', '', l, flags=re.I).strip().lower()
        if test in ALL_COUNTIES or l.strip().lower() in ALL_COUNTIES:
            if not county:
                county = l.strip().title()
        else:
            lines_keep.append(l)
    lines = lines_keep
    city = ""
    city_idx = None
    for idx, l in enumerate(lines):
        if l.strip().lower() in UK_CITIES:
            city = l.strip().title()
            city_idx = idx
            break
    if city_idx is not None:
        lines = [l for i, l in enumerate(lines) if i != city_idx]
    elif len(lines) >= 2:
        city = lines[-1].strip().title()
        lines = lines[:-1]
    street = ", ".join(l for l in lines if l)
    street = re.sub(r'^[\d\s,]+$', '', street).strip().strip(',').strip()
    return {"street": street, "city": city, "county": county,
            "postcode": pc_norm, "country": country}


def extract_address_from_html(html):
    text   = clean_html(html)
    blocks = segment_blocks(text)
    best, best_score = None, -1
    for block in blocks:
        if EXCLUDED_TERMS.search(block):
            continue
        postcode = find_postcode(block)
        if not postcode:
            continue
        score = 0
        idx   = text.find(block)
        ctx   = text[max(0, idx - 300): idx] if idx != -1 else ""
        if TRADING_SIGNAL_RE.search(ctx):
            score += 10
        if STREET_NUM_RE.search(block):
            score += 5
        if IE_EIRCODE_RE.search(block):
            score += 2
        if score > best_score:
            best_score = score
            best = (block, postcode)
    if not best:
        return {}
    return parse_address_block(best[0], best[1])


def find_trading_address(raw_url):
    url = raw_url if raw_url.startswith("http") else "https://" + raw_url
    home_html, current_url = fetch_html(url)
    if not home_html:
        home_html, current_url = fetch_html(url.replace("https://", "http://"))
    if not home_html:
        return {}
    pages = [home_html]
    for pat in [r'contact', r'about', r'find.?us', r'location', r'visit']:
        link = find_link(home_html, current_url, pat)
        if link:
            h, _ = fetch_html(link)
            if h:
                pages.append(h)
            break
    pages.reverse()
    for html in pages:
        result = extract_address_from_html(html)
        if result:
            return result
    return {}


def detect_columns(df_columns):
    result = {}
    for key, pattern in HEADER_PATTERNS.items():
        match = next((c for c in df_columns if pattern.search(str(c))), None)
        result[key] = match
    return result

# ---------------------------------------------------------------------------
# CSV helper
# ---------------------------------------------------------------------------
def results_to_csv_bytes(results: list) -> bytes:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=OUTPUT_COLS)
    w.writeheader()
    w.writerows(results)
    return buf.getvalue().encode("utf-8")

# ---------------------------------------------------------------------------
# Worker thread
# ---------------------------------------------------------------------------
def run_worker(rows, col_name, col_website, col_country,
               output_filename, already_done, q,
               drive_service, drive_folder_id, drive_file_id_ref,
               shared_results, address_cache):
    """
    Cache logic per row:
      1. Company name key in cache  → use cached address, no HTTP
      2. Website in Drive resume    → already in shared_results, skip
      3. Otherwise                  → scrape, write to cache + Drive
    """
    total = len(rows)
    already_done_websites = set(already_done.keys())

    # Pre-populate shared_results with Drive resume rows
    for row_data in already_done.values():
        shared_results.append(row_data)

    def _checkpoint():
        if drive_service and drive_folder_id:
            data = results_to_csv_bytes(shared_results)
            fid  = gdrive_upload_csv(
                drive_service, data,
                output_filename, drive_folder_id,
                drive_file_id_ref[0]
            )
            if fid:
                drive_file_id_ref[0] = fid

    cache_hits = 0

    try:
        for i, row in enumerate(rows, 1):
            name    = str(row.get(col_name,    "") or "").strip() if col_name    else ""
            website = str(row.get(col_website, "") or "").strip()
            country = str(row.get(col_country, "") or "").strip() if col_country else ""

            # ── No website ────────────────────────────────────────────────
            if not website:
                q.put(f"[{i}/{total}] SKIP — no website  {name}")
                shared_results.append({
                    "Company Name": name, "Website": website, "Country": country,
                    "Street": "", "City": "", "State/County": "",
                    "Post Code": "", "Found Country": ""
                })
                _checkpoint()
                continue

            # ── Cache hit (company name) ───────────────────────────────────
            cached = cache_get(address_cache, name) if name else None
            if cached is not None:
                cache_hits += 1
                q.put(f"[{i}/{total}] CACHE  {name}")
                shared_results.append({
                    "Company Name":  name,
                    "Website":       website,
                    "Country":       country,
                    "Street":        cached.get("street",   ""),
                    "City":          cached.get("city",     ""),
                    "State/County":  cached.get("county",   ""),
                    "Post Code":     cached.get("postcode", ""),
                    "Found Country": cached.get("country",  ""),
                })
                _checkpoint()
                continue

            # ── Drive resume hit (website key) ────────────────────────────
            if website in already_done_websites:
                q.put(f"[{i}/{total}] RESUME  {website}")
                # row already in shared_results from pre-populate
                continue

            # ── Live scrape ───────────────────────────────────────────────
            q.put(f"[{i}/{total}]  {name or website}")
            addr = find_trading_address(website)

            street   = addr.get("street",   "")
            city     = addr.get("city",     "")
            county   = addr.get("county",   "")
            postcode = addr.get("postcode", "")
            fc       = addr.get("country",  "")

            if any([street, city, county, postcode]):
                q.put(f"         ✓  {street} | {city} | {county} | {postcode}")
            else:
                q.put(f"         ✗  No trading address found")

            shared_results.append({
                "Company Name":  name,
                "Website":       website,
                "Country":       country,
                "Street":        street,
                "City":          city,
                "State/County":  county,
                "Post Code":     postcode,
                "Found Country": fc,
            })
            already_done_websites.add(website)

            # Cache only successful scrapes
            if name and any([street, city, county, postcode]):
                cache_set(address_cache, name, addr)

            _checkpoint()
            time.sleep(0.4)

        _checkpoint()
        q.put({"__DONE__": True, "cache_hits": cache_hits})

    except Exception as e:
        q.put(f"__ERROR__ {e}")

# ---------------------------------------------------------------------------
# Streamlit UI
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Nexus · Trading Address Finder",
    page_icon="🏢",
    layout="wide"
)
st.title("🏢 Nexus Trading Address Finder")
st.caption("UK & Ireland  ·  Scrapes trading addresses from company websites  ·  Company-name cache  ·  Auto-saves to Google Drive")

for key, default in [
    ("df", None), ("col_map", {}), ("confirmed", False),
    ("running", False), ("log_lines", []), ("shared_results", None),
    ("output_filename", ""), ("worker_queue", None),
    ("drive_file_id", [None]),
    ("already_done", {}),
    ("address_cache", None),
    ("cache_hits_final", None),
]:
    if key not in st.session_state:
        st.session_state[key] = default

# Load cache once per session
if st.session_state.address_cache is None:
    st.session_state.address_cache = cache_load()

address_cache = st.session_state.address_cache

# Drive setup
drive_service   = _drive_service() if HAS_GDRIVE else None
drive_folder_id = None
if drive_service:
    try:
        drive_folder_id = st.secrets.get("gdrive_folder_id", None)
    except Exception:
        pass

if drive_service and drive_folder_id:
    st.success("✅ Google Drive connected — results will auto-save after every row.")
else:
    st.info("ℹ️ Google Drive not configured — results available via download button only.")

cache_size = len(address_cache)
st.caption(f"📦 Address cache: **{cache_size:,}** {'entry' if cache_size == 1 else 'entries'} loaded")

# ── Cache management ──────────────────────────────────────────────────────────
with st.expander("🗂️ Cache management"):
    st.write(
        "Stores scraped addresses keyed by company name. "
        "Cache hits skip HTTP entirely — no scraping, no waiting."
    )
    col_dl, col_clr = st.columns([2, 1])
    with col_dl:
        if cache_size > 0:
            st.download_button(
                label=f"⬇️ Export cache ({cache_size:,} entries)",
                data=json.dumps(address_cache, indent=2, ensure_ascii=False).encode("utf-8"),
                file_name="address_cache.json",
                mime="application/json",
            )
        else:
            st.caption("Cache is empty.")
    with col_clr:
        if st.button("🗑️ Clear cache", disabled=(cache_size == 0)):
            st.session_state.address_cache = {}
            cache_save({})
            st.rerun()

# ── Step 1: Upload ────────────────────────────────────────────────────────────
st.header("Step 1 — Upload your file")
uploaded = st.file_uploader("CSV or XLSX", type=["csv", "xlsx"])

if uploaded and not st.session_state.running:
    name = uploaded.name
    base, _ = os.path.splitext(name)
    output_filename = f"{base}-addresses.csv"
    st.session_state.output_filename = output_filename
    try:
        if name.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(uploaded.read()), dtype=str).fillna("")
        else:
            df = pd.read_excel(io.BytesIO(uploaded.read()), dtype=str).fillna("")
    except Exception as e:
        st.error(f"Could not read file: {e}")
        st.stop()

    st.session_state.df             = df
    st.session_state.col_map        = detect_columns(df.columns.tolist())
    st.session_state.confirmed      = False
    st.session_state.shared_results = None
    st.session_state.drive_file_id  = [None]
    st.session_state.cache_hits_final = None

    already_done = {}
    if drive_service and drive_folder_id:
        fid = gdrive_find_file(drive_service, output_filename, drive_folder_id)
        if fid:
            raw = gdrive_download_csv(drive_service, fid)
            if raw:
                try:
                    done_df = pd.read_csv(io.BytesIO(raw), dtype=str).fillna("")
                    for _, row in done_df.iterrows():
                        w = str(row.get("Website", "")).strip()
                        if w:
                            already_done[w] = row.to_dict()
                    st.session_state.drive_file_id = [fid]
                except Exception:
                    pass
    st.session_state.already_done = already_done

# ── Step 2: Column mapping ────────────────────────────────────────────────────
if st.session_state.df is not None:
    df       = st.session_state.df
    detected = st.session_state.col_map

    st.header("Step 2 — Confirm column mapping")
    cols = ["(not mapped)"] + df.columns.tolist()

    def safe_idx(col):
        if col and col in df.columns.tolist():
            return cols.index(col)
        return 0

    c1, c2, c3 = st.columns(3)
    with c1:
        sel_name    = st.selectbox("Company Name column", cols, index=safe_idx(detected.get("name")))
    with c2:
        sel_website = st.selectbox("Website column",      cols, index=safe_idx(detected.get("website")))
    with c3:
        sel_country = st.selectbox("Country column",      cols, index=safe_idx(detected.get("country")))

    conf_rows = []
    for label, sel, key in [
        ("Company Name", sel_name,    "name"),
        ("Website",      sel_website, "website"),
        ("Country",      sel_country, "country"),
    ]:
        auto = detected.get(key)
        status = ("✅ Auto-detected" if auto and auto == sel
                  else "⚠️ Not mapped" if sel == "(not mapped)"
                  else "✏️ Manually selected")
        conf_rows.append({"Field": label, "Mapped to": sel, "Status": status})
    st.table(pd.DataFrame(conf_rows))

    if sel_website == "(not mapped)":
        st.warning("Website column must be mapped before running.")
    else:
        already_done = st.session_state.already_done

        # Count how many rows the cache will absorb
        cache_skippable = 0
        if sel_name and sel_name != "(not mapped)":
            for _, row in df.iterrows():
                n = str(row.get(sel_name, "") or "").strip()
                if n and cache_get(address_cache, n) is not None:
                    cache_skippable += 1

        remaining = max(0, len([
            r for _, r in df.iterrows()
            if str(r.get(sel_website, "")).strip() not in already_done
               and str(r.get(sel_website, "")).strip()
        ]) - cache_skippable)

        if already_done:
            st.info(
                f"♻️ Resuming — **{len(already_done)}** rows already in Drive · "
                f"**{cache_skippable}** cache hits · "
                f"**{remaining}** to scrape"
            )
        else:
            st.caption(
                f"{len(df)} total rows  ·  "
                f"{cache_skippable} cache hits  ·  "
                f"{remaining} to scrape"
            )

        if st.button("✅ Confirm and start", type="primary",
                     disabled=st.session_state.running):
            shared_results = []
            st.session_state.shared_results   = shared_results
            st.session_state.confirmed        = True
            st.session_state.running          = True
            st.session_state.log_lines        = []
            st.session_state.worker_queue     = queue.Queue()
            st.session_state.cache_hits_final = None

            rows = df.to_dict(orient="records")
            t = threading.Thread(
                target=run_worker,
                args=(
                    rows,
                    sel_name    if sel_name    != "(not mapped)" else None,
                    sel_website,
                    sel_country if sel_country != "(not mapped)" else None,
                    st.session_state.output_filename,
                    already_done,
                    st.session_state.worker_queue,
                    drive_service,
                    drive_folder_id,
                    st.session_state.drive_file_id,
                    shared_results,
                    address_cache,
                ),
                daemon=True
            )
            t.start()
            st.rerun()

# ── Step 3: Live progress ─────────────────────────────────────────────────────
if st.session_state.running:
    st.header("Step 3 — Live Progress")

    q    = st.session_state.worker_queue
    done = False

    while not q.empty():
        msg = q.get_nowait()
        if isinstance(msg, dict) and "__DONE__" in msg:
            st.session_state.running          = False
            st.session_state.cache_hits_final = msg.get("cache_hits", 0)
            done = True
        elif isinstance(msg, str) and msg.startswith("__ERROR__"):
            st.session_state.log_lines.append(f"❌ {msg[9:]}")
            st.session_state.running = False
            done = True
        else:
            st.session_state.log_lines.append(str(msg))

    log_text = "\n".join(st.session_state.log_lines[-300:])
    st.text_area("", value=log_text, height=400, label_visibility="collapsed")

    # Mid-run partial download — always visible during processing
    partial = st.session_state.shared_results
    if partial:
        st.download_button(
            label=f"⬇️ Download partial results ({len(partial)} rows so far)",
            data=results_to_csv_bytes(partial),
            file_name="partial-" + st.session_state.output_filename,
            mime="text/csv",
            key="partial_dl",
        )

    if not done:
        time.sleep(1.5)
        st.rerun()
    else:
        st.success("✅ Complete!")
        st.rerun()

# ── Step 4: Download ──────────────────────────────────────────────────────────
if not st.session_state.running and st.session_state.shared_results:
    results = st.session_state.shared_results
    st.header("Step 4 — Results")

    result_df = pd.DataFrame(results, columns=OUTPUT_COLS)
    found     = result_df["Post Code"].astype(bool).sum()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total rows",      len(result_df))
    c2.metric("Addresses found", found)
    c3.metric("Not found",       len(result_df) - found)
    if st.session_state.cache_hits_final is not None:
        c4.metric("Cache hits", st.session_state.cache_hits_final)

    st.dataframe(result_df, use_container_width=True)

    st.download_button(
        label="⬇️ Download results CSV",
        data=results_to_csv_bytes(results),
        file_name=st.session_state.output_filename,
        mime="text/csv",
        type="primary",
    )

    if drive_service and drive_folder_id:
        st.info(f"📁 Also saved to Google Drive as `{st.session_state.output_filename}`")
