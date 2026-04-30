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

# Google Drive (service account) — gracefully optional so local dev still works
try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
    HAS_GDRIVE = True
except ImportError:
    HAS_GDRIVE = False

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
    """Build a Drive service from Streamlit secrets (service account JSON)."""
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
    """Return file ID if name exists in folder, else None."""
    try:
        q = (f"name='{name}' and '{folder_id}' in parents "
             f"and trashed=false")
        res = service.files().list(q=q, fields="files(id,name)").execute()
        files = res.get("files", [])
        return files[0]["id"] if files else None
    except Exception:
        return None


def gdrive_upload_csv(service, csv_bytes, name, folder_id, file_id=None):
    """
    Create or update a CSV file in Drive.
    Returns the file ID.
    """
    try:
        media = MediaIoBaseUpload(
            io.BytesIO(csv_bytes),
            mimetype="text/csv",
            resumable=False
        )
        if file_id:
            service.files().update(fileId=file_id, media_body=media).execute()
            return file_id
        else:
            meta = {"name": name, "parents": [folder_id]}
            f = service.files().create(
                body=meta, media_body=media, fields="id"
            ).execute()
            return f["id"]
    except Exception:
        return None


def gdrive_download_csv(service, file_id):
    """Download a Drive file and return its bytes."""
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
# Worker thread
# ---------------------------------------------------------------------------

def run_worker(rows, col_name, col_website, col_country,
               output_filename, already_done, q,
               drive_service, drive_folder_id, drive_file_id_ref):
    """
    Processes rows, checkpoints to an in-memory buffer after every row,
    optionally syncs to Google Drive, and pushes log strings to queue q.

    drive_file_id_ref is a list[str|None] — mutable so we can update the
    Drive file ID from inside the thread.
    """
    total   = len(rows)
    # In-memory CSV buffer (rebuilt fully each checkpoint for Drive sync)
    results = []  # list of dicts

    # Pre-populate with already-done rows from Drive if resuming
    already_done_set = set(already_done.keys())
    for website, row_data in already_done.items():
        results.append(row_data)

    def _csv_bytes(rows_data):
        buf = io.StringIO()
        w   = csv.DictWriter(buf, fieldnames=OUTPUT_COLS)
        w.writeheader()
        w.writerows(rows_data)
        return buf.getvalue().encode("utf-8")

    def _checkpoint():
        """Push current results to Drive."""
        if drive_service and drive_folder_id:
            data = _csv_bytes(results)
            fid  = gdrive_upload_csv(
                drive_service, data,
                output_filename, drive_folder_id,
                drive_file_id_ref[0]
            )
            if fid:
                drive_file_id_ref[0] = fid

    try:
        for i, row in enumerate(rows, 1):
            name    = str(row.get(col_name,    "") or "").strip() if col_name    else ""
            website = str(row.get(col_website, "") or "").strip()
            country = str(row.get(col_country, "") or "").strip() if col_country else ""

            if not website:
                q.put(f"[{i}/{total}] SKIP — no website  {name}")
                results.append({
                    "Company Name": name, "Website": website, "Country": country,
                    "Street": "", "City": "", "State/County": "",
                    "Post Code": "", "Found Country": ""
                })
                _checkpoint()
                continue

            if website in already_done_set:
                q.put(f"[{i}/{total}] SKIP — already done  {website}")
                continue

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

            results.append({
                "Company Name": name,    "Website": website,  "Country": country,
                "Street": street,        "City": city,        "State/County": county,
                "Post Code": postcode,   "Found Country": fc
            })

            already_done_set.add(website)
            _checkpoint()
            time.sleep(0.4)

        # Final sync
        _checkpoint()
        q.put({"__DONE__": True, "results": results})

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
st.caption("UK & Ireland  ·  Scrapes trading addresses from company websites  ·  Auto-saves to Google Drive")

# Session state
for key, default in [
    ("df", None), ("col_map", {}), ("confirmed", False),
    ("running", False), ("log_lines", []), ("results", []),
    ("output_filename", ""), ("worker_queue", None),
    ("drive_file_id", [None]),   # mutable ref
    ("already_done", {}),
]:
    if key not in st.session_state:
        st.session_state[key] = default

# Drive setup (silent — only shown if secrets exist)
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

    st.session_state.df        = df
    st.session_state.col_map   = detect_columns(df.columns.tolist())
    st.session_state.confirmed = False
    st.session_state.results   = []
    st.session_state.drive_file_id = [None]

    # Check Drive for existing checkpoint
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

    # Confidence table
    conf_rows = []
    for label, sel, key in [
        ("Company Name", sel_name, "name"),
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
        remaining = len([
            r for _, r in df.iterrows()
            if str(r.get(sel_website, "")).strip() not in already_done
               and str(r.get(sel_website, "")).strip()
        ])

        if already_done:
            st.info(
                f"♻️ Resuming — **{len(already_done)}** rows already done in Drive, "
                f"**{remaining}** remaining."
            )
        else:
            st.caption(f"{len(df)} total rows  ·  {remaining} to process")

        if st.button("✅ Confirm and start", type="primary",
                     disabled=st.session_state.running):
            st.session_state.confirmed    = True
            st.session_state.running      = True
            st.session_state.log_lines    = []
            st.session_state.worker_queue = queue.Queue()

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
            st.session_state.results = msg["results"]
            st.session_state.running = False
            done = True
        elif isinstance(msg, str) and msg.startswith("__ERROR__"):
            st.session_state.log_lines.append(f"❌ {msg[9:]}")
            st.session_state.running = False
            done = True
        else:
            st.session_state.log_lines.append(str(msg))

    log_text = "\n".join(st.session_state.log_lines[-300:])
    st.text_area("", value=log_text, height=460, label_visibility="collapsed")

    if not done:
        time.sleep(1.5)
        st.rerun()
    else:
        st.success("✅ Complete!")
        st.rerun()

# ── Step 4: Download ──────────────────────────────────────────────────────────
if not st.session_state.running and st.session_state.results:
    st.header("Step 4 — Results")

    result_df = pd.DataFrame(st.session_state.results, columns=OUTPUT_COLS)
    found     = result_df["Post Code"].astype(bool).sum()
    st.caption(f"{len(result_df)} rows  ·  {found} addresses found  ·  {len(result_df) - found} not found")
    st.dataframe(result_df, use_container_width=True)

    buf = io.StringIO()
    result_df.to_csv(buf, index=False)
    st.download_button(
        label="⬇️ Download results CSV",
        data=buf.getvalue().encode("utf-8"),
        file_name=st.session_state.output_filename,
        mime="text/csv"
    )

    if drive_service and drive_folder_id:
        st.info(f"📁 Also saved to Google Drive as `{st.session_state.output_filename}`")
