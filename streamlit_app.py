# nexus_trading_address.py
# Curtley James — BMI / Central
# Run with: streamlit run nexus_trading_address.py

import streamlit as st
import pandas as pd
import urllib.request
import urllib.error
import urllib.parse
import re
import time
import csv
import os
import threading
import queue
from io import StringIO

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

HEADERS = {
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

# ---------------------------------------------------------------------------
# Header auto-detection
# ---------------------------------------------------------------------------

HEADER_PATTERNS = {
    "name":    re.compile(r'company|business|organisation|organization|name', re.I),
    "website": re.compile(r'website|url|web|domain|site', re.I),
    "country": re.compile(r'country|nation|market|territory', re.I),
}

def detect_columns(df_columns):
    """
    Returns dict {name, website, country} → column name or None if not found.
    """
    result = {}
    for key, pattern in HEADER_PATTERNS.items():
        match = next((c for c in df_columns if pattern.search(str(c))), None)
        result[key] = match
    return result

# ---------------------------------------------------------------------------
# Scraping engine (same logic as tkinter version, extracted to plain functions)
# ---------------------------------------------------------------------------

def fetch_html(url):
    req = urllib.request.Request(url, headers=HEADERS)
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
    if r in {"ireland", "republic of ireland", "eire"}:
        return "Ireland"
    return "United Kingdom"


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

    pages.reverse()   # contact/about checked first
    for html in pages:
        result = extract_address_from_html(html)
        if result:
            return result
    return {}

# ---------------------------------------------------------------------------
# Worker thread
# ---------------------------------------------------------------------------

def run_worker(rows, col_name, col_website, col_country,
               output_path, already_done, q):
    """
    Processes rows, writes checkpoint CSV after every row,
    pushes log strings to queue q.
    Skips websites already in already_done set.
    """
    total = len(rows)

    # Open in append mode if resuming, write mode if fresh
    file_exists = os.path.exists(output_path)
    mode = 'a' if file_exists and already_done else 'w'

    try:
        with open(output_path, mode, newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if mode == 'w':
                writer.writerow(OUTPUT_COLS)

            for i, row in enumerate(rows, 1):
                name    = str(row.get(col_name,    "") or "").strip()
                website = str(row.get(col_website, "") or "").strip()
                country = str(row.get(col_country, "") or "").strip()

                if not website:
                    q.put(f"[{i}/{total}] SKIP (no website)  {name}")
                    writer.writerow([name, website, country, "", "", "", "", ""])
                    f.flush()
                    continue

                if website in already_done:
                    q.put(f"[{i}/{total}] SKIP (already done)  {website}")
                    continue

                q.put(f"[{i}/{total}] Processing  {name or website}")
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

                writer.writerow([name, website, country,
                                 street, city, county, postcode, fc])
                f.flush()
                time.sleep(0.4)

        q.put("__DONE__")

    except Exception as e:
        q.put(f"__ERROR__ {e}")

# ---------------------------------------------------------------------------
# Streamlit UI
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Nexus Trading Address Finder",
    page_icon="🏢",
    layout="wide"
)

st.title("🏢 Nexus Trading Address Finder")
st.caption("UK & Ireland · Scrapes trading addresses from company websites")

# ── Session state init ──────────────────────────────────────────────────────
for key, default in [
    ("df", None),
    ("col_map", {}),
    ("confirmed", False),
    ("running", False),
    ("log_lines", []),
    ("output_path", ""),
    ("worker_queue", None),
    ("upload_name", ""),
    ("upload_dir", ""),
]:
    if key not in st.session_state:
        st.session_state[key] = default

# ── Step 1: Upload ───────────────────────────────────────────────────────────
st.header("Step 1 — Upload your file")
uploaded = st.file_uploader("CSV or XLSX", type=["csv", "xlsx"])

if uploaded:
    name = uploaded.name
    # Save to a temp location alongside a fixed output name
    save_dir = os.path.join(os.path.expanduser("~"), "NexusOutput")
    os.makedirs(save_dir, exist_ok=True)
    input_path  = os.path.join(save_dir, name)
    base, _     = os.path.splitext(name)
    output_path = os.path.join(save_dir, f"{base}-addresses.csv")

    # Write uploaded bytes to disk so we can re-read with pandas
    with open(input_path, "wb") as fout:
        fout.write(uploaded.read())

    try:
        if name.endswith(".csv"):
            df = pd.read_csv(input_path, encoding="utf-8-sig", dtype=str).fillna("")
        else:
            df = pd.read_excel(input_path, dtype=str).fillna("")
    except Exception as e:
        st.error(f"Could not read file: {e}")
        st.stop()

    st.session_state.df          = df
    st.session_state.upload_name = name
    st.session_state.output_path = output_path
    st.session_state.confirmed   = False

    # Auto-detect columns
    detected = detect_columns(df.columns.tolist())
    st.session_state.col_map = detected

# ── Step 2: Confirm column mapping ──────────────────────────────────────────
if st.session_state.df is not None:
    df = st.session_state.df
    st.header("Step 2 — Confirm column mapping")

    cols = ["(not found)"] + df.columns.tolist()
    detected = st.session_state.col_map

    def safe_index(col_name):
        if col_name and col_name in df.columns.tolist():
            return cols.index(col_name)
        return 0

    col1, col2, col3 = st.columns(3)
    with col1:
        sel_name = st.selectbox(
            "Company Name column",
            cols, index=safe_index(detected.get("name")),
            help="Auto-detected from header keywords"
        )
    with col2:
        sel_website = st.selectbox(
            "Website column",
            cols, index=safe_index(detected.get("website")),
            help="Auto-detected from header keywords"
        )
    with col3:
        sel_country = st.selectbox(
            "Country column",
            cols, index=safe_index(detected.get("country")),
            help="Auto-detected from header keywords"
        )

    # Show detection confidence
    confidence_rows = []
    for label, sel, key in [
        ("Company Name", sel_name,    "name"),
        ("Website",      sel_website, "website"),
        ("Country",      sel_country, "country"),
    ]:
        auto = detected.get(key)
        if auto and auto == sel:
            status = "✅ Auto-detected"
        elif sel == "(not found)":
            status = "⚠️ Not mapped"
        else:
            status = "✏️ Manually selected"
        confidence_rows.append({"Field": label, "Mapped to": sel, "Status": status})

    st.table(pd.DataFrame(confidence_rows))

    if sel_website == "(not found)":
        st.warning("Website column must be mapped before you can run.")
    else:
        # Resume detection
        output_path   = st.session_state.output_path
        already_done  = set()
        resume_note   = ""
        if os.path.exists(output_path):
            try:
                done_df = pd.read_csv(output_path, dtype=str).fillna("")
                if "Website" in done_df.columns:
                    already_done = set(done_df["Website"].tolist())
                    resume_note  = (
                        f"♻️ Existing output found with **{len(already_done)}** "
                        f"processed rows — will resume from where it left off."
                    )
            except Exception:
                pass

        if resume_note:
            st.info(resume_note)

        remaining = len([
            r for _, r in df.iterrows()
            if str(r.get(sel_website, "")).strip() not in already_done
               and str(r.get(sel_website, "")).strip()
        ])
        st.caption(f"{len(df)} total rows · {len(already_done)} already done · **{remaining} to process**")

        if st.button("✅ Confirm mapping and start", type="primary",
                     disabled=st.session_state.running):
            st.session_state.confirmed  = True
            st.session_state.running    = True
            st.session_state.log_lines  = []
            st.session_state.worker_queue = queue.Queue()

            rows = df.to_dict(orient="records")
            t = threading.Thread(
                target=run_worker,
                args=(
                    rows,
                    sel_name    if sel_name    != "(not found)" else None,
                    sel_website,
                    sel_country if sel_country != "(not found)" else None,
                    output_path,
                    already_done,
                    st.session_state.worker_queue,
                ),
                daemon=True
            )
            t.start()
            st.rerun()

# ── Step 3: Live progress ────────────────────────────────────────────────────
if st.session_state.running:
    st.header("Step 3 — Live Progress")

    q = st.session_state.worker_queue
    done = False

    # Drain the queue into session log
    while not q.empty():
        msg = q.get_nowait()
        if msg == "__DONE__":
            done = True
            st.session_state.running = False
        elif msg.startswith("__ERROR__"):
            st.session_state.log_lines.append(f"❌ {msg[9:]}")
            st.session_state.running = False
            done = True
        else:
            st.session_state.log_lines.append(msg)

    log_text = "\n".join(st.session_state.log_lines[-200:])  # last 200 lines
    st.text_area("Log", value=log_text, height=420, label_visibility="collapsed")

    if not done:
        time.sleep(1.5)
        st.rerun()   # poll until worker finishes
    else:
        st.success("✅ Process complete!")

# ── Step 4: Download ─────────────────────────────────────────────────────────
if not st.session_state.running and st.session_state.output_path:
    output_path = st.session_state.output_path
    if os.path.exists(output_path):
        st.header("Step 4 — Download Results")
        result_df = pd.read_csv(output_path, dtype=str).fillna("")
        st.dataframe(result_df, use_container_width=True)

        with open(output_path, "rb") as f:
            st.download_button(
                label="⬇️ Download addresses CSV",
                data=f.read(),
                file_name=os.path.basename(output_path),
                mime="text/csv"
            )
        st.caption(f"File also saved locally at: `{output_path}`")
