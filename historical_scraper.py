"""
historical_scraper.py  –  Kenya pump price history (pre-2024).

Sources
───────
open_africa        Open.Africa dataset     (town-level monthly PDFs, Dec 2010 – 2016)
energypedia        Energypedia wiki table  (national annual KES/USD, 1991 – 2014)
trading_economics  Trading Economics API   (national monthly USD, 1990+)
                   → set TE_API_KEY in .env to enable; skipped if absent.

All rows land in the same fuel_price_rows table used by scraper.py.
Dedup key: (period_start, period_end, town) — same as main scraper.

NOTE: Trading Economics and Energypedia prices are stored in USD/litre
      with extraction_method ending in "_usd".  Open.Africa PDFs are KES.
"""

import asyncio
import hashlib
import io
import json
import logging
import os
import re
import sys
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, quote_plus

import aiohttp
import asyncpg
import pdfplumber
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import warnings
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
from dateutil import parser as dtparser
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────

DB_HOST     = os.getenv("DB_HOST")
DB_PORT     = os.getenv("DB_PORT")
DB_NAME     = os.getenv("DB_NAME")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
TE_API_KEY  = os.getenv("TE_API_KEY", "")   # optional; Trading Economics API key

if not all([DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD]):
    raise RuntimeError("Missing database environment variables in .env")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}
TIMEOUT     = aiohttp.ClientTimeout(total=120)
CONCURRENCY = 6

LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

log = logging.getLogger("hist_scraper")
log.setLevel(logging.INFO)
log.propagate = False
if not log.handlers:
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    sh  = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    log.addHandler(sh)
    fh = logging.FileHandler(LOG_DIR / "historical_scraper.log", encoding="utf-8")
    fh.setFormatter(fmt)
    log.addHandler(fh)

# ─────────────────────────────────────────────────────────────────
# SHARED UTILITIES
# ─────────────────────────────────────────────────────────────────

MONTH_MAP = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
}
PRICE_KEYWORDS_RE = re.compile(
    r"(pump\s*prices?|maximum\s+retail\s+petroleum|petroleum\s+products?|fuel\s+prices?)",
    re.I,
)
DATE_PREFIX_RE = re.compile(
    r'^(\d{1,2}[-/]\d{1,2}[-/]\d{4})\s+(\d{1,2}[-/]\d{1,2}[-/]\d{4})\s+'
)
TOWN_GUARD = {"town", "towns", "super", "diesel", "kerosene", "pms", "ago", "ik", "from", "to"}

Row = Dict  # plain dict; keys match fuel_price_rows columns


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode()).hexdigest()


def clean_text(s: str) -> str:
    return re.sub(r"[ \t]+", " ", s.replace("\xa0", " ")).strip()


def parse_date_fuzzy(s: str) -> Optional[date]:
    try:
        return dtparser.parse(s, fuzzy=True, dayfirst=True).date()
    except Exception:
        return None


def title_case_town(s: str) -> str:
    parts = re.split(r"(\s+|-)", s.strip())
    out = []
    for p in parts:
        if not p or p.isspace() or p == "-":
            out.append(p)
        elif p.isupper() and len(p) > 1:
            out.append(p.title())
        else:
            out.append(p[:1].upper() + p[1:].lower())
    return "".join(out).strip()


def extract_period(text: str) -> Tuple[Optional[date], Optional[date]]:
    text = re.sub(r"[–—]", "-", text)
    text = re.sub(r"\s+", " ", text)
    patterns = [
        r'(\d{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]+\s+\d{4})\s*(?:-|to)\s*(\d{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]+\s+\d{4})',
        r'(\d{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]+)\s*(?:-|to)\s*(\d{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]+\s+\d{4})',
        r'([A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?)\s*(?:-|to)\s*([A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?\s+\d{4})',
        r'([A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?)\s*(?:-|to)\s*(\d{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]+\s+\d{4})',
        r'(\d{1,2}[-/]\d{1,2}[-/]\d{4})\s*(?:-|to| )\s*(\d{1,2}[-/]\d{1,2}[-/]\d{4})',
    ]
    for pat in patterns:
        m = re.search(pat, text, re.I)
        if not m:
            continue
        a, b = m.group(1), m.group(2)
        yr = re.search(r"(\d{4})", b)
        if yr and not re.search(r"\d{4}", a):
            a = f"{a} {yr.group(1)}"
        start, end = parse_date_fuzzy(a), parse_date_fuzzy(b)
        if start and end and start > end:
            try:
                start = start.replace(year=end.year - 1)
            except Exception:
                pass
        return start, end
    return None, None


def parse_price_line(
    line: str,
) -> Optional[Tuple[str, float, float, float, Optional[date], Optional[date]]]:
    line = clean_text(line)
    line = re.sub(r"^\d+\s+", "", line)

    inline_start = inline_end = None
    dm = DATE_PREFIX_RE.match(line)
    if dm:
        inline_start = parse_date_fuzzy(dm.group(1))
        inline_end   = parse_date_fuzzy(dm.group(2))
        line = line[dm.end():]

    nums = re.findall(r"\d{2,3}\.\d{1,2}", line)
    if len(nums) < 3:
        return None

    pos  = line.find(nums[0])
    town = line[:pos].strip(" -:")
    if len(town) < 2:
        return None

    tl = town.lower().strip()
    if tl in TOWN_GUARD:
        return None
    if re.search(r"\b(from|to|super|diesel|kerosene|pms|ago|ik)\b", tl):
        return None

    try:
        return title_case_town(town), float(nums[0]), float(nums[1]), float(nums[2]), inline_start, inline_end
    except Exception:
        return None


def unique_rows(rows: Iterable[Row]) -> List[Row]:
    out, seen = [], set()
    for r in rows:
        key = (
            r["period_start"].isoformat() if r.get("period_start") else "",
            r["period_end"].isoformat()   if r.get("period_end")   else "",
            r["town"].lower(),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def make_row(
    source: str,
    document_url: str,
    town: str,
    sp: Optional[float],
    di: Optional[float],
    ke: Optional[float],
    period_start: Optional[date],
    period_end: Optional[date],
    extraction_method: str,
) -> Row:
    canonical = date(period_start.year, period_start.month, 1) if period_start else None
    return {
        "source":            source,
        "document_url":      document_url,
        "canonical_month":   canonical,
        "period_start":      period_start,
        "period_end":        period_end,
        "town":              town,
        "super_petrol":      round(sp, 2) if sp is not None else None,
        "diesel":            round(di, 2) if di is not None else None,
        "kerosene":          round(ke, 2) if ke is not None else None,
        "extraction_method": extraction_method,
    }


# ─────────────────────────────────────────────────────────────────
# HTTP HELPERS
# ─────────────────────────────────────────────────────────────────

async def get_bytes(
    session: aiohttp.ClientSession, url: str
) -> Tuple[Optional[bytes], Optional[str]]:
    try:
        async with session.get(url, allow_redirects=True) as resp:
            if resp.status >= 400:
                return None, f"HTTP {resp.status}"
            return await resp.read(), None
    except Exception as e:
        return None, str(e)


async def get_text(
    session: aiohttp.ClientSession, url: str
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    try:
        async with session.get(url, allow_redirects=True) as resp:
            if resp.status >= 400:
                return None, f"HTTP {resp.status}", None
            return await resp.text(), None, str(resp.url)
    except Exception as e:
        return None, str(e), None


# ─────────────────────────────────────────────────────────────────
# PDF / TEXT EXTRACTION
# ─────────────────────────────────────────────────────────────────

def rows_from_text(
    text: str,
    source: str,
    url: str,
    year_hint: Optional[int] = None,
    month_hint: Optional[int] = None,
) -> List[Row]:
    period_start, period_end = extract_period(text)
    if not period_start and year_hint and month_hint:
        period_start = date(year_hint, month_hint, 1)

    rows = []
    for raw in text.splitlines():
        line = clean_text(raw)
        if not line:
            continue
        parsed = parse_price_line(line)
        if not parsed:
            continue
        town, sp, di, ke, i_s, i_e = parsed
        rows.append(make_row(
            source, url, town, sp, di, ke,
            i_s or period_start,
            i_e or period_end,
            "text",
        ))
    return unique_rows(rows)


def rows_from_pdf(
    data: bytes,
    source: str,
    url: str,
    year_hint: Optional[int] = None,
    month_hint: Optional[int] = None,
) -> List[Row]:
    rows: List[Row] = []
    try:
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            text_parts: List[str] = []

            for page in pdf.pages:
                page_text = page.extract_text() or ""
                if page_text:
                    text_parts.append(page_text)

                cur_text = "\n".join(text_parts)
                ps, pe = extract_period(cur_text)
                if not ps and year_hint and month_hint:
                    ps = date(year_hint, month_hint, 1)

                try:
                    tables = page.extract_tables() or []
                except Exception:
                    tables = []

                for tbl in tables:
                    for tbl_row in tbl or []:
                        if not tbl_row:
                            continue
                        line = " ".join(str(c).strip() for c in tbl_row if c)
                        parsed = parse_price_line(line)
                        if not parsed:
                            continue
                        town, sp, di, ke, i_s, i_e = parsed
                        rows.append(make_row(
                            source, url, town, sp, di, ke,
                            i_s or ps, i_e or pe,
                            "pdf_table",
                        ))

            full_text = "\n".join(text_parts)
            if full_text and PRICE_KEYWORDS_RE.search(full_text):
                rows += rows_from_text(full_text, source, url, year_hint, month_hint)

    except Exception as e:
        log.warning("PDF parse error url=%s: %s", url, e)

    return unique_rows(rows)


# ─────────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────────

async def ensure_schema(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute("""
        create table if not exists fuel_price_rows (
            id               bigserial primary key,
            row_hash         text unique not null,
            source           text not null,
            document_url     text not null,
            canonical_month  date,
            period_start     date,
            period_end       date,
            town             text not null,
            super_petrol     numeric,
            diesel           numeric,
            kerosene         numeric,
            extraction_method text,
            inserted_at      timestamptz default now()
        )
        """)
    log.info("Schema ready")


async def upsert_rows(pool: asyncpg.Pool, rows: List[Row]) -> int:
    if not rows:
        return 0
    inserted = 0
    async with pool.acquire() as conn:
        async with conn.transaction():
            for r in rows:
                h = sha1("|".join([
                    r["period_start"].isoformat() if r.get("period_start") else "",
                    r["period_end"].isoformat()   if r.get("period_end")   else "",
                    r["town"].lower(),
                ]))
                result = await conn.execute("""
                insert into fuel_price_rows (
                    row_hash, source, document_url, canonical_month,
                    period_start, period_end, town,
                    super_petrol, diesel, kerosene, extraction_method
                )
                values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                on conflict (row_hash) do nothing
                """,
                h,
                r["source"], r["document_url"],
                r.get("canonical_month"), r.get("period_start"), r.get("period_end"),
                r["town"], r.get("super_petrol"), r.get("diesel"), r.get("kerosene"),
                r.get("extraction_method", ""),
                )
                if result == "INSERT 0 1":
                    inserted += 1
    return inserted


# ─────────────────────────────────────────────────────────────────
# SOURCE 1 — OPEN.AFRICA (town-level PDFs, Dec 2010 – 2016)
# ─────────────────────────────────────────────────────────────────
# Dataset page lists 41 PDF resources via <a class="resource-url-analytics">.
# Each PDF is an EPRA/ERC monthly pump price notice (same format as scraper.py).

OPEN_AFRICA_URL = "https://open.africa/dataset/monthly-fuel-pump-prices-in-kenya"
_MONTH_ABBR = {v: k for k, v in MONTH_MAP.items() if len(k) == 3}  # 1->"jan" etc.
_MONTH_RE   = re.compile(
    r'\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|'
    r'jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|'
    r'nov(?:ember)?|dec(?:ember)?)\b', re.I
)


async def discover_open_africa_pdfs(session: aiohttp.ClientSession) -> List[Tuple[str, Optional[int], Optional[int]]]:
    """Return list of (pdf_url, year_hint, month_hint)."""
    html, err, _ = await get_text(session, OPEN_AFRICA_URL)
    if not html:
        log.warning("[open_africa] Dataset page fetch failed: %s", err)
        return []

    soup = BeautifulSoup(html, "lxml")
    results = []

    for a in soup.select("a.resource-url-analytics"):
        href = a.get("href", "").strip()
        if not href or ".pdf" not in href.lower():
            continue

        # year hint from URL
        yr_m = re.search(r"(20\d{2})", href)
        yr   = int(yr_m.group(1)) if yr_m else None

        # month hint from URL or resource title
        title_el = a.find_parent("li")
        title_text = (title_el.get_text(" ", strip=True) if title_el else "") + " " + href
        mo_m = _MONTH_RE.search(title_text)
        mo   = MONTH_MAP.get(mo_m.group(1).lower()[:3]) if mo_m else None

        results.append((href, yr, mo))

    log.info("[open_africa] %d PDF URLs discovered", len(results))
    return results


async def scrape_open_africa(session: aiohttp.ClientSession) -> List[Row]:
    pdf_list = await discover_open_africa_pdfs(session)
    if not pdf_list:
        return []

    sem      = asyncio.Semaphore(CONCURRENCY)
    all_rows: List[Row] = []

    async def process(url: str, yr: Optional[int], mo: Optional[int]) -> None:
        async with sem:
            data, err = await get_bytes(session, url)
        if not data:
            log.warning("[open_africa] PDF fetch failed %s: %s", url, err)
            return
        rows = rows_from_pdf(data, "open_africa", url, yr, mo)
        if rows:
            log.info("[open_africa] %d rows  %s", len(rows), url)
            all_rows.extend(rows)
        else:
            log.debug("[open_africa] 0 rows  %s", url)

    await asyncio.gather(*[process(u, y, m) for u, y, m in pdf_list])
    result = unique_rows(all_rows)
    log.info("[open_africa] %d unique rows total", len(result))
    return result


# ─────────────────────────────────────────────────────────────────
# SOURCE 2 — ENERGYPEDIA (national annual KES, 1991 – 2014)
# ─────────────────────────────────────────────────────────────────
# Wiki table: Year | Super Gasoline (USD) | Super Gasoline (KES) |
#             Diesel (USD) | Diesel (KES)
# We store KES prices; USD stored separately with _usd suffix in method.

ENERGYPEDIA_URL = "https://energypedia.info/wiki/Fuel_Price_Data_Kenya"


def _safe_float(s: str) -> Optional[float]:
    try:
        return float(re.sub(r"[^\d.]", "", s)) or None
    except Exception:
        return None


async def scrape_energypedia(session: aiohttp.ClientSession) -> List[Row]:
    html, err, _ = await get_text(session, ENERGYPEDIA_URL)
    if not html:
        log.warning("[energypedia] Fetch failed: %s", err)
        return []

    soup = BeautifulSoup(html, "lxml")
    rows: List[Row] = []

    for table in soup.find_all("table"):
        ths = [th.get_text(" ", strip=True).lower() for th in table.find_all("th")]
        tds_first = [td.get_text(" ", strip=True).lower()
                     for td in (table.find("tr") or []).find_all("td")]  # type: ignore[union-attr]
        headers = ths or tds_first

        # Must have a year-like column and look like a price table
        has_year  = any("year" in h for h in headers)
        has_price = any(k in " ".join(headers) for k in ("usd", "kes", "ksh", "price", "petrol", "diesel"))
        if not (has_year or has_price):
            continue

        # Column index detection
        def col_index(keywords):
            for i, h in enumerate(headers):
                if any(k in h for k in keywords):
                    return i
            return None

        year_col    = col_index(["year"]) or 0
        # Try to find KES columns; fall back to positional heuristic
        super_kes_col  = col_index(["super", "petrol", "gasoline", "pms"])
        diesel_kes_col = col_index(["diesel", "ago"])

        data_rows = table.find_all("tr")
        # skip header rows (rows where all cells are <th>)
        data_start = 0
        for i, tr in enumerate(data_rows):
            if tr.find("td"):
                data_start = i
                break

        year_prices: Dict[int, Dict] = {}
        for tr in data_rows[data_start:]:
            cells = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
            if not cells:
                continue

            # Parse year
            yr_raw = cells[year_col] if year_col < len(cells) else ""
            yr_m   = re.search(r"(19|20)\d{2}", yr_raw)
            if not yr_m:
                continue
            year = int(yr_m.group(0))
            if year < 1985 or year > 2030:
                continue

            # Collect all numeric values from non-year cells
            numeric = []
            for i, c in enumerate(cells):
                if i == year_col:
                    continue
                v = _safe_float(c)
                if v and v > 0:
                    numeric.append((i, v))

            if not numeric:
                continue

            # Classify values: > 5 → KES (historical KES/litre ~11–120), < 5 → USD
            kes_vals = [(i, v) for i, v in numeric if v > 5]
            usd_vals = [(i, v) for i, v in numeric if v <= 5]

            if year not in year_prices:
                year_prices[year] = {}

            # Store: first KES value = super petrol, second = diesel
            if len(kes_vals) >= 1:
                year_prices[year].setdefault("sp_kes", kes_vals[0][1])
            if len(kes_vals) >= 2:
                year_prices[year].setdefault("di_kes", kes_vals[1][1])
            if len(usd_vals) >= 1:
                year_prices[year].setdefault("sp_usd", usd_vals[0][1])
            if len(usd_vals) >= 2:
                year_prices[year].setdefault("di_usd", usd_vals[1][1])

        for year, prices in sorted(year_prices.items()):
            sp  = prices.get("sp_kes") or prices.get("sp_usd")
            di  = prices.get("di_kes") or prices.get("di_usd")
            if sp is None and di is None:
                continue
            method = "energypedia_html_kes" if prices.get("sp_kes") else "energypedia_html_usd"
            rows.append(make_row(
                source           = "energypedia",
                document_url     = ENERGYPEDIA_URL,
                town             = "National Average",
                sp               = sp,
                di               = di,
                ke               = None,
                period_start     = date(year, 1, 1),
                period_end       = date(year, 12, 31),
                extraction_method = method,
            ))

        if rows:
            break  # only process first matching table

    result = unique_rows(rows)
    log.info("[energypedia] %d rows", len(result))
    return result


# ─────────────────────────────────────────────────────────────────
# SOURCE 3 — TRADING ECONOMICS API (national monthly USD, 1990+)
# ─────────────────────────────────────────────────────────────────
# Requires TE_API_KEY in .env (free tier: 500 calls/month).
# Get a key at https://tradingeconomics.com/api/
#
# Response format:
#   [{"Country":"Kenya","Category":"Gasoline Prices",
#     "DateTime":"2026-03-31T00:00:00","Value":1.37,...}, ...]

TE_HIST_URL = (
    "https://api.tradingeconomics.com/historical/country/kenya"
    "/indicator/gasoline-prices"
)


async def scrape_trading_economics(session: aiohttp.ClientSession) -> List[Row]:
    if not TE_API_KEY:
        log.info("[trading_economics] TE_API_KEY not set — skipping")
        return []

    url = f"{TE_HIST_URL}?c={TE_API_KEY}&f=json"
    raw, err = await get_bytes(session, url)
    if not raw:
        log.warning("[trading_economics] API request failed: %s", err)
        return []

    try:
        entries = json.loads(raw)
    except Exception as e:
        log.warning("[trading_economics] JSON parse failed: %s", e)
        return []

    if not isinstance(entries, list):
        log.warning("[trading_economics] Unexpected response format: %s", type(entries))
        return []

    rows: List[Row] = []
    for entry in entries:
        try:
            dt  = dtparser.parse(entry["DateTime"]).date()
            val = float(entry["Value"])
        except (KeyError, ValueError, TypeError):
            continue
        if val <= 0:
            continue

        # period = calendar month of the data point
        import calendar
        last_day = calendar.monthrange(dt.year, dt.month)[1]
        rows.append(make_row(
            source            = "trading_economics",
            document_url      = TE_HIST_URL,
            town              = "National Average",
            sp                = val,
            di                = None,
            ke                = None,
            period_start      = date(dt.year, dt.month, 1),
            period_end        = date(dt.year, dt.month, last_day),
            extraction_method = "trading_economics_api_usd",
        ))

    result = unique_rows(rows)
    log.info("[trading_economics] %d rows", len(result))
    return result


# ─────────────────────────────────────────────────────────────────
# SOURCE 4 — SCRIBD (EPRA pump price PDFs/pages)
# ─────────────────────────────────────────────────────────────────

SCRIBD_BASE = "https://www.scribd.com"
SCRIBD_SEARCH_QUERIES = [
    "EPRA pump prices Kenya",
    "EPRA petroleum pump prices Kenya monthly",
    "Kenya fuel pump prices EPRA",
    "Kenya maximum retail petroleum prices",
]


def _scribd_doc_urls(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    urls: set = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href.startswith("http"):
            href = urljoin(SCRIBD_BASE, href)
        if re.match(r'https://www\.scribd\.com/(?:document|doc)/\d+', href):
            urls.add(href.split("?")[0])
    return list(urls)


def _scribd_extract_text(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "lxml")

    for script in soup.find_all("script"):
        content = script.string or ""
        if "page_text" in content or "text_content" in content:
            texts = re.findall(
                r'"(?:page_text|text_content|text)"\s*:\s*"((?:[^"\\]|\\.){50,})"',
                content,
            )
            if texts:
                return "\n".join(
                    t.replace("\\n", "\n").replace('\\"', '"') for t in texts
                )

    for tag in soup(["script", "style", "nav", "header", "footer"]):
        tag.decompose()
    text = soup.get_text("\n", strip=True)
    return text if len(text) > 300 else None


async def scrape_scribd() -> List[Row]:
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        log.warning("[scribd] playwright not installed — skipping. Run: pip install playwright && playwright install chromium")
        return []

    seen: set = set()
    doc_urls: List[str] = []
    all_rows: List[Row] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=HEADERS["User-Agent"],
            viewport={"width": 1280, "height": 800},
        )

        # Discover documents via search
        for query in SCRIBD_SEARCH_QUERIES:
            url = f"{SCRIBD_BASE}/search?query={quote_plus(query)}&content_type=documents"
            page = await context.new_page()
            try:
                await page.goto(url, wait_until="networkidle", timeout=30_000)
                html = await page.content()
                found = _scribd_doc_urls(html)
                log.info("[scribd] Query %r → %d docs", query, len(found))
                for u in found:
                    if u not in seen:
                        seen.add(u)
                        doc_urls.append(u)
            except Exception as e:
                log.warning("[scribd] Search failed query=%r: %s", query, e)
            finally:
                await page.close()

        log.info("[scribd] %d unique document URLs discovered", len(doc_urls))

        sem = asyncio.Semaphore(3)

        async def process(doc_url: str) -> None:
            async with sem:
                page = await context.new_page()
                try:
                    await page.goto(doc_url, wait_until="networkidle", timeout=30_000)
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
                    await page.wait_for_timeout(2_000)
                    html = await page.content()
                except Exception as e:
                    log.warning("[scribd] Page load failed url=%s: %s", doc_url, e)
                    await page.close()
                    return
                await page.close()

            text = _scribd_extract_text(html)
            if not text or not PRICE_KEYWORDS_RE.search(text):
                log.debug("[scribd] No price data url=%s", doc_url)
                return

            yr_m = re.search(r'(20\d{2})', doc_url)
            yr = int(yr_m.group(1)) if yr_m else None
            mo_m = _MONTH_RE.search(doc_url)
            mo = MONTH_MAP.get(mo_m.group(1).lower()[:3]) if mo_m else None

            rows = rows_from_text(text, "scribd", doc_url, yr, mo)
            if rows:
                log.info("[scribd] %d rows url=%s", len(rows), doc_url)
                all_rows.extend(rows)

        await asyncio.gather(*[process(u) for u in doc_urls])
        await browser.close()

    result = unique_rows(all_rows)
    log.info("[scribd] %d unique rows total", len(result))
    return result


# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────

async def main() -> None:
    log.info("Starting historical fuel scraper")

    pool = await asyncpg.create_pool(
        user=DB_USER, password=DB_PASSWORD,
        database=DB_NAME, host=DB_HOST, port=int(DB_PORT),
        ssl="require", min_size=1, max_size=6,
    )
    await ensure_schema(pool)

    connector = aiohttp.TCPConnector(limit=20, limit_per_host=4, ssl=False)
    async with aiohttp.ClientSession(
        headers=HEADERS, timeout=TIMEOUT, connector=connector
    ) as session:

        # ── Open.Africa PDFs (town-level, KES) ───────────────────
        oa_rows = await scrape_open_africa(session)
        n = await upsert_rows(pool, oa_rows)
        log.info("[open_africa] inserted %d new rows", n)

        # ── Energypedia (national annual, KES/USD) ────────────────
        ep_rows = await scrape_energypedia(session)
        n = await upsert_rows(pool, ep_rows)
        log.info("[energypedia] inserted %d new rows", n)

        # ── Trading Economics (national monthly, USD) ─────────────
        te_rows = await scrape_trading_economics(session)
        n = await upsert_rows(pool, te_rows)
        log.info("[trading_economics] inserted %d new rows", n)

    # ── Scribd (EPRA pump price documents, Playwright-rendered) ──
    sc_rows = await scrape_scribd()
    n = await upsert_rows(pool, sc_rows)
    log.info("[scribd] inserted %d new rows", n)

    # ── Summary ──────────────────────────────────────────────────
    async with pool.acquire() as conn:
        total = await conn.fetchval("select count(*) from fuel_price_rows")
        log.info("Total rows in DB: %s", total)

        by_source = await conn.fetch("""
            select source, count(*) as n
            from fuel_price_rows
            group by source order by n desc
        """)
        for row in by_source:
            log.info("  %-25s %s rows", row["source"], row["n"])

        coverage = await conn.fetch("""
            select
                extract(year  from coalesce(canonical_month, period_start))::int as yr,
                extract(month from coalesce(canonical_month, period_start))::int as mo,
                array_agg(distinct source order by source) as sources,
                count(*) as rows
            from fuel_price_rows
            where coalesce(canonical_month, period_start) is not null
            group by 1, 2
            order by 1, 2
        """)
        for row in coverage:
            log.info("  %s-%02d  sources=%s  rows=%s",
                     row["yr"], row["mo"], row["sources"], row["rows"])

    await pool.close()
    log.info("Done")


if __name__ == "__main__":
    asyncio.run(main())
