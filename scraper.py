import os
import re
import io
import sys
import json
import asyncio
import hashlib
import logging
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, List, Tuple, Iterable
from urllib.parse import urljoin, urlparse, quote_plus
from datetime import date

import aiohttp
import asyncpg
import pdfplumber
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import warnings
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
from dateutil import parser as dtparser
from dotenv import load_dotenv

load_dotenv()

# =========================
# ENV / CONFIG
# =========================

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not all([DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD]):
    raise RuntimeError("Missing one or more required database environment variables in .env")

BASE_EPRA = "https://www.epra.go.ke"
LIVE_URL = "https://www.epra.go.ke/pump-prices"
ARCHIVE_URL = "https://www.epra.go.ke/EPRA%20Pump%20Prices"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=120)
CONCURRENCY = 8
ARCHIVE_MAX_PAGES = 10

LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "fuel_scraper.log"

logger = logging.getLogger("fuel_scraper")
logger.setLevel(logging.INFO)
logger.propagate = False

if not logger.handlers:
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

log = logger

MONTH_MAP = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}

PRICE_KEYWORDS_RE = re.compile(
    r"(pump\s*prices?|maximum\s+retail\s+petroleum\s+prices?|petroleum\s+products?)",
    re.I,
)

DATE_PREFIX_RE = re.compile(
    r'^(\d{1,2}[-/]\d{1,2}[-/]\d{4})\s+(\d{1,2}[-/]\d{1,2}[-/]\d{4})\s+'
)

TOWN_HEADER_GUARD = {
    "town", "towns", "super", "diesel", "kerosene", "pms", "ago", "ik", "from", "to"
}


# =========================
# MODELS
# =========================

@dataclass
class DocumentCandidate:
    source: str
    page_url: str
    file_url: Optional[str]
    title: str
    year_hint: Optional[int]
    month_hint: Optional[int]
    mime_hint: Optional[str]
    discovery_method: str


@dataclass
class ParsedRow:
    source: str
    document_url: str
    canonical_month: Optional[date]
    period_start: Optional[date]
    period_end: Optional[date]
    town: str
    super_petrol: Optional[float]
    diesel: Optional[float]
    kerosene: Optional[float]
    extraction_method: str


# =========================
# UTILITIES
# =========================

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def norm_url(u: str) -> str:
    return u.split("#")[0].strip()


def clean_text(s: str) -> str:
    s = s.replace("\xa0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{2,}", "\n", s)
    return s.strip()


def safe_title_case_town(s: str) -> str:
    # Preserve internal capitalization a bit better than str.title()
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


def parse_date_fuzzy(s: str) -> Optional[date]:
    try:
        return dtparser.parse(s, fuzzy=True, dayfirst=True).date()
    except Exception:
        return None


def parse_month_date(year: int, month: int) -> date:
    return date(year, month, 1)


def contains_price_keywords(text: str) -> bool:
    return bool(PRICE_KEYWORDS_RE.search(text or ""))


def looks_like_pump_price_document(text: str) -> bool:
    if not text:
        return False

    low = text.lower()
    keyword_hit = contains_price_keywords(low)

    tableish_hit = (
        ("super" in low or "pms" in low)
        and ("diesel" in low or "ago" in low)
        and ("kerosene" in low or "ik" in low)
        and ("town" in low or "mombasa" in low or "nairobi" in low)
    )

    period_hit = extract_period(text)[0] is not None

    return keyword_hit and (tableish_hit or period_hit)


def month_from_text(text: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Find month/year using text order, not dict order.
    This avoids turning:
      '15th December 2024 - 14th January 2025'
    into January 2024.
    """
    low = (text or "").lower()

    year_hits = [(m.start(), int(m.group(1))) for m in re.finditer(r"\b(20\d{2})\b", low)]
    month_hits: List[Tuple[int, int, str]] = []

    for name, num in MONTH_MAP.items():
        for m in re.finditer(rf"\b{name}\b", low):
            month_hits.append((m.start(), num, name))

    for m in re.finditer(r"\b(0?[1-9]|1[0-2])[-_/ ](20\d{2})\b", low):
        month_hits.append((m.start(), int(m.group(1)), "numeric"))
        year_hits.append((m.start(), int(m.group(2))))

    if not month_hits and not year_hits:
        return None, None

    month_hits.sort(key=lambda x: x[0])
    year_hits.sort(key=lambda x: x[0])

    month = month_hits[0][1] if month_hits else None

    year = None
    if year_hits:
        anchor = month_hits[0][0] if month_hits else year_hits[0][0]
        after = [y for pos, y in year_hits if pos >= anchor]
        before = [y for pos, y in year_hits if pos < anchor]
        year = after[0] if after else (before[-1] if before else None)

    return year, month


def canonical_month_from_period(
    period_start: Optional[date],
    period_end: Optional[date],
    year_hint: Optional[int],
    month_hint: Optional[int],
    trust_hint: bool = False,
) -> Optional[date]:
    if period_start:
        return date(period_start.year, period_start.month, 1)

    if trust_hint and year_hint and month_hint and 2018 <= year_hint <= 2035:
        return parse_month_date(year_hint, month_hint)

    return None


def extract_period(text: str) -> Tuple[Optional[date], Optional[date]]:
    if not text:
        return None, None

    text = text.replace("–", "-").replace("—", "-")
    text = re.sub(r"\s+", " ", text)

    patterns = [
        # 15th December 2024 - 14th January 2025
        r'(\d{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]+\s+\d{4})\s*(?:-|to)\s*(\d{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]+\s+\d{4})',

        # 15th November to 14th December 2024
        r'(\d{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]+)\s*(?:-|to)\s*(\d{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]+\s+\d{4})',

        # October 15 - November 14 2024
        r'([A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?)\s*(?:-|to)\s*([A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?\s+\d{4})',

        # August 15th -14th September 2024
        r'([A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?)\s*(?:-|to)\s*(\d{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]+\s+\d{4})',

        # 15-12-2025 14-01-2026
        r'(\d{1,2}[-/]\d{1,2}[-/]\d{4})\s*(?:-|to| )\s*(\d{1,2}[-/]\d{1,2}[-/]\d{4})',
    ]

    for pat in patterns:
        m = re.search(pat, text, flags=re.I)
        if not m:
            continue

        a, b = m.group(1), m.group(2)

        year_b = re.search(r'(\d{4})', b)
        if year_b and not re.search(r'\d{4}', a):
            a = f"{a} {year_b.group(1)}"

        start = parse_date_fuzzy(a)
        end = parse_date_fuzzy(b)

        if start and end and start > end:
            # Handle rollover cases like "December 2024 - January 2025"
            try:
                start = start.replace(year=end.year - 1)
            except Exception:
                pass

        return start, end

    return None, None


def parse_town_price_line(line: str) -> Optional[Tuple[str, float, float, float, Optional[date], Optional[date]]]:
    line = clean_text(line)
    line = re.sub(r"^\d+\s+", "", line)

    # Strip inline date pair (e.g. "15-12-2025 14-01-2026 Kainuk ...")
    inline_start: Optional[date] = None
    inline_end: Optional[date] = None
    dm = DATE_PREFIX_RE.match(line)
    if dm:
        inline_start = parse_date_fuzzy(dm.group(1))
        inline_end = parse_date_fuzzy(dm.group(2))
        line = line[dm.end():]

    nums = re.findall(r"\d{2,3}\.\d{1,2}", line)
    if len(nums) < 3:
        return None

    pos = line.find(nums[0])
    town = line[:pos].strip(" -:")

    if len(town) < 2:
        return None

    town_low = town.lower().strip()
    if town_low in TOWN_HEADER_GUARD:
        return None

    if re.search(r"\b(from|to|super|diesel|kerosene|pms|ago|ik)\b", town_low):
        return None

    try:
        return town, float(nums[0]), float(nums[1]), float(nums[2]), inline_start, inline_end
    except Exception:
        return None


def unique_rows(rows: Iterable[ParsedRow]) -> List[ParsedRow]:
    out: List[ParsedRow] = []
    seen = set()

    for r in rows:
        key = (
            r.period_start.isoformat() if r.period_start else "",
            r.period_end.isoformat() if r.period_end else "",
            r.town.lower(),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)

    return out


def build_row(
    source: str,
    document_url: str,
    canonical_month: Optional[date],
    period_start: Optional[date],
    period_end: Optional[date],
    town: str,
    sp: float,
    di: float,
    ke: float,
    extraction_method: str,
) -> ParsedRow:
    return ParsedRow(
        source=source,
        document_url=document_url,
        canonical_month=canonical_month,
        period_start=period_start,
        period_end=period_end,
        town=safe_title_case_town(town),
        super_petrol=round(sp, 2) if sp is not None else None,
        diesel=round(di, 2) if di is not None else None,
        kerosene=round(ke, 2) if ke is not None else None,
        extraction_method=extraction_method,
    )


# =========================
# FETCH HELPERS
# =========================

async def fetch_bytes(
    session: aiohttp.ClientSession,
    url: str
) -> Tuple[Optional[bytes], Optional[str], Optional[str], Optional[str]]:
    try:
        async with session.get(url, allow_redirects=True) as resp:
            if resp.status >= 400:
                return None, None, f"HTTP {resp.status}", None
            data = await resp.read()
            ctype = str(resp.headers.get("Content-Type", "")).lower()
            final_url = str(resp.url)
            return data, ctype, None, final_url
    except Exception as e:
        return None, None, str(e), None


async def fetch_text(
    session: aiohttp.ClientSession,
    url: str
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    try:
        async with session.get(url, allow_redirects=True) as resp:
            if resp.status >= 400:
                return None, f"HTTP {resp.status}", None
            text = await resp.text()
            return text, None, str(resp.url)
    except Exception as e:
        return None, str(e), None


# =========================
# DB
# =========================

async def init_db(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute("""
        create table if not exists fuel_price_rows (
            id bigserial primary key,
            row_hash text unique not null,
            source text not null,
            document_url text not null,
            canonical_month date,
            period_start date,
            period_end date,
            town text not null,
            super_petrol numeric,
            diesel numeric,
            kerosene numeric,
            extraction_method text,
            inserted_at timestamptz default now()
        )
        """)

        log.info("Database schema initialized")


async def upsert_rows(pool: asyncpg.Pool, rows: List[ParsedRow]) -> None:
    if not rows:
        return

    async with pool.acquire() as conn:
        async with conn.transaction():
            for r in rows:
                row_hash = sha1("|".join([
                    r.period_start.isoformat() if r.period_start else "",
                    r.period_end.isoformat() if r.period_end else "",
                    r.town.lower(),
                ]))

                await conn.execute("""
                insert into fuel_price_rows (
                    row_hash, source, document_url, canonical_month, period_start, period_end,
                    town, super_petrol, diesel, kerosene, extraction_method
                )
                values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                on conflict (row_hash) do nothing
                """, row_hash, r.source, r.document_url, r.canonical_month,
                     r.period_start, r.period_end, r.town,
                     r.super_petrol, r.diesel, r.kerosene, r.extraction_method)


# =========================
# DISCOVERY
# =========================

def article_link_is_relevant(title: str, href: str) -> bool:
    blob = f"{title} {href}"
    return contains_price_keywords(blob)


async def discover_epra_archive(session: aiohttp.ClientSession) -> List[DocumentCandidate]:
    docs: List[DocumentCandidate] = []
    empty_pages = 0

    for page in range(ARCHIVE_MAX_PAGES):
        url = ARCHIVE_URL if page == 0 else f"{ARCHIVE_URL}?page={page}"
        html, err, final_url = await fetch_text(session, url)
        if not html:
            log.warning("Archive page fetch failed url=%s err=%s", url, err)
            break

        soup = BeautifulSoup(html, "lxml")
        found = 0

        for a in soup.find_all("a", href=True):
            href = norm_url(urljoin(final_url or url, a["href"]))
            text = clean_text(a.get_text(" ", strip=True))
            if not text and not href:
                continue

            if href.endswith(".pdf") or ".pdf?" in href.lower():
                blob = f"{text} {href}"
                if not article_link_is_relevant(text, href):
                    continue

                y, mth = month_from_text(blob)
                docs.append(
                    DocumentCandidate(
                        source="epra",
                        page_url=href,
                        file_url=href,
                        title=text or href.rsplit("/", 1)[-1],
                        year_hint=y,
                        month_hint=mth,
                        mime_hint="application/pdf",
                        discovery_method="epra_archive_direct_pdf",
                    )
                )
                found += 1
                continue

            if urlparse(href).netloc.endswith("epra.go.ke") and article_link_is_relevant(text, href):
                y, mth = month_from_text(f"{text} {href}")
                docs.append(
                    DocumentCandidate(
                        source="epra",
                        page_url=href,
                        file_url=None,
                        title=text,
                        year_hint=y,
                        month_hint=mth,
                        mime_hint="text/html",
                        discovery_method="epra_archive_article",
                    )
                )
                found += 1

        empty_pages = empty_pages + 1 if found == 0 else 0
        if empty_pages >= 3:
            break

    # enrich article pages by finding their PDF
    dedup = {}
    for d in docs:
        dedup[(d.page_url, d.file_url or "")] = d

    base_docs = list(dedup.values())
    article_docs = [d for d in base_docs if not d.file_url]
    sem = asyncio.Semaphore(CONCURRENCY)

    async def enrich_article(doc: DocumentCandidate) -> Optional[DocumentCandidate]:
        async with sem:
            html, _, final_url = await fetch_text(session, doc.page_url)
        if not html:
            return None

        soup = BeautifulSoup(html, "lxml")
        # If the article page itself is about pump prices, accept any PDF found on it
        article_is_prices = contains_price_keywords(doc.title)

        # first pass: explicit anchors
        for a in soup.find_all("a", href=True):
            href = norm_url(urljoin(final_url or doc.page_url, a["href"]))
            text = clean_text(a.get_text(" ", strip=True))
            blob = f"{doc.title} {text} {href}"

            if href.endswith(".pdf") or ".pdf?" in href.lower():
                if article_is_prices or contains_price_keywords(blob):
                    y, mth = month_from_text(blob)
                    return DocumentCandidate(
                        source="epra",
                        page_url=doc.page_url,
                        file_url=href,
                        title=doc.title,
                        year_hint=y or doc.year_hint,
                        month_hint=mth or doc.month_hint,
                        mime_hint="application/pdf",
                        discovery_method="epra_article_pdf_link",
                    )

        # second pass: raw html regex scan for any PDF path
        for m in re.finditer(r'(/sites/default/files/[^\s"<>]+\.pdf)', html, re.I):
            href = norm_url(urljoin(final_url or doc.page_url, m.group(1)))
            blob = f"{doc.title} {href}"
            if article_is_prices or contains_price_keywords(blob):
                y, mth = month_from_text(blob)
                return DocumentCandidate(
                    source="epra",
                    page_url=doc.page_url,
                    file_url=href,
                    title=doc.title,
                    year_hint=y or doc.year_hint,
                    month_hint=mth or doc.month_hint,
                    mime_hint="application/pdf",
                    discovery_method="epra_article_pdf_regex",
                )

        return None

    enriched = await asyncio.gather(*[enrich_article(d) for d in article_docs])
    for item in enriched:
        if item:
            # Replace the original article entry with the enriched PDF-backed entry
            dedup.pop((item.page_url, ""), None)
            dedup[(item.page_url, item.file_url or "")] = item

    out = list(dedup.values())
    log.info("EPRA archive discovered: %d documents", len(out))
    return out


# =========================
# SCRIBD
# =========================

SCRIBD_BASE = "https://www.scribd.com"
SCRIBD_SEARCH_QUERIES = [
    "EPRA pump prices Kenya",
    "EPRA petroleum pump prices Kenya monthly",
    "Kenya fuel pump prices EPRA",
]


def _extract_scribd_doc_urls(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    urls: set = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href.startswith("http"):
            href = urljoin(SCRIBD_BASE, href)
        if re.match(r'https://www\.scribd\.com/(?:document|doc)/\d+', href):
            urls.add(href.split("?")[0])
    return list(urls)


def _extract_text_from_scribd_page(html: str) -> Optional[str]:
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


async def scrape_scribd_with_playwright(pool: asyncpg.Pool) -> None:
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        log.warning("[scribd] playwright not installed — skipping. Run: pip install playwright && playwright install chromium")
        return

    seen: set = set()
    doc_urls: List[str] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=HEADERS["User-Agent"],
            viewport={"width": 1280, "height": 800},
        )

        # Search for documents
        for query in SCRIBD_SEARCH_QUERIES:
            url = f"{SCRIBD_BASE}/search?query={quote_plus(query)}&content_type=documents"
            page = await context.new_page()
            try:
                await page.goto(url, wait_until="networkidle", timeout=30_000)
                html = await page.content()
                found = _extract_scribd_doc_urls(html)
                log.info("[scribd] Query %r → %d docs", query, len(found))
                for u in found:
                    if u not in seen:
                        seen.add(u)
                        doc_urls.append(u)
            except Exception as e:
                log.warning("[scribd] Search failed query=%r: %s", query, e)
            finally:
                await page.close()

        log.info("[scribd] %d unique document URLs", len(doc_urls))

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

            text = _extract_text_from_scribd_page(html)
            if not text or not looks_like_pump_price_document(text):
                log.debug("[scribd] No price data url=%s", doc_url)
                return

            y, mth = month_from_text(doc_url)
            rows = parse_rows_from_text(
                text=text,
                source="scribd",
                document_url=doc_url,
                extraction_method="scribd_playwright",
                year_hint=y,
                month_hint=mth,
                trust_hint=True,
            )
            if rows:
                await upsert_rows(pool, rows)
                log.info("[scribd] rows=%d url=%s", len(rows), doc_url)

        await asyncio.gather(*[process(u) for u in doc_urls])
        await browser.close()


# =========================
# EXTRACTION
# =========================

def parse_rows_from_text(
    text: str,
    source: str,
    document_url: str,
    extraction_method: str,
    year_hint: Optional[int],
    month_hint: Optional[int],
    trust_hint: bool = False,
) -> List[ParsedRow]:
    rows: List[ParsedRow] = []
    period_start, period_end = extract_period(text)
    canonical_month = canonical_month_from_period(
        period_start, period_end, year_hint, month_hint, trust_hint=trust_hint
    )

    for raw in text.splitlines():
        line = clean_text(raw)
        if not line:
            continue

        parsed = parse_town_price_line(line)
        if not parsed:
            continue

        town, sp, di, ke, inline_start, inline_end = parsed
        row_start = inline_start or period_start
        row_end = inline_end or period_end
        row_canonical = (
            canonical_month_from_period(row_start, row_end, year_hint, month_hint, trust_hint=trust_hint)
            if inline_start else canonical_month
        )
        rows.append(
            build_row(
                source=source,
                document_url=document_url,
                canonical_month=row_canonical,
                period_start=row_start,
                period_end=row_end,
                town=town,
                sp=sp,
                di=di,
                ke=ke,
                extraction_method=extraction_method,
            )
        )

    return unique_rows(rows)


def extract_from_html_text(
    html: str,
    source: str,
    document_url: str,
    year_hint: Optional[int],
    month_hint: Optional[int],
) -> List[ParsedRow]:
    soup = BeautifulSoup(html, "lxml")
    all_text = soup.get_text("\n", strip=True)

    if not looks_like_pump_price_document(all_text):
        return []

    period_start, period_end = extract_period(all_text)
    canonical_month = canonical_month_from_period(
        period_start,
        period_end,
        year_hint,
        month_hint,
        trust_hint=contains_price_keywords(all_text),
    )

    rows: List[ParsedRow] = []

    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
            if not cells:
                continue

            line = " ".join(cells)
            parsed = parse_town_price_line(line)
            if not parsed:
                continue

            town, sp, di, ke, inline_start, inline_end = parsed
            row_start = inline_start or period_start
            row_end = inline_end or period_end
            row_canonical = (
                canonical_month_from_period(row_start, row_end, None, None)
                if inline_start else canonical_month
            )
            rows.append(
                build_row(
                    source=source,
                    document_url=document_url,
                    canonical_month=row_canonical,
                    period_start=row_start,
                    period_end=row_end,
                    town=town,
                    sp=sp,
                    di=di,
                    ke=ke,
                    extraction_method="html_table",
                )
            )

    rows.extend(
        parse_rows_from_text(
            all_text,
            source,
            document_url,
            "html_text",
            year_hint,
            month_hint,
            trust_hint=contains_price_keywords(all_text),
        )
    )

    return unique_rows(rows)


def extract_from_pdf_bytes(
    data: bytes,
    source: str,
    document_url: str,
    year_hint: Optional[int],
    month_hint: Optional[int],
) -> List[ParsedRow]:
    rows: List[ParsedRow] = []

    try:
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            full_text_parts: List[str] = []

            for page in pdf.pages:
                page_text = page.extract_text() or ""
                if page_text:
                    full_text_parts.append(page_text)

                try:
                    tables = page.extract_tables()
                except Exception:
                    tables = []

                current_text = "\n".join(full_text_parts)
                period_start, period_end = extract_period(current_text)
                canonical_month = canonical_month_from_period(
                    period_start,
                    period_end,
                    year_hint,
                    month_hint,
                    trust_hint=contains_price_keywords(current_text),
                )

                for table in tables or []:
                    for r in table or []:
                        if not r:
                            continue
                        line = " ".join(str(x).strip() for x in r if x is not None)
                        parsed = parse_town_price_line(line)
                        if not parsed:
                            continue

                        town, sp, di, ke, inline_start, inline_end = parsed
                        row_start = inline_start or period_start
                        row_end = inline_end or period_end
                        row_canonical = (
                            canonical_month_from_period(row_start, row_end, year_hint, month_hint, trust_hint=contains_price_keywords(current_text))
                            if inline_start else canonical_month
                        )
                        rows.append(
                            build_row(
                                source=source,
                                document_url=document_url,
                                canonical_month=row_canonical,
                                period_start=row_start,
                                period_end=row_end,
                                town=town,
                                sp=sp,
                                di=di,
                                ke=ke,
                                extraction_method="pdf_table",
                            )
                        )

            full_text = "\n".join(full_text_parts)
            if full_text and looks_like_pump_price_document(full_text):
                rows.extend(
                    parse_rows_from_text(
                        full_text,
                        source,
                        document_url,
                        "pdf_text",
                        year_hint,
                        month_hint,
                        trust_hint=contains_price_keywords(full_text),
                    )
                )

    except Exception as e:
        log.warning("PDF parse failed url=%s err=%s", document_url, e)
        return []

    return unique_rows(rows)


async def parse_live_epra_table(session: aiohttp.ClientSession) -> List[ParsedRow]:
    html, err, final_url = await fetch_text(session, LIVE_URL)
    if not html:
        log.warning("Live EPRA table fetch failed: %s", err)
        return []

    soup = BeautifulSoup(html, "lxml")
    table = None

    for tbl in soup.find_all("table"):
        txt = tbl.get_text(" ", strip=True).lower()
        if "town" in txt and ("super" in txt or "pms" in txt) and ("diesel" in txt or "ago" in txt):
            table = tbl
            break

    if not table:
        log.warning("No EPRA live table found")
        return []

    rows: List[ParsedRow] = []
    period_start = None
    period_end = None

    for tr in table.find_all("tr"):
        cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
        joined = " ".join(cells)
        a, b = extract_period(joined)
        if a and b:
            period_start, period_end = a, b
            break

        if len(cells) >= 2:
            d1 = parse_date_fuzzy(cells[0])
            d2 = parse_date_fuzzy(cells[1])
            if d1 and d2:
                period_start, period_end = d1, d2
                break

    canonical_month = canonical_month_from_period(period_start, period_end, None, None)

    for tr in table.find_all("tr"):
        cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
        if len(cells) < 4:
            continue

        line = " ".join(cells)
        parsed = parse_town_price_line(line)
        if not parsed:
            continue

        town, sp, di, ke, inline_start, inline_end = parsed
        row_start = inline_start or period_start
        row_end = inline_end or period_end
        row_canonical = (
            canonical_month_from_period(row_start, row_end, None, None)
            if inline_start else canonical_month
        )
        rows.append(
            build_row(
                source="epra_live",
                document_url=final_url or LIVE_URL,
                canonical_month=row_canonical,
                period_start=row_start,
                period_end=row_end,
                town=town,
                sp=sp,
                di=di,
                ke=ke,
                extraction_method="epra_live_table",
            )
        )

    rows = unique_rows(rows)
    log.info("Live EPRA rows parsed: %d", len(rows))
    return rows


# =========================
# RESOLUTION / PARSING
# =========================

async def resolve_and_parse_document(
    session: aiohttp.ClientSession,
    pool: asyncpg.Pool,
    doc: DocumentCandidate,
) -> None:
    candidates: List[str] = []
    tried: List[str] = []

    if doc.file_url:
        candidates.append(doc.file_url)
    if doc.page_url and doc.page_url != doc.file_url:
        candidates.append(doc.page_url)

    for url in candidates:
        if not url or url in tried:
            continue
        tried.append(url)

        data, ctype, err_bytes, final_url = await fetch_bytes(session, url)
        if data and (("pdf" in (ctype or "")) or url.lower().endswith(".pdf") or ".pdf?" in url.lower()):
            parsed_rows = extract_from_pdf_bytes(
                data=data,
                source=doc.source,
                document_url=final_url or url,
                year_hint=doc.year_hint,
                month_hint=doc.month_hint,
            )
            if parsed_rows:
                await upsert_rows(pool, parsed_rows)
                log.info("Parsed PDF rows=%d url=%s", len(parsed_rows), final_url or url)
                return

        html, err_text, final_html_url = await fetch_text(session, url)
        if html:
            parsed_rows = extract_from_html_text(
                html=html,
                source=doc.source,
                document_url=final_html_url or url,
                year_hint=doc.year_hint,
                month_hint=doc.month_hint,
            )
            if parsed_rows:
                await upsert_rows(pool, parsed_rows)
                log.info("Parsed HTML rows=%d url=%s", len(parsed_rows), final_html_url or url)
                return

    log.warning("Document failed title=%s page=%s", doc.title, doc.page_url)


# =========================
# MAIN
# =========================

async def main() -> None:
    log.info("Starting fuel scraper")
    log.info("Connecting to database %s:%s/%s", DB_HOST, DB_PORT, DB_NAME)

    pool = await asyncpg.create_pool(
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        host=DB_HOST,
        port=int(DB_PORT),
        ssl="require",
        min_size=1,
        max_size=8,
    )

    await init_db(pool)

    connector = aiohttp.TCPConnector(limit=30, limit_per_host=8, ssl=False)

    async with aiohttp.ClientSession(
        headers=HEADERS,
        timeout=REQUEST_TIMEOUT,
        connector=connector,
    ) as session:
        # 1) current table
        live_rows = await parse_live_epra_table(session)
        await upsert_rows(pool, live_rows)

        # 2) archive discovery
        docs = await discover_epra_archive(session)

        # 3) de-dup by (source, page_url, file_url) — title can vary for the same URL
        #    also skip the live pump-prices page (already handled above)
        dedup = {}
        for d in docs:
            if norm_url(d.page_url) == norm_url(LIVE_URL):
                continue
            key = (d.source, norm_url(d.page_url), norm_url(d.file_url) if d.file_url else "")
            if key not in dedup or (d.file_url and not dedup[key].file_url):
                dedup[key] = d
        docs = list(dedup.values())

        log.info("Documents queued: %d", len(docs))

        # 4) parse docs concurrently
        sem = asyncio.Semaphore(CONCURRENCY)

        async def worker(doc: DocumentCandidate) -> None:
            async with sem:
                try:
                    await resolve_and_parse_document(session, pool, doc)
                except Exception as e:
                    log.exception("Worker failed for %s", doc.page_url)

        await asyncio.gather(*[worker(d) for d in docs])

    # 5) Scribd documents (Playwright-rendered, outside aiohttp session)
    await scrape_scribd_with_playwright(pool)

    async with pool.acquire() as conn:
        total_rows = await conn.fetchval("select count(*) from fuel_price_rows")
        log.info("rows_total=%s", total_rows)

        coverage = await conn.fetch("""
            select
                extract(year from coalesce(canonical_month, period_start))::int as year,
                extract(month from coalesce(canonical_month, period_start))::int as month,
                count(distinct document_url)::int as documents,
                count(*)::int as rows
            from fuel_price_rows
            where coalesce(canonical_month, period_start) is not null
            group by 1,2
            order by 1,2
        """)

        for row in coverage:
            log.info(
                "coverage %s-%02d docs=%s rows=%s",
                row["year"], row["month"], row["documents"], row["rows"]
            )

    await pool.close()
    log.info("Done")


if __name__ == "__main__":
    asyncio.run(main())