import argparse
import io
import json
import os
import re
import sqlite3
import ssl
import subprocess
import threading
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

from pypdf import PdfReader

try:
    from redis import Redis
    from rq import Queue
except ImportError:  # pragma: no cover - exercised when optional deps are missing
    Redis = None
    Queue = None

BASE_URL = "https://justice.gov.et"
REGULATION_URL = "https://justice.gov.et/en/laws/regulations/"
DEFAULT_MIN_YEAR = 1987
DEFAULT_MAX_YEAR = datetime.now().year
DOWNLOAD_FOLDER = Path(__file__).resolve().parent / "pdfs"
DOWNLOAD_FOLDER.mkdir(exist_ok=True)

DB_LOCK = threading.Lock()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": BASE_URL + "/",
}

# Rate limiting: prevent server from banning IP due to rapid burst requests
_RATE_LIMIT_LOCK = threading.Lock()
_LAST_REQUEST_TIME: float = 0.0
_REQUEST_DELAY: float = 6.0  # seconds between requests


def normalize_url(url: str) -> str | None:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return parsed.geturl()


def sanitize_filename(text: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "_", text).strip("._-")
    return sanitized[:90] or "download"


def extract_year_from_date(date_str: str | None) -> int | None:
    """Extract year from various date formats."""
    if not date_str:
        return None
    # Try to find 4-digit year
    year_match = re.search(r'(19\d{2}|20\d{2})', str(date_str))
    if year_match:
        return int(year_match.group(1))
    return None


def is_within_date_range(publication_year: int | None, min_year: int, max_year: int) -> bool:
    """Check if publication year is within the specified range."""
    if publication_year is None:
        return True  # Include documents with unknown dates
    return min_year <= publication_year <= max_year


def extract_publication_date_from_html(html: str) -> str | None:
    """Extract a publication date from common HTML metadata tags."""
    patterns = [
        r'''<meta[^>]+property=["']article:published_time["'][^>]+content=["']([^"']+)["']''',
        r'''<meta[^>]+property=["']article:modified_time["'][^>]+content=["']([^"']+)["']''',
        r'''<meta[^>]+name=["'](?:pubdate|publishdate|date|publication_date)["'][^>]+content=["']([^"']+)["']''',
        r'''<meta[^>]+itemprop=["']datePublished["'][^>]+content=["']([^"']+)["']''',
        r'''<time[^>]+datetime=["']([^"']+)["']''',
    ]
    for pattern in patterns:
        match = re.search(pattern, html, re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(1).strip()
    return None


def update_publication_metadata(conn: sqlite3.Connection, url: str, publication_date: str | None) -> None:
    normalized = normalize_url(url)
    if not normalized:
        return
    publication_year = None
    if publication_date:
        publication_year = extract_year_from_date(publication_date)
    with DB_LOCK:
        conn.execute(
            "UPDATE discovered_urls SET publication_date=?, publication_year=? WHERE url=?",
            (publication_date, publication_year, normalized),
        )
        conn.commit()


def is_pdf_url(url: str) -> bool:
    lower = url.lower()
    return lower.endswith(".pdf") or "jet_download" in lower or "/download" in lower or "/downloadfile" in lower


def init_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    with DB_LOCK:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS discovered_urls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE NOT NULL,
                kind TEXT NOT NULL,
                source TEXT,
                status TEXT NOT NULL DEFAULT 'queued',
                last_error TEXT,
                publication_date TEXT,
                publication_year INTEGER,
                discovered_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS downloads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE NOT NULL,
                output_path TEXT,
                status TEXT NOT NULL,
                bytes INTEGER,
                content_type TEXT,
                error TEXT,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cursor = conn.execute("PRAGMA table_info(discovered_urls)")
        columns = [row[1] for row in cursor.fetchall()]
        if "publication_date" not in columns:
            conn.execute("ALTER TABLE discovered_urls ADD COLUMN publication_date TEXT")
        if "publication_year" not in columns:
            conn.execute("ALTER TABLE discovered_urls ADD COLUMN publication_year INTEGER")
        conn.commit()
    return conn


def enqueue_url(conn: sqlite3.Connection, url: str, kind: str, source: str | None = None, publication_date: str | None = None) -> None:
    normalized = normalize_url(url)
    if not normalized:
        return
    publication_year = None
    if publication_date:
        try:
            year = extract_year_from_date(publication_date)
            if year:
                publication_year = year
        except Exception:
            pass
    try:
        with DB_LOCK:
            conn.execute(
                """
                INSERT INTO discovered_urls(url, kind, source, publication_date, publication_year, status)
                VALUES (?, ?, ?, ?, ?, 'queued')
                ON CONFLICT(url) DO UPDATE SET
                    kind=excluded.kind,
                    source=excluded.source,
                    publication_date=excluded.publication_date,
                    publication_year=excluded.publication_year,
                    status=CASE WHEN discovered_urls.status IN ('processing', 'failed') THEN 'queued' ELSE discovered_urls.status END,
                    last_error=NULL
                """,
                (normalized, kind, source, publication_date, publication_year),
            )
            conn.commit()
    except sqlite3.Error:
        pass


def mark_discovered(conn: sqlite3.Connection, url: str, status: str, error: str | None = None) -> None:
    normalized = normalize_url(url)
    if not normalized:
        return
    with DB_LOCK:
        conn.execute(
            "UPDATE discovered_urls SET status=?, last_error=? WHERE url=?",
            (status, error, normalized),
        )
        conn.commit()


def get_pending_pages(conn: sqlite3.Connection) -> list[str]:
    with DB_LOCK:
        rows = conn.execute(
            "SELECT url FROM discovered_urls WHERE kind='page' AND status='queued' ORDER BY id LIMIT 200"
        ).fetchall()
    return [row[0] for row in rows]


def claim_pending_pages(conn: sqlite3.Connection, limit: int) -> list[str]:
    with DB_LOCK:
        rows = conn.execute(
            "SELECT url FROM discovered_urls WHERE kind='page' AND status='queued' ORDER BY id LIMIT ?",
            (limit,)
        ).fetchall()
        urls = [row[0] for row in rows]
        for url in urls:
            conn.execute(
                "UPDATE discovered_urls SET status='processing' WHERE url=?",
                (url,)
            )
        conn.commit()
    return urls


def get_pending_pdfs(conn: sqlite3.Connection, min_year: int = DEFAULT_MIN_YEAR, max_year: int = DEFAULT_MAX_YEAR) -> list[tuple[str, str]]:
    with DB_LOCK:
        rows = conn.execute(
            """SELECT url, source FROM discovered_urls 
               WHERE kind='pdf' AND status='queued'
               AND (publication_year IS NULL OR (publication_year >= ? AND publication_year <= ?))
               ORDER BY id""",
            (min_year, max_year)
        ).fetchall()
    return [(row[0], row[1] or "unknown") for row in rows]


def save_download_record(conn: sqlite3.Connection, url: str, output_path: str | None, status: str, bytes_size: int | None, content_type: str | None, error: str | None = None) -> None:
    normalized = normalize_url(url)
    if not normalized:
        return
    with DB_LOCK:
        conn.execute(
            """
            INSERT INTO downloads(url, output_path, status, bytes, content_type, error)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
                output_path=excluded.output_path,
                status=excluded.status,
                bytes=excluded.bytes,
                content_type=excluded.content_type,
                error=excluded.error,
                updated_at=CURRENT_TIMESTAMP
            """,
            (normalized, output_path, status, bytes_size, content_type, error),
        )
        conn.commit()


def _is_transient_fetch_error(exc: Exception) -> bool:
    if isinstance(exc, (TimeoutError, ConnectionResetError, ssl.SSLError, BrokenPipeError, OSError)):
        return True
    if isinstance(exc, urllib.error.URLError):
        reason = exc.reason
        if isinstance(reason, Exception):
            return _is_transient_fetch_error(reason)
        reason_text = str(reason).lower()
        return any(token in reason_text for token in ("timed out", "reset by peer", "connection aborted", "temporary failure", "connection reset", "failed to connect", "connection refused", "proxy"))
    return False


def _curl_fetch(url: str, timeout: int, binary: bool, proxies: dict[str, str] | None = None) -> tuple[bytes | str, dict[str, str]]:
    env = os.environ.copy()
    if proxies:
        if proxies.get("http_proxy"):
            env["http_proxy"] = proxies["http_proxy"]
            env["HTTP_PROXY"] = proxies["http_proxy"]
        if proxies.get("https_proxy"):
            env["https_proxy"] = proxies["https_proxy"]
            env["HTTPS_PROXY"] = proxies["https_proxy"]

    cmd = [
        "curl",
        "-L",
        "--max-time",
        str(timeout),
        "-A",
        HEADERS["User-Agent"],
        "-sS",
        "--http1.1",
        url,
    ]
    result = subprocess.run(cmd, capture_output=True, check=False, env=env)
    if result.returncode != 0:
        raise urllib.error.URLError(f"curl failed with exit code {result.returncode}: {result.stderr.decode('utf-8', 'ignore')}")
    body = result.stdout if binary else result.stdout.decode("utf-8", errors="ignore")
    return body, {}


def build_url_opener(proxies: dict[str, str] | None = None) -> urllib.request.OpenerDirector:
    proxy_map: dict[str, str] = {}
    if proxies:
        for key in ("http", "https"):
            if proxies.get(key):
                proxy_map[key] = proxies[key]
        for key in ("http_proxy", "https_proxy"):
            if proxies.get(key) and key.replace("_proxy", "") not in proxy_map:
                proxy_map[key.replace("_proxy", "")] = proxies[key]
    if proxy_map:
        return urllib.request.build_opener(urllib.request.ProxyHandler(proxy_map))
    return urllib.request.build_opener()


def fetch_text(url: str, timeout: int = 120, proxies: dict[str, str] | None = None) -> tuple[str, dict[str, str]]:
    """Fetch text using curl only with HTTP/1.1 and rate limiting.

    urllib is intentionally skipped: its TLS fingerprint is detected and
    blocked by the target server.  curl --http1.1 with a spoofed Chrome
    User-Agent is the only method confirmed to work.
    """
    global _LAST_REQUEST_TIME
    with _RATE_LIMIT_LOCK:
        elapsed = time.time() - _LAST_REQUEST_TIME
        if elapsed < _REQUEST_DELAY:
            time.sleep(_REQUEST_DELAY - elapsed)
        _LAST_REQUEST_TIME = time.time()

    last_error: Exception | None = None
    for attempt in range(3):
        try:
            body, headers = _curl_fetch(url, timeout, binary=False, proxies=proxies)  # type: ignore[assignment]
            return body, headers  # type: ignore[return-value]
        except Exception as exc:
            last_error = exc
            if not _is_transient_fetch_error(exc) or attempt == 2:
                break
            time.sleep(2.0 * (attempt + 1))

    if last_error is not None:
        raise last_error
    raise urllib.error.URLError("fetch_text failed")


def fetch_bytes(url: str, timeout: int = 240, proxies: dict[str, str] | None = None) -> tuple[bytes, dict[str, str]]:
    """Fetch binary data using curl only with HTTP/1.1 and rate limiting.

    urllib is intentionally skipped: its TLS fingerprint is detected and
    blocked by the target server.  curl --http1.1 with a spoofed Chrome
    User-Agent is the only method confirmed to work.
    """
    global _LAST_REQUEST_TIME
    with _RATE_LIMIT_LOCK:
        elapsed = time.time() - _LAST_REQUEST_TIME
        if elapsed < _REQUEST_DELAY:
            time.sleep(_REQUEST_DELAY - elapsed)
        _LAST_REQUEST_TIME = time.time()

    last_error: Exception | None = None
    for attempt in range(3):
        try:
            body, headers = _curl_fetch(url, timeout, binary=True, proxies=proxies)  # type: ignore[assignment]
            return body, headers  # type: ignore[return-value]
        except Exception as exc:
            last_error = exc
            if not _is_transient_fetch_error(exc) or attempt == 2:
                break
            time.sleep(2.0 * (attempt + 1))

    if last_error is not None:
        raise last_error
    raise urllib.error.URLError("fetch_bytes failed")


def extract_links_from_html(html: str, page_url: str) -> list[str]:
    links = []
    for match in re.finditer(r'''href=["']([^"']+)["']''', html, re.IGNORECASE):
        href = match.group(1).strip()
        if not href:
            continue
        if href.startswith("mailto:") or href.startswith("tel:"):
            continue
        full_url = urljoin(page_url, href)
        if full_url.startswith(("http://", "https://")):
            links.append(full_url)
    return links


def discover_sitemaps(base_url: str, conn: sqlite3.Connection, proxies: dict[str, str] | None = None) -> None:
    candidates = [
        urljoin(base_url.rstrip("/") + "/", "sitemap_index.xml"),
        urljoin(base_url.rstrip("/") + "/", "sitemap.xml"),
        urljoin(base_url.rstrip("/") + "/", "wp-sitemap.xml"),
        urljoin(base_url.rstrip("/") + "/", "wp-sitemap-pages.xml"),
        urljoin(base_url.rstrip("/") + "/", "wp-sitemap-posts.xml"),
        urljoin(base_url.rstrip("/") + "/", "sitemap.xml.gz"),
    ]
    seen = set()

    while candidates:
        sitemap_url = candidates.pop()
        if sitemap_url in seen:
            continue
        seen.add(sitemap_url)
        try:
            text, _ = fetch_text(sitemap_url, proxies=proxies)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError):
            continue
        try:
            root = ET.fromstring(text)
        except ET.ParseError:
            continue
        for loc in [elem.text.strip() for elem in root.findall(".//{*}loc") if elem.text and elem.text.strip()]:
            if loc.endswith(".xml"):
                candidates.append(loc)
            else:
                enqueue_url(conn, loc, "page", "sitemap")


def discover_wordpress_api(base_url: str, conn: sqlite3.Connection, proxies: dict[str, str] | None = None) -> None:
    endpoints = [
        "wp-json/wp/v2/pages",
        "wp-json/wp/v2/posts",
    ]
    for endpoint in endpoints:
        api_url = urljoin(base_url.rstrip("/") + "/", endpoint)
        page = 1
        while page <= 10:
            request_url = f"{api_url}?per_page=100&page={page}"
            try:
                text, headers = fetch_text(request_url, proxies=proxies)
            except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError):
                break
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                break
            if not isinstance(payload, list) or not payload:
                break
            for item in payload:
                link = item.get("link") or item.get("url") or item.get("guid", {}).get("rendered")
                if link:
                    enqueue_url(conn, link, "page", "wordpress_api")
            page += 1
            if not headers.get("x-wp-totalpages"):
                break


def seed_discovery(base_url: str, conn: sqlite3.Connection, proxies: dict[str, str] | None = None) -> None:
    enqueue_url(conn, base_url, "page", "seed")
    discover_sitemaps(base_url, conn, proxies=proxies)
    discover_wordpress_api(base_url, conn, proxies=proxies)


def crawl_pages(conn: sqlite3.Connection, max_pages: int = 0, proxies: dict[str, str] | None = None, workers: int = 10) -> None:
    processed = 0

    def crawl_worker(page_url: str) -> None:
        try:
            html, _ = fetch_text(page_url, proxies=proxies)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError) as exc:
            mark_discovered(conn, page_url, "failed", str(exc))
            return

        publication_date = extract_publication_date_from_html(html)
        if publication_date:
            update_publication_metadata(conn, page_url, publication_date)

        for link in extract_links_from_html(html, page_url):
            if is_pdf_url(link):
                enqueue_url(conn, link, "pdf", "discovered_from_page")
            elif urlparse(link).netloc == urlparse(page_url).netloc:
                enqueue_url(conn, link, "page", "discovered_from_page", publication_date=publication_date)

        mark_discovered(conn, page_url, "done")

    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = set()
        while True:
            if max_pages > 0:
                remaining = max_pages - processed
                if remaining <= 0:
                    break
                batch_size = min(workers * 2, remaining)
            else:
                batch_size = workers * 2

            urls = claim_pending_pages(conn, batch_size)
            if not urls and not futures:
                break

            for url in urls:
                futures.add(executor.submit(crawl_worker, url))

            if futures:
                done, futures = wait(futures, return_when=FIRST_COMPLETED)
                processed += len(done)

            if not urls and futures:
                time.sleep(0.5)


def download_pdfs(conn: sqlite3.Connection, output_dir: Path, workers: int = 20, min_year: int = DEFAULT_MIN_YEAR, max_year: int = DEFAULT_MAX_YEAR, proxies: dict[str, str] | None = None) -> list[tuple[str, str]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    pending = get_pending_pdfs(conn, min_year=min_year, max_year=max_year)
    results: list[tuple[str, str]] = []

    def worker(url: str, source: str) -> tuple[str, str]:
        normalized = normalize_url(url)
        if not normalized:
            return "", "invalid"

        parsed = urlparse(normalized)
        candidate_name = Path(parsed.path).name or "download"
        if not candidate_name.lower().endswith(".pdf"):
            candidate_name = f"{sanitize_filename(candidate_name)}.pdf"

        if "jet_download" in parsed.query:
            token = parsed.query.split("jet_download=", 1)[1].split("&", 1)[0][:12]
            candidate_name = f"{sanitize_filename(Path(candidate_name).stem)}_{token}.pdf"

        output_path = output_dir / candidate_name
        if output_path.exists():
            save_download_record(conn, normalized, str(output_path), "skipped", output_path.stat().st_size, "application/pdf")
            return str(output_path), "skipped"

        try:
            body, headers = fetch_bytes(normalized, proxies=proxies)
            content_type = headers.get("content-type", "")
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError) as exc:
            mark_discovered(conn, normalized, "failed", str(exc))
            save_download_record(conn, normalized, None, "failed", None, None, str(exc))
            return "", "failed"

        if not body or len(body) < 20:
            mark_discovered(conn, normalized, "failed", "empty response")
            save_download_record(conn, normalized, None, "failed", None, content_type, "empty response")
            return "", "failed"

        is_pdf_payload = body.startswith(b"%PDF") or "pdf" in content_type.lower() or normalized.lower().endswith(".pdf")
        if not is_pdf_payload:
            mark_discovered(conn, normalized, "failed", "not a PDF response")
            save_download_record(conn, normalized, None, "failed", len(body), content_type, "not a PDF response")
            return "", "failed"

        try:
            reader = PdfReader(io.BytesIO(body))
            text = "".join((page.extract_text() or "") for page in reader.pages[:2])
        except Exception:
            text = ""

        output_path.write_bytes(body)
        status = "downloaded"
        if not text.strip():
            status = "downloaded_needs_ocr"
        save_download_record(conn, normalized, str(output_path), status, len(body), content_type)
        mark_discovered(conn, normalized, status)
        if status == "downloaded_needs_ocr":
            if Redis is None or Queue is None:
                print(f"Redis/RQ not installed; skipping OCR queue for {output_path}")
            else:
                try:
                    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
                    redis_conn = Redis.from_url(redis_url)
                    q = Queue("ocr", connection=redis_conn)
                    q.enqueue("ocr_tasks.ocr_pdf", str(output_path), job_timeout=60 * 60)
                except Exception as exc:
                    mark_discovered(conn, normalized, status, f"enqueue_error:{exc}")
        return str(output_path), status

    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = [executor.submit(worker, url, source) for url, source in pending]
        for future in as_completed(futures):
            results.append(future.result())
    return results


def recover_stale_processing(conn: sqlite3.Connection) -> int:
    """Reset pages stuck in 'processing' back to 'queued' (run after stopping a crashed crawler)."""
    with DB_LOCK:
        cursor = conn.execute(
            """
            UPDATE discovered_urls
            SET status='queued', last_error=NULL
            WHERE kind='page' AND status='processing'
            """
        )
        conn.commit()
        return cursor.rowcount


def run(
    base_url: str,
    output_dir: Path,
    workers: int,
    max_pages: int,
    min_year: int = DEFAULT_MIN_YEAR,
    max_year: int = DEFAULT_MAX_YEAR,
    proxies: dict[str, str] | None = None,
    *,
    download_only: bool = False,
    skip_crawl: bool = False,
    recover_processing: bool = False,
) -> None:
    if not download_only and not base_url.startswith(("http://", "https://")):
        raise SystemExit("Base URL must start with http:// or https://")

    output_dir.mkdir(parents=True, exist_ok=True)
    db_path = output_dir / "download_queue.sqlite3"
    conn = init_db(db_path)
    try:
        if recover_processing:
            recovered = recover_stale_processing(conn)
            print(f"Recovered {recovered} processing pages back to queued")

        if download_only:
            results = download_pdfs(conn, output_dir, workers=workers, min_year=min_year, max_year=max_year, proxies=proxies)
        else:
            seed_discovery(base_url, conn, proxies=proxies)
            if not skip_crawl:
                crawl_pages(conn, max_pages=max_pages, proxies=proxies, workers=workers)
            results = download_pdfs(conn, output_dir, workers=workers, min_year=min_year, max_year=max_year, proxies=proxies)
    finally:
        conn.close()

    print(f"Queue database: {db_path}")
    print(f"Output directory: {output_dir}")
    print(f"Date range: {min_year}-{max_year}")
    print(f"Finished {len(results)} download tasks")
    for path, status in results:
        if path:
            print(f"[{status}] {Path(path).name}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Robust WordPress/sitemap PDF downloader with date filtering")
    parser.add_argument("--base-url", default=REGULATION_URL, help="Seed URL to crawl")
    parser.add_argument("--output-dir", default=str(DOWNLOAD_FOLDER), help="Directory to save PDFs")
    parser.add_argument("--workers", type=int, default=20, help="Concurrent download workers")
    parser.add_argument("--max-pages", type=int, default=0, help="Maximum number of HTML pages to crawl (0 for unlimited)")
    parser.add_argument("--min-year", type=int, default=DEFAULT_MIN_YEAR, help="Minimum publication year (default: 1987)")
    parser.add_argument("--max-year", type=int, default=DEFAULT_MAX_YEAR, help=f"Maximum publication year (default: {DEFAULT_MAX_YEAR})")
    parser.add_argument("--http-proxy", help="Optional HTTP proxy URL")
    parser.add_argument("--https-proxy", help="Optional HTTPS proxy URL")
    parser.add_argument("--download-only", action="store_true", help="Skip discovery/crawl; download queued PDFs only")
    parser.add_argument("--skip-crawl", action="store_true", help="Run discovery but skip page crawling")
    parser.add_argument("--recover-processing", action="store_true", help="Reset all 'processing' pages to 'queued' (stop crawler first)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    proxies: dict[str, str] = {}
    if args.http_proxy:
        proxies["http_proxy"] = args.http_proxy
    if args.https_proxy:
        proxies["https_proxy"] = args.https_proxy
    if not proxies:
        proxies = None
    run(
        args.base_url,
        Path(args.output_dir),
        args.workers,
        args.max_pages,
        args.min_year,
        args.max_year,
        proxies=proxies,
        download_only=args.download_only,
        skip_crawl=args.skip_crawl,
        recover_processing=args.recover_processing,
    )


if __name__ == "__main__":
    main()
