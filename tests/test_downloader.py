import importlib
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import downloader
from downloader import extract_links_from_html, extract_publication_date_from_html, is_pdf_url


class DownloaderTests(unittest.TestCase):
    def test_extract_links_from_html_finds_pdf_and_page_links(self) -> None:
        html = """
        <html><body>
          <a href="https://example.com/page">Page</a>
          <a href="/nested/page">Nested</a>
          <a href="https://example.com/file.pdf">PDF</a>
          <a href="https://example.com/?jet_download=abc123">Jet download</a>
        </body></html>
        """
        links = extract_links_from_html(html, "https://example.com/landing")
        self.assertIn("https://example.com/page", links)
        self.assertIn("https://example.com/nested/page", links)
        self.assertIn("https://example.com/file.pdf", links)
        self.assertIn("https://example.com/?jet_download=abc123", links)

    def test_is_pdf_url_detects_direct_and_jet_download_urls(self) -> None:
        self.assertTrue(is_pdf_url("https://example.com/file.pdf"))
        self.assertTrue(is_pdf_url("https://example.com/?jet_download=abc123"))
        self.assertTrue(is_pdf_url("https://example.com/download"))
        self.assertFalse(is_pdf_url("https://example.com/page"))

    def test_extract_publication_date_from_html_parses_metadata(self) -> None:
        html = """
        <html><head>
          <meta property="article:published_time" content="2010-05-20T12:34:56+00:00" />
          <meta name="pubdate" content="2011-01-01" />
          <meta itemprop="datePublished" content="2012-02-02" />
          <time datetime="2013-03-03">March 3, 2013</time>
        </head></html>
        """
        self.assertEqual(extract_publication_date_from_html(html), "2010-05-20T12:34:56+00:00")

    def test_module_imports_without_redis_dependency(self) -> None:
        sys.modules.pop("downloader", None)
        module = importlib.import_module("downloader")
        self.assertTrue(hasattr(module, "extract_links_from_html"))

    def test_parse_args_defaults_max_year_to_current_year(self) -> None:
        with patch.object(sys, "argv", ["downloader.py"]):
            args = downloader.parse_args()
        self.assertEqual(args.max_year, datetime.now().year)

    def test_module_can_be_invoked_with_python_m(self) -> None:
        project_dir = Path(__file__).resolve().parents[1]
        completed = subprocess.run(
            [sys.executable, "-m", "justice_regulation_downloader.downloader", "--help"],
            cwd=project_dir,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, msg=completed.stderr)
        self.assertIn("--http-proxy", completed.stdout)

    def test_crawl_pages_does_not_propagate_page_date_to_pdf_links(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.sqlite3"
            conn = downloader.init_db(db_path)
            try:
                conn.execute(
                    "INSERT INTO discovered_urls(url, kind, source, status, publication_date, publication_year) VALUES (?, ?, ?, ?, ?, ?)",
                    ("https://example.com/page", "page", "seed", "queued", "2025-01-01", 2025),
                )
                conn.commit()
                with patch.object(downloader, "fetch_text", return_value=("<html></html>", {})):
                    with patch.object(downloader, "extract_links_from_html", return_value=["https://example.com/file.pdf"]):
                        with patch.object(downloader, "extract_publication_date_from_html", return_value="2025-01-01"):
                            downloader.crawl_pages(conn, max_pages=1)
                pdf_row = conn.execute(
                    "SELECT publication_date, publication_year FROM discovered_urls WHERE url=?",
                    ("https://example.com/file.pdf",),
                ).fetchone()
                self.assertIsNone(pdf_row[0])
                self.assertIsNone(pdf_row[1])
            finally:
                conn.close()

    def test_build_url_opener_normalizes_proxy_keys(self) -> None:
        with patch.object(downloader.urllib.request, "ProxyHandler", return_value=object()) as mock_proxy_handler:
            with patch.object(downloader.urllib.request, "build_opener", return_value=object()) as mock_build_opener:
                downloader.build_url_opener({"http_proxy": "http://proxy", "https_proxy": "http://proxy"})

        mock_proxy_handler.assert_called_once_with({"http": "http://proxy", "https": "http://proxy"})
        mock_build_opener.assert_called_once()

    def test_fetch_text_falls_back_to_curl_on_timeout(self) -> None:
        class DummyResult:
            returncode = 0
            stdout = b"<html>ok</html>"
            stderr = b""

        with patch.object(downloader.urllib.request, "urlopen", side_effect=TimeoutError("boom")):
            with patch.object(downloader.subprocess, "run", return_value=DummyResult()) as mock_run:
                body, headers = downloader.fetch_text("https://example.com")

        self.assertEqual(body, "<html>ok</html>")
        self.assertEqual(headers, {})
        self.assertEqual(mock_run.call_count, 1)

    def test_enqueue_url_requeues_processing_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.sqlite3"
            conn = downloader.init_db(db_path)
            try:
                conn.execute(
                    "INSERT INTO discovered_urls(url, kind, source, status) VALUES (?, ?, ?, ?)",
                    ("https://example.com", "page", "seed", "processing"),
                )
                conn.commit()
                downloader.enqueue_url(conn, "https://example.com", "page", "seed")
                status = conn.execute(
                    "SELECT status FROM discovered_urls WHERE url=?",
                    ("https://example.com",),
                ).fetchone()[0]
                self.assertEqual(status, "queued")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
