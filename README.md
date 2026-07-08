# pdf_reader

Download regulation PDFs from [justice.gov.et](https://justice.gov.et/en/laws/regulations/).

## Files in this repo (what you need)

```
pdf_reader/
├── README.md
├── requirements.txt              # full project deps
├── .gitignore
└── justice_regulation_downloader/
    ├── downloader.py             # main script
    ├── ocr_tasks.py              # OCR worker (optional)
    ├── requirements.txt          # minimal deps for downloader only
    ├── README_OCR.md
    ├── tests/
    └── pdfs/                     # output folder (empty on clone)
```

**Not in git (local only):** downloaded PDFs, SQLite queue DB, venv, debug files.

## Setup on another PC / server

```bash
git clone https://github.com/blengebre-ug-hub/pdf_reader.git
cd pdf_reader

python3 -m venv .venv
source .venv/bin/activate

# minimal install (downloader only)
pip install -r justice_regulation_downloader/requirements.txt

# system tool used as network fallback
sudo apt install curl   # Debian/Ubuntu

cd justice_regulation_downloader
python3 downloader.py --download-only --output-dir pdfs --workers 20
```

## Continue an existing download queue

Copy the queue DB separately (too large / changes often for git):

```bash
scp user@SOURCE:pdf_reader/justice_regulation_downloader/pdfs/download_queue.sqlite3 \
    justice_regulation_downloader/pdfs/
```

## Network check (required)

The site blocks some IPs. Test before running:

```bash
curl -L --max-time 30 --http1.1 \
  "https://justice.gov.et/en/laws/regulations/" -o /tmp/test.html
```

If you get `Connection reset by peer`, use a proxy:

```bash
python3 downloader.py --download-only --workers 20 \
  --https-proxy "http://PROXY:PORT" --output-dir pdfs
```
# pdf_reader
# pdf_reader
