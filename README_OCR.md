OCR integration and worker instructions

Requirements (system):
- ocrmypdf (apt: `ocrmypdf`)
- tesseract-ocr (and language packs you need)
- poppler-utils (for `pdftoppm` used by ocrmypdf)
- Redis server

Python requirements (see `requirements.txt`): `redis`, `rq`

Quick start

1. Install system deps (Debian/Ubuntu):
```bash
sudo apt update
sudo apt install -y ocrmypdf tesseract-ocr poppler-utils redis-server
```

2. Start Redis (or use system service):
```bash
sudo service redis-server start
# or docker: docker run -p 6379:6379 redis
```

3. Start an RQ worker for the `ocr` queue from the project root:
```bash
export PYTHONPATH=.
rq worker ocr
```

4. Run the downloader as normal; files flagged `downloaded_needs_ocr` are enqueued and processed by workers.

Notes
- `ocr_tasks.ocr_pdf` will run `ocrmypdf --skip-text --deskew`. Adjust flags as needed.
- For production, run Redis in a managed way and scale workers across machines.
