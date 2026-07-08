import subprocess
from pathlib import Path


def ocr_pdf(input_path: str) -> str:
    """Run ocrmypdf on input_path in-place (writes to same path). Returns output path.

    Requires system `ocrmypdf` and Tesseract to be installed.
    """
    inp = Path(input_path)
    out = inp.with_suffix(".ocr.pdf")

    cmd = [
        "ocrmypdf",
        "--skip-text",
        "--deskew",
        "--rotate-pages",
        str(inp),
        str(out),
    ]

    subprocess.run(cmd, check=True)

    # Replace original file with OCRed version
    out.replace(inp)
    return str(inp)
