import sys
from pathlib import Path

project_dir = Path(__file__).resolve().parent
parent_dir = project_dir.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))
