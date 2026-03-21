import sys
from pathlib import Path

# Ensure project root is importable when pytest is executed from any cwd.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
