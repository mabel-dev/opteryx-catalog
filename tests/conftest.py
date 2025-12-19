import sys
from pathlib import Path

# Ensure tests import the local package source instead of an installed distribution
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
