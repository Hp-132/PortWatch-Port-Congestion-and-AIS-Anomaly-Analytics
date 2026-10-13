"""Headless-browser test of the running dashboard (skipped if it is not running)."""
import shutil
import subprocess
import urllib.request
from pathlib import Path

import pytest

BASE = "http://localhost:8000"
SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "ui_check.mjs"


def _up():
    try:
        with urllib.request.urlopen(f"{BASE}/api/health", timeout=5) as r:
            return r.status == 200
    except Exception:
        return False


@pytest.mark.skipif(not shutil.which("node"), reason="node not installed")
@pytest.mark.skipif(not _up(), reason="dashboard not running on localhost:8000")
def test_all_views_render_without_errors():
    out = subprocess.run(["node", str(SCRIPT), BASE], capture_output=True, text=True, timeout=400)
    print(out.stdout)
    assert out.returncode == 0, out.stdout + out.stderr
    assert out.stdout.count("PASS") == 5
