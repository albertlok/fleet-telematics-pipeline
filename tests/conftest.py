"""
Shared pytest setup.

The ingestion/ and processor/ folders are two separate services, each
with its own modules. Tests live outside both, so we add the service
folders to Python's import path here. conftest.py runs automatically
before any test file — no need to repeat this in each test.
"""

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# NOTE: both services have a main.py and a config.py, so path order
# matters — "ingestion" is listed last so it ends up FIRST on sys.path,
# and `import main` in tests resolves to the ingestion service. The
# processor tests only import its uniquely-named modules (transformer,
# deduplication), so they are unaffected.
sys.path.insert(0, os.path.join(ROOT, "processor"))
sys.path.insert(0, os.path.join(ROOT, "ingestion"))
