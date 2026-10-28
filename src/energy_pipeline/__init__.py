"""Energy data ELT pipeline — extraction, raw storage, and PostgreSQL staging.

Scope of this package: extraction, record typing, the immutable raw zone,
the Phase 3 PostgreSQL staging loader, and a thin warehouse connection seam.
The warehouse seam (``warehouse.py``) is imported lazily so the package still
works without ``psycopg`` installed when only extraction and the raw zone are
needed. The staging loader (``load.py``) is pure Python and always available.
dbt, Airflow and the dashboard are deliberately absent until their phases, so
that nothing here depends on an unverified assumption made downstream.

Storage is local: :mod:`energy_pipeline.storage` writes the raw zone to a
filesystem, immutably.
"""

__all__ = ["__version__", "load", "warehouse"]

__version__ = "0.1.0"

# Stage 2: raw zone → PostgreSQL staging loader (pure Python, always available).
# Phase 3: PostgreSQL warehouse seam (imported lazily so the package still
# works without psycopg installed when only extraction/raw-zone is used).
import contextlib

from . import load

with contextlib.suppress(ImportError):
    from . import warehouse
