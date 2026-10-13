"""MongoDB connection for the dashboard API."""
from __future__ import annotations

import os
from functools import lru_cache

from pymongo import MongoClient
from pymongo.database import Database


@lru_cache(maxsize=1)
def _client() -> MongoClient:
    return MongoClient(os.environ.get("MX_MONGO_URI", "mongodb://localhost:27017"),
                       serverSelectionTimeoutMS=5000)


def get_db() -> Database:
    return _client()[os.environ.get("MX_MONGO_DB", "maritime_x")]
