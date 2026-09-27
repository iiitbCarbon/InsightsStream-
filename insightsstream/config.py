"""Environment-backed configuration for local and R2 object storage."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Settings:
    storage_backend: str = "local"
    local_lake_path: Path = Path("data/lakehouse")
    r2_endpoint_url: str | None = None
    r2_access_key_id: str | None = None
    r2_secret_access_key: str | None = None
    r2_bucket: str | None = None

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()
        backend = os.getenv("INSIGHTS_STORAGE_BACKEND", "local").lower()
        if backend not in {"local", "r2"}:
            raise ValueError("INSIGHTS_STORAGE_BACKEND must be 'local' or 'r2'")

        local_lake_path = Path(
            os.getenv("INSIGHTS_LOCAL_LAKE_PATH", "data/lakehouse")
        )
        if not local_lake_path.is_absolute():
            local_lake_path = PROJECT_ROOT / local_lake_path

        settings = cls(
            storage_backend=backend,
            local_lake_path=local_lake_path,
            r2_endpoint_url=os.getenv("R2_ENDPOINT_URL"),
            r2_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
            r2_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
            r2_bucket=os.getenv("R2_BUCKET"),
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        if self.storage_backend != "r2":
            return

        required = {
            "R2_ENDPOINT_URL": self.r2_endpoint_url,
            "R2_ACCESS_KEY_ID": self.r2_access_key_id,
            "R2_SECRET_ACCESS_KEY": self.r2_secret_access_key,
            "R2_BUCKET": self.r2_bucket,
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise ValueError(
                "R2 storage selected but these variables are missing: "
                + ", ".join(missing)
            )
