"""Dictionary/regex asset extraction from article text."""

from __future__ import annotations

import re
from pathlib import Path

import structlog
import yaml

logger = structlog.get_logger()


class AssetExtractor:
    """Extracts asset and sector mentions from text using a versioned dictionary."""

    def __init__(
        self,
        dictionary_path: str,
        active_assets: set[str] | None = None,
    ) -> None:
        self._active_assets = active_assets or set()
        self._patterns: list[tuple[str, re.Pattern[str]]] = []
        self._load_dictionary(dictionary_path)

    def _load_dictionary(self, dictionary_path: str) -> None:
        """Load and compile regex patterns from YAML dictionary."""
        path = Path(dictionary_path)
        if not path.exists():
            msg = f"Asset dictionary not found: {dictionary_path}"
            raise FileNotFoundError(msg)

        try:
            with path.open() as f:
                data = yaml.safe_load(f)
        except Exception as e:
            msg = f"Malformed asset dictionary: {e}"
            raise ValueError(msg) from e

        if not isinstance(data, dict) or "version" not in data:
            msg = "Asset dictionary must have a 'version' field"
            raise ValueError(msg)

        # Compile asset alias patterns
        assets = data.get("assets", {})
        if isinstance(assets, dict):
            for canonical_id, info in assets.items():
                if not isinstance(info, dict):
                    continue
                for alias in info.get("aliases", []):
                    pattern = re.compile(rf"\b{re.escape(alias)}\b", re.IGNORECASE)
                    self._patterns.append((canonical_id, pattern))

        # Compile sector keyword patterns
        sectors = data.get("sectors", {})
        if isinstance(sectors, dict):
            for sector_tag, info in sectors.items():
                if not isinstance(info, dict):
                    continue
                for keyword in info.get("keywords", []):
                    pattern = re.compile(rf"\b{re.escape(keyword)}\b", re.IGNORECASE)
                    self._patterns.append((sector_tag, pattern))

        logger.info(
            "asset_dictionary_loaded",
            path=dictionary_path,
            version=data.get("version"),
            pattern_count=len(self._patterns),
        )

    def extract(self, text: str) -> list[str]:
        """Extract asset/sector identifiers from text.

        Returns deduplicated list of canonical IDs that match and are in active_assets.
        """
        seen: set[str] = set()
        result: list[str] = []

        for canonical_id, pattern in self._patterns:
            if canonical_id in seen:
                continue
            if pattern.search(text):
                seen.add(canonical_id)
                result.append(canonical_id)

        return result
