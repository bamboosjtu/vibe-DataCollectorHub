from __future__ import annotations

from typing import Any

from storage.sqlite_store import SQLiteStore


class CanonicalScopeSelector:
    """Select downloader scope_items from canonical_entities."""

    def __init__(self, store: SQLiteStore):
        self.store = store

    def select_scope_items(
        self,
        selector: dict[str, Any] | None,
        *,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        if not selector:
            return []
        entity_type = selector.get("entity_type")
        if not entity_type:
            return []
        effective_limit = int(selector.get("limit") or limit or 1000)
        filters = selector.get("filter") or {}
        if not isinstance(filters, dict):
            filters = {}

        items: list[dict[str, Any]] = []
        offset_limit = max(effective_limit * 5, effective_limit)
        for entity in self.store.list_canonical_entities(
            entity_type=str(entity_type),
            limit=offset_limit,
        ):
            attributes = entity.get("attributes") or {}
            if not self._matches(attributes, filters):
                continue
            items.append(
                {
                    "entity_type": entity["entity_type"],
                    "entity_key": entity["entity_key"],
                    "attributes": attributes,
                }
            )
            if len(items) >= effective_limit:
                break
        return items

    @staticmethod
    def _matches(attributes: dict[str, Any], filters: dict[str, Any]) -> bool:
        for key, expected in filters.items():
            if attributes.get(key) != expected:
                return False
        return True
