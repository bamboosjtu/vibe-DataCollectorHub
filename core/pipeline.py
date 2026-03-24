"""
Data Pipeline for Data Collector Hub v1.0

Assumptions:
- Pipeline generates unique_key (not the plugin)
- normalize() output is weakly structured (no strict validation)
- Data flows: fetch -> raw_data -> normalize -> unique_key -> normalized_data
- plugin_state is updated after successful processing
- task_stats are updated after execution
"""

import hashlib
from datetime import datetime
from typing import Any, Dict, List, Optional

from core.base_adapter import BaseAdapter, DataItem
from storage.sqlite_store import SQLiteStore


def generate_unique_key(plugin_id: str, event_source: str,
                        title: str, event_timestamp) -> str:
    """
    Generate deduplication key.

    Algorithm: MD5(plugin_id + ":" + event_source + ":" + title + ":" + event_timestamp)

    Args:
        plugin_id: Plugin identifier
        event_source: Event source name
        title: Content title/identifier (first 50 chars)
        event_timestamp: Event timestamp

    Returns:
        MD5 hash string (32 chars)
    """
    # Title: first 50 chars
    title_short = title[:50] if title else ""

    # Normalize timestamp to string
    if isinstance(event_timestamp, datetime):
        ts_str = event_timestamp.isoformat()
    else:
        ts_str = str(event_timestamp) if event_timestamp else ""

    unique_str = f"{plugin_id}:{event_source}:{title_short}:{ts_str}"
    return hashlib.md5(unique_str.encode("utf-8")).hexdigest()


class DataPipeline:
    """
    Data processing pipeline.

    Responsible for:
    1. Executing plugin fetch()
    2. Saving raw_data
    3. Calling normalize()
    4. Generating unique_key
    5. Saving normalized_data (with dedup)
    6. Updating plugin_state (for incremental)
    7. Updating task_stats
    8. Writing logs
    """

    def __init__(self, store: SQLiteStore):
        self.store = store

    async def process_plugin(self, adapter: BaseAdapter,
                            incremental: bool = True) -> Dict[str, Any]:
        """
        Process a plugin: fetch -> save raw -> normalize -> save normalized.

        Args:
            adapter: Plugin adapter instance
            incremental: Whether to use incremental mode

        Returns:
            Processing result statistics
        """
        plugin_id = adapter.name
        result = {
            "plugin_id": plugin_id,
            "items_fetched": 0,
            "raw_saved": 0,
            "normalized_saved": 0,
            "duplicates": 0,
            "errors": [],
            "success": False
        }

        try:
            # 1. Get plugin state for incremental collection
            state = None
            if incremental and adapter.collection_mode == "incremental":
                state = self.store.get_plugin_state(plugin_id)
                if state:
                    print(f"[Pipeline] Loaded plugin state: {state}")

            # 2. Execute fetch with state
            print(f"[Pipeline] Fetching data from {plugin_id}...")
            await adapter.before_fetch()
            items = await adapter.fetch(state=state)
            items = await adapter.after_fetch(items)
            result["items_fetched"] = len(items)
            print(f"[Pipeline] Fetched {len(items)} items from {plugin_id}")

            # 2. Process each item
            for item in items:
                try:
                    # Save raw_data
                    raw_data_id = self._save_raw_data(item)
                    result["raw_saved"] += 1

                    # Normalize and save normalized_data
                    norm_result = self._process_normalized(
                        adapter, item.data, raw_data_id
                    )

                    if norm_result == -1:
                        result["duplicates"] += 1
                    elif norm_result > 0:
                        result["normalized_saved"] += 1

                except Exception as e:
                    error_msg = f"Failed to process item: {e}"
                    print(f"[Pipeline] {error_msg}")
                    result["errors"].append(error_msg)
                    self.store.write_log(plugin_id, "ERROR", error_msg)

            # 3. Update plugin_state (for incremental collection)
            if incremental and items:
                self._update_plugin_state(adapter, items)

            # 4. Update task_stats (success)
            self.store.update_task_stats(plugin_id, success=True)

            result["success"] = True
            self.store.write_log(
                plugin_id, "INFO",
                f"Plugin execution completed: {result['items_fetched']} items fetched, "
                f"{result['normalized_saved']} normalized saved"
            )

        except Exception as e:
            error_msg = f"Plugin execution failed: {e}"
            print(f"[Pipeline] {error_msg}")
            result["errors"].append(error_msg)

            # Update task_stats (failure)
            self.store.update_task_stats(plugin_id, success=False)
            self.store.write_log(plugin_id, "ERROR", error_msg)

        return result

    def _save_raw_data(self, item: DataItem) -> int:
        """Save raw data to database"""
        return self.store.save_raw_data(
            plugin_id=item.plugin_id,
            source=item.source,
            data=item.data,
            metadata=item.metadata
        )

    def _process_normalized(self, adapter: BaseAdapter,
                           raw_data: Dict[str, Any],
                           raw_data_id: int) -> int:
        """
        Normalize data and save to normalized_data table.

        Returns:
            -1: Duplicate data
             0: No normalization (adapter.normalize returned None)
            >0: normalized_data ID
        """
        # Call adapter's normalize method
        normalized = adapter.normalize(raw_data, raw_data_id)

        if normalized is None:
            return 0

        # Extract fields (weak structure - fields are optional)
        event_type = normalized.get("event_type")
        event_source = normalized.get("event_source", "")
        entity = normalized.get("entity")
        event_timestamp = normalized.get("event_timestamp")
        title = normalized.get("title", "")
        payload = normalized.get("payload", raw_data)
        confidence = normalized.get("confidence", 1.0)

        # Generate unique_key (Pipeline responsibility)
        unique_key = generate_unique_key(
            adapter.name,
            event_source,
            title,
            event_timestamp
        )

        # Save to normalized_data
        return self.store.save_normalized_data(
            raw_data_id=raw_data_id,
            plugin_id=adapter.name,
            event_type=event_type,
            event_source=event_source,
            entity=entity,
            event_timestamp=event_timestamp,
            unique_key=unique_key,
            payload=payload,
            confidence=confidence
        )

    def _update_plugin_state(self, adapter: BaseAdapter,
                            items: List[DataItem]) -> None:
        """Update plugin state for incremental collection"""
        if not items:
            return

        plugin_id = adapter.name

        # Ensure plugin exists in plugins table (required for FK constraint)
        existing_plugin = self.store.get_plugin(plugin_id)
        if not existing_plugin:
            # Register plugin metadata first
            self.store.save_plugin(
                plugin_id=plugin_id,
                name=adapter.name,
                version=adapter.version,
                description=adapter.description,
                author=adapter.author,
                tags=adapter.tags,
                config_schema=adapter.config_schema,
                enabled=True
            )

        # Get last item's timestamp
        last_item = items[-1]
        last_timestamp = last_item.timestamp

        # Save state
        self.store.save_plugin_state(
            plugin_id=plugin_id,
            last_timestamp=last_timestamp
        )
        print(f"[Pipeline] Updated plugin state: last_timestamp={last_timestamp}")

    def process_single_item(self, adapter: BaseAdapter,
                           data: Dict[str, Any],
                           source: str = "manual") -> Dict[str, Any]:
        """
        Process a single data item (for manual/testing use).

        Args:
            adapter: Plugin adapter
            data: Raw data dictionary
            source: Source identifier

        Returns:
            Processing result
        """
        result = {
            "plugin_id": adapter.name,
            "raw_data_id": None,
            "normalized_data_id": None,
            "is_duplicate": False,
            "success": False
        }

        try:
            # Create a DataItem
            item = DataItem(
                source=source,
                plugin_id=adapter.name,
                timestamp=datetime.now(),
                data=data,
                metadata={"manual": True}
            )

            # Save raw
            raw_data_id = self._save_raw_data(item)
            result["raw_data_id"] = raw_data_id
            print(f"[Pipeline] Saved raw_data: id={raw_data_id}")

            # Normalize and save
            norm_id = self._process_normalized(adapter, data, raw_data_id)

            if norm_id == -1:
                result["is_duplicate"] = True
                print("[Pipeline] Duplicate data detected, skipped")
            elif norm_id > 0:
                result["normalized_data_id"] = norm_id
                print(f"[Pipeline] Saved normalized_data: id={norm_id}")

            result["success"] = True

        except Exception as e:
            print(f"[Pipeline] Failed to process single item: {e}")
            result["error"] = str(e)

        return result
