"""
Demo Plugin for Data Collector Hub v1.0

A minimal example plugin demonstrating the plugin contract:
- Required metadata attributes
- fetch() implementation returning DataItem list
- normalize() implementation (optional but recommended)
- Weak structure in normalize() output
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from core.base_adapter import BaseAdapter, DataItem


class DemoPluginAdapter(BaseAdapter):
    """
    Demo plugin adapter - generates sample data for testing.

    This plugin demonstrates:
    1. Required metadata attributes
    2. fetch() returning DataItem list
    3. normalize() with weak structure (optional fields)
    4. Pipeline generates unique_key (plugin doesn't)
    """

    # Required metadata
    name = "demo_plugin"
    version = "1.0.0"
    description = "Demo plugin generating sample data for testing"
    author = "admin"
    tags = ["demo", "test"]

    # Configuration schema
    config_schema = {
        "item_count": {
            "type": "integer",
            "required": False,
            "description": "Number of items to generate",
            "default": 3
        },
        "prefix": {
            "type": "string",
            "required": False,
            "description": "Title prefix for generated items",
            "default": "Demo"
        }
    }

    # MVP: dependencies must be empty
    dependencies = []

    # Collection mode
    collection_mode = "full"  # or "incremental"

    async def fetch(self, **kwargs) -> List[DataItem]:
        """
        Generate sample data items.

        Returns:
            List of DataItem objects
        """
        # Get config with defaults
        item_count = self.config.get("item_count", 3)
        prefix = self.config.get("prefix", "Demo")

        items = []
        now = datetime.now()

        for i in range(item_count):
            item_data = {
                "id": i + 1,
                "title": f"{prefix} News Item {i + 1}",
                "content": f"This is sample content for item {i + 1}",
                "category": "technology" if i % 2 == 0 else "business",
                "views": 100 * (i + 1),
                "published_at": now.isoformat()
            }

            items.append(DataItem(
                source="demo_source",
                plugin_id=self.name,
                timestamp=now,
                data=item_data,
                metadata={
                    "sequence": i + 1,
                    "generated": True
                }
            ))

        return items

    def normalize(self, raw_data: Dict[str, Any], raw_data_id: int) -> Optional[Dict[str, Any]]:
        """
        Convert raw data to normalized format.

        Note: This is weakly structured - fields are optional.
        Pipeline generates unique_key, not this method.

        Args:
            raw_data: The raw data from fetch()
            raw_data_id: ID of the raw_data record

        Returns:
            Normalized data dict with recommended fields
        """
        # Extract fields from raw data
        title = raw_data.get("title", "")
        category = raw_data.get("category", "")

        # Build normalized output
        # Note: unique_key is NOT included - Pipeline generates it
        return {
            "event_type": "news",  # news/social/finance/alert
            "event_source": "DemoSource",  # Event source name
            "entity": [category] if category else [],  # Optional entity list
            "event_timestamp": datetime.now(),  # Event time
            "title": title,  # Required for dedup key generation
            "payload": raw_data,  # Standardized container
            "confidence": 1.0
        }

    async def health_check(self) -> bool:
        """
        Health check - demo plugin is always healthy.

        Returns:
            True
        """
        return True

    def get_default_schedule(self) -> Optional[str]:
        """
        Default schedule - run every 5 minutes.

        Returns:
            Cron expression
        """
        return "*/5 * * * *"
