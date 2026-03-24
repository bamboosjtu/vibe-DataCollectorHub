"""
RSS News Plugin for Data Collector Hub v1.0

Fetches news from China News RSS feed.
RSS Source: https://www.chinanews.com.cn/rss/scroll-news.xml

Assumptions:
- Uses Python standard library xml.etree.ElementTree for parsing
- No external dependencies (feedparser not required)
- Graceful handling of parse failures
- HTTP request via urllib (standard library)
"""

import urllib.request
import urllib.error
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional
from xml.etree import ElementTree as ET

from core.base_adapter import BaseAdapter, DataItem


class RssNewsAdapter(BaseAdapter):
    """
    RSS News adapter - fetches news from China News RSS feed.

    RSS Source: https://www.chinanews.com.cn/rss/scroll-news.xml
    """

    # Required metadata
    name = "rss_news"
    version = "1.0.0"
    description = "中国新闻网滚动新闻 RSS 采集插件"
    author = "admin"
    tags = ["news", "rss", "china"]

    # Minimal config schema (no required configuration)
    config_schema = {
        "rss_url": {
            "type": "string",
            "required": False,
            "description": "RSS feed URL",
            "default": "https://www.chinanews.com.cn/rss/scroll-news.xml"
        },
        "timeout": {
            "type": "integer",
            "required": False,
            "description": "HTTP request timeout in seconds",
            "default": 30
        },
        "max_items": {
            "type": "integer",
            "required": False,
            "description": "Maximum number of items to fetch",
            "default": 50
        }
    }

    # MVP: dependencies must be empty
    dependencies = []

    # Collection mode
    collection_mode = "incremental"

    async def fetch(self, **kwargs) -> List[DataItem]:
        """
        Fetch RSS feed and parse items.

        Args:
            state: Optional plugin state dict from plugin_state table
                   Contains: last_cursor, last_timestamp, last_offset, state_data

        Returns:
            List of DataItem objects
        """
        # Get config with defaults
        rss_url = self.config.get("rss_url", "https://www.chinanews.com.cn/rss/scroll-news.xml")
        timeout = self.config.get("timeout", 30)
        max_items = self.config.get("max_items", 50)

        # Get state for incremental collection
        state = kwargs.get("state")
        last_timestamp = None
        if state:
            last_timestamp = state.get("last_timestamp")
            if last_timestamp:
                last_timestamp = self._parse_pub_date(last_timestamp)
                print(f"[RssNews] Incremental mode: last_timestamp={last_timestamp}")

        items = []

        try:
            # Fetch RSS feed using standard library
            print(f"[RssNews] Fetching RSS from: {rss_url}")
            xml_content = self._fetch_rss(rss_url, timeout)

            if not xml_content:
                raise RuntimeError(f"Failed to fetch RSS content from {rss_url}")

            # Parse XML
            rss_items = self._parse_rss(xml_content, max_items)
            print(f"[RssNews] Parsed {len(rss_items)} items from RSS")
            if not rss_items:
                raise RuntimeError(f"No RSS items parsed from {rss_url}")

            # Filter items for incremental collection
            if last_timestamp:
                filtered_items = []
                for item in rss_items:
                    item_timestamp = self._parse_pub_date(item.get("pub_date", ""))
                    if item_timestamp:
                        # Make both timestamps naive (remove timezone) for comparison
                        item_ts_naive = item_timestamp.replace(tzinfo=None) if item_timestamp.tzinfo else item_timestamp
                        last_ts_naive = last_timestamp.replace(tzinfo=None) if last_timestamp.tzinfo else last_timestamp
                        if item_ts_naive > last_ts_naive:
                            filtered_items.append(item)
                    else:
                        # If can't parse date, include it to be safe
                        filtered_items.append(item)
                print(f"[RssNews] Filtered {len(filtered_items)} new items (after {last_timestamp})")
                rss_items = filtered_items

            # Convert to DataItem
            now = datetime.now()
            for rss_item in rss_items:
                items.append(DataItem(
                    source="chinanews_rss",
                    plugin_id=self.name,
                    timestamp=now,
                    data=rss_item,
                    metadata={
                        "rss_url": rss_url,
                        "guid": rss_item.get("guid", "")
                    }
                ))

        except Exception as e:
            print(f"[RssNews] Error fetching RSS: {e}")
            raise

        return items

    def _fetch_rss(self, url: str, timeout: int) -> Optional[str]:
        """
        Fetch RSS XML content from URL.

        Args:
            url: RSS feed URL
            timeout: Request timeout in seconds

        Returns:
            XML content string or None on failure
        """
        try:
            # Create request with user agent
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.0"
            }
            request = urllib.request.Request(url, headers=headers)

            # Fetch content
            with urllib.request.urlopen(request, timeout=timeout) as response:
                # Read and decode content
                content = response.read()

                # Try different encodings
                for encoding in ["utf-8", "gbk", "gb2312", "latin-1"]:
                    try:
                        return content.decode(encoding)
                    except UnicodeDecodeError:
                        continue

                # Fallback to utf-8 with errors ignored
                return content.decode("utf-8", errors="ignore")

        except urllib.error.URLError as e:
            print(f"[RssNews] URL error: {e}")
            return None
        except Exception as e:
            print(f"[RssNews] Fetch error: {e}")
            return None

    def _parse_rss(self, xml_content: str, max_items: int) -> List[Dict[str, Any]]:
        """
        Parse RSS XML content and extract items.

        Args:
            xml_content: RSS XML string
            max_items: Maximum number of items to return

        Returns:
            List of RSS item dictionaries
        """
        items = []

        try:
            # Parse XML
            root = ET.fromstring(xml_content)

            # Find channel element (RSS 2.0 format)
            # RSS structure: <rss><channel><item>...</item></channel></rss>
            channel = root.find(".//channel")
            if channel is None:
                # Try Atom format
                channel = root

            # Find all item elements
            item_elements = channel.findall("item") if channel else root.findall(".//item")

            # Also try Atom entry elements
            if not item_elements:
                item_elements = root.findall(".//{http://www.w3.org/2005/Atom}entry")

            # Parse each item
            for i, item_elem in enumerate(item_elements):
                if i >= max_items:
                    break

                item = self._parse_item_element(item_elem)
                if item:
                    items.append(item)

        except ET.ParseError as e:
            print(f"[RssNews] XML parse error: {e}")
        except Exception as e:
            print(f"[RssNews] Parse error: {e}")

        return items

    def _parse_item_element(self, item_elem: ET.Element) -> Optional[Dict[str, Any]]:
        """
        Parse a single RSS item element.

        Args:
            item_elem: XML element representing an RSS item

        Returns:
            Dictionary with item data or None
        """
        try:
            # Helper function to get text from child element
            def get_text(tag: str) -> str:
                elem = item_elem.find(tag)
                return elem.text.strip() if elem is not None and elem.text else ""

            # Extract fields
            title = get_text("title")
            link = get_text("link")
            description = get_text("description")
            pub_date = get_text("pubDate")
            guid = get_text("guid")

            # Try Atom format if RSS fields are empty
            if not title:
                # Atom uses different tag names
                title_elem = item_elem.find("{http://www.w3.org/2005/Atom}title")
                title = title_elem.text if title_elem is not None and title_elem.text else ""

            if not link:
                link_elem = item_elem.find("{http://www.w3.org/2005/Atom}link")
                if link_elem is not None:
                    link = link_elem.get("href", "")

            if not description:
                summary_elem = item_elem.find("{http://www.w3.org/2005/Atom}summary")
                content_elem = item_elem.find("{http://www.w3.org/2005/Atom}content")
                if summary_elem is not None and summary_elem.text:
                    description = summary_elem.text
                elif content_elem is not None and content_elem.text:
                    description = content_elem.text

            if not pub_date:
                updated_elem = item_elem.find("{http://www.w3.org/2005/Atom}updated")
                if updated_elem is not None and updated_elem.text:
                    pub_date = updated_elem.text

            # Skip if no title
            if not title:
                return None

            return {
                "title": title,
                "url": link,
                "summary": description,
                "pub_date": pub_date,
                "guid": guid
            }

        except Exception as e:
            print(f"[RssNews] Error parsing item: {e}")
            return None

    def normalize(self, raw_data: Dict[str, Any], raw_data_id: int) -> Optional[Dict[str, Any]]:
        """
        Convert raw RSS data to normalized format.

        Mapping:
        - title → item.title
        - link → item.url
        - pubDate → event_timestamp
        - description → payload.summary
        - entity: empty list
        - event_type: "news"

        Args:
            raw_data: The raw data from fetch()
            raw_data_id: ID of the raw_data record

        Returns:
            Normalized data dict
        """
        # Parse pub_date to datetime
        event_timestamp = self._parse_pub_date(raw_data.get("pub_date", ""))

        return {
            "event_type": "news",
            "event_source": "中国新闻网",
            "entity": [],  # Empty entity list as required
            "event_timestamp": event_timestamp,
            "title": raw_data.get("title", ""),
            "payload": {
                "url": raw_data.get("url", ""),
                "summary": raw_data.get("summary", ""),
                "guid": raw_data.get("guid", "")
            },
            "confidence": 1.0
        }

    def _parse_pub_date(self, pub_date: str) -> Optional[datetime]:
        """
        Parse RSS pubDate string to datetime.

        RSS date format: Mon, 06 Sep 2021 12:00:00 GMT
        Also handles other common formats.

        Args:
            pub_date: Date string from RSS

        Returns:
            datetime object or None
        """
        if not pub_date:
            return None

        if isinstance(pub_date, datetime):
            return pub_date

        if not isinstance(pub_date, str):
            pub_date = str(pub_date)

        # Common RSS date formats
        formats = [
            "%a, %d %b %Y %H:%M:%S %Z",      # Mon, 06 Sep 2021 12:00:00 GMT
            "%a, %d %b %Y %H:%M:%S %z",      # Mon, 06 Sep 2021 12:00:00 +0800
            "%Y-%m-%dT%H:%M:%SZ",             # 2021-09-06T12:00:00Z (ISO 8601)
            "%Y-%m-%dT%H:%M:%S%z",            # 2021-09-06T12:00:00+08:00
            "%Y-%m-%d %H:%M:%S",              # 2021-09-06 12:00:00
            "%d %b %Y %H:%M:%S",              # 06 Sep 2021 12:00:00
        ]

        for fmt in formats:
            try:
                return datetime.strptime(pub_date.strip(), fmt)
            except ValueError:
                continue

        try:
            return parsedate_to_datetime(pub_date.strip())
        except (TypeError, ValueError, IndexError):
            pass

        # If all formats fail, return None
        return None

    async def health_check(self) -> bool:
        """
        Check RSS feed availability.

        Returns:
            True if RSS is accessible
        """
        try:
            rss_url = self.config.get("rss_url", "https://www.chinanews.com.cn/rss/scroll-news.xml")
            timeout = self.config.get("timeout", 10)

            headers = {"User-Agent": "Mozilla/5.0"}
            request = urllib.request.Request(rss_url, headers=headers)

            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.status == 200

        except Exception:
            return False

    def get_default_schedule(self) -> Optional[str]:
        """
        Default schedule - run every 15 minutes.

        Returns:
            Cron expression
        """
        return "*/15 * * * *"
