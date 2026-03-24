"""
Test script for RSS Feed verification
"""

import requests
import xml.etree.ElementTree as ET

BASE_URL = "http://localhost:8082"


def test_rss_feed_basic():
    """Test basic RSS feed."""
    print("\n[TEST] GET /feed/rss - Basic")

    resp = requests.get(f"{BASE_URL}/feed/rss", timeout=10)

    if resp.status_code != 200:
        print(f"  ❌ Status: {resp.status_code}")
        return False

    content_type = resp.headers.get("Content-Type", "")
    if "rss+xml" not in content_type and "xml" not in content_type:
        print(f"  ⚠️ Content-Type: {content_type}")

    # Parse XML
    try:
        root = ET.fromstring(resp.content)
        channel = root.find("channel")

        if channel is None:
            print("  ❌ No channel element")
            return False

        # Check required channel elements
        title = channel.find("title")
        link = channel.find("link")
        desc = channel.find("description")

        print(f"  ✅ Title: {title.text if title is not None else 'N/A'}")
        print(f"  ✅ Link: {link.text if link is not None else 'N/A'}")

        # Count items
        items = channel.findall("item")
        print(f"  ✅ Items: {len(items)}")

        if items:
            item = items[0]
            item_title = item.find("title")
            item_link = item.find("link")
            item_desc = item.find("description")
            print(f"  ✅ First item title: {item_title.text[:50] if item_title is not None else 'N/A'}...")

        return True

    except ET.ParseError as e:
        print(f"  ❌ XML Parse Error: {e}")
        return False


def test_rss_feed_with_limit():
    """Test RSS feed with limit parameter."""
    print("\n[TEST] GET /feed/rss?limit=3")

    resp = requests.get(f"{BASE_URL}/feed/rss?limit=3", timeout=10)

    if resp.status_code != 200:
        print(f"  ❌ Status: {resp.status_code}")
        return False

    try:
        root = ET.fromstring(resp.content)
        channel = root.find("channel")
        items = channel.findall("item")

        if len(items) > 3:
            print(f"  ❌ Too many items: {len(items)} (expected <= 3)")
            return False

        print(f"  ✅ Items count: {len(items)} (limit=3)")
        return True

    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False


def test_rss_feed_with_tag():
    """Test RSS feed with tag filter."""
    print("\n[TEST] GET /feed/rss?tag=news")

    resp = requests.get(f"{BASE_URL}/feed/rss?tag=news", timeout=10)

    if resp.status_code != 200:
        print(f"  ❌ Status: {resp.status_code}")
        return False

    try:
        root = ET.fromstring(resp.content)
        channel = root.find("channel")
        items = channel.findall("item")

        print(f"  ✅ Items with tag 'news': {len(items)}")
        return True

    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False


def test_rss_feed_invalid_tag():
    """Test RSS feed with non-existent tag."""
    print("\n[TEST] GET /feed/rss?tag=nonexistent")

    resp = requests.get(f"{BASE_URL}/feed/rss?tag=nonexistent", timeout=10)

    if resp.status_code != 200:
        print(f"  ❌ Status: {resp.status_code}")
        return False

    try:
        root = ET.fromstring(resp.content)
        channel = root.find("channel")
        items = channel.findall("item")

        print(f"  ✅ Items with invalid tag: {len(items)} (expected 0)")
        return len(items) == 0

    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False


def main():
    print("=" * 70)
    print(" RSS Feed Verification")
    print("=" * 70)
    print(f"Base URL: {BASE_URL}")

    tests = [
        ("Basic RSS feed", test_rss_feed_basic),
        ("RSS with limit", test_rss_feed_with_limit),
        ("RSS with tag filter", test_rss_feed_with_tag),
        ("RSS with invalid tag", test_rss_feed_invalid_tag),
    ]

    results = []
    for name, test_func in tests:
        try:
            success = test_func()
            results.append((name, success))
        except Exception as e:
            print(f"  ❌ Exception: {e}")
            results.append((name, False))

    print("\n" + "=" * 70)
    print(" Summary")
    print("=" * 70)

    for name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status}: {name}")

    passed = sum(1 for _, s in results if s)
    total = len(results)
    print(f"\nTotal: {passed}/{total} tests passed")

    if passed == total:
        print("\n🎉 RSS Feed verification PASSED!")
        print("\n✅ Phase 5A 正式通过")
        return 0
    else:
        print(f"\n⚠️ {total - passed} test(s) failed")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
