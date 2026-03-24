"""
Phase 4 Regression Test - API Contract Verification

Tests API response format matches v1.0 documentation.
"""

import requests
import json

BASE_URL = "http://localhost:8081"


def check_field(data, field, expected_type=None):
    """Check if field exists and has correct type."""
    if field not in data:
        return False, f"Missing field: {field}"
    if expected_type and not isinstance(data[field], expected_type):
        return False, f"Wrong type for {field}: expected {expected_type}, got {type(data[field])}"
    return True, "OK"


def test_plugins_format():
    """Test GET /api/plugins returns correct format."""
    print("\n[TEST] GET /api/plugins - Format Check")

    resp = requests.get(f"{BASE_URL}/api/plugins", timeout=5)
    if resp.status_code != 200:
        return False, f"Status: {resp.status_code}"

    data = resp.json()

    # Check top-level structure
    ok, msg = check_field(data, "plugins", list)
    if not ok:
        return False, msg

    # Check plugin fields
    if data["plugins"]:
        plugin = data["plugins"][0]
        required_fields = ["id", "name", "version", "description", "tags", "enabled", "health_status"]
        for field in required_fields:
            ok, msg = check_field(plugin, field)
            if not ok:
                return False, f"Plugin missing {field}"

    print(f"  ✅ Format correct, {len(data['plugins'])} plugins")
    return True, "OK"


def test_data_format():
    """Test GET /api/data returns correct format."""
    print("\n[TEST] GET /api/data - Format Check")

    resp = requests.get(f"{BASE_URL}/api/data?limit=2", timeout=5)
    if resp.status_code != 200:
        return False, f"Status: {resp.status_code}"

    data = resp.json()

    # Check top-level structure (v1.0 spec uses "data" not "items")
    ok, msg = check_field(data, "data", list)
    if not ok:
        return False, msg

    ok, msg = check_field(data, "total", int)
    if not ok:
        return False, msg

    ok, msg = check_field(data, "limit", int)
    if not ok:
        return False, msg

    ok, msg = check_field(data, "offset", int)
    if not ok:
        return False, msg

    print(f"  ✅ Format correct, total={data['total']}")
    return True, "OK"


def test_normalized_format():
    """Test GET /api/data/normalized returns correct format."""
    print("\n[TEST] GET /api/data/normalized - Format Check")

    resp = requests.get(f"{BASE_URL}/api/data/normalized?limit=2", timeout=5)
    if resp.status_code != 200:
        return False, f"Status: {resp.status_code}"

    data = resp.json()

    # Check top-level structure
    ok, msg = check_field(data, "data", list)
    if not ok:
        return False, msg

    # Check normalized item fields
    if data["data"]:
        item = data["data"][0]
        required_fields = ["id", "plugin_id", "event_type", "unique_key", "payload"]
        for field in required_fields:
            ok, msg = check_field(item, field)
            if not ok:
                return False, f"Item missing {field}"

    print(f"  ✅ Format correct, total={data['total']}")
    return True, "OK"


def test_trigger_format():
    """Test POST /api/plugins/{id}/trigger returns correct format."""
    print("\n[TEST] POST /api/plugins/rss_news/trigger - Format Check")

    resp = requests.post(
        f"{BASE_URL}/api/plugins/rss_news/trigger",
        json={},
        timeout=35
    )
    if resp.status_code != 200:
        return False, f"Status: {resp.status_code}"

    data = resp.json()

    # Check v1.0 spec fields
    ok, msg = check_field(data, "success", bool)
    if not ok:
        return False, msg

    ok, msg = check_field(data, "plugin_id", str)
    if not ok:
        return False, msg

    ok, msg = check_field(data, "collected", int)
    if not ok:
        return False, msg

    # saved_ids is in v1.0 spec
    ok, msg = check_field(data, "saved_ids", list)
    if not ok:
        return False, msg

    print(f"  ✅ Format correct, success={data['success']}, collected={data['collected']}")
    return True, "OK"


def test_pagination():
    """Test pagination parameters work correctly."""
    print("\n[TEST] Pagination - limit/offset")

    # Get first page
    resp1 = requests.get(f"{BASE_URL}/api/data?limit=3&offset=0", timeout=5)
    data1 = resp1.json()

    # Get second page
    resp2 = requests.get(f"{BASE_URL}/api/data?limit=3&offset=3", timeout=5)
    data2 = resp2.json()

    # Check different items
    if data1["data"] and data2["data"]:
        if data1["data"][0]["id"] == data2["data"][0]["id"]:
            return False, "Same item returned for different offsets"

    print(f"  ✅ Pagination working, page1={len(data1['data'])} items, page2={len(data2['data'])} items")
    return True, "OK"


def test_filtering():
    """Test plugin_id filter works."""
    print("\n[TEST] Filtering - plugin_id")

    # Get all
    resp_all = requests.get(f"{BASE_URL}/api/data?limit=100", timeout=5)
    data_all = resp_all.json()

    # Get filtered
    resp_filtered = requests.get(f"{BASE_URL}/api/data?plugin_id=rss_news&limit=100", timeout=5)
    data_filtered = resp_filtered.json()

    # Filtered should be <= all
    if data_filtered["total"] > data_all["total"]:
        return False, "Filtered count > total count"

    # All filtered items should have correct plugin_id
    for item in data_filtered["data"]:
        if item.get("plugin_id") != "rss_news":
            return False, f"Wrong plugin_id in filtered result: {item.get('plugin_id')}"

    print(f"  ✅ Filtering working, filtered={data_filtered['total']}, all={data_all['total']}")
    return True, "OK"


def main():
    print("=" * 70)
    print(" Phase 4 API Regression Test")
    print("=" * 70)
    print(f"Base URL: {BASE_URL}")

    tests = [
        ("Plugins format", test_plugins_format),
        ("Data format", test_data_format),
        ("Normalized format", test_normalized_format),
        ("Trigger format", test_trigger_format),
        ("Pagination", test_pagination),
        ("Filtering", test_filtering),
    ]

    results = []
    for name, test_func in tests:
        try:
            success, msg = test_func()
            results.append((name, success, msg))
        except Exception as e:
            results.append((name, False, str(e)))

    print("\n" + "=" * 70)
    print(" Summary")
    print("=" * 70)

    for name, success, msg in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status}: {name}")
        if not success:
            print(f"      Error: {msg}")

    passed = sum(1 for _, s, _ in results if s)
    total = len(results)
    print(f"\nTotal: {passed}/{total} tests passed")

    if passed == total:
        print("\n🎉 Phase 4 Regression Test PASSED!")
        print("\n✅ Phase 4 正式通过")
        return 0
    else:
        print(f"\n⚠️ {total - passed} test(s) failed")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
