"""
Test script for REST API verification
"""

import requests
import json

BASE_URL = "http://localhost:8080"


def test_root():
    """Test root endpoint."""
    print("\n[TEST] GET /")
    try:
        resp = requests.get(f"{BASE_URL}/", timeout=5)
        print(f"  Status: {resp.status_code}")
        print(f"  Response: {resp.json()}")
        return resp.status_code == 200
    except Exception as e:
        print(f"  Error: {e}")
        return False


def test_list_plugins():
    """Test list plugins endpoint."""
    print("\n[TEST] GET /api/plugins")
    try:
        resp = requests.get(f"{BASE_URL}/api/plugins", timeout=5)
        print(f"  Status: {resp.status_code}")
        data = resp.json()
        print(f"  Plugins count: {len(data)}")
        for plugin in data:
            print(f"    - {plugin['id']} ({plugin['name']})")
        return resp.status_code == 200 and len(data) > 0
    except Exception as e:
        print(f"  Error: {e}")
        return False


def test_trigger_plugin():
    """Test trigger plugin endpoint."""
    print("\n[TEST] POST /api/plugins/rss_news/trigger")
    try:
        resp = requests.post(
            f"{BASE_URL}/api/plugins/rss_news/trigger",
            json={},
            timeout=35
        )
        print(f"  Status: {resp.status_code}")
        data = resp.json()
        print(f"  Response: {json.dumps(data, indent=2)}")
        return resp.status_code == 200 and data.get("success")
    except Exception as e:
        print(f"  Error: {e}")
        return False


def test_query_raw_data():
    """Test query raw data endpoint."""
    print("\n[TEST] GET /api/data")
    try:
        resp = requests.get(
            f"{BASE_URL}/api/data?limit=5",
            timeout=5
        )
        print(f"  Status: {resp.status_code}")
        data = resp.json()
        print(f"  Total: {data.get('total')}")
        print(f"  Items returned: {len(data.get('items', []))}")
        return resp.status_code == 200
    except Exception as e:
        print(f"  Error: {e}")
        return False


def test_query_normalized_data():
    """Test query normalized data endpoint."""
    print("\n[TEST] GET /api/data/normalized")
    try:
        resp = requests.get(
            f"{BASE_URL}/api/data/normalized?limit=5",
            timeout=5
        )
        print(f"  Status: {resp.status_code}")
        data = resp.json()
        print(f"  Total: {data.get('total')}")
        print(f"  Items returned: {len(data.get('items', []))}")
        return resp.status_code == 200
    except Exception as e:
        print(f"  Error: {e}")
        return False


def test_stats():
    """Test stats endpoint."""
    print("\n[TEST] GET /api/stats")
    try:
        resp = requests.get(f"{BASE_URL}/api/stats", timeout=5)
        print(f"  Status: {resp.status_code}")
        data = resp.json()
        print(f"  Plugins: {data.get('plugins')}")
        print(f"  Raw data: {data.get('raw_data')}")
        print(f"  Normalized data: {data.get('normalized_data')}")
        return resp.status_code == 200
    except Exception as e:
        print(f"  Error: {e}")
        return False


def main():
    print("=" * 70)
    print(" REST API Verification")
    print("=" * 70)
    print(f"Base URL: {BASE_URL}")

    results = []

    results.append(("Root endpoint", test_root()))
    results.append(("List plugins", test_list_plugins()))
    results.append(("Query raw data", test_query_raw_data()))
    results.append(("Query normalized data", test_query_normalized_data()))
    results.append(("System stats", test_stats()))
    results.append(("Trigger plugin", test_trigger_plugin()))

    print("\n" + "=" * 70)
    print(" Summary")
    print("=" * 70)

    for name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status}: {name}")

    passed = sum(1 for _, s in results if s)
    print(f"\nTotal: {passed}/{len(results)} tests passed")

    if passed == len(results):
        print("\n🎉 All API tests PASSED!")
        return 0
    else:
        print(f"\n⚠️ {len(results) - passed} test(s) failed")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
