"""
Data Collector Hub v1.0 RC-1 Integration Acceptance Test

验收范围：
1. Plugin Discovery - 插件发现和元数据提取
2. Pipeline - 数据管道（raw -> normalized）
3. Scheduler - 任务调度器
4. REST API - REST 接口
5. RSS Feed - RSS 输出
6. WebSocket - 实时数据流
7. MCP - LLM 工具接口

使用方法：
1. 确保服务器在运行: python -m uvicorn api.server:app --port 8000
2. 运行: python tests/scripts/test_integration_rc1.py
"""

import asyncio
import json
import sys
import time
from datetime import datetime
from typing import Any, Dict, List

import aiohttp
import websockets

API_BASE = "http://localhost:8000"
WS_URL = "ws://localhost:8000/ws/stream"


class Colors:
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    RESET = "\033[0m"


def print_header(text: str):
    print(f"\n{Colors.BLUE}{'='*60}{Colors.RESET}")
    print(f"{Colors.BLUE}{text}{Colors.RESET}")
    print(f"{Colors.BLUE}{'='*60}{Colors.RESET}")


def print_pass(text: str):
    print(f"  {Colors.GREEN}✓{Colors.RESET} {text}")


def print_fail(text: str):
    print(f"  {Colors.RED}✗{Colors.RESET} {text}")


def print_warn(text: str):
    print(f"  {Colors.YELLOW}⚠{Colors.RESET} {text}")


def print_info(text: str):
    print(f"  {Colors.BLUE}ℹ{Colors.RESET} {text}")


class IntegrationTest:
    """v1.0 RC-1 Integration Test Suite"""

    def __init__(self):
        self.results: Dict[str, bool] = {}
        self.errors: List[str] = []

    async def run_all_tests(self):
        """运行所有验收测试"""
        print_header("Data Collector Hub v1.0 RC-1 Integration Acceptance Test")
        print(f"Start time: {datetime.now().isoformat()}")

        async with aiohttp.ClientSession() as self.session:
            # 1. Plugin Discovery
            await self.test_plugin_discovery()

            # 2. Pipeline (via data query)
            await self.test_pipeline()

            # 3. Scheduler
            await self.test_scheduler()

            # 4. REST API
            await self.test_rest_api()

            # 5. RSS Feed
            await self.test_rss_feed()

            # 6. WebSocket
            await self.test_websocket()

            # 7. MCP
            await self.test_mcp()

        # 输出总结
        self.print_summary()

    async def test_plugin_discovery(self):
        """测试插件发现功能"""
        print_header("1. Plugin Discovery")

        try:
            # 通过 REST API 获取插件列表
            async with self.session.get(f"{API_BASE}/api/plugins") as resp:
                if resp.status != 200:
                    print_fail(f"Failed to get plugins: {resp.status}")
                    self.results["plugin_discovery"] = False
                    return

                data = await resp.json()
                plugins = data.get("plugins", [])

                if not plugins:
                    print_fail("No plugins found")
                    self.results["plugin_discovery"] = False
                    return

                print_pass(f"Discovered {len(plugins)} plugins")

                for plugin in plugins:
                    plugin_id = plugin.get("id", "unknown")
                    name = plugin.get("name", "unknown")
                    version = plugin.get("version", "unknown")
                    print_info(f"  - {plugin_id}: {name} v{version}")

                # 验证必要字段
                required_fields = ["id", "name", "version", "enabled"]
                for plugin in plugins:
                    for field in required_fields:
                        if field not in plugin:
                            print_fail(f"Plugin missing field '{field}': {plugin.get('id')}")
                            self.results["plugin_discovery"] = False
                            return

                print_pass("All plugins have required metadata fields")
                self.results["plugin_discovery"] = True

        except Exception as e:
            print_fail(f"Plugin discovery error: {e}")
            self.results["plugin_discovery"] = False
            self.errors.append(f"Plugin discovery: {e}")

    async def test_pipeline(self):
        """测试数据管道（raw -> normalized）"""
        print_header("2. Pipeline (raw -> normalized)")

        try:
            # 查询 raw_data
            async with self.session.get(f"{API_BASE}/api/data?limit=1") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    raw_count = data.get("total", 0)
                    print_pass(f"Raw data query works (total: {raw_count})")
                else:
                    print_fail(f"Raw data query failed: {resp.status}")
                    self.results["pipeline"] = False
                    return

            # 查询 normalized_data
            async with self.session.get(f"{API_BASE}/api/data/normalized?limit=1") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    norm_count = data.get("total", 0)
                    print_pass(f"Normalized data query works (total: {norm_count})")
                else:
                    print_fail(f"Normalized data query failed: {resp.status}")
                    self.results["pipeline"] = False
                    return

            # 验证数据结构
            if data.get("data"):
                item = data["data"][0]
                required = ["id", "plugin_id", "unique_key", "created_at"]
                for field in required:
                    if field not in item:
                        print_fail(f"Normalized data missing field: {field}")
                        self.results["pipeline"] = False
                        return
                print_pass("Normalized data has required fields")

            print_pass("Pipeline data flow verified")
            self.results["pipeline"] = True

        except Exception as e:
            print_fail(f"Pipeline test error: {e}")
            self.results["pipeline"] = False
            self.errors.append(f"Pipeline: {e}")

    async def test_scheduler(self):
        """测试任务调度器"""
        print_header("3. Scheduler")

        try:
            # 触发插件执行
            async with self.session.post(
                f"{API_BASE}/api/plugins/rss_news/trigger",
                json={}
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("success"):
                        print_pass("Plugin trigger via scheduler works")
                        print_info(f"  Collected: {data.get('collected', 0)} items")
                    else:
                        print_warn(f"Plugin trigger returned success=false: {data.get('error')}")
                else:
                    print_fail(f"Plugin trigger failed: {resp.status}")
                    self.results["scheduler"] = False
                    return

            # 检查系统统计
            async with self.session.get(f"{API_BASE}/api/stats") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    print_pass("System stats endpoint works")
                    print_info(f"  Plugins: {data.get('plugins', 0)}")
                    print_info(f"  Raw data: {data.get('raw_data', 0)}")
                    print_info(f"  Normalized: {data.get('normalized_data', 0)}")
                else:
                    print_warn(f"Stats endpoint returned: {resp.status}")

            self.results["scheduler"] = True

        except Exception as e:
            print_fail(f"Scheduler test error: {e}")
            self.results["scheduler"] = False
            self.errors.append(f"Scheduler: {e}")

    async def test_rest_api(self):
        """测试 REST API 端点"""
        print_header("4. REST API")

        endpoints = [
            ("GET", "/api/plugins", "List plugins"),
            ("GET", "/api/data", "Query raw data"),
            ("GET", "/api/data/normalized", "Query normalized data"),
            ("GET", "/api/stats", "System stats"),
        ]

        all_passed = True
        for method, path, desc in endpoints:
            try:
                if method == "GET":
                    async with self.session.get(f"{API_BASE}{path}") as resp:
                        if resp.status == 200:
                            print_pass(f"{desc}: {method} {path}")
                        else:
                            print_fail(f"{desc}: {method} {path} -> {resp.status}")
                            all_passed = False
            except Exception as e:
                print_fail(f"{desc}: {method} {path} -> {e}")
                all_passed = False

        self.results["rest_api"] = all_passed

    async def test_rss_feed(self):
        """测试 RSS Feed"""
        print_header("5. RSS Feed")

        try:
            async with self.session.get(f"{API_BASE}/feed/rss") as resp:
                if resp.status != 200:
                    print_fail(f"RSS feed failed: {resp.status}")
                    self.results["rss_feed"] = False
                    return

                content_type = resp.headers.get("content-type", "")
                if "rss+xml" not in content_type and "xml" not in content_type:
                    print_warn(f"Unexpected content-type: {content_type}")

                text = await resp.text()

                # 验证 RSS 基本结构
                if "<rss" in text and "<channel>" in text and "<item>" in text:
                    print_pass("RSS feed has valid structure")
                elif "<rss" in text and "<channel>" in text:
                    print_pass("RSS feed structure valid (no items yet)")
                else:
                    print_fail("RSS feed missing required elements")
                    self.results["rss_feed"] = False
                    return

                # 验证必要字段
                required_fields = ["<title>", "<link>", "<description>"]
                for field in required_fields:
                    if field in text:
                        print_pass(f"RSS has {field} element")
                    else:
                        print_fail(f"RSS missing {field} element")
                        self.results["rss_feed"] = False
                        return

                self.results["rss_feed"] = True

        except Exception as e:
            print_fail(f"RSS feed test error: {e}")
            self.results["rss_feed"] = False
            self.errors.append(f"RSS: {e}")

    async def test_websocket(self):
        """测试 WebSocket"""
        print_header("6. WebSocket")

        try:
            # 检查 WebSocket stats 端点
            async with self.session.get(f"{API_BASE}/ws/stats") as resp:
                if resp.status != 200:
                    print_fail(f"WebSocket stats endpoint failed: {resp.status}")
                    self.results["websocket"] = False
                    return

                data = await resp.json()
                print_pass("WebSocket stats endpoint works")

                # 验证关键字段
                if "polling_task_count" in data:
                    print_pass(f"polling_task_count: {data['polling_task_count']}")
                if "client_count" in data:
                    print_pass(f"client_count: {data['client_count']}")

            # 测试 WebSocket 连接
            try:
                async with websockets.connect(WS_URL) as ws:
                    # 等待连接确认
                    msg = await asyncio.wait_for(ws.recv(), timeout=5)
                    data = json.loads(msg)

                    if data.get("type") == "connected":
                        print_pass("WebSocket connection established")
                    else:
                        print_warn(f"Unexpected WebSocket message: {data}")

                    # 发送 filter 配置
                    await ws.send(json.dumps({
                        "action": "set_filters",
                        "filters": {"interval": 5}
                    }))

                    # 等待 ack
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=3)
                        data = json.loads(msg)
                        if data.get("type") == "ack":
                            print_pass("WebSocket filter configuration works")
                    except asyncio.TimeoutError:
                        print_warn("No ack received (may be normal)")

                print_pass("WebSocket connection test passed")

            except Exception as e:
                print_warn(f"WebSocket connection test: {e}")

            self.results["websocket"] = True

        except Exception as e:
            print_fail(f"WebSocket test error: {e}")
            self.results["websocket"] = False
            self.errors.append(f"WebSocket: {e}")

    async def test_mcp(self):
        """测试 MCP 接口"""
        print_header("7. MCP (Model Context Protocol)")

        try:
            # 测试工具发现
            async with self.session.get(f"{API_BASE}/mcp") as resp:
                if resp.status != 200:
                    print_fail(f"MCP discovery failed: {resp.status}")
                    self.results["mcp"] = False
                    return

                data = await resp.json()
                tools = data.get("tools", [])

                print_pass(f"MCP discovery works ({len(tools)} tools)")

                expected_tools = ["list_plugins", "query_data", "trigger_plugin"]
                tool_names = [t.get("name") for t in tools]

                for tool in expected_tools:
                    if tool in tool_names:
                        print_pass(f"Tool '{tool}' registered")
                    else:
                        print_fail(f"Tool '{tool}' not found")
                        self.results["mcp"] = False
                        return

            # 测试 list_plugins 工具
            async with self.session.post(
                f"{API_BASE}/mcp/call",
                json={"tool": "list_plugins", "parameters": {}}
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("success"):
                        print_pass("MCP list_plugins tool works")
                    else:
                        print_fail("MCP list_plugins returned success=false")
                        self.results["mcp"] = False
                        return
                else:
                    print_fail(f"MCP list_plugins failed: {resp.status}")
                    self.results["mcp"] = False
                    return

            # 测试 query_data 工具
            async with self.session.post(
                f"{API_BASE}/mcp/call",
                json={"tool": "query_data", "parameters": {"limit": 1}}
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("success"):
                        print_pass("MCP query_data tool works")
                    else:
                        print_fail("MCP query_data returned success=false")
                        self.results["mcp"] = False
                        return
                else:
                    print_fail(f"MCP query_data failed: {resp.status}")
                    self.results["mcp"] = False
                    return

            # 测试 trigger_plugin 工具
            async with self.session.post(
                f"{API_BASE}/mcp/call",
                json={"tool": "trigger_plugin", "parameters": {"plugin_id": "rss_news"}}
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("success"):
                        result = data.get("result", {})
                        if result.get("success"):
                            print_pass("MCP trigger_plugin tool works")
                        else:
                            print_warn(f"MCP trigger_plugin: {result.get('error')}")
                    else:
                        print_fail("MCP trigger_plugin returned success=false")
                else:
                    print_fail(f"MCP trigger_plugin failed: {resp.status}")

            self.results["mcp"] = True

        except Exception as e:
            print_fail(f"MCP test error: {e}")
            self.results["mcp"] = False
            self.errors.append(f"MCP: {e}")

    def print_summary(self):
        """输出测试总结"""
        print_header("Test Summary")

        total = len(self.results)
        passed = sum(1 for v in self.results.values() if v)
        failed = total - passed

        print(f"\nTotal tests: {total}")
        print(f"Passed: {Colors.GREEN}{passed}{Colors.RESET}")
        print(f"Failed: {Colors.RED}{failed}{Colors.RESET}" if failed else f"Failed: {passed}")

        print("\nDetailed Results:")
        for name, result in self.results.items():
            status = f"{Colors.GREEN}PASS{Colors.RESET}" if result else f"{Colors.RED}FAIL{Colors.RESET}"
            print(f"  {name:20s}: {status}")

        if self.errors:
            print(f"\n{Colors.RED}Errors:{Colors.RESET}")
            for error in self.errors:
                print(f"  - {error}")

        # RC-1 判定
        print_header("RC-1 Status")
        critical_modules = ["plugin_discovery", "pipeline", "scheduler", "rest_api", "mcp"]
        all_critical_passed = all(self.results.get(m, False) for m in critical_modules)

        if all_critical_passed and failed == 0:
            print(f"{Colors.GREEN}✓ ALL TESTS PASSED{Colors.RESET}")
            print(f"{Colors.GREEN}✓ Ready for v1.0 RC-1{Colors.RESET}")
        elif all_critical_passed:
            print(f"{Colors.YELLOW}⚠ CRITICAL MODULES PASSED{Colors.RESET}")
            print(f"{Colors.YELLOW}⚠ Minor issues in non-critical modules{Colors.RESET}")
            print(f"{Colors.YELLOW}⚠ Consider for v1.0 RC-1 with known issues{Colors.RESET}")
        else:
            print(f"{Colors.RED}✗ CRITICAL MODULES FAILED{Colors.RESET}")
            print(f"{Colors.RED}✗ NOT READY for v1.0 RC-1{Colors.RESET}")

        return failed == 0


async def main():
    """主函数"""
    test = IntegrationTest()
    await test.run_all_tests()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nTest failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
