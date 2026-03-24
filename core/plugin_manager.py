"""
Plugin Manager for Data Collector Hub v1.0

Assumptions:
- Lazy loading: AST parse metadata without importing modules
- Only import module when create_adapter() is called
- _base/ directory is for shared base classes, not auto-registered as plugins
- Plugin instances are created per use (no mutable singleton cache)
"""

import ast
import importlib.util
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Type

from core.base_adapter import BaseAdapter
from core.paths import resolve_project_path


class PluginMetadata:
    """Plugin metadata extracted via AST (no module import)"""

    def __init__(self, plugin_id: str, name: str, version: str,
                 description: str, author: str, tags: List[str],
                 config_schema: Dict[str, Any], class_name: str,
                 file_path: Path, collection_mode: str = "full"):
        self.plugin_id = plugin_id
        self.name = name
        self.version = version
        self.description = description
        self.author = author
        self.tags = tags
        self.config_schema = config_schema
        self.class_name = class_name
        self.file_path = file_path
        self.collection_mode = collection_mode

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.plugin_id,
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "author": self.author,
            "tags": self.tags,
            "config_schema": self.config_schema,
            "collection_mode": self.collection_mode,
        }


class PluginManager:
    """
    Plugin manager with lazy loading support.

    Discovery phase: Scan files, parse AST for metadata (no import)
    Usage phase: Import module and create instance on demand
    """

    def __init__(self, plugins_dir: str | Path = "plugins"):
        self.plugins_dir = resolve_project_path(plugins_dir)
        self._metadata_cache: Dict[str, PluginMetadata] = {}
        self._class_cache: Dict[str, Type[BaseAdapter]] = {}

    def discover_plugins(self) -> List[PluginMetadata]:
        """
        Discover all plugins in the plugins directory.
        Uses AST parsing to extract metadata without importing modules.

        Returns:
            List of PluginMetadata objects
        """
        discovered = []

        if not self.plugins_dir.exists():
            print(f"[PluginManager] Plugins directory not found: {self.plugins_dir}")
            return discovered

        for file_path in self.plugins_dir.glob("*.py"):
            # Skip __init__.py and files starting with underscore
            if file_path.name.startswith("_"):
                continue

            try:
                metadata = self._parse_plugin_metadata(file_path)
                if metadata:
                    self._metadata_cache[metadata.plugin_id] = metadata
                    discovered.append(metadata)
                    print(f"[PluginManager] Discovered plugin: {metadata.plugin_id} ({metadata.name})")
            except Exception as e:
                print(f"[PluginManager] Failed to parse {file_path}: {e}")

        return discovered

    def _parse_plugin_metadata(self, file_path: Path) -> Optional[PluginMetadata]:
        """
        Parse plugin file using AST to extract metadata without importing.

        Args:
            file_path: Path to the plugin Python file

        Returns:
            PluginMetadata or None if not a valid plugin
        """
        with open(file_path, "r", encoding="utf-8") as f:
            source = f.read()

        try:
            tree = ast.parse(source)
        except SyntaxError as e:
            print(f"[PluginManager] Syntax error in {file_path}: {e}")
            return None

        # Find class that inherits from BaseAdapter
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                # Check if it inherits from BaseAdapter
                if any(
                    (isinstance(base, ast.Name) and base.id == "BaseAdapter") or
                    (isinstance(base, ast.Attribute) and base.attr == "BaseAdapter")
                    for base in node.bases
                ):
                    return self._extract_metadata_from_class(node, file_path)

        return None

    def _extract_metadata_from_class(self, class_node: ast.ClassDef,
                                     file_path: Path) -> Optional[PluginMetadata]:
        """Extract metadata from class definition AST node"""

        # Get class name
        class_name = class_node.name

        # Extract class attributes
        attrs = {}
        for item in class_node.body:
            if isinstance(item, ast.Assign):
                for target in item.targets:
                    if isinstance(target, ast.Name):
                        attr_name = target.id
                        try:
                            # Evaluate the assignment value
                            attrs[attr_name] = ast.literal_eval(item.value)
                        except (ValueError, SyntaxError):
                            # Can't evaluate, skip
                            pass

        # Required attributes
        plugin_id = attrs.get("name", "")
        if not plugin_id:
            print(f"[PluginManager] Plugin class {class_name} missing 'name' attribute")
            return None

        return PluginMetadata(
            plugin_id=plugin_id,
            name=attrs.get("name", ""),
            version=attrs.get("version", "1.0.0"),
            description=attrs.get("description", ""),
            author=attrs.get("author", ""),
            tags=attrs.get("tags", []),
            config_schema=attrs.get("config_schema", {}),
            class_name=class_name,
            file_path=file_path,
            collection_mode=attrs.get("collection_mode", "full")
        )

    def get_plugin_metadata(self, plugin_id: str) -> Optional[PluginMetadata]:
        """Get metadata for a specific plugin"""
        return self._metadata_cache.get(plugin_id)

    def list_plugins(self) -> List[PluginMetadata]:
        """List all discovered plugins"""
        return list(self._metadata_cache.values())

    def save_discovered_plugins(self, store: Any) -> int:
        """Persist discovered plugin metadata into the configured store."""
        count = 0
        for metadata in self.list_plugins():
            existing_plugin = store.get_plugin(metadata.plugin_id)
            enabled = bool(existing_plugin.get("enabled", 1)) if existing_plugin else True
            store.save_plugin(
                plugin_id=metadata.plugin_id,
                name=metadata.name,
                version=metadata.version,
                description=metadata.description,
                author=metadata.author,
                tags=metadata.tags,
                config_schema=metadata.config_schema,
                enabled=enabled,
            )
            count += 1
        return count

    def create_adapter(self, plugin_id: str,
                       config: Optional[Dict[str, Any]] = None) -> Optional[BaseAdapter]:
        """
        Create an adapter instance for the given plugin.
        This is where the actual module import happens (lazy loading).

        Args:
            plugin_id: Plugin identifier
            config: Optional configuration dictionary

        Returns:
            BaseAdapter instance or None if plugin not found
        """
        metadata = self._metadata_cache.get(plugin_id)
        if not metadata:
            print(f"[PluginManager] Plugin not found: {plugin_id}")
            return None

        # Check if class is already cached
        if plugin_id in self._class_cache:
            adapter_class = self._class_cache[plugin_id]
            return adapter_class(config=config)

        # Import the module
        try:
            module_name = f"plugins.{metadata.file_path.stem}"

            # Load module using importlib
            spec = importlib.util.spec_from_file_location(
                module_name, metadata.file_path
            )
            if not spec or not spec.loader:
                print(f"[PluginManager] Cannot load module: {metadata.file_path}")
                return None

            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)

            # Get the adapter class
            adapter_class = getattr(module, metadata.class_name)
            if not issubclass(adapter_class, BaseAdapter):
                print(f"[PluginManager] {metadata.class_name} is not a BaseAdapter subclass")
                return None

            # Cache the class (not instance)
            self._class_cache[plugin_id] = adapter_class

            # Create and return instance
            return adapter_class(config=config)

        except Exception as e:
            print(f"[PluginManager] Failed to create adapter for {plugin_id}: {e}")
            return None

    def reload_plugin(self, plugin_id: str) -> bool:
        """
        Reload a plugin (useful for development).

        Args:
            plugin_id: Plugin identifier

        Returns:
            True if successful
        """
        metadata = self._metadata_cache.get(plugin_id)
        if not metadata:
            return False

        # Remove from caches
        self._class_cache.pop(plugin_id, None)
        module_name = f"plugins.{metadata.file_path.stem}"
        if module_name in sys.modules:
            del sys.modules[module_name]

        # Re-parse metadata
        try:
            new_metadata = self._parse_plugin_metadata(metadata.file_path)
            if new_metadata:
                self._metadata_cache[plugin_id] = new_metadata
            return True
        except Exception as e:
            print(f"[PluginManager] Failed to reload {plugin_id}: {e}")
            return False
