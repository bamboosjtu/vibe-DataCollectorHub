# Plugin Development Guide

DataHub source plugins define source systems, downloader endpoints, dataset
registration, normalizer mappings, domain mappings, processing policy, and
health policy.

Plugins do not implement remote pagination, DCP login, request dependencies, or
raw response splitting. Those concerns belong to the downloader.
