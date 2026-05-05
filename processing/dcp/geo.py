"""DCP geography helpers."""

import math
from typing import Any


def _is_hunan_coordinate(longitude: float, latitude: float) -> bool:
    return 108.6 <= longitude <= 114.3 and 24.6 <= latitude <= 30.2


def _is_valid_coordinate(longitude: Any, latitude: Any) -> bool:
    try:
        lon = float(longitude)
        lat = float(latitude)
    except (TypeError, ValueError):
        return False

    if not math.isfinite(lon) or not math.isfinite(lat):
        return False

    if lon < -180 or lon > 180:
        return False

    if lat < -90 or lat > 90:
        return False

    # DCP 里 0,0 通常是无效定位，不应进入地图点。
    if lon == 0 and lat == 0:
        return False

    return True
