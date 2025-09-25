# $AUTHOR$: SriVinayA
# $DATE$: 09/23/2025
# $VERSION$: 1.8
# $LICENSE$: MIT

import os
import io
import time
import json
import math
import threading
from typing import List, Tuple, Optional
from dotenv import load_dotenv

from boto3 import session
from botocore import UNSIGNED
from botocore.client import Config

import snappy
import FileFormatDetection
from TripEvent import TripEvent
import AdaptTimeOption  # noqa: F401

# Meteostat for weather
from meteostat import Hourly, Point
from datetime import datetime, timedelta, timezone
import pandas as pd

import urllib.request


class NYCTaxiEventReader:
    """
    NYC Taxi Event Reader:
    - Downloads & decompresses Snappy files from S3
    - Computes straight-line (Haversine) distance
    - Gets all possible routes from Google Directions API
      -> Distances, durations, polylines (decoded into lat/lon)
    - Adds weather info at pickup time/location (temp, wind, precip, condition)
    """

    def __init__(
        self,
        region: str = "us-east-1",
        bucket_name: str = "aws-bigdata-blog",
        object_prefix: str = "artifacts/flink-refarch/data/nyc-tlc-trips.snz/",
        max_files: int = 2,
        output_dir: str = "./snappy_decompressed_events",
        google_api_key: Optional[str] = None,
    ) -> None:
        self.region = region
        self.bucket_name = bucket_name
        self.object_prefix = object_prefix
        self.max_files = max_files
        self.output_dir = output_dir
        self.google_api_key = google_api_key

        # Session/resource for listing
        self._session = session.Session()
        self._s3_resource = self._session.resource("s3", region_name=self.region)
        self._bucket = self._s3_resource.Bucket(self.bucket_name)

        # Stats
        self._stats_lock = threading.Lock()
        self.total_events: int = 0
        self.total_processing_time: float = 0.0
        self.earliest_time: Optional[int] = None
        self.latest_time: Optional[int] = None

    # ----------------------
    # Helpers
    # ----------------------
    @staticmethod
    def safe_filename_from_key(key: str, ext: str = ".ndjson") -> str:
        fname = key.replace("/", "")
        return fname + ext

    def list_s3_objects(self) -> List:
        s3_res = session.Session().resource("s3", config=Config(signature_version=UNSIGNED))
        bucket = s3_res.Bucket(self.bucket_name)
        out = []
        for i, obj in enumerate(bucket.objects.filter(Prefix=self.object_prefix)):
            print(f"Found: {obj.key}")
            out.append(obj)
            if i + 1 >= self.max_files:
                break
        return out

    @staticmethod
    def _download_and_decompress(region: str, obj_summary) -> Tuple[io.BytesIO, float, int]:
        start = time.time()
        sess = session.Session()
        s3_client = sess.client("s3", region_name=region)
        try:
            resp = s3_client.get_object(Bucket=obj_summary.bucket_name, Key=obj_summary.key)
            payload = resp["Body"].read()
            size = resp["ContentLength"]
            src = io.BytesIO(payload)
            dst = io.BytesIO()
            snappy.stream_decompress(src=src, dst=dst)
            dst.seek(0)
            read_time = max(0.0, time.time() - start)
            return dst, read_time, size
        finally:
            try:
                s3_client.close()
            except Exception:
                pass

    # ----------------------
    # Distance & Weather
    # ----------------------
    @staticmethod
    def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Great-circle distance (km) on WGS-84 sphere."""
        R = 6371.0088
        φ1, φ2 = math.radians(lat1), math.radians(lat2)
        dφ = φ2 - φ1
        dλ = math.radians(lon2 - lon1)
        a = math.sin(dφ / 2) ** 2 + math.cos(φ1) * math.cos(φ2) * math.sin(dλ / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c

    # --- Polyline decoding ---
    @staticmethod
    def _decode_polyline(polyline_str: str) -> List[Tuple[float, float]]:
        """Decode a Google encoded polyline into list of (lat, lon)."""
        coords = []
        index, lat, lon = 0, 0, 0
        while index < len(polyline_str):
            for coord in [lat, lon]:
                shift, result = 0, 0
                while True:
                    b = ord(polyline_str[index]) - 63
                    index += 1
                    result |= (b & 0x1F) << shift
                    shift += 5
                    if b < 0x20:
                        break
                dcoord = ~(result >> 1) if (result & 1) else (result >> 1)
                if coord is lat:
                    lat += dcoord
                    coord = lat
                else:
                    lon += dcoord
                    coord = lon
            coords.append((lat / 1e5, lon / 1e5))
        return coords

    def _google_routes(self, p_lat, p_lon, d_lat, d_lon):
        """Fetch all Google driving routes with alternatives."""
        if not self.google_api_key:
            return None
        url = (
            "https://maps.googleapis.com/maps/api/directions/json?"
            f"origin={p_lat},{p_lon}&destination={d_lat},{d_lon}"
            f"&mode=driving&alternatives=true&key={self.google_api_key}"
        )
        try:
            with urllib.request.urlopen(url, timeout=8) as resp:
                out = json.loads(resp.read().decode())
                routes = out.get("routes", [])
                result = []
                for r in routes:
                    legs = r.get("legs", [])
                    if not legs:
                        continue
                    leg = legs[0]
                    distance_km = leg["distance"]["value"] / 1000.0 if "distance" in leg else None
                    duration_sec = leg["duration"]["value"] if "duration" in leg else None
                    polyline = r.get("overview_polyline", {}).get("points")
                    decoded_points = self._decode_polyline(polyline) if polyline else None
                    steps = []
                    for step in leg.get("steps", []):
                        steps.append({
                            "instruction": step.get("html_instructions"),
                            "distance_m": step.get("distance", {}).get("value"),
                            "duration_s": step.get("duration", {}).get("value")
                        })
                    result.append({
                        "distance_km": round(distance_km, 4) if distance_km else None,
                        "duration_s": duration_sec,
                        "polyline": polyline,
                        "polyline_points": decoded_points,
                        "steps": steps
                    })
                return result if result else None
        except Exception as e:
            print(f"Google API error: {e}")
            return None

    # --- Time helpers ---
    @staticmethod
    def _add_one_year(dt):
        """
        Return dt + 1 calendar year (keeps timezone).
        Uses dateutil.relativedelta if available; otherwise falls back to .replace()
        with a Feb-29 safe adjustment.
        """
        try:
            from dateutil.relativedelta import relativedelta
            return dt + relativedelta(years=1)
        except Exception:
            # Fallback without extra dependency
            try:
                return dt.replace(year=dt.year + 1)
            except ValueError:
                # Handle Feb 29 -> Feb 28 next year
                return dt.replace(month=2, day=28, year=dt.year + 1)

    def _historic_weather(self, lat: float, lon: float, when_utc: datetime):
        """Fetch hourly weather at location/time via Meteostat with conditions (pd.NA-safe)."""
        try:
            # Ensure UTC-aware then make naive (Meteostat treats naive as UTC)
            if when_utc.tzinfo is None:
                when_utc = when_utc.replace(tzinfo=timezone.utc)
            else:
                when_utc = when_utc.astimezone(timezone.utc)

            start_aware = when_utc.replace(minute=0, second=0, microsecond=0)
            start = start_aware.replace(tzinfo=None)
            end = (start_aware + timedelta(hours=1)).replace(tzinfo=None)

            loc = Point(lat, lon)
            df = Hourly(loc, start, end, model=True).fetch()
            if df is None or df.empty:
                return None
            row = df.iloc[0]

            # ---- helpers that won't choke on pd.NA / NaN / None ----
            def to_float(x):
                # Return None for pd.NA/NaN/None; else float(x)
                if pd.isna(x):
                    return None
                try:
                    return float(x)
                except (TypeError, ValueError):
                    return None

            def to_int(x):
                if pd.isna(x):
                    return None
                try:
                    return int(x)
                except (TypeError, ValueError):
                    return None

            # Meteostat condition code mapping
            condition_map = {
                0: "Clear", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
                45: "Fog", 48: "Rime fog",
                51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
                61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
                66: "Light freezing rain", 67: "Heavy freezing rain",
                71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow",
                80: "Rain showers", 81: "Moderate rain showers", 82: "Violent rain showers",
                95: "Thunderstorm", 96: "Thunderstorm with hail", 99: "Severe thunderstorm with hail"
            }

            temp_c = to_float(row.get("temp"))

            val = row.get("prcp")
            if val is None or pd.isna(val):
                precip_mm = 0.0
            elif isinstance(val, str) and val.strip().lower() in ("", "na", "nan", "null", "none"):
                precip_mm = 0.0
            else:
                try:
                    precip_mm = float(val)
                except (TypeError, ValueError):
                    precip_mm = 0.0

            # wind speed in m/s -> km/h
            wspd_ms = row.get("wspd")
            wind_kph = to_float(wspd_ms)
            if wind_kph is not None:
                wind_kph *= 3.6

            coco = to_int(row.get("coco"))
            condition = condition_map.get(coco, "Unknown") if coco is not None else "Unknown"

            return {
                "temp_c": temp_c,
                "precip_mm": precip_mm,
                "wind_kph": wind_kph,
                "condition": condition
            }

        except Exception as e:
            print(f"Weather error: {e}")
            return None

    # ----------------------
    # Processing
    # ----------------------
    def process_object(self, obj_summary) -> Tuple[str, int, float, str]:
        thread_name = threading.current_thread().name
        print(f"{thread_name} - Starting {obj_summary.key}")

        stream, _, _ = self._download_and_decompress(self.region, obj_summary)
        stream.seek(0)

        os.makedirs(self.output_dir, exist_ok=True)
        out_path = os.path.join(self.output_dir, self.safe_filename_from_key(obj_summary.key, ".ndjson"))

        events = 0
        start_proc = time.time()

        with open(out_path, "w", encoding="utf-8", buffering=1024 * 1024) as fh:
            count = 0
            for raw in stream:
                if count >= 10:
                    break
                count = count + 1
                try:
                    line = raw.decode("unicode_escape")
                    obj = json.loads(line)

                    ev = TripEvent(line)
                    ts_ms = ev.timestamp
                    # Original event time (UTC-aware), then shift by +1 calendar year
                    when_utc = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc) if ts_ms else None
                    when_utc_shifted = self._add_one_year(when_utc) if when_utc else None

                    # Use the shifted timestamp everywhere downstream (stats + weather)
                    ts_ms_shifted = int(when_utc_shifted.timestamp() * 1000) if when_utc_shifted else None
                    if ts_ms_shifted is not None:
                        with self._stats_lock:
                            if self.earliest_time is None or ts_ms_shifted < self.earliest_time:
                                self.earliest_time = ts_ms_shifted
                            if self.latest_time is None or ts_ms_shifted > self.latest_time:
                                self.latest_time = ts_ms_shifted

                    p_lat, p_lon = obj.get("pickup_lat"), obj.get("pickup_lon")
                    d_lat, d_lon = obj.get("dropoff_lat"), obj.get("dropoff_lon")

                    straight_line_km = None
                    if all(isinstance(v, (int, float)) for v in (p_lat, p_lon, d_lat, d_lon)):
                        straight_line_km = round(self._haversine_km(p_lat, p_lon, d_lat, d_lon), 4)

                    routes_google = None
                    if all(isinstance(v, (int, float)) for v in (p_lat, p_lon, d_lat, d_lon)):
                        routes_google = self._google_routes(p_lat, p_lon, d_lat, d_lon)

                    weather = None
                    if when_utc_shifted and isinstance(p_lat, (int, float)) and isinstance(p_lon, (int, float)):
                        weather = self._historic_weather(p_lat, p_lon, when_utc_shifted)


                    obj["straight_line_km"] = straight_line_km
                    obj["google_routes"] = routes_google
                    obj["weather_at_pickup"] = weather

                    fh.write(json.dumps(obj, separators=(",", ":")) + "\n")
                    events += 1
                except Exception as e:
                    print(f"{obj_summary.key}: error {e}")


        proc_time = max(0.0, time.time() - start_proc)
        with self._stats_lock:
            self.total_events += events
            self.total_processing_time += proc_time

        return out_path, events, proc_time, thread_name

    # ----------------------
    # Orchestration
    # ----------------------
    def run(self) -> None:
        objs = self.list_s3_objects()
        if not objs:
            print("No S3 objects found")
            return
        threads = []
        for i, obj in enumerate(objs, start=1):
            th = threading.Thread(target=self._thread_wrapper, args=(obj,), name=f"Thread-{i}")
            threads.append(th)
            th.start()
        for th in threads:
            th.join()

    def _thread_wrapper(self, obj):
        try:
            out_path, events, proc_time, tname = self.process_object(obj)
            print(f"{tname} | {obj.key}: {events} events in {proc_time:.2f}s")
        except Exception as e:
            print(f"Failed {obj.key}: {e}")


# ----------------------
# Main
# ----------------------
def main():
    # Load environment variables from .env file
    load_dotenv()
    
    # Get Google API key from environment
    google_api_key = os.getenv("GOOGLE_API_KEY")
    if not google_api_key:
        raise ValueError("GOOGLE_API_KEY not found in environment variables. Please check your .env file.")
    
    reader = NYCTaxiEventReader(
        google_api_key=google_api_key
    )
    reader.run()


if __name__ == "__main__":
    main()
