"""Lightweight wall-time and sampled peak-RSS profiling for lab runs."""
from __future__ import annotations

import csv
import os
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import psutil

OUTPUT_PATH = Path("results/profiling/measurements.csv")
SAMPLE_INTERVAL_SECONDS = 0.05


@contextmanager
def profile_run(algorithm: str, **parameters):
    """Measure full wrapped execution and periodically sample process RSS.

    The memory value is the highest RSS observed by polling, not a guarantee
    of capturing very brief peaks between samples.
    """
    process = psutil.Process(os.getpid())
    stop_event = threading.Event()
    peak_rss = [process.memory_info().rss]

    def sample_memory():
        while not stop_event.wait(SAMPLE_INTERVAL_SECONDS):
            try:
                peak_rss[0] = max(peak_rss[0], process.memory_info().rss)
            except psutil.Error:
                return

    sampler = threading.Thread(target=sample_memory, daemon=True)
    started_at = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    start = time.perf_counter()
    sampler.start()
    status = "success"
    error = ""
    try:
        yield
    except BaseException as exc:
        status = "error"
        error = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        elapsed = time.perf_counter() - start
        stop_event.set()
        sampler.join()
        try:
            peak_rss[0] = max(peak_rss[0], process.memory_info().rss)
        except psutil.Error:
            pass

        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        row = {
            "started_at": started_at,
            "algorithm": algorithm,
            "chunk_size": parameters.get("chunk_size", ""),
            "upload_results": parameters.get("upload", ""),
            "duration_seconds": round(elapsed, 3),
            "peak_rss_mb": round(peak_rss[0] / (1024 * 1024), 2),
            "memory_sample_interval_seconds": SAMPLE_INTERVAL_SECONDS,
            "status": status,
            "error": error,
        }
        fields = list(row.keys())
        write_header = not OUTPUT_PATH.exists() or OUTPUT_PATH.stat().st_size == 0
        with OUTPUT_PATH.open("a", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=fields)
            if write_header:
                writer.writeheader()
            writer.writerow(row)
        print(
            "\nПрофилирование: "
            f"алгоритм={algorithm}, время={elapsed:.3f} с, "
            f"пиковый RSS={peak_rss[0] / (1024 * 1024):.2f} MiB, "
            f"статус={status}. Результаты: {OUTPUT_PATH}"
        )
