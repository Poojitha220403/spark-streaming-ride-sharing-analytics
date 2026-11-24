import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent


@pytest.fixture
def run_stream_job(tmp_path):
    """Run data_generator.py plus one task script for `seconds`, then stop both.

    The task writes its CSV output relative to its working directory, so it runs
    inside tmp_path and the repo stays clean. Returns (tmp_path, job_exit_code),
    where job_exit_code is None if the job was still running when stopped.
    """
    procs = []

    def _run(script, seconds):
        env = {**os.environ, "PYSPARK_PYTHON": sys.executable}
        generator = subprocess.Popen(
            [sys.executable, "-u", str(REPO / "tests" / "run_generator.py")],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env,
        )
        procs.append(generator)
        # Wait until the socket server is listening before starting Spark.
        first_line = generator.stdout.readline()
        assert "Streaming data" in first_line, f"data generator failed to start: {first_line}"

        job = subprocess.Popen(
            [sys.executable, str(REPO / script)],
            cwd=tmp_path, env=env,
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
        )
        procs.append(job)
        time.sleep(seconds)
        exit_code = job.poll()
        return tmp_path, exit_code

    yield _run

    for p in procs:
        if p.poll() is None:
            p.terminate()
    for p in procs:
        try:
            p.wait(timeout=20)
        except subprocess.TimeoutExpired:
            p.kill()
