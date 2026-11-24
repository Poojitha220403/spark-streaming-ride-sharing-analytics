"""Start the project's data_generator with SO_REUSEADDR set.

data_generator.py binds port 9999 without SO_REUSEADDR, so a second test that
starts it right after the first one fails with "Address already in use" while
the old socket lingers. This wrapper fixes that without touching the project
code: it patches socket.bind, then calls the real start_streaming().
"""
import socket
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

_orig_bind = socket.socket.bind


def _bind(self, address):
    self.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    return _orig_bind(self, address)


socket.socket.bind = _bind

import data_generator  # noqa: E402

data_generator.start_streaming()
