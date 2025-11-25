import asyncio
import sys

from pyutils import log

if __name__ == "__main__":
    if len(sys.argv) < 2:
        log.error("Usage: python -m foo <batch|server|proxy>")
        sys.exit(1)

    mode = sys.argv[1]

    if mode == "batch":
        from .app import BatchRunner

        asyncio.run(BatchRunner().run())
    elif mode == "server":
        from .app import run_server

        run_server()
    elif mode == "proxy":
        from .app import run_proxy

        run_proxy()
    else:
        log.error(f"Unknown mode: {mode}")
        sys.exit(1)
