import asyncio
import sys


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m foo <batch|server|proxy> [port]")
        sys.exit(1)

    mode = sys.argv[1]
    port = int(sys.argv[2]) if len(sys.argv) > 2 else None

    if mode == "batch":
        from .app import BatchRunner

        asyncio.run(BatchRunner().run())
    elif mode == "server":
        from .app import run_server

        run_server(port=port)
    elif mode == "proxy":
        from .app import run_proxy

        run_proxy()
    else:
        print(f"Unknown mode: {mode}")
        sys.exit(1)
