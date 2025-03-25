import sys


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m foo [batch|server]")
        sys.exit(1)

    mode = sys.argv[1]

    if mode == "batch":
        from .app import BatchRunner

        BatchRunner().run()
    elif mode == "server":
        from .app import run_server

        run_server()
    elif mode == "watcher":
        from .app import WatcherRunner

        WatcherRunner().run()
    else:
        print(f"Unknown mode: {mode}")
        sys.exit(1)
