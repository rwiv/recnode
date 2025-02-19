import sys


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m foo [batch|server]")
        sys.exit(1)

    mode = sys.argv[1]

    if mode == "batch":
        from .app import BatchRunner

        r = BatchRunner()
        r.run()
    elif mode == "server":
        from stdl.app import run_server

        run_server()
    else:
        print(f"Unknown mode: {mode}")
        sys.exit(1)
