"""A scripted stand-in for clink_node, for the upgrade cluster's limits.

It answers /api/v1/health on its --http-port, then misbehaves as
FAKE_NODE_MODE says: run (do nothing more), exit (exit 3 after a moment),
killed (die by SIGKILL, which unlike SIGABRT raises no crash report on
macOS), alloc (hold FAKE_NODE_MIB of memory) or write (print
until killed, so its log grows).
"""
import http.server
import os
import signal
import sys
import threading
import time


class _Health(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"{}")

    def log_message(self, *args):
        pass


def main():
    port = next(int(a.split("=", 1)[1]) for a in sys.argv if a.startswith("--http-port="))
    srv = http.server.HTTPServer(("127.0.0.1", port), _Health)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    mode = os.environ.get("FAKE_NODE_MODE", "run")
    hold = []
    time.sleep(0.5)
    if mode == "exit":
        sys.exit(3)
    if mode == "killed":
        os.kill(os.getpid(), signal.SIGKILL)
    if mode == "alloc":
        hold.append(bytearray(int(os.environ.get("FAKE_NODE_MIB", "200")) << 20))
        for i in range(0, len(hold[0]), 4096):
            hold[0][i] = 1
    while True:
        if mode == "write":
            sys.stdout.write("x" * 65536 + "\n")
            sys.stdout.flush()
        else:
            time.sleep(0.1)


if __name__ == "__main__":
    main()
