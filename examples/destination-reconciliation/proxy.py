"""A TCP proxy in front of ClickHouse's native port, for the faults the example
needs the network to cause. It follows tests/integration/tcp_fault_proxy.hpp:

* hold_next_insert(): the next INSERT's data is forwarded to the server, and
  the server's reply to it is withheld, so ClickHouse writes the rows while
  the client waits for an answer that never comes; reset_held() then resets
  the connection. The client is left in doubt and resends.
* stall(): forward nothing either way on new and open connections, so a
  client waits on its own timeouts.

An INSERT is recognised as tcp_fault_proxy.hpp recognises one: a query
packet carries its statement uncompressed, so "INSERT INTO" in the client's
bytes marks one, and the client's next bytes after any reply from the server
are its data blocks.
"""
import select
import socket
import struct
import threading
import time


class FaultProxy:
    def __init__(self, upstream_port, host="127.0.0.1"):
        self.upstream = (host, upstream_port)
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind((host, 0))
        self.listener.listen(64)
        self.port = self.listener.getsockname()[1]
        self.lock = threading.Lock()
        self.hold_requests = 0
        self.held = []          # connections whose server side is withheld
        self.inserts_seen = 0
        self.inserts_held = 0
        self.stalled = False
        self.stop_flag = False
        threading.Thread(target=self._accept, daemon=True).start()

    # -- control -----------------------------------------------------------
    def hold_next_insert(self, n=1):
        with self.lock:
            self.hold_requests += n

    def held_quiet(self, quiet_s=0.5):
        """Whether a held connection's client has sent nothing for quiet_s: it
        has sent its whole INSERT and is waiting for the reply."""
        with self.lock:
            now = time.monotonic()
            return any(now - c["last_client"] >= quiet_s for c in self.held)

    def reset_held(self):
        with self.lock:
            for c in self.held:
                c["kill"] = True
            self.held = []

    def stall(self, on=True):
        with self.lock:
            self.stalled = on

    def close(self):
        self.stop_flag = True
        try:
            self.listener.close()
        except OSError:
            pass

    # -- the pumps -----------------------------------------------------------
    def _accept(self):
        while not self.stop_flag:
            try:
                client, _ = self.listener.accept()
            except OSError:
                return
            try:
                up = socket.create_connection(self.upstream, timeout=5)
            except OSError:
                client.close()
                continue
            up.settimeout(None)
            conn = {"client": client, "up": up, "held": False, "kill": False,
                    "statement": False, "reply": False, "last_client": time.monotonic()}
            threading.Thread(target=self._pump, args=(conn,), daemon=True).start()

    def _pump(self, c):
        client, up = c["client"], c["up"]
        try:
            while not self.stop_flag:
                if c["kill"]:
                    break
                with self.lock:
                    frozen = self.stalled
                watch = []
                if not frozen:
                    watch.append(client)
                    if not c["held"]:
                        watch.append(up)
                if not watch:
                    time.sleep(0.05)
                    continue
                ready, _, _ = select.select(watch, [], [], 0.05)
                if client in ready:
                    data = client.recv(65536)
                    if not data:
                        break
                    c["last_client"] = time.monotonic()
                    if b"INSERT INTO" in data:
                        c["statement"], c["reply"] = True, False
                        with self.lock:
                            self.inserts_seen += 1
                    elif c["statement"] and c["reply"]:
                        # The first bytes after the server's reply to the
                        # statement: the INSERT's data is under way.
                        c["statement"] = False
                        with self.lock:
                            if self.hold_requests > 0:
                                self.hold_requests -= 1
                                self.inserts_held += 1
                                c["held"] = True
                                self.held.append(c)
                    up.sendall(data)
                if up in ready and not c["held"]:
                    data = up.recv(65536)
                    if not data:
                        break
                    if c["statement"]:
                        c["reply"] = True
                    client.sendall(data)
        except OSError:
            pass
        finally:
            for s in (client, up):
                try:
                    # A reset, not an orderly close: the client must not read
                    # the end of the stream as an answer.
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
                except OSError:
                    pass
                try:
                    s.close()
                except OSError:
                    pass
