"""Brings loopback up inside a new network namespace, then runs a command.

Run as `unshare -rn python3 netns_exec.py -- <argv...>`: a new user and
network namespace starts with `lo` down and no other interface, so this sets
`lo` up with the SIOCSIFFLAGS ioctl (no `ip` binary needed) and executes the
command, which then reaches 127.0.0.1 and nothing else. Standalone: it
imports nothing from the package, since it runs before the package's path is
known.
"""

import fcntl
import os
import socket
import struct
import sys

SIOCGIFFLAGS = 0x8913
SIOCSIFFLAGS = 0x8914
IFF_UP = 0x1


def lo_up():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        req = struct.pack("16sH14x", b"lo", 0)
        flags = struct.unpack("16sH14x", fcntl.ioctl(s.fileno(), SIOCGIFFLAGS, req))[1]
        fcntl.ioctl(s.fileno(), SIOCSIFFLAGS, struct.pack("16sH14x", b"lo", flags | IFF_UP))
    finally:
        s.close()


def main(argv):
    if not argv or argv[0] != "--":
        print("usage: netns_exec.py -- <command...>", file=sys.stderr)
        return 2
    lo_up()
    os.execvp(argv[1], argv[1:])
    return 127


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
