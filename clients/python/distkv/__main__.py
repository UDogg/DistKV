"""Tiny CLI: python -m distkv [--endpoints URL,...] get|put|del|cluster ..."""

from __future__ import annotations

import argparse
import json
import os
import sys

from .client import Client, NotFound


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m distkv")
    p.add_argument("--endpoints", default=os.environ.get("DISTKV_ENDPOINTS", "http://127.0.0.1:7001"))
    sub = p.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("get")
    g.add_argument("key")
    g.add_argument("--stale", action="store_true")
    w = sub.add_parser("put")
    w.add_argument("key")
    w.add_argument("value")
    w.add_argument("--ttl", type=float, help="seconds")
    w.add_argument("--if-version", type=int)
    d = sub.add_parser("del")
    d.add_argument("key")
    sub.add_parser("cluster")
    args = p.parse_args(argv)

    c = Client(args.endpoints)
    try:
        if args.cmd == "get":
            it = c.get(args.key, stale=args.stale)
            sys.stdout.buffer.write(it.value + b"\n")
            print(f"version={it.version} shard={it.shard} served_by={it.served_by}", file=sys.stderr)
        elif args.cmd == "put":
            print(f"OK version={c.put(args.key, args.value, ttl=args.ttl, if_version=args.if_version)}")
        elif args.cmd == "del":
            c.delete(args.key)
            print("OK")
        else:
            print(json.dumps(c.cluster(), indent=2))
    except NotFound:
        print("not found", file=sys.stderr)
        return 4
    return 0


if __name__ == "__main__":
    sys.exit(main())
