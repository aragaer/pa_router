#!/usr/bin/env python3
import logging
import sys

from router import Router


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Personal assistant message router")
    parser.add_argument("--session", default="pa", help="Tmux session name")
    parser.add_argument("--config", default="router.conf", help="Config file")
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    logging.getLogger('router').setLevel(logging.DEBUG)
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    router = Router(parser.parse_args())
    router.run()
