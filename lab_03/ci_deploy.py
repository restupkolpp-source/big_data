"""HTTP-деплой на временном стенде GitHub runner (не внешний сервер)."""

import argparse
import json
from http.server import ThreadingHTTPServer
from pathlib import Path
import threading
from urllib.parse import urlencode
from urllib.request import urlopen

from lab_03.release import deploy
from lab_03.service import make_handler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--channel', choices=['staging', 'production'], required=True)
    args = parser.parse_args()
    server = ThreadingHTTPServer(('127.0.0.1', 0), make_handler(args.root))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    def smoke(channel, key, version):
        url = f'http://127.0.0.1:{server.server_port}/health?' + urlencode({'channel': channel, 'key': key})
        with urlopen(url, timeout=10) as response:
            body = json.load(response)
        assert body['status'] == 'ok' and body['version'] == version

    try:
        print(deploy(args.candidate, args.root, args.channel, smoke=smoke))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


if __name__ == '__main__':
    main()
