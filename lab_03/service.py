"""HTTP-витрина читает версию через атомарный указатель, без перезапуска."""

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from lab_03.release import active, read_json, validate_candidate


def make_handler(root):
    root = Path(root)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            request = urlparse(self.path)
            query = parse_qs(request.query)
            if request.path == '/ready':
                payload = b'{"status":"ready"}'
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            try:
                channel = query.get('channel', ['production'])[0]
                version, folder = active(root, channel, query.get('key', [''])[0])
                info, frame = validate_candidate(folder)
                if request.path == '/health':
                    body = {'status': 'ok', 'version': version, 'channel': channel,
                            'rows': info['rows'], 'accidents': info['accidents']}
                elif request.path == '/daily':
                    if 'region' in query:
                        frame = frame[frame.reg_code == int(query['region'][0])]
                    body = json.loads(frame.head(1000).to_json(orient='records', date_format='iso'))
                elif request.path == '/metrics':
                    events = root / 'deployments.jsonl'
                    body = [json.loads(line) for line in events.read_text(encoding='utf-8').splitlines()] if events.exists() else []
                else:
                    self.send_error(404)
                    return
                payload = json.dumps(body, ensure_ascii=False).encode()
                self.send_response(200)
            except (ValueError, KeyError, OSError) as exc:
                payload = json.dumps({'error': str(exc)}, ensure_ascii=False).encode()
                self.send_response(503)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args):
            pass

    return Handler


def main():
    from lab_03.runtime import deployment_root
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=Path, default=deployment_root())
    parser.add_argument('--port', type=int, default=8033)
    args = parser.parse_args()
    print(f'http://127.0.0.1:{args.port}/health', flush=True)
    ThreadingHTTPServer(('127.0.0.1', args.port), make_handler(args.root)).serve_forever()


if __name__ == '__main__':
    main()
