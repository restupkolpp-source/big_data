"""Воспроизводимые HTTP-проверки релиза, canary, drift и автоматического отката."""

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import threading
from http.server import ThreadingHTTPServer
from urllib.parse import urlencode
from urllib.request import urlopen

from lab_03.release import active, deploy, read_json, select_version, validate_candidate
from lab_03.service import make_handler


def deployment_demo(candidate, root):
    root = Path(root)
    # Каждый опыт изолирован от рабочего production-указателя.
    if (root / 'verification.json').exists():
        raise ValueError('Для повторной проверки используйте новую папку')
    root.mkdir(parents=True, exist_ok=True)
    server = ThreadingHTTPServer(('127.0.0.1', 0), make_handler(root))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f'http://127.0.0.1:{server.server_port}'
    checks = []

    def get(path):
        with urlopen(base + path, timeout=10) as response:
            return json.load(response)

    def smoke(channel, key, version):
        result = get('/health?' + urlencode({'channel': channel, 'key': key}))
        assert result['status'] == 'ok' and result['version'] == version

    try:
        first = deploy(candidate, root, 'staging', smoke=smoke)
        assert get('/health?channel=staging')['version'] == first
        deploy(candidate, root, 'production', smoke=smoke)
        assert get('/health')['version'] == first
        checks.append({'check': 'staging + production HTTP smoke', 'passed': True})

        # Создаём контролируемую новую версию с изменением бизнес-метрики.
        info, data = validate_candidate(candidate)
        changed = root / 'changed-candidate'
        (changed / 'snapshots').mkdir(parents=True, exist_ok=True)
        data = data.copy()
        data.loc[data.index[0], 'injured'] += 1
        data.loc[data.index[0], 'victims'] += 1
        snapshot = changed / 'snapshots' / info['snapshot']
        data.to_parquet(snapshot, index=False)
        info = {**info, 'sha256': hashlib.sha256(snapshot.read_bytes()).hexdigest()}
        (changed / 'release.json').write_text(json.dumps(info), encoding='utf-8')

        def broken_smoke(channel, key, version):
            smoke(channel, key, version)
            pointer = read_json(root / f'{channel}.json')
            assert pointer['stable'] == first and pointer['percent'] == 10
            selected = {select_version(pointer, str(i)) for i in range(1000)}
            assert selected == {first, version}
            raise ValueError('Injected HTTP smoke failure')

        try:
            deploy(changed, root, 'production', smoke=broken_smoke)
        except ValueError as exc:
            assert 'Injected' in str(exc)
        else:
            raise AssertionError('Ожидался автоматический откат')
        assert get('/health')['version'] == first
        checks.append({'check': '10% canary + failed smoke + HTTP rollback', 'passed': True})
        second = deploy(changed, root, 'production', smoke=smoke)
        assert second != first and get('/health')['version'] == second
        checks.append({'check': 'successful promotion after rollback', 'passed': True})

        # Повреждение артефакта не должно переключить указатель.
        damaged = root / 'damaged-candidate'
        shutil.copytree(changed, damaged, dirs_exist_ok=True)
        (damaged / 'snapshots' / info['snapshot']).write_bytes(b'corrupted')
        try:
            deploy(damaged, root, 'production', smoke=smoke)
        except ValueError:
            pass
        else:
            raise AssertionError('Повреждённый релиз был принят')
        assert get('/health')['version'] == second
        checks.append({'check': 'corrupted artifact rejected before publication', 'passed': True})
        events = get('/metrics')
        promoted = [e for e in events if e['channel'] == 'production' and e['status'] == 'promoted']
        assert promoted[-1]['victims'] == promoted[0]['victims'] + 1
        assert promoted[-1]['region_distribution_tvd'] == 0
        assert all('business_delta' in e for e in promoted)
        checks.append({'check': 'business metrics + regional distribution drift', 'passed': True})
        shifted = root / 'shifted-candidate'
        (shifted / 'snapshots').mkdir(parents=True, exist_ok=True)
        data.loc[data.index[0], 'accidents'] += 1_000_000
        shifted_snapshot = shifted / 'snapshots' / info['snapshot']
        data.to_parquet(shifted_snapshot, index=False)
        shifted_info = {**info, 'sha256': hashlib.sha256(shifted_snapshot.read_bytes()).hexdigest(),
                        'accidents': int(data.accidents.sum())}
        (shifted / 'release.json').write_text(json.dumps(shifted_info), encoding='utf-8')
        deploy(shifted, root, 'staging', smoke=smoke)
        events = get('/metrics')
        assert events[-1]['drift_alert'] and events[-1]['region_distribution_tvd'] > 0.1
        assert get('/health')['version'] == second
        checks.append({'check': 'injected regional drift triggers alert in staging', 'passed': True})
        result = {'checks': checks, 'events': events}
        (root / 'verification.json').write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
        return result
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--root', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(deployment_demo(args.candidate, args.root), ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
