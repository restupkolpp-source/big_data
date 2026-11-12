"""Команды стенда: сервер, worker, deployment, публикация и smoke test."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from urllib.parse import urlencode
import urllib.request

from lab_03.runtime import ROOT, bootstrap, deployment_root
from lab_03.release import deploy, read_json, validate_candidate

POOL = 'lab03-local'
API = os.environ.get('PREFECT_API_URL', 'http://127.0.0.1:4203/api')
SERVICE = os.environ.get('LAB3_SERVICE_URL', 'http://127.0.0.1:8033')


def request_json(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)


def wait_url(url, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            return request_json(url)
        except (OSError, ValueError):
            time.sleep(0.5)
    raise TimeoutError('Сервис не готов: ' + url)


def http_smoke(channel, key, version):
    body = request_json(SERVICE + '/health?' + urlencode({'channel': channel, 'key': key}))
    if body.get('status') != 'ok' or body.get('version') != version:
        raise ValueError('HTTP smoke test не подтвердил версию')


def start_process(name, command):
    import psutil
    root = deployment_root()
    root.mkdir(parents=True, exist_ok=True)
    pid_file = root / f'{name}.pid.json'
    if pid_file.exists():
        previous = read_json(pid_file)
        try:
            process = psutil.Process(previous['pid'])
            if process.create_time() == previous['created'] and process.is_running():
                return previous['pid']
        except psutil.Error:
            pass
    env = os.environ.copy()
    env.pop('RUNNER_TRACKING_ID', None)
    log = (root / f'{name}.log').open('a', encoding='utf-8')
    options = {'creationflags': subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS} if os.name == 'nt' else {'start_new_session': True}
    process = subprocess.Popen(command, cwd=ROOT, env=env, stdin=subprocess.DEVNULL, stdout=log, stderr=log, **options)
    log.close()
    created = psutil.Process(process.pid).create_time()
    pid_file.write_text(json.dumps({'pid': process.pid, 'created': created, 'command': command}), encoding='utf-8')
    return process.pid


def ensure(worker=True):
    bootstrap()
    os.environ['PREFECT_API_URL'] = API
    try:
        request_json(API + '/health')
    except OSError:
        start_process('prefect-server', [sys.executable, '-m', 'prefect', 'server', 'start', '--host', '127.0.0.1',
                                         '--port', '4203', '--analytics-off'])
    wait_url(API + '/health')
    # Службы и API работают вместе: in-memory messaging не делится между процессами.
    try:
        request_json(SERVICE + '/ready')
    except OSError:
        start_process('report-service', [sys.executable, '-m', 'lab_03.service', '--root', str(deployment_root())])
    wait_url(SERVICE + '/ready')
    if worker:
        inspect = subprocess.run([sys.executable, '-m', 'prefect', 'work-pool', 'inspect', POOL], capture_output=True)
        if inspect.returncode:
            subprocess.run([sys.executable, '-m', 'prefect', 'work-pool', 'create', '--type', 'process', POOL], check=True)
        start_process('prefect-worker', [sys.executable, '-m', 'prefect', 'worker', 'start', '--pool', POOL, '--type', 'process', '--limit', '1'])
    return {'api': API, 'service': SERVICE, 'root': str(deployment_root())}


def freeze_code(version=None):
    files = [ROOT / 'requirements.txt'] + [p for p in (ROOT / 'lab_03').rglob('*')
                                         if p.is_file() and not any(x in p.parts for x in ['__pycache__', '.ipynb_checkpoints'])]
    digest = hashlib.sha256()
    for path in sorted(files):
        digest.update(str(path.relative_to(ROOT)).encode())
        digest.update(path.read_bytes())
    code_hash = digest.hexdigest()
    version = version or ('local-' + code_hash[:12])
    if not version.replace('-', '').isalnum():
        raise ValueError('Недопустимый номер версии кода')
    destination = deployment_root() / 'code' / version
    if destination.exists():
        if read_json(destination / 'code.json')['sha256'] != code_hash:
            raise ValueError('Эта версия кода уже существует с другим содержимым')
        return destination
    destination.mkdir(parents=True)
    for path in files:
        target = destination / path.relative_to(ROOT)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)
    (destination / 'code.json').write_text(json.dumps({'version': version, 'sha256': code_hash}), encoding='utf-8')
    return destination


def register(version=None, channel='staging', ci=False):
    ensure()
    from lab_03.orchestration import scheduled_pipeline
    from prefect.client.schemas.schedules import CronSchedule
    source = freeze_code(version)
    flow = scheduled_pipeline.from_source(source=str(source), entrypoint='lab_03/orchestration.py:scheduled_pipeline')
    variables = {k: v for k, v in os.environ.items() if k in [
        'DTP_DATA_DIR', 'LAB3_WORK_DIR', 'LAB3_DEPLOY_ROOT', 'LAB3_SERVICE_URL', 'PYTHONPATH', 'PREFECT_HOME']}
    deployment = flow.deploy(name='dtp-daily', work_pool_name=POOL,
                             schedule=CronSchedule(cron='0 5 * * *', timezone='Europe/Moscow'),
                             parameters={'enforce_deadline': True, 'channel': channel, 'ci': ci},
                             version=source.name, build=False, push=False, job_variables={'env': variables})
    return str(deployment)


def trigger(timeout=300):
    from prefect.deployments import run_deployment
    result = run_deployment(name='DTP daily deployment/dtp-daily', parameters={'enforce_deadline': False},
                            timeout=timeout, poll_interval=1)
    if not result.state.is_completed():
        raise RuntimeError(f'Запуск deployment не завершился успешно: {result.state}')
    return str(result.id)


def export_latest(sessions, destination):
    candidates = list(Path(sessions).glob('*/release.json'))
    if not candidates:
        raise FileNotFoundError('Нет подготовленного релиза')
    manifest = max(candidates, key=lambda p: p.stat().st_mtime)
    info, _ = validate_candidate(manifest.parent)
    destination = Path(destination)
    (destination / 'snapshots').mkdir(parents=True, exist_ok=True)
    shutil.copy2(manifest, destination / 'release.json')
    shutil.copy2(manifest.parent / 'snapshots' / info['snapshot'], destination / 'snapshots' / info['snapshot'])
    return destination


def check_review_gate(repository, token):
    request = urllib.request.Request(f'https://api.github.com/repos/{repository}/environments/production',
                                     headers={'Authorization': 'Bearer ' + token, 'Accept': 'application/vnd.github+json'})
    with urllib.request.urlopen(request, timeout=20) as response:
        protection = json.load(response)['protection_rules']
    if not any(rule['type'] == 'required_reviewers' and rule.get('reviewers') for rule in protection):
        raise RuntimeError('Production не защищён обязательным review; публикация запрещена')


def stop():
    import psutil
    stopped = []
    root = deployment_root()
    for name in ['prefect-worker', 'prefect-services', 'report-service', 'prefect-server']:
        path = root / f'{name}.pid.json'
        if not path.exists():
            continue
        info = read_json(path)
        try:
            process = psutil.Process(info['pid'])
            if process.create_time() != info['created']:
                continue
            descendants = process.children(recursive=True)
            for child in reversed(descendants):
                child.terminate()
            process.terminate()
            _, alive = psutil.wait_procs([process, *descendants], timeout=10)
            for remaining in alive:
                remaining.kill()
            stopped.append(name)
        except psutil.NoSuchProcess:
            pass
        path.unlink(missing_ok=True)
    return stopped


def main():
    bootstrap()
    os.environ['PREFECT_API_URL'] = API
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['ensure', 'stop', 'schedule', 'trigger', 'export', 'deploy', 'validate', 'review-gate'])
    parser.add_argument('--candidate', type=Path)
    parser.add_argument('--out', type=Path)
    parser.add_argument('--sessions', type=Path, default=ROOT / '_work/lab_03/sessions')
    parser.add_argument('--channel', choices=['staging', 'production'], default='staging')
    parser.add_argument('--version')
    parser.add_argument('--ci', action='store_true')
    args = parser.parse_args()
    if args.command == 'ensure':
        print(json.dumps(ensure(), ensure_ascii=False))
    elif args.command == 'stop':
        print(stop())
    elif args.command == 'schedule':
        print(register(args.version, args.channel, args.ci))
    elif args.command == 'trigger':
        print(trigger())
    elif args.command == 'export':
        print(export_latest(args.sessions, args.out))
    elif args.command == 'validate':
        print(validate_candidate(args.candidate)[0])
    elif args.command == 'review-gate':
        check_review_gate(os.environ['GITHUB_REPOSITORY'], os.environ['GITHUB_TOKEN'])
        print('Production review gate: OK')
    else:
        ensure(worker=False)
        print(deploy(args.candidate, deployment_root(), args.channel, smoke=http_smoke))


if __name__ == '__main__':
    main()
