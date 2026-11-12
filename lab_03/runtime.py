"""Загрузка определений из ноутбука для worker, без запуска опытов со сбоями."""

import contextlib
import hashlib
import json
import linecache
import os
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def bootstrap():
    os.environ['PYTHONUTF8'] = '1'
    os.environ.setdefault('PREFECT_SERVER_ANALYTICS_ENABLED', 'false')
    os.environ.setdefault('GX_ANALYTICS_ENABLED', 'False')
    home = Path(os.environ.setdefault('PREFECT_HOME', str(ROOT / '_work/lab_03/prefect')))
    home.mkdir(parents=True, exist_ok=True)
    if os.name == 'nt':
        site = Path(sys.prefix) / 'Lib/site-packages'
        alias = Path(tempfile.gettempdir()) / 'opencode' / ('lab03-site-' + hashlib.sha256(str(site).encode()).hexdigest()[:8])
        alias.parent.mkdir(parents=True, exist_ok=True)
        if not alias.exists():
            command = "New-Item -ItemType Junction -Path '" + str(alias).replace("'", "''") + "' -Target '" + str(site).replace("'", "''") + "' | Out-Null"
            subprocess.run(['powershell', '-NoProfile', '-Command', command], check=True, capture_output=True)
        sys.path.insert(0, str(alias))
        os.environ['PYTHONPATH'] = str(alias) + os.pathsep + os.environ.get('PYTHONPATH', '')


def deployment_root():
    return Path(os.environ.get('LAB3_DEPLOY_ROOT', str(Path(tempfile.gettempdir()) / 'opencode/lab03-deploy'))).resolve()


def load_runtime(session='scheduled'):
    bootstrap()
    if not session.replace('-', '').replace('_', '').isalnum():
        raise ValueError('Недопустимое имя сессии')
    notebook = next((ROOT / 'lab_03').glob('*.ipynb'))
    document = json.loads(notebook.read_text(encoding='utf-8'))
    namespace = {'__name__': '__lab03_runtime__'}
    previous = os.environ.get('LAB3_SESSION')
    os.environ['LAB3_SESSION'] = session
    try:
        with contextlib.chdir(ROOT):
            for i, cell in enumerate(document['cells']):
                if 'lab03-runtime' not in cell.get('metadata', {}).get('tags', []):
                    continue
                source = ''.join(cell['source'])
                filename = str(notebook) + f'#cell-{i}'
                linecache.cache[filename] = (len(source), None, source.splitlines(True), filename)
                exec(compile(source, filename, 'exec'), namespace)
        if 'initialize_runtime' not in namespace:
            raise RuntimeError('В ноутбуке не найдены определения пайплайна')
        namespace['initialize_runtime']()
        return namespace
    finally:
        if previous is None:
            os.environ.pop('LAB3_SESSION', None)
        else:
            os.environ['LAB3_SESSION'] = previous


def export_release(namespace, result, destination=None):
    work = namespace['WORK']
    snapshot = Path(namespace['sql']('SELECT snapshot FROM versions WHERE version=?', [result['version']]).iloc[0, 0])
    state = namespace['signature']()
    manifest = {'snapshot': snapshot.name, 'sha256': hashlib.sha256(snapshot.read_bytes()).hexdigest(),
                'rows': state['rows'], 'accidents': state['accidents'], 'run_id': result['run_id']}
    (work / 'release.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
    if destination:
        import shutil
        destination = Path(destination)
        (destination / 'snapshots').mkdir(parents=True, exist_ok=True)
        shutil.copy2(snapshot, destination / 'snapshots' / snapshot.name)
        shutil.copy2(work / 'release.json', destination / 'release.json')
        return destination
    return work
