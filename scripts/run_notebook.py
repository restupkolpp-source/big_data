"""Запуск определений из ноутбуков для CI, API и Prefect worker."""

import contextlib
import json
import linecache
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]


def load_definitions(lab, full=True):
    folder = ROOT / f'lab_{int(lab):02d}'
    notebook = next(folder.glob('*.ipynb'))
    document = json.loads(notebook.read_text(encoding='utf-8'))
    tags = ({'lab03-common', 'lab03-utility', 'lab03-runtime', 'lab03-orchestration'}
            if int(lab) == 3 and full else {'lab03-common', 'lab03-utility'}
            if int(lab) == 3 else {'lab04-runtime'})
    namespace = {'__name__': '__notebook_definitions__'}
    with contextlib.chdir(ROOT):
        for index, cell in enumerate(document['cells']):
            if cell['cell_type'] != 'code' or not tags.intersection(cell.get('metadata', {}).get('tags', [])):
                continue
            source = ''.join(cell['source'])
            filename = str(notebook) + f'#cell-{index}'
            linecache.cache[filename] = (len(source), None, source.splitlines(True), filename)
            exec(compile(source, filename, 'exec'), namespace)
    return namespace


def main():
    if len(sys.argv) < 3:
        raise SystemExit('python scripts/run_notebook.py 3|4 ops|run|verify|ci-deploy|service|review-gate|api [аргументы]')
    lab, command, *args = sys.argv[1:]
    full = command == 'run' or command == 'ops' and any(x in args for x in ['schedule', 'trigger'])
    namespace = load_definitions(lab, full=full)
    if command == 'run':
        print(namespace['scheduled_pipeline'](ci='--ci' in args, channel=None if '--no-deploy' in args else 'staging'))
    elif command == 'review-gate':
        namespace['check_review_gate'](os.environ['GITHUB_REPOSITORY'], os.environ['GITHUB_TOKEN'])
        print('Production review gate: OK')
    else:
        function = {'ops': 'ops_main', 'verify': 'verify_main', 'ci-deploy': 'ci_deploy_main',
                    'service': 'service_main', 'api': 'serve_api'}.get(command)
        if function not in namespace:
            raise SystemExit('Команда отсутствует в этом ноутбуке: ' + command)
        namespace[function](args)


def __getattr__(name):
    if name != 'scheduled_pipeline':
        raise AttributeError(name)
    # Prefect запрашивает flow по entrypoint; реализация остаётся в ячейке ЛР-3.
    flow = load_definitions(3, full=True)['scheduled_pipeline']
    globals()[name] = flow
    return flow


if __name__ == '__main__':
    main()
