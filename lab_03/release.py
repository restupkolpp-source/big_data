"""Проверка, canary и атомарное переключение опубликованной версии."""

import hashlib
import json
import os
from pathlib import Path
import shutil
import uuid
from datetime import datetime, timezone

import pandas as pd


def local_path(path):
    path = Path(path).resolve()
    if os.name == 'nt' and not str(path).startswith('\\\\?\\'):
        text = str(path)
        return Path('\\\\?\\UNC\\' + text[2:] if text.startswith('\\\\') else '\\\\?\\' + text)
    return path


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')
    temp.replace(path)


def validate_candidate(folder):
    folder = local_path(folder)
    info = read_json(folder / 'release.json')
    filename = info['snapshot']
    if Path(filename).name != filename or not filename.endswith('.parquet'):
        raise ValueError('Недопустимый путь снимка')
    snapshot = folder / 'snapshots' / filename
    if hashlib.sha256(snapshot.read_bytes()).hexdigest() != info['sha256']:
        raise ValueError('Контрольная сумма снимка не совпала')
    data = pd.read_parquet(snapshot)
    required = ['day', 'reg_code', 'reg_name', 'accidents', 'dead', 'injured', 'victims']
    if set(data.columns) != set(required) or data.empty or data[required].isna().any().any():
        raise ValueError('Схема или заполненность витрины нарушена')
    if data[['day', 'reg_code']].duplicated().any() or (data[['accidents', 'dead', 'injured', 'victims']] < 0).any().any():
        raise ValueError('Повторы или отрицательные значения в витрине')
    if not (data.victims == data.dead + data.injured).all():
        raise ValueError('Не сходится число пострадавших')
    if len(data) != info['rows'] or int(data.accidents.sum()) != info['accidents']:
        raise ValueError('Итоги не совпали с манифестом')
    return info, data


def select_version(pointer, key=''):
    canary = pointer.get('canary')
    bucket = int(hashlib.sha256(key.encode()).hexdigest()[:8], 16) % 100
    return canary if canary and bucket < pointer.get('percent', 0) else pointer['stable']


def active(root, channel='production', key=''):
    if channel not in ['staging', 'production']:
        raise ValueError('Неизвестное окружение')
    root = local_path(root)
    pointer = read_json(root / f'{channel}.json')
    version = select_version(pointer, key)
    return version, root / 'releases' / version


def record(root, event):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    with (root / 'deployments.jsonl').open('a', encoding='utf-8') as out:
        out.write(json.dumps({'at': datetime.now(timezone.utc).isoformat(), **event}, ensure_ascii=False) + '\n')


def deploy(candidate, root, channel='staging', smoke=None):
    from filelock import FileLock
    root = local_path(root)
    root.mkdir(parents=True, exist_ok=True)
    with FileLock(str(root / '.deploy.lock'), timeout=30):
        return _deploy(candidate, root, channel, smoke)


def _deploy(candidate, root, channel='staging', smoke=None):
    if channel not in ['staging', 'production']:
        raise ValueError(channel)
    root = Path(root)
    info, data = validate_candidate(candidate)
    version = info['sha256']
    destination = root / 'releases' / version
    if not destination.exists():
        destination.parent.mkdir(parents=True, exist_ok=True)
        # Короткое имя сохраняет совместимость с Windows MAX_PATH.
        temporary = destination.with_name('.tmp-' + uuid.uuid4().hex[:12])
        temporary.mkdir()
        (temporary / 'snapshots').mkdir()
        shutil.copy2(Path(candidate) / 'release.json', temporary / 'release.json')
        shutil.copy2(Path(candidate) / 'snapshots' / info['snapshot'], temporary / 'snapshots' / info['snapshot'])
        validate_candidate(temporary)
        temporary.replace(destination)
    pointer_path = root / f'{channel}.json'
    previous = read_json(pointer_path) if pointer_path.exists() else None
    old = previous['stable'] if previous else version
    try:
        atomic_json(pointer_path, {'stable': old, 'canary': version, 'percent': 10 if previous else 100})
        # Одна и та же функция чтения используется сервисом и smoke test.
        key = next(str(i) for i in range(1000) if select_version(read_json(pointer_path), str(i)) == version)
        selected, folder = active(root, channel, key)
        if selected != version:
            raise ValueError('Canary не получил новую версию')
        validate_candidate(folder)
        if smoke:
            smoke(channel, key, version)
        atomic_json(pointer_path, {'stable': version, 'canary': None, 'percent': 0})
        metrics = {'rows': len(data), 'accidents': info['accidents'], 'victims': int(data.victims.sum())}
        baseline = None
        drift = None
        if previous:
            _, baseline = validate_candidate(root / 'releases' / old)
            current_share = data.groupby('reg_code').accidents.sum()
            previous_share = baseline.groupby('reg_code').accidents.sum()
            current_share = current_share / max(float(current_share.sum()), 1)
            previous_share = previous_share / max(float(previous_share.sum()), 1)
            left, right = current_share.align(previous_share, fill_value=0)
            drift = float((left - right).abs().sum() / 2)
        before_metrics = ({'rows': len(baseline), 'accidents': int(baseline.accidents.sum()),
                           'victims': int(baseline.victims.sum())} if baseline is not None else metrics)
        record(root, {'channel': channel, 'status': 'promoted', 'before': old, 'after': version,
                      **metrics, 'business_delta': {k: metrics[k] - before_metrics[k] for k in metrics},
                      'region_distribution_tvd': drift, 'drift_alert': drift is not None and drift > 0.1})
    except Exception as exc:
        if previous is not None:
            atomic_json(pointer_path, previous)
        else:
            pointer_path.unlink(missing_ok=True)
        record(root, {'channel': channel, 'status': 'rolled_back', 'before': old, 'after': version, 'error': str(exc)})
        raise
    return version
