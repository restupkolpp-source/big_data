"""Реальный entrypoint Prefect; определения ELT берутся из ноутбука."""

import os
from pathlib import Path
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from lab_03.runtime import bootstrap, deployment_root, export_release, load_runtime

bootstrap()
from prefect import flow
from prefect.context import get_run_context


@flow(name='DTP daily deployment', log_prints=True, persist_result=False)
def scheduled_pipeline(enforce_deadline=False, channel='staging', ci=False):
    previous = os.environ.get('LAB3_CI')
    os.environ['LAB3_CI'] = '1' if ci else '0'
    try:
        runtime = load_runtime('scheduled-ci' if ci else 'scheduled')
        budget = 3600
        if enforce_deadline:
            expected = get_run_context().flow_run.expected_start_time
            deadline = expected.astimezone(ZoneInfo('Europe/Moscow')).replace(hour=6, minute=0, second=0, microsecond=0)
            budget = max(0, (deadline.astimezone(timezone.utc) - datetime.now(timezone.utc)).total_seconds())
        source = runtime['DATA'] / 'clean/accidents/year=2025/part.parquet'
        revision = 1 if ci else source.stat().st_mtime_ns
        result = runtime['pipeline'](revision=revision, budget=budget)
        candidate = export_release(runtime, result)
        if channel:
            from lab_03.release import deploy
            from lab_03.ops import http_smoke
            deploy(candidate, deployment_root(), channel, smoke=http_smoke)
        print(f"Опубликовано {result['rows']} ДТП, версия {result['version']}")
        return {**result, 'candidate': str(candidate)}
    finally:
        if previous is None:
            os.environ.pop('LAB3_CI', None)
        else:
            os.environ['LAB3_CI'] = previous
