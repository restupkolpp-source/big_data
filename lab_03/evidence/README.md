# Результаты проверок

- `prefect-worker.jpg` — запуск через worker.
- `prefect-tasks.jpg` — задачи ELT и обработка регионов в Prefect.
- `dbt-lineage.jpg` — граф зависимостей в dbt Docs.
- `deployment-verification.json` — шесть проверок публикации, canary, отката и drift.
- `metrics-real.txt` — запуск на данных ДТП за 2025 год.
- `metrics-ci-local.txt` — запуск на тестовом наборе из 15 записей.
- `quality_*.html` — отчёты Great Expectations.

[GitHub Actions и staging](https://github.com/restupkolpp-source/big_data/actions/runs/43230793996) прошли.
Production подтверждаю вручную после merge в main.
