# Материалы проверки

- `dbt-lineage.jpg`: настоящий граф dbt Docs, построенный по manifest и catalog реального набора ДТП.
- `prefect-tasks.jpg`: граф зависимостей и шесть завершённых задач ELT, три региональных subflow.
- `prefect-worker.jpg`: успешный запуск deployment через process worker.

Локальные запуски и GitHub Actions проверяются отдельно. `deployment-verification.json` содержит шесть прошедших HTTP-проверок на реальной витрине. `metrics-real.txt` — обязательные метрики. `quality_*.html` — результаты GX до очистки и на корректных данных.

`metrics-ci-local.txt` — полный локальный прогон встроенного набора (15 ДТП), без доступа к исходному датасету.
Результаты чистого Linux-окружения и staging доступны в [Checks PR №1](https://github.com/restupkolpp-source/big_data/pull/1/checks).
Production после merge подтверждает вручную `restupkolpp-source`; автоматическое подтверждение не выполняется.
