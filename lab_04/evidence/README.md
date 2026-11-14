# Результаты

Скриншоты MLflow — `mlflow-experiment.jpg` и `mlflow-registry.jpg`.
Графики из ноутбука сохранил отдельно в PNG.

`metrics.txt` — обязательные метрики, `summary.json` — результаты основного прогона.
`model_comparison.csv` — сравнение моделей на validation.
`http_metrics.json` — нагрузка, canary, shadow и откат.
`drift_*.html` — отчёты по признакам.
`sensor_thresholds.json` — параметры, выбранные до скрытой аварии.
`sensor_alarm_log.json` — журнал тревог на финальном участке. В этом прогоне тревог не было, скрытая авария пропущена.

Оценка модели и время полного HTTP-запроса приведены отдельно.
