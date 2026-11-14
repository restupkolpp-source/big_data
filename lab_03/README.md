# Лабораторная работа 3. ETL и ELT

Взял данные ДТП из первой лабораторной и собрал ежедневную сводку по трём регионам
за 2025 год: число аварий, погибших и раненых. Данные собирал для hotspot-ai.

Для обработки использовал Prefect, DuckDB, dbt и Great Expectations. Проверил повторную
загрузку, исправление записей, сбои на этапах обработки и откат витрины.

Весь Python-код собрал в ноутбуке. SQL и конфиги dbt оставил в `dbt/`, workflow — в `.github/workflows`.
Для CI и worker использую `scripts/run_notebook.py`: он загружает определения из ячеек без запуска опытов.

## Запуск с нуля

Все команды выполняются из корня `big_data`, Python 3.13. В PowerShell:

```powershell
python -m venv .venv
.venv/Scripts/Activate.ps1
python -m pip install -r lab_03/requirements.txt
$env:LAB3_CI='1'
python -m nbconvert --execute --to notebook --inplace 'lab_03/Лабораторная работа 3.ipynb' --ExecutePreprocessor.timeout=600
```

Встроенные данные: 15 ДТП, 3 региона, 5 дней. Они создаются кодом, внешняя выгрузка не требуется.
Проверяются повторная загрузка, исправление записи, устаревшая ревизия, Time Travel, DLQ,
schema drift, retries на каждом этапе, предел числа попыток, ошибки dbt, smoke test и SLA.
В конце поднимаю HTTP-сервис на свободном порту и проверяю публикацию витрины.
Ноутбук завершается четырьмя метриками `## METRICS ##`.

Для реальных данных снимите `LAB3_CI` и задайте корень подготовленных данных:

```powershell
Remove-Item Env:LAB3_CI -ErrorAction SilentlyContinue
$env:DTP_DATA_DIR=(Resolve-Path data).Path
python -m nbconvert --execute --to notebook --inplace 'lab_03/Лабораторная работа 3.ipynb' --ExecutePreprocessor.timeout=600
```

Нужны `dict/regions.json` (список объектов `rows_code`, `rows_name`) и
`clean/accidents/year=2025/part.parquet` с полями `accident_id`, `reg_code`, `dt`,
`dead_n`, `injured_n`, `vehicles_n`. Берутся регионы 1132, 1140 и 1145.
Можно указать `DTP_DATA_DIR` на `hotspot-ai/experiment/data` с такой структурой.
Либо установить зависимости корневого `requirements.txt` и подготовить данные загрузчиком ЛР-1:

```powershell
python -m pip install -r requirements.txt
python scripts/download_dataset.py --from 2025-01 --to 2025-12 --regions 1132,1140,1145
```

Загрузчик обращается к внешнему API; встроенный режим позволяет проверить работу независимо от его доступности.
Рабочие БД, логи и снимки создаются в `_work/lab_03/sessions/`, исходные данные не меняются.
Каждое выполнение ноутбука создаёт отдельную сессию.

## Запуск по расписанию

```powershell
$env:LAB3_DEPLOY_ROOT=(Join-Path (Get-Location) '_work/deploy')
$env:LAB3_WORK_DIR=(Join-Path (Get-Location) '_work/lab_03')
$env:PREFECT_API_URL='http://127.0.0.1:4203/api'
python scripts/run_notebook.py 3 ops ensure
python scripts/run_notebook.py 3 ops schedule --ci
python scripts/run_notebook.py 3 ops trigger
```

`ensure` запускает Prefect server, scheduler, process worker и HTTP-витрину.
`schedule --ci` регистрирует deployment со встроенными данными; для реальных данных уберите `--ci`
и предварительно задайте абсолютный `DTP_DATA_DIR`.
Ноутбук и загрузчик deployment копируются в неизменяемую папку; повторная регистрация обновляет версию кода.
Расписание: ежедневно в 05:00 Europe/Moscow, deadline 06:00, доступность отчёта к 07:00.
Ручной `trigger` отключает только проверку текущего времени суток, чтобы опыт можно было провести вечером.
Worker ограничен одним запуском для исключения конкурентной записи в DuckDB.
Постоянная сессия worker сохраняет UPSERT между запусками. Ревизия реального источника определяется временем изменения файла.

Prefect UI: http://127.0.0.1:4203 . HTTP: http://127.0.0.1:8033/health?channel=staging .
Для публикации витрины в production:

```powershell
python scripts/run_notebook.py 3 ops export --out _work/candidate
python scripts/run_notebook.py 3 ops deploy --candidate _work/candidate --channel staging
python scripts/run_notebook.py 3 ops deploy --candidate _work/candidate --channel production
```

`/daily?channel=production` отдаёт витрину, `/metrics` — журнал релизов и бизнес-метрики.
Службы работают фоном; закрытие терминала их не останавливает. После опыта остановите стенд командой
`python scripts/run_notebook.py 3 ops stop`.

## Безопасная публикация и мониторинг

`release.json` фиксирует SHA-256 снимка, число строк и ДТП. Перед публикацией проверяются схема,
пропуски, ключ витрины, неотрицательные значения и сумма пострадавших.
В существующем канале 10% ключей запроса направляются на canary, затем выполняется HTTP smoke test.
При успехе атомарно переключается указатель; при ошибке восстанавливается предыдущий.
При первом релизе предыдущей версии нет: используется проверка кандидата без полноценного fallback.
Одновременные публикации блокирую через filelock. Стенд работает локально.

`deployment-demo/verification.json` в папке сессии содержит успешную публикацию,
искусственный провал smoke test, подтверждённый HTTP-откат и отказ от повреждённого артефакта.
Метрики: число строк, ДТП, пострадавших, разница с прошлой версией и total variation distance
распределения ДТП по регионам. При TVD > 0.1 журнал содержит `drift_alert=true`.
В опыте отдельно подмешивается сильный сдвиг в staging и проверяется срабатывание индикатора.
Drift служит сигналом для анализа; автоматического запрета релиза по этой эвристике нет.

Отдельное повторение тестов на подготовленном кандидате:

```powershell
python scripts/run_notebook.py 3 verify --candidate _work/candidate --root _work/verification-new
```

## dbt и качество

GX проверяет обязательные поля, уникальность, диапазоны, принадлежность региона справочнику и
равенство `victims = dead + injured`. dbt дополнительно проверяет foreign key региона и
сверяет итоговые суммы с raw. Невалидные строки уходят в DLQ с причиной и исходным содержимым.
Неизвестная обязательная схема останавливает загрузку; совместимый алиас `killed_n` поддерживается.
HTML-отчёты GX находятся в папке сессии; они показаны также в ноутбуке.

Ноутбук создаёт dbt Docs (`dbt/target/index.html`, `manifest.json`, `catalog.json`).
Для просмотра выберите путь сессии из вывода ноутбука:

```powershell
$env:LAB3_DB='<абсолютный путь к warehouse.duckdb сессии>'
$env:LAB3_SCHEMA='<версия candidate_... из ноутбука>'
python -m dbt.cli.main docs serve --project-dir '<папка сессии>/dbt' --profiles-dir '<папка сессии>/dbt' --port 8083 --no-browser
```

## CI/CD

Workflow находится в [lab03.yml](../.github/workflows/lab03.yml). На push и PR запускаю
ноутбук на тестовом наборе, проверяю запуск через worker, публикацию по HTTP и откат.
Затем тот же Parquet-снимок передаю в staging и проверяю его через `/health`.

[PR с третьей работой](https://github.com/restupkolpp-source/big_data/pull/1).
[Проверки и staging](https://github.com/restupkolpp-source/big_data/actions/runs/43230793996) прошли.

Production запускается после merge в main. Для него настроил environment `production`
с ручным подтверждением от `restupkolpp-source` и доступом только из main. Перед
публикацией workflow проверяет, что подтверждение включено в настройках.

В Actions сервис работает на временном runner. После завершения job остаются снимок
данных и журнал публикации; для постоянной работы нужен отдельный сервер.

Скриншоты Prefect и dbt Docs, отчёты GX и результаты проверок лежат в [evidence](evidence/).

## Выводы

1. Выбрал ELT, потому что сводка собирается SQL-запросами и её удобно пересчитывать из сохранённых данных. Для этого объёма хватает DuckDB. Затраты в ноутбуке оценил условно.
2. Повторный запуск не добавил дубликатов. Новая ревизия обновила запись, а старая не затёрла исправление.
   DuckDB не предоставляет нужный Time Travel автоматически, поэтому опубликованные версии сохраняются в Parquet.
3. Проверил сбои на каждом этапе: временные ошибки проходят после повтора, плохие записи уходят в DLQ, при несовместимой схеме загрузка останавливается.
4. В Prefect вижу состояние и время задач. Регионы обрабатываю дочерними flow, ежедневный запуск задаю расписанием.
5. GX проверяет контракт до загрузки и витрину после трансформации; dbt проверяет связи и согласованность сумм.
6. dbt отделяет staging и бизнес-агрегацию и формирует граф зависимостей из реального исполнения.
7. Неизменяемый снимок, canary, HTTP smoke test и атомарный указатель позволяют откатить неудачный релиз.
   Журнал сравнивает бизнес-метрики версий и отмечает сдвиг распределения регионов.
