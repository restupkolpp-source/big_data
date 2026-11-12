"""Загрузка исходных данных первой лабораторной через hotspot-retriever."""

import argparse
import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


async def prepare_dictionaries(settings, *, refresh: bool = False) -> None:
    from hotspot_retriever.client import GibddClient
    from hotspot_retriever.fetch import DICTIONARIES
    from hotspot_retriever.models import DictionaryResponse, DictRow
    from pydantic import ValidationError

    settings.dict_dir.mkdir(parents=True, exist_ok=True)
    async with GibddClient(settings) as client:
        for code, name in DICTIONARIES.items():
            path = settings.dict_dir / f"{name}.json"
            if path.exists() and not refresh:
                try:
                    rows = json.loads(path.read_text(encoding="utf-8"))
                    if not isinstance(rows, list) or not rows:
                        raise ValueError("Пустой справочник")
                    for row in rows:
                        DictRow.model_validate(row)
                except (ValueError, ValidationError):
                    print(f"{name}: локальный справочник повреждён, запрашиваю заново")
                else:
                    print(f"{name}: использую локальный справочник ({len(rows)} строк)")
                    continue
            print(f"{name}: запрос {settings.base_url.rstrip('/')}/dictionary/rows?code={code}", flush=True)
            payload, _, _ = await client.get_json("/dictionary/rows", {"code": str(code)})
            parsed = DictionaryResponse.model_validate(payload)
            rows = [row.model_dump() for block in parsed.results for row in block.dict_rows
                    if block.dict_code == code]
            if parsed.status != 200 or not rows:
                raise ValueError(f"API вернул пустой/неуспешный справочник {name}")
            temporary = path.with_suffix(".json.tmp")
            temporary.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
            temporary.replace(path)


def month(value: str) -> str:
    try:
        parsed = datetime.strptime(value, "%Y-%m")
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Ожидается YYYY-MM") from exc
    if parsed.strftime("%Y-%m") != value:
        raise argparse.ArgumentTypeError("Ожидается YYYY-MM")
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from", dest="start", type=month, default="2015-01")
    parser.add_argument("--to", dest="end", type=month, default="2026-08")
    parser.add_argument("--regions", help="Коды регионов через запятую; по умолчанию все")
    parser.add_argument("--data-dir", type=Path, default=ROOT / "data")
    parser.add_argument("--concurrency", type=int, choices=range(1, 9), default=4)
    parser.add_argument("--overwrite", action="store_true", help="Заново скачать выбранный период")
    parser.add_argument("--refresh-dicts", action="store_true", help="Обновить даже уже скачанные справочники")
    parser.add_argument("--base-url", help="Адрес API; по умолчанию настройка HOTSPOT_BASE_URL или адрес проекта")
    parser.add_argument("--attempts", type=int, default=4, help="Число попыток одного запроса (по умолчанию 4)")
    parser.add_argument("--timeout", type=float, default=20, help="Таймаут запроса в секундах")
    parser.add_argument("--check", action="store_true", help="Только проверить доступность API, без выгрузки")
    args = parser.parse_args()
    if args.start > args.end:
        parser.error("Начало периода позже конца")
    if args.attempts < 1 or args.timeout <= 0:
        parser.error("--attempts и --timeout должны быть положительными")

    from hotspot_retriever.client import ApiError, GibddClient
    from hotspot_retriever.config import Settings
    from hotspot_retriever.fetch import fetch_cards, load_regions
    from hotspot_retriever.models import DictionaryResponse
    from hotspot_retriever.normalize import normalize
    from hotspot_retriever.storage import RawStore
    from pydantic import ValidationError

    overrides = {"base_url": args.base_url} if args.base_url else {}
    settings = Settings(data_dir=args.data_dir.resolve(), concurrency=args.concurrency,
                        max_attempts=args.attempts, timeout_s=args.timeout, **overrides)
    print("Python:", sys.executable)
    print("API:", settings.base_url, flush=True)

    async def download() -> None:
        if args.check:
            async with GibddClient(settings) as client:
                payload, _, _ = await client.get_json("/dictionary/rows", {"code": "1"})
            parsed = DictionaryResponse.model_validate(payload)
            if parsed.status != 200 or not any(b.dict_code == 1 and b.dict_rows for b in parsed.results):
                raise ValueError("API не вернул непустой справочник регионов")
            print("API доступен: справочник регионов получен. Доступность карточек проверяется отдельно.")
            return
        await prepare_dictionaries(settings, refresh=args.refresh_dicts)
        available = load_regions(settings)
        codes = list(dict.fromkeys(c.strip() for c in args.regions.split(",") if c.strip())) if args.regions else available
        if not codes or set(codes) - set(available):
            parser.error("Указаны неизвестные или пустые коды регионов")

        def progress(record, done, total):
            print(f"[{done}/{total}] {record.year}-{record.month:02d} {record.reg_code}: {record.status}, {record.cards} ДТП")

        await fetch_cards(settings, codes, args.start, args.end, overwrite=args.overwrite,
                          refresh_recent=0, on_done=progress)

    try:
        asyncio.run(download())
    except (ApiError, ValidationError, ValueError) as exc:
        raise SystemExit(
            f"Не удалось получить данные API: {exc}\n"
            f"Адрес: {settings.base_url}\n"
            "Сервер может быть недоступен или ограничивать доступ с вашей сети. "
            "Повторите проверку с --check --attempts 1; проверьте доступ из другой сети.\n"
            "Скачанные файлы сохранены. Если справочников ещё нет, продолжить выгрузку без API нельзя."
        ) from None
    if args.check:
        return
    summary, _ = RawStore(settings).coverage()
    if summary.is_empty():
        raise SystemExit("Журнал выгрузки пуст; clean не пересобран.")
    bad = summary.filter(~summary["status"].is_in(["ok", "empty"]))
    if bad.height:
        raise SystemExit("Есть неполные/ошибочные выгрузки. Повторите команду для докачки; clean не пересобран.")
    totals = normalize(settings, on_year=lambda year, counts: print(year, counts))
    print("Итого:", totals)
    if totals["invalid_files"] or not totals["accidents"]:
        raise SystemExit("Не удалось построить полный непустой датасет; проверьте raw и журнал.")
    print("Данные:", settings.data_dir)


if __name__ == "__main__":
    main()
