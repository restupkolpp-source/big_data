"""Прямой и рекурсивный прогноз NAB и проверка автоотключения."""

import json
from pathlib import Path
import urllib.request

import numpy as np
import pandas as pd
from lightgbm import LGBMRegressor
from scipy.special import ndtr
from sklearn.metrics import mean_absolute_error, mean_pinball_loss

URL = 'https://raw.githubusercontent.com/numenta/NAB/master/data/realKnownCause/ambient_temperature_system_failure.csv'
H = 24
LAGS = [1, 2, 3, 6, 12, 24, 48]


def load_sensor(data_dir):
    path = Path(data_dir) / 'lab04/ambient_temperature_system_failure.csv'
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        urllib.request.urlretrieve(URL, path)
    raw = pd.read_csv(path, parse_dates=['timestamp']).set_index('timestamp').value.sort_index()
    grid = raw.resample('h').mean()
    # Многодневные разрывы не заменяю выдуманной температурой.
    return raw, grid, int(grid.isna().sum())


def design(values):
    x = pd.DataFrame({f'lag{lag}': values.shift(lag) for lag in LAGS})
    x['hour_sin'] = np.sin(2 * np.pi * x.index.hour / 24)
    x['hour_cos'] = np.cos(2 * np.pi * x.index.hour / 24)
    x['weekday'] = x.index.dayofweek
    return x


def make_model(alpha=.5):
    return LGBMRegressor(objective='quantile', alpha=alpha, n_estimators=70, max_depth=4,
                         num_leaves=12, learning_rate=.05, verbosity=-1, n_jobs=2, random_state=42)


def train_forecasters(values):
    x = design(values)
    models = {}
    for horizon in range(1, H + 1):
        target = values.shift(-(horizon - 1))
        # Для каждой цели проверяю время самой метки, а не только origin.
        available = x.notna().all(axis=1) & target.notna() & (x.index + pd.Timedelta(hours=horizon - 1) < '2013-12-01')
        for alpha in [.5, .9]:
            models[horizon, alpha] = make_model(alpha).fit(x.loc[available], target.loc[available])
    return models


def forecasts(values, models, origins):
    x = design(values)
    direct = {alpha: np.column_stack([models[h, alpha].predict(x.loc[origins]) for h in range(1, H + 1)])
              for alpha in [.5, .9]}
    # Векторизую origins: каждой строке соответствует отдельная доступная на тот момент история.
    positions = values.index.get_indexer(origins)
    history = np.stack([values.iloc[p - 48:p].to_numpy() for p in positions])
    recursive = {alpha: [] for alpha in [.5, .9]}
    for h in range(H):
        dates = origins + pd.Timedelta(hours=h)
        features = pd.DataFrame({f'lag{lag}': history[:, -lag] for lag in LAGS})
        features['hour_sin'] = np.sin(2 * np.pi * dates.hour / 24)
        features['hour_cos'] = np.cos(2 * np.pi * dates.hour / 24)
        features['weekday'] = dates.dayofweek
        point = models[1, .5].predict(features)
        one_step_q = models[1, .9].predict(features)
        upper = point + np.maximum(one_step_q - point, .05) * np.sqrt(h + 1)
        recursive[.5].append(point)
        recursive[.9].append(upper)
        history = np.column_stack([history, point])
    recursive = {alpha: np.column_stack(rows) for alpha, rows in recursive.items()}
    truth = np.stack([values.iloc[p:p + H].to_numpy() for p in positions])
    # Квантили могут пересекаться; фиксирую порядок до оценки.
    for result in [direct, recursive]:
        result[.9] = np.maximum(result[.9], result[.5] + .05)
    return {'direct': direct, 'recursive': recursive}, truth


def alarms(origins, truth, forecasts, critical, p_star, k, cooldown, cusum_limit):
    point, upper = forecasts[.5], forecasts[.9]
    sigma = np.maximum((upper - point) / 1.2815515655, .05)
    probability = (1 - ndtr((critical - point) / sigma)).max(axis=1)
    residuals = truth[:, 0] - point[:, 0]
    cumulative, persistence = 0., 0
    next_allowed = pd.Timestamp.min
    events = []
    actions = ['уведомить', 'сбросить нагрузку', 'контролируемое отключение']
    for i, now in enumerate(origins):
        if i and now - origins[i - 1] > pd.Timedelta(hours=1):
            cumulative, persistence = 0., 0
        cumulative = max(0., cumulative + residuals[i] - .25)
        confirmed = cumulative > cusum_limit
        condition = probability[i] > p_star and confirmed
        persistence = persistence + 1 if condition else 0
        if condition and persistence >= k and now >= next_allowed:
            # Это журнал управляющих команд, оборудование не подключено.
            recent = [e for e in events if now - pd.Timestamp(e['timestamp']) <= pd.Timedelta(hours=48)]
            action = actions[min(len(recent), 2)]
            events.append({'timestamp': str(now), 'action': action, 'prob_over': float(probability[i]),
                           'cusum': float(cumulative), 'persistence': persistence})
            next_allowed = now + pd.Timedelta(hours=cooldown)
            cumulative, persistence = 0., 0
    return events


def event_metrics(log, origins, events, tolerance=24):
    matched, true_alarms, false_alarms, lead = set(), 0, 0, []
    for row in log:
        timestamp = pd.Timestamp(row['timestamp'])
        hits = [i for i, (start, end) in enumerate(events)
                if start - pd.Timedelta(hours=tolerance) <= timestamp <= end + pd.Timedelta(hours=tolerance)]
        if hits:
            true_alarms += 1
            matched.update(hits)
        else:
            false_alarms += 1
    for i, (start, end) in enumerate(events):
        times = [pd.Timestamp(r['timestamp']) for r in log
                 if start - pd.Timedelta(hours=tolerance) <= pd.Timestamp(r['timestamp']) <= end + pd.Timedelta(hours=tolerance)]
        lead.append((start - min(times)).total_seconds() / 3600 if times else None)
    days = max(len(origins) / 24, 1 / 24)
    missed = len(events) - len(matched)
    return {'precision': true_alarms / max(len(log), 1), 'recall': len(matched) / max(len(events), 1),
            'false_per_day': false_alarms / days, 'false_alarms': false_alarms, 'missed_failures': missed,
            'lead_hours': lead, 'alarms': len(log)}


def experiment(work, data_dir):
    raw, values, missing = load_sensor(data_dir)
    models = train_forecasters(values)
    history_ok = values.notna().rolling(48).sum().shift(1).eq(48)
    future_ok = values.notna().rolling(H).sum().shift(-(H - 1)).eq(H)
    origins = values.index[(values.index >= '2013-12-01') & history_ok & future_ok]
    forecasts_by_strategy, truth = forecasts(values, models, origins)
    tune_mask = origins < '2014-03-01'
    tune_origins = origins[tune_mask]
    first_event = [(pd.Timestamp('2013-12-15'), pd.Timestamp('2013-12-30 23:00'))]
    scale = values.loc[:'2013-11-30'].diff(24).abs().mean()
    settings = []
    # Цена пропуска=100, ложного отключения=10, сброса нагрузки=3, уведомления=1.
    costs = {'уведомить': 1, 'сбросить нагрузку': 3, 'контролируемое отключение': 10}
    criticals = [float(values.loc[:'2013-11-30'].quantile(q)) for q in [.95, .99]]
    for strategy, forecast in forecasts_by_strategy.items():
        tune = {q: array[tune_mask] for q, array in forecast.items()}
        for critical in criticals:
            for p_star in [.5, .8]:
                for k in [2, 4]:
                    for cooldown in [12, 24]:
                        for limit in [2., 5.]:
                            log = alarms(tune_origins, truth[tune_mask], tune, critical, p_star, k, cooldown, limit)
                            metrics = event_metrics(log, tune_origins, first_event)
                            false_cost = sum(costs[e['action']] for e in log if not (
                                first_event[0][0] - pd.Timedelta(hours=24) <= pd.Timestamp(e['timestamp']) <=
                                first_event[0][1] + pd.Timedelta(hours=24)))
                            cost = 100 * metrics['missed_failures'] + false_cost + .01 * len(log)
                            settings.append({'strategy': strategy, 'critical': critical, 'p_star': p_star, 'k': k,
                                             'cooldown': cooldown, 'cusum_limit': limit, 'cost': cost})
    tuning = pd.DataFrame(settings).sort_values(['cost', 'strategy', 'k'])
    selected = tuning.iloc[0].to_dict()
    (Path(work) / 'sensor_thresholds.json').write_text(json.dumps(selected, indent=2), encoding='utf-8')
    # Только после фиксации настроек открываю метки второй аварии.
    final_mask = ~tune_mask
    final_origins = origins[final_mask]
    hidden_event = [(pd.Timestamp('2014-03-29'), pd.Timestamp('2014-04-20 23:00'))]
    forecast_metrics, warning_metrics, logs = [], [], {}
    for strategy, forecast in forecasts_by_strategy.items():
        final = {q: array[final_mask] for q, array in forecast.items()}
        for h in range(H):
            mae = mean_absolute_error(truth[final_mask, h], final[.5][:, h])
            forecast_metrics.append({'strategy': strategy, 'horizon': h + 1, 'mae': mae, 'mase': mae / scale,
                                     'pinball_90': mean_pinball_loss(truth[final_mask, h], final[.9][:, h], alpha=.9)})
        params = {name: selected[name] for name in ['critical', 'p_star', 'k', 'cooldown', 'cusum_limit']}
        params['k'] = int(params['k'])
        log = alarms(final_origins, truth[final_mask], final, **params)
        logs[strategy] = log
        warning_metrics.append({'strategy': strategy, **event_metrics(log, final_origins, hidden_event)})
    (Path(work) / 'sensor_alarm_log.json').write_text(json.dumps(logs, ensure_ascii=False, indent=2), encoding='utf-8')
    return {'raw_rows': len(raw), 'hourly_rows': len(values), 'missing_hours': missing, 'values': values,
            'origins': origins, 'forecasts': forecasts_by_strategy, 'truth': truth, 'settings': selected,
            'forecast_metrics': pd.DataFrame(forecast_metrics), 'warning_metrics': pd.DataFrame(warning_metrics),
            'logs': logs, 'hidden_event': hidden_event, 'tuning': tuning}
