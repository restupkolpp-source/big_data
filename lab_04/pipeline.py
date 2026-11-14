"""Обучение, Feature Store и мониторинг модели тяжести ДТП."""

from datetime import timedelta
import json
import os
from pathlib import Path
import time

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from scipy.stats import ks_2samp
from sklearn.metrics import roc_auc_score, average_precision_score, f1_score, accuracy_score

ROOT = Path(__file__).resolve().parents[1]
FEATURES = ['hour', 'weekday', 'month', 'latitude', 'longitude', 'vehicles',
            'night', 'snow', 'rain', 'kuzbass', 'petersburg', 'moscow']


def prepare(work):
    data = Path(os.environ.get('DTP_DATA_DIR', str(ROOT / 'data')))
    fields = ['accident_id', 'dt', 'reg_code', 'lat', 'lon', 'vehicles_n', 'lighting', 'weather', 'dead_n']
    raw = pq.ParquetFile(data / 'clean/accidents/year=2025/part.parquet').read(columns=fields).to_pandas()
    raw = raw[raw.reg_code.isin([1132, 1140, 1145])].drop_duplicates('accident_id').sort_values('dt').copy()
    raw = raw[raw.dt.notna() & raw.dead_n.notna()].reset_index(drop=True)
    frame = pd.DataFrame(index=raw.index)
    # Границы заданы до чтения выборки; они же используются приватной моделью.
    frame['hour'] = raw.dt.dt.hour / 23
    frame['weekday'] = raw.dt.dt.dayofweek / 6
    frame['month'] = (raw.dt.dt.month - 1) / 11
    frame['latitude'] = (raw.lat.fillna(55).clip(40, 80) - 40) / 40
    frame['longitude'] = (raw.lon.fillna(60).clip(20, 180) - 20) / 160
    frame['vehicles'] = raw.vehicles_n.fillna(1).clip(0, 10) / 10
    frame['night'] = raw.lighting.fillna('').str.contains('темное', case=False).astype(float)
    weather = raw.weather.map(lambda values: ' '.join(values) if values is not None else '')
    frame['snow'] = weather.str.contains('снег|метель', case=False).astype(float)
    frame['rain'] = weather.str.contains('дожд', case=False).astype(float)
    for name, code in [('kuzbass', 1132), ('petersburg', 1140), ('moscow', 1145)]:
        frame[name] = raw.reg_code.eq(code).astype(float)
    frame['accident_id'] = raw.accident_id.astype(str)
    frame['event_timestamp'] = pd.to_datetime(raw.dt, utc=True)
    frame['target'] = raw.dead_n.gt(0).astype(int)
    frame.to_parquet(Path(work) / 'features.parquet', index=False)
    masks = [frame.event_timestamp.dt.month.le(8), frame.event_timestamp.dt.month.isin([9, 10]),
             frame.event_timestamp.dt.month.ge(11)]
    return raw, frame, [frame.loc[mask].reset_index(drop=True) for mask in masks]


def feature_store(work, frame):
    from feast import FeatureStore, Entity, FeatureView, Field, FileSource, ValueType
    from feast.types import Float64
    work = Path(work).resolve()
    repo = work / 'feast'
    repo.mkdir(exist_ok=True)
    config = {'project': 'dtp_lab4', 'provider': 'local', 'registry': str(repo / 'registry.db'),
              'offline_store': {'type': 'file'}, 'online_store': {'type': 'sqlite', 'path': str(repo / 'online.db')},
              'entity_key_serialization_version': 3}
    import yaml
    (repo / 'feature_store.yaml').write_text(yaml.safe_dump(config), encoding='utf-8')
    store = FeatureStore(repo_path=str(repo))
    entity = Entity(name='accident', join_keys=['accident_id'], value_type=ValueType.STRING)
    view = FeatureView(name='conditions', entities=[entity], ttl=timedelta(days=730),
                       schema=[Field(name=name, dtype=Float64) for name in FEATURES], online=True,
                       source=FileSource(path=str(work / 'features.parquet'), timestamp_field='event_timestamp'))
    store.apply([entity, view])
    refs = ['conditions:' + name for name in FEATURES]
    query = frame[['accident_id', 'event_timestamp']].copy()
    historical = store.get_historical_features(entity_df=query, features=refs).to_df()
    historical = historical.set_index('accident_id').loc[frame.accident_id]
    assert np.allclose(historical[FEATURES].to_numpy(), frame[FEATURES].to_numpy())
    store.materialize(frame.event_timestamp.min().to_pydatetime() - timedelta(days=1),
                      frame.event_timestamp.max().to_pydatetime() + timedelta(days=1))
    sample = frame.sample(min(100, len(frame)), random_state=42)
    rows = sample[['accident_id']].to_dict('records')
    online = pd.DataFrame(store.get_online_features(features=refs, entity_rows=rows).to_dict())
    online = online.set_index('accident_id').loc[sample.accident_id]
    assert np.allclose(online[FEATURES].to_numpy(dtype=float), sample[FEATURES].to_numpy())
    # Запрос до появления карточки не должен получить её будущие признаки.
    early = frame.head(1)[['accident_id', 'event_timestamp']].copy()
    early['event_timestamp'] -= pd.Timedelta(days=1)
    before = store.get_historical_features(entity_df=early, features=refs).to_df()
    assert before[FEATURES].isna().all().all()
    return store, refs, {'historical_rows': len(frame), 'online_rows': len(sample),
                         'feature_match': True, 'future_leakage_test': True}


def scores(model, frame):
    y = frame.target.to_numpy()
    probability = model.predict_proba(frame[FEATURES])[:, 1]
    return {'auc': float(roc_auc_score(y, probability)), 'pr_auc': float(average_precision_score(y, probability)),
            'f1': float(f1_score(y, probability >= .5, zero_division=0)),
            'accuracy': float(accuracy_score(y, probability >= .5))}


def fit_models(work, train, valid, test):
    from sklearn.dummy import DummyClassifier
    from sklearn.linear_model import LogisticRegression
    from sklearn.ensemble import RandomForestClassifier, HistGradientBoostingClassifier
    from sklearn.naive_bayes import GaussianNB
    from diffprivlib.models import GaussianNB as PrivateNB
    from diffprivlib.accountant import BudgetAccountant
    import mlflow
    import mlflow.sklearn
    from mlflow.models import infer_signature

    uri = 'sqlite:///' + (Path(work).resolve() / 'mlflow.db').as_posix()
    mlflow.set_tracking_uri(uri)
    if mlflow.get_experiment_by_name('dtp-severity') is None:
        mlflow.create_experiment('dtp-severity', artifact_location=(Path(work).resolve() / 'artifacts').as_uri())
    mlflow.set_experiment('dtp-severity')
    models = {'dummy': DummyClassifier(strategy='prior'),
              'logistic': LogisticRegression(max_iter=500, random_state=42),
              'forest': RandomForestClassifier(n_estimators=100, max_depth=8, min_samples_leaf=10, n_jobs=1, random_state=42),
              'boosting': HistGradientBoostingClassifier(max_iter=100, max_leaf_nodes=15, random_state=42),
              'naive_bayes': GaussianNB()}
    # Две приватные публикации, общий бюджет epsilon=3, delta=0 на одну карточку.
    accountant = BudgetAccountant(epsilon=3, delta=0)
    models['private_eps1'] = PrivateNB(epsilon=1, bounds=(np.zeros(len(FEATURES)), np.ones(len(FEATURES))), accountant=accountant)
    models['private_eps2'] = PrivateNB(epsilon=2, bounds=(np.zeros(len(FEATURES)), np.ones(len(FEATURES))), accountant=accountant)
    result, uris = [], {}
    for name, model in models.items():
        with mlflow.start_run(run_name=name):
            start = time.perf_counter()
            if name.startswith('private'):
                model.partial_fit(train[FEATURES], train.target, classes=np.array([0, 1]))
            else:
                model.fit(train[FEATURES], train.target)
            seconds = time.perf_counter() - start
            model.fit_seconds_ = seconds
            metrics = scores(model, valid)
            mlflow.log_params({'model': name, 'train_rows': len(train), 'split': 'time: Jan-Aug / Sep-Oct / Nov-Dec'})
            if name.startswith('private'):
                mlflow.log_params({'epsilon': model.epsilon, 'delta': 0, 'adjacency': 'one accident card', 'bounds': '[0,1]'})
            mlflow.log_metrics({**metrics, 'fit_seconds': seconds})
            example = pd.DataFrame(np.zeros((2, len(FEATURES))), columns=FEATURES)
            info = mlflow.sklearn.log_model(model, name='model', serialization_format='cloudpickle', signature=infer_signature(example, model.predict(example)),
                                           input_example=example)
            uris[name] = info.model_uri
            result.append({'model': name, **metrics, 'fit_seconds': seconds})
    table = pd.DataFrame(result).sort_values('pr_auc', ascending=False)
    # Выбор только по validation; test до этого не участвует ни в одном решении.
    winner = table.iloc[0].model
    best = models[winner]
    test_metrics = scores(best, test)
    version = mlflow.register_model(uris[winner], 'DTPSeverity')
    client = mlflow.MlflowClient()
    client.set_registered_model_alias('DTPSeverity', 'candidate', version.version)
    latency = model_latency(best, test[FEATURES].head(1))
    gate = test_metrics['pr_auc'] > float(test.target.mean()) and latency['p95_ms'] < 50
    if not gate:
        raise ValueError(f'Candidate failed validation: {test_metrics}, {latency}')
    client.set_registered_model_alias('DTPSeverity', 'production', version.version)
    challenger_name = 'boosting' if winner != 'boosting' else 'forest'
    challenger = mlflow.register_model(uris[challenger_name], 'DTPSeverity')
    client.set_registered_model_alias('DTPSeverity', 'candidate', challenger.version)
    loaded = mlflow.sklearn.load_model('models:/DTPSeverity@production')
    assert np.allclose(best.predict_proba(test[FEATURES].head(5)), loaded.predict_proba(test[FEATURES].head(5)))
    metadata = {'winner': winner, 'version': version.version, 'tracking_uri': uri, 'test': test_metrics,
                'latency': latency, 'privacy_budget': list(accountant.total()), 'gate': True,
                'candidate_name': challenger_name, 'candidate_version': challenger.version}
    (Path(work) / 'model.json').write_text(json.dumps(metadata, indent=2), encoding='utf-8')
    table.to_csv(Path(work) / 'model_comparison.csv', index=False)
    return models, loaded, table, metadata


def model_latency(model, sample, n=100):
    for _ in range(5):
        model.predict_proba(sample)
    times = []
    for _ in range(n):
        start = time.perf_counter()
        model.predict_proba(sample)
        times.append((time.perf_counter() - start) * 1000)
    return dict(zip(['p50_ms', 'p95_ms', 'p99_ms'], np.percentile(times, [50, 95, 99]).tolist()))


def drift(reference, current):
    result = []
    for column in FEATURES:
        a, b = reference[column], current[column]
        bins = np.linspace(0, 1, 11)
        p = np.histogram(a, bins=bins)[0] / max(len(a), 1)
        q = np.histogram(b, bins=bins)[0] / max(len(b), 1)
        p, q = np.maximum(p, 1e-6), np.maximum(q, 1e-6)
        psi = float(np.sum((p - q) * np.log(p / q)))
        result.append({'feature': column, 'psi': psi, 'ks': float(ks_2samp(a, b).statistic),
                       'missing': float(b.isna().mean()), 'out_of_range': int(((b < 0) | (b > 1)).sum())})
    return pd.DataFrame(result)


def compare_online(work, train, valid, test, batch_model):
    from river import linear_model, optim
    from river.drift import ADWIN
    import mlflow
    from sklearn.base import clone
    history = train.copy()
    daily_model = clone(batch_model)
    batch_fit_seconds = 0.
    batch_updates = 0
    model = linear_model.LogisticRegression(optimizer=optim.SGD(.03))
    detector = ADWIN()
    start = time.perf_counter()
    for row, target in zip(train[FEATURES].to_dict('records'), train.target):
        model.learn_one(row, bool(target))
    online_fit = time.perf_counter() - start
    records, triggers = [], []
    online_update_seconds = 0.
    after24 = None
    # События идут по времени. В каждом дне сначала прогнозы, затем поступают метки.
    for day, part in valid.groupby(valid.event_timestamp.dt.floor('D')):
        start_batch = time.perf_counter()
        daily_model.fit(history[FEATURES], history.target)
        batch_fit_seconds += time.perf_counter() - start_batch
        batch_updates += 1
        start_online = time.perf_counter()
        rows = part[FEATURES].to_dict('records')
        probabilities = [model.predict_proba_one(x).get(True, 0) for x in rows]
        batch_p = daily_model.predict_proba(part[FEATURES])[:, 1]
        records.append({'day': day, 'online_accuracy': accuracy_score(part.target, np.array(probabilities) >= .5),
                        'batch_accuracy': accuracy_score(part.target, batch_p >= .5)})
        for row, probability, target in zip(rows, probabilities, part.target):
            detector.update(abs(target - probability))
            if detector.drift_detected:
                triggers.append(str(day))
            model.learn_one(row, bool(target))
        history = pd.concat([history, part], ignore_index=True)
        if after24 is None:
            next_day = valid[(valid.event_timestamp.dt.floor('D') == day + pd.Timedelta(days=1))]
            if len(next_day):
                next_p = np.array([model.predict_proba_one(x).get(True, 0) for x in next_day[FEATURES].to_dict('records')])
                after24 = {'day': str(day + pd.Timedelta(days=1)), 'online_pr_auc': float(average_precision_score(next_day.target, next_p)),
                           'frozen_batch_pr_auc': scores(batch_model, next_day)['pr_auc'], 'evaluated_before_labels': True}
        online_update_seconds += time.perf_counter() - start_online
    start_batch = time.perf_counter()
    daily_model.fit(history[FEATURES], history.target)
    batch_fit_seconds += time.perf_counter() - start_batch
    batch_updates += 1
    y = test.target.to_numpy()
    start = time.perf_counter()
    probability = np.array([model.predict_proba_one(x).get(True, 0) for x in test[FEATURES].to_dict('records')])
    elapsed = time.perf_counter() - start
    result = pd.DataFrame([{'strategy': 'batch daily through October', **scores(daily_model, test),
                            'fit_seconds': batch_fit_seconds, 'updates': batch_updates,
                            'inference_ms_per_row': model_latency(daily_model, test[FEATURES].head(1))['p50_ms']},
                           {'strategy': 'online through October', 'auc': roc_auc_score(y, probability),
                            'pr_auc': average_precision_score(y, probability), 'f1': f1_score(y, probability >= .5, zero_division=0),
                            'accuracy': accuracy_score(y, probability >= .5),
                            'fit_seconds': online_fit + online_update_seconds,
                            'updates': len(train) + len(valid), 'inference_ms_per_row': elapsed * 1000 / len(test)},
                           {'strategy': 'batch frozen after August', **scores(batch_model, test),
                            'fit_seconds': getattr(batch_model, 'fit_seconds_', np.nan), 'updates': 1,
                            'inference_ms_per_row': model_latency(batch_model, test[FEATURES].head(1))['p50_ms']}])
    with mlflow.start_run(run_name='batch-vs-online'):
        mlflow.log_metrics({'online_pr_auc': result.iloc[1].pr_auc, 'batch_pr_auc': result.iloc[0].pr_auc,
                            'online_fit_seconds': online_fit, 'online_ms_per_row': elapsed * 1000 / len(test),
                            'online_updates': len(train) + len(valid), 'adwin_triggers': len(triggers)})
        mlflow.log_dict({'daily_accuracy': pd.DataFrame(records).astype(str).to_dict('records'), 'adwin': triggers,
                         'after_24_hours': after24}, 'online.json')
        mlflow.sklearn.log_model(daily_model, name='daily-batch-model', serialization_format='cloudpickle')
        class RiverProbability(mlflow.pyfunc.PythonModel):
            def __init__(self, estimator):
                self.estimator = estimator
            def predict(self, context, model_input, params=None):
                return np.array([self.estimator.predict_proba_one(row).get(True, 0)
                                 for row in model_input[FEATURES].to_dict('records')])
        example = pd.DataFrame(np.zeros((2, len(FEATURES))), columns=FEATURES)
        mlflow.pyfunc.log_model(name='online-model', python_model=RiverProbability(model), input_example=example)
    (Path(work) / 'after24h.json').write_text(json.dumps(after24, indent=2), encoding='utf-8')
    return result, pd.DataFrame(records), model
