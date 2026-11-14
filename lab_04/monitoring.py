"""Отчёт о сдвиге данных, webhook и переобучение по доступным меткам."""

import json
from pathlib import Path
import urllib.request

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression

from lab_04.pipeline import FEATURES, drift, scores


def retrain_due(drift_score, accuracy_drop, days_since_training):
    return drift_score > .3 or accuracy_drop > .15 or days_since_training >= 7


def notify(url, event, work):
    with (Path(work) / 'alerts.jsonl').open('a', encoding='utf-8') as stream:
        stream.write(json.dumps(event, ensure_ascii=False) + '\n')
    if url:
        request = urllib.request.Request(url, data=json.dumps({'text': event['message']}).encode(),
                                         headers={'Content-Type': 'application/json'}, method='POST')
        with urllib.request.urlopen(request, timeout=10) as response:
            if response.status >= 300:
                raise ValueError('Webhook rejected alert')


def monitor(work, train, valid, test, model, webhook=None):
    from river.drift import ADWIN
    reference = train.sample(frac=.5, random_state=42)
    control = train.drop(reference.index)
    natural = drift(reference, control)
    shifted = control.copy()
    shifted[FEATURES[:6]] = 1.
    injected = drift(reference, shifted)
    real = drift(train, test)
    score = float(natural.psi.gt(.2).mean())
    injected_score = float(injected.psi.gt(.2).mean())
    assert score < .3 and injected_score > .3
    natural.to_html(Path(work) / 'drift_control.html', index=False)
    real.to_html(Path(work) / 'drift_real.html', index=False)
    injected.to_html(Path(work) / 'drift_injected.html', index=False)
    base_accuracy = scores(model, valid)['accuracy']
    corrupted = valid.copy()
    corrupted['target'] = 1 - corrupted.target
    accuracy_drop = base_accuracy - scores(model, corrupted)['accuracy']
    detector = ADWIN()
    detection = False
    for error in [0.] * 500 + [1.] * 500:
        detector.update(error)
        detection |= detector.drift_detected
    assert accuracy_drop > .15 and detection
    prediction_shift = drift(pd.DataFrame({'hour': model.predict_proba(train[FEATURES])[:, 1],
                                           **{f: train[f].to_numpy() for f in FEATURES[1:]}}),
                             pd.DataFrame({'hour': model.predict_proba(test[FEATURES])[:, 1],
                                           **{f: test[f].to_numpy() for f in FEATURES[1:]}})).iloc[0].to_dict()
    event = {'message': 'Проверка: обнаружен сдвиг признаков и падение качества',
             'data_drift_score': injected_score, 'accuracy_drop': accuracy_drop,
             'retrain': retrain_due(injected_score, accuracy_drop, 0)}
    assert event['retrain'] and not retrain_due(score, 0, 0) and retrain_due(score, 0, 7)
    notify(webhook, event, work)
    # Проверка триггера приводит к обучению, а не только к записи флага.
    cut = int(len(valid) * .75)
    retrain_data = pd.concat([train, valid.iloc[:cut]], ignore_index=True)
    holdout = valid.iloc[cut:]
    retrained = LogisticRegression(max_iter=500, random_state=42).fit(retrain_data[FEATURES], retrain_data.target)
    candidate_metrics = scores(retrained, holdout)
    baseline_metrics = scores(model, holdout)
    accepted = candidate_metrics['pr_auc'] >= baseline_metrics['pr_auc'] - .01
    with mlflow.start_run(run_name='monitoring-and-retrain'):
        mlflow.log_metrics({'control_drift': score, 'injected_drift': injected_score,
                            'accuracy_drop_injected': accuracy_drop,
                            'retrain_pr_auc': candidate_metrics['pr_auc'], 'retrain_gate_passed': int(accepted)})
        mlflow.log_dict({'alert': event, 'adwin_injected': detection, 'prediction_shift': prediction_shift,
                         'weekly_due_example': True, 'retrain_rows': len(retrain_data),
                         'holdout_rows': len(holdout)}, 'monitoring.json')
        mlflow.sklearn.log_model(retrained, name='retrained-model', serialization_format='cloudpickle')
    return {'control_score': score, 'injected_score': injected_score,
            'real_score': float(real.psi.gt(.2).mean()), 'accuracy_drop_injected': accuracy_drop,
            'adwin_checked': detection, 'retrain_rows': len(retrain_data), 'retrain_accepted': accepted,
            'prediction_shift': prediction_shift}, real
