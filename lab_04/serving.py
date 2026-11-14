"""REST-инференс с Feast, shadow и canary 5%."""

import hashlib
import json
from pathlib import Path
import threading
import time

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import numpy as np
import pandas as pd
import uvicorn
import httpx

from lab_04.pipeline import FEATURES, scores, model_latency


class Request(BaseModel):
    accident_id: str = Field(min_length=1, max_length=100)
    request_id: str = Field(min_length=1, max_length=100)


class Deployment:
    def __init__(self, model, version):
        self.stable, self.version = model, str(version)
        self.candidate, self.candidate_version = None, None
        self.events, self.shadow = [], []

    def predict(self, x, key):
        main = float(self.stable.predict_proba(x)[0, 1])
        if self.candidate is None:
            return main, self.version, False
        other = float(self.candidate.predict_proba(x)[0, 1])
        self.shadow.append({'stable': main, 'candidate': other})
        canary = int(hashlib.sha256(key.encode()).hexdigest()[:8], 16) % 100 < 5
        return (other, self.candidate_version, True) if canary else (main, self.version, False)

    def start(self, model, version):
        self.candidate, self.candidate_version = model, str(version)
        self.events.append({'event': 'canary_started', 'version': str(version), 'percent': 5})

    def finish(self, labeled):
        current = scores(self.stable, labeled)['pr_auc']
        candidate = scores(self.candidate, labeled)['pr_auc']
        latency = model_latency(self.candidate, labeled[FEATURES].head(1))['p95_ms']
        passed = candidate >= current - .01 and latency < 50
        if passed:
            self.stable, self.version = self.candidate, self.candidate_version
        self.events.append({'event': 'promoted' if passed else 'rolled_back', 'version': self.candidate_version,
                            'pr_auc': candidate, 'baseline_pr_auc': current, 'p95_ms': latency})
        self.candidate, self.candidate_version = None, None
        return passed


def create_app(store, refs, deployment):
    app = FastAPI(title='DTP severity')
    counters = {'requests': 0, 'errors': 0}
    store_lock = threading.Lock()

    @app.middleware('http')
    async def count(request, call_next):
        response = await call_next(request)
        if request.url.path == '/predict':
            counters['requests'] += 1
            counters['errors'] += int(response.status_code >= 400)
        return response

    @app.get('/health')
    def health():
        return {'status': 'ok', 'version': deployment.version}

    @app.get('/metrics')
    def metrics():
        return {**counters, 'shadow_predictions': len(deployment.shadow), 'deployments': deployment.events}

    @app.post('/predict')
    def predict(request: Request):
        # Локальный SQLite backend Feast использует общий объект подключения.
        with store_lock:
            values = store.get_online_features(features=refs, entity_rows=[{'accident_id': request.accident_id}]).to_dict()
        if any(values.get(name, [None])[0] is None for name in FEATURES):
            raise HTTPException(status_code=404, detail='Карточка не найдена в Feature Store')
        frame = pd.DataFrame({name: values[name] for name in FEATURES})
        if not np.isfinite(frame.to_numpy()).all() or ((frame < 0) | (frame > 1)).any().any():
            raise HTTPException(status_code=503, detail='Некорректные признаки')
        try:
            probability, version, canary = deployment.predict(frame, request.request_id)
        except Exception as exc:
            raise HTTPException(status_code=503, detail='Модель недоступна') from exc
        return {'probability_fatal': probability, 'model_version': version, 'canary': canary}

    return app


def test_service(work, store, refs, model, version, candidate, valid, test, candidate_version='candidate'):
    from concurrent.futures import ThreadPoolExecutor
    from sklearn.dummy import DummyClassifier
    import socket
    deployment = Deployment(model, version)
    app = create_app(store, refs, deployment)
    sock = socket.socket()
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, log_level='error', lifespan='off'))
    thread = threading.Thread(target=lambda: server.run(sockets=[sock]), daemon=True)
    thread.start()
    while not server.started:
        if not thread.is_alive():
            raise RuntimeError('HTTP-сервис не запустился')
        time.sleep(.05)
    with httpx.Client(base_url=f'http://127.0.0.1:{port}', timeout=30, trust_env=False) as client:
        try:
            entity = str(test.iloc[0].accident_id)
            for i in range(5):
                assert client.post('/predict', json={'accident_id': entity, 'request_id': f'warm-{i}'}).status_code == 200
            deployment.start(candidate, candidate_version)
            def request(i):
                start = time.perf_counter()
                result = client.post('/predict', json={'accident_id': entity, 'request_id': str(i)})
                result.raise_for_status()
                return (time.perf_counter() - start) * 1000, result.json()['canary']
            start = time.perf_counter()
            with ThreadPoolExecutor(max_workers=4) as pool:
                responses = list(pool.map(request, range(200)))
            duration = time.perf_counter() - start
            latency = np.array([x[0] for x in responses])
            routed = sum(x[1] for x in responses)
            assert 0 < routed < 40 and len(deployment.shadow) == 200
            promoted = deployment.finish(valid)
            stable = deployment.version
            bad = DummyClassifier(strategy='prior').fit(valid[FEATURES], valid.target)
            deployment.start(bad, 'bad-candidate')
            assert not deployment.finish(valid) and deployment.version == stable
            assert client.post('/predict', json={'accident_id': 'unknown', 'request_id': 'x'}).status_code == 404
            assert client.post('/predict', json={'accident_id': '', 'request_id': 'x'}).status_code == 422
            assert client.post('/predict', json={}).status_code == 422
            boundaries = pd.DataFrame([np.zeros(len(FEATURES)), np.ones(len(FEATURES))], columns=FEATURES)
            assert np.isfinite(deployment.stable.predict_proba(boundaries)).all()
            good_model = deployment.stable
            class BrokenModel:
                def predict_proba(self, frame):
                    raise RuntimeError('Injected model failure')
            deployment.stable = BrokenModel()
            try:
                assert client.post('/predict', json={'accident_id': entity, 'request_id': 'fault'}).status_code == 503
            finally:
                deployment.stable = good_model
            assert client.get('/health').json()['version'] == stable
            report = {'p50_ms': float(np.percentile(latency, 50)), 'p95_ms': float(np.percentile(latency, 95)),
                      'p99_ms': float(np.percentile(latency, 99)), 'throughput_rps': 200 / duration,
                      'canary_requests': routed, 'shadow_requests': len(deployment.shadow),
                      'candidate_promoted': promoted, 'rollback_checked': True,
                      'expected_error_cases': 4, 'boundaries_checked': True, 'load_errors': 0, 'load_error_rate': 0,
                      'load_errors_per_second': 0, 'metrics': client.get('/metrics').json()}
            (Path(work) / 'http_metrics.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
            return report
        finally:
            server.should_exit = True
            thread.join(timeout=10)
            sock.close()


def main():
    import argparse
    import mlflow
    import mlflow.sklearn
    from feast import FeatureStore
    from lab_04.pipeline import ROOT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, default=ROOT / '_work/lab04')
    parser.add_argument('--port', type=int, default=8044)
    args = parser.parse_args()
    work = args.work.resolve()
    mlflow.set_tracking_uri('sqlite:///' + (work / 'mlflow.db').as_posix())
    version = mlflow.MlflowClient().get_model_version_by_alias('DTPSeverity', 'production').version
    model = mlflow.sklearn.load_model('models:/DTPSeverity@production')
    store = FeatureStore(repo_path=str(work / 'feast'))
    app = create_app(store, ['conditions:' + name for name in FEATURES], Deployment(model, version))
    uvicorn.run(app, host='127.0.0.1', port=args.port)


if __name__ == '__main__':
    main()
