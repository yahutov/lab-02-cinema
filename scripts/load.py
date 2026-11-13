"""python scripts/load.py --label timeout --rate 5 --seconds 30 > evidence/timeout.json"""
import argparse
import concurrent.futures
import json
import threading
import time
import urllib.request
import uuid

p = argparse.ArgumentParser()
p.add_argument('--label', default='run')
p.add_argument('--rate', type=float, default=5)
p.add_argument('--seconds', type=int, default=30)
args = p.parse_args()
results, health = [], []
stop = threading.Event()

def purchase(i):
    start = time.monotonic()
    body = json.dumps(dict(session_id='load', seat=str(i), customer_id='load')).encode()
    req = urllib.request.Request('http://localhost:8001/tickets', data=body,
        headers={'Content-Type': 'application/json', 'Idempotency-Key': str(uuid.uuid4())})
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            response.read()
            status = response.status
    except Exception:
        status = 0
    duration = time.monotonic() - start
    return dict(status=status, seconds=round(duration, 3), success=status == 201 and duration <= 5)

def monitor():
    while not stop.is_set():
        start = time.monotonic()
        try:
            with urllib.request.urlopen('http://localhost:8001/health', timeout=1) as response:
                ok = response.status == 200
        except Exception:
            ok = False
        health.append(dict(ok=ok, seconds=round(time.monotonic()-start, 3)))
        stop.wait(max(0, 1-(time.monotonic()-start)))

thread = threading.Thread(target=monitor)
thread.start()
start = time.monotonic()
with concurrent.futures.ThreadPoolExecutor(max_workers=200) as pool:
    futures = []
    for i in range(int(args.seconds * args.rate)):
        time.sleep(max(0, start + i / args.rate - time.monotonic()))
        futures.append(pool.submit(purchase, i))
    results = [f.result() for f in futures]
stop.set()
thread.join()
times = sorted(r['seconds'] for r in results)
print(json.dumps(dict(label=args.label, count=len(results),
    success_fraction=sum(r['success'] for r in results)/len(results),
    mean_seconds=sum(times)/len(times), p95_seconds=times[int((len(times)-1)*.95)],
    health=health, requests=results), indent=2))
