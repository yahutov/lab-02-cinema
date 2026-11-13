"""Local teaching stand: intentionally insecure modes reproduce assignment failures."""
import json
import os
import sqlite3
import threading
import time
import uuid
import logging
import requests
import pika
from flask import Flask, request, jsonify

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
log = logging.getLogger('cinema')
app = Flask(__name__)
ROLE = os.getenv('ROLE', 'tickets')
DB = os.getenv('DB', '/data/app.db')
AMQP = os.getenv('AMQP_URL', 'amqp://cinema:cinema-local@rabbitmq:5672/%2F')
config = dict(mode='sync', idempotent=True, protection='breaker', delay=0., dedup=True)

def db():
    conn = sqlite3.connect(DB, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn

os.makedirs(os.path.dirname(os.path.abspath(DB)), exist_ok=True)
with db() as c:
    c.executescript('''
    PRAGMA journal_mode=WAL;
    CREATE TABLE IF NOT EXISTS tickets(id TEXT PRIMARY KEY, payload TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS keys(key TEXT PRIMARY KEY, payload TEXT NOT NULL, result TEXT);
    CREATE TABLE IF NOT EXISTS outbox(message_id TEXT PRIMARY KEY, body TEXT NOT NULL, sent INTEGER DEFAULT 0);
    CREATE TABLE IF NOT EXISTS accruals(ticket_id TEXT PRIMARY KEY, customer_id TEXT, points INTEGER);
    CREATE TABLE IF NOT EXISTS unsafe_accruals(id INTEGER PRIMARY KEY, ticket_id TEXT, customer_id TEXT, points INTEGER);
    ''')

class Breaker:
    def __init__(self, threshold=5, cooldown=10, clock=time.monotonic):
        self.lock = threading.Lock()
        self.threshold, self.cooldown, self.clock = threshold, cooldown, clock
        self.failures, self.open_until, self.probe = 0, 0., False
        self.generation = 0

    def enter(self):
        with self.lock:
            if self.open_until:
                if self.clock() < self.open_until or self.probe:
                    raise RuntimeError('circuit OPEN: no network request')
                self.probe = True
                log.info('breaker HALF_OPEN probe')
            return self.generation

    def finish(self, generation, success):
        with self.lock:
            if generation != self.generation:
                return  # Ignore results of calls started before the last opening.
            if success:
                if self.probe:
                    log.info('breaker CLOSED')
                self.failures, self.open_until, self.probe = 0, 0., False
            else:
                self.failures += 1
                if self.probe or self.failures >= self.threshold:
                    self.open_until = self.clock() + self.cooldown
                    self.probe = False
                    self.generation += 1
                    log.info('breaker OPEN cooldown=%s', self.cooldown)

breaker = Breaker()

def topology():
    connection = pika.BlockingConnection(pika.URLParameters(AMQP))
    channel = connection.channel()
    channel.exchange_declare(exchange='cinema', exchange_type='direct', durable=True)
    channel.queue_declare(queue='loyalty.ticket-sold', durable=True, exclusive=False, auto_delete=False)
    channel.queue_bind(queue='loyalty.ticket-sold', exchange='cinema', routing_key='TicketSold')
    return connection, channel

def accrue(event):
    if config['delay']:
        time.sleep(config['delay'])
    with db() as c:
        if config['dedup']:
            cursor = c.execute('INSERT INTO accruals VALUES(?,?,?) ON CONFLICT(ticket_id) DO NOTHING',
                               (event['ticket_id'], event['customer_id'], 10))
        else:
            cursor = c.execute('INSERT INTO unsafe_accruals(ticket_id,customer_id,points) VALUES(?,?,?)',
                               (event['ticket_id'], event['customer_id'], 10))
        changed = cursor.rowcount
    log.info('accrual ticket=%s inserted=%s dedup=%s', event['ticket_id'], changed, config['dedup'])

def worker():
    while True:
        connection = None
        try:
            connection, channel = topology()
            if ROLE == 'loyalty':
                channel.basic_qos(prefetch_count=1)
                def receive(ch, method, properties, body):
                    try:
                        accrue(json.loads(body))
                        ch.basic_ack(method.delivery_tag)
                    except Exception:
                        log.exception('consumer failed: requeue')
                        ch.basic_nack(method.delivery_tag, requeue=True)
                channel.basic_consume(queue='loyalty.ticket-sold', on_message_callback=receive)
                channel.start_consuming()
            else:
                channel.confirm_delivery()
                while True:
                    with db() as c:
                        rows = c.execute('SELECT * FROM outbox WHERE sent=0').fetchall()
                    for row in rows:
                        channel.basic_publish(exchange='cinema', routing_key='TicketSold', body=row['body'],
                            mandatory=True, properties=pika.BasicProperties(delivery_mode=2,
                                message_id=row['message_id'], content_type='application/json'))
                        with db() as c:
                            c.execute('UPDATE outbox SET sent=1 WHERE message_id=?', (row['message_id'],))
                        log.info('published TicketSold message_id=%s', row['message_id'])
                    connection.process_data_events(time_limit=.2)
        except Exception:
            log.exception('broker disconnected; retry in 2s')
            time.sleep(2)
        finally:
            if connection and connection.is_open:
                connection.close()

def notify(event):
    protection = config['protection']
    token = breaker.enter() if protection == 'breaker' else None
    try:
        # This timeout is independent of the incoming client's connection lifetime.
        response = requests.post(os.getenv('LOYALTY_URL', 'http://loyalty:8000') + '/accrue',
                                 json=event, timeout=60 if protection == 'none' else 1.5)
        if token is not None:
            breaker.finish(token, response.status_code < 500)
            token = None
        response.raise_for_status()
    except Exception:
        if token is not None:
            breaker.finish(token, False)
        raise

@app.get('/health')
def health():
    return jsonify(status='ok', role=ROLE)

@app.post('/admin/config')
def configure():
    global breaker
    values = request.get_json()
    allowed = {'mode': ('sync', 'async'), 'protection': ('none', 'timeout', 'breaker')}
    for key, value in values.items():
        if key not in config or (key in allowed and value not in allowed[key]):
            return jsonify(error='invalid configuration'), 400
        if key in ('idempotent', 'dedup') and type(value) is not bool:
            return jsonify(error='expected boolean'), 400
        if key == 'delay' and (type(value) not in (int, float) or not 0 <= value <= 30):
            return jsonify(error='delay must be 0..30'), 400
    config.update(values)
    breaker = Breaker()
    return jsonify(config)

@app.post('/tickets')
def tickets():
    if ROLE != 'tickets':
        return '', 404
    data = request.get_json()
    if not isinstance(data, dict) or any(not isinstance(data.get(k), str) or not data[k]
                                       for k in ('session_id', 'seat', 'customer_id')):
        return jsonify(error='session_id, seat, customer_id must be nonempty strings'), 400
    payload = json.dumps({k: data[k] for k in ('session_id', 'seat', 'customer_id')}, sort_keys=True)
    key = request.headers.get('Idempotency-Key') if config['idempotent'] else None
    if config['idempotent'] and not key:
        return jsonify(error='Idempotency-Key required'), 400
    ticket_id = str(uuid.uuid4())
    event = dict(type='TicketSold', message_id=str(uuid.uuid4()), ticket_id=ticket_id, **json.loads(payload))
    result = dict(ticket_id=ticket_id, **json.loads(payload))
    with db() as c:
        c.execute('BEGIN IMMEDIATE')
        if key:
            old = c.execute('SELECT * FROM keys WHERE key=?', (key,)).fetchone()
            if old:
                if old['payload'] != payload:
                    return jsonify(error='key reused for different request'), 409
                if old['result'] is None:
                    return jsonify(error='request in progress'), 409
                return jsonify(json.loads(old['result'])), 201
            c.execute('INSERT INTO keys VALUES(?,?,NULL)', (key, payload))
        c.execute('INSERT INTO tickets VALUES(?,?)', (ticket_id, payload))
        if config['mode'] == 'async':
            c.execute('INSERT INTO outbox(message_id,body) VALUES(?,?)', (event['message_id'], json.dumps(event)))
            if key:
                c.execute('UPDATE keys SET result=? WHERE key=?', (json.dumps(result), key))
    log.info('ticket saved id=%s mode=%s', ticket_id, config['mode'])
    if config['mode'] == 'sync':
        try:
            notify(event)
        except Exception as error:
            log.warning('loyalty not completed ticket=%s error=%s', ticket_id, error)
        if key:
            with db() as c:
                c.execute('UPDATE keys SET result=? WHERE key=?', (json.dumps(result), key))
    return jsonify(result), 201

@app.post('/accrue')
def accrue_http():
    if ROLE != 'loyalty':
        return '', 404
    accrue(request.get_json())
    return jsonify(ok=True)

@app.get('/state')
def state():
    with db() as c:
        tables = ('tickets', 'outbox') if ROLE == 'tickets' else ('accruals', 'unsafe_accruals')
        result = {t: [dict(row) for row in c.execute('SELECT * FROM ' + t)] for t in tables}
        if ROLE == 'loyalty':
            result['balances'] = [dict(row) for row in c.execute('''SELECT customer_id,SUM(points) AS points
                FROM (SELECT customer_id,points FROM accruals UNION ALL
                      SELECT customer_id,points FROM unsafe_accruals) GROUP BY customer_id''')]
    return jsonify(result)

if os.getenv('START_WORKER', '1') == '1':
    threading.Thread(target=worker, daemon=True).start()
