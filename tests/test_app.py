import importlib
import os
import tempfile
import unittest
from unittest.mock import patch

temp = tempfile.TemporaryDirectory()
os.environ.update(DB=os.path.join(temp.name, 'test.db'), ROLE='tickets', START_WORKER='0')
import app as module

class Tests(unittest.TestCase):
    def setUp(self):
        module.config.update(mode='sync', idempotent=True, protection='breaker', delay=0, dedup=True)
        with module.db() as c:
            for table in ('tickets', 'keys', 'outbox', 'accruals', 'unsafe_accruals'):
                c.execute('DELETE FROM ' + table)
        self.client = module.app.test_client()
        self.body = dict(session_id='s', seat='A1', customer_id='c')

    def buy(self, key='k'):
        return self.client.post('/tickets', json=self.body, headers={'Idempotency-Key': key})

    def test_idempotency(self):
        with patch.object(module, 'notify') as notify:
            first, second = self.buy(), self.buy()
        self.assertEqual(first.status_code, 201)
        self.assertEqual(first.json, second.json)
        notify.assert_called_once()

    def test_pending_and_payload_conflict(self):
        with module.db() as c:
            c.execute('INSERT INTO keys VALUES(?,?,NULL)', ('k', module.json.dumps(self.body, sort_keys=True)))
        self.assertEqual(self.buy().status_code, 409)
        self.body['seat'] = 'B1'
        self.assertEqual(self.buy().status_code, 409)

    def test_unsafe_duplicates(self):
        module.config['idempotent'] = False
        with patch.object(module, 'notify') as notify:
            self.assertNotEqual(self.buy().json['ticket_id'], self.buy().json['ticket_id'])
        self.assertEqual(notify.call_count, 2)

    def test_failure_does_not_cancel_sale(self):
        with patch.object(module, 'notify', side_effect=RuntimeError('timeout')):
            self.assertEqual(self.buy().status_code, 201)

    def test_transactional_outbox(self):
        module.config['mode'] = 'async'
        with patch.object(module, 'notify') as notify:
            self.buy()
            self.buy()
        notify.assert_not_called()
        with module.db() as c:
            self.assertEqual(c.execute('SELECT COUNT(*) FROM tickets').fetchone()[0], 1)
            self.assertEqual(c.execute('SELECT COUNT(*) FROM outbox').fetchone()[0], 1)

    def test_accrual_unique_ticket_survives_reopen(self):
        event = dict(ticket_id='t', customer_id='c')
        module.accrue(event)
        module.accrue(event)  # Each operation opens a fresh persistent connection.
        with module.db() as c:
            self.assertEqual(c.execute('SELECT SUM(points) FROM accruals').fetchone()[0], 10)
        module.config['dedup'] = False
        module.accrue(event)
        module.accrue(event)
        with module.db() as c:
            self.assertEqual(c.execute('SELECT SUM(points) FROM unsafe_accruals').fetchone()[0], 20)

    def test_breaker_probe_and_stale_results(self):
        now = [0.]
        b = module.Breaker(threshold=2, cooldown=10, clock=lambda: now[0])
        stale = b.enter()
        b.finish(b.enter(), False)
        b.finish(b.enter(), False)
        b.finish(stale, True)
        with self.assertRaises(RuntimeError): b.enter()
        now[0] = 10
        probe = b.enter()
        with self.assertRaises(RuntimeError): b.enter()
        b.finish(probe, False)
        now[0] = 20
        b.finish(b.enter(), True)
        self.assertEqual(b.open_until, 0)

    def test_http_5xx_opens_breaker(self):
        module.breaker = module.Breaker(threshold=1)
        response = unittest.mock.Mock(status_code=503)
        response.raise_for_status.side_effect = RuntimeError('503')
        with patch.object(module.requests, 'post', return_value=response) as post:
            with self.assertRaises(RuntimeError): module.notify({})
            with self.assertRaises(RuntimeError): module.notify({})
            post.assert_called_once()

if __name__ == '__main__':
    unittest.main()
