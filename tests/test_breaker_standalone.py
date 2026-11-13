"""Exercise the actual Breaker class without importing Flask or starting services."""
import ast
import logging
from pathlib import Path
import threading
import time
import unittest

tree = ast.parse(Path(__file__).resolve().parents[1].joinpath('app.py').read_text(encoding='utf-8'))
definition = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == 'Breaker')
namespace = dict(threading=threading, time=time, log=logging.getLogger('test'))
exec(compile(ast.Module(body=[definition], type_ignores=[]), 'app.py', 'exec'), namespace)
Breaker = namespace['Breaker']

class Tests(unittest.TestCase):
    def test_open_blocks_network_permission(self):
        b = Breaker(threshold=2)
        b.finish(b.enter(), False)
        b.finish(b.enter(), False)
        with self.assertRaises(RuntimeError): b.enter()

    def test_only_one_probe_and_recovery(self):
        clock = [0.]
        b = Breaker(threshold=1, cooldown=10, clock=lambda: clock[0])
        b.finish(b.enter(), False)
        clock[0] = 10
        probe = b.enter()
        with self.assertRaises(RuntimeError): b.enter()
        b.finish(probe, True)
        self.assertEqual(b.open_until, 0)

    def test_stale_success_cannot_close_open_breaker(self):
        b = Breaker(threshold=1)
        old = b.enter()
        b.finish(b.enter(), False)
        b.finish(old, True)
        with self.assertRaises(RuntimeError): b.enter()

if __name__ == '__main__': unittest.main()
