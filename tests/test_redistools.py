# -*- coding: utf-8 -*-
from __future__ import print_function
import os
import time
import threading
import redistools
from unittest import TestCase
import mock
import redislite


class TestGetRedisApp(TestCase):
    def tearDown(self):
        redistools._redis_master = redistools._redis_slave = None

    @mock.patch.dict(os.environ, {"REDIS_URL": "redis://myhost"})
    @mock.patch.object(redistools.redis, "Redis", wraps=redistools.redis.Redis)
    def test_defaults(self, redis_mock):
        master = redistools.get_redis()
        redis_mock.assert_called_once_with(host="myhost", port=6379, db=0)
        slave = redistools.get_redis(master=False)
        assert master is slave
        return master

    @mock.patch.dict(os.environ, {"REDIS_URL": "redis://myhost"})
    @mock.patch.object(redistools.redis, "Redis", wraps=redistools.redis.Redis)
    def test_reconnect(self, redis_mock):
        master = redistools.get_redis()
        redis_mock.reset_mock()
        assert master is redistools.get_redis()
        redis_mock.assert_not_called()

        master_new = redistools.get_redis(reconnect=True)
        assert master_new is not master
        redis_mock.assert_called_once_with(host="myhost", port=6379, db=0)

    @mock.patch.dict(os.environ, {"REDIS_URL": "redis://myhost:4567/23"})
    @mock.patch.object(redistools.redis, "Redis")
    def test_non_standard_db_port(self, redis_mock):
        redistools.get_redis()
        redis_mock.assert_called_once_with(host="myhost", port=4567, db=23)

    @mock.patch.dict(os.environ, {"REDIS_URL": "sentinel://sentinel1"})
    @mock.patch.object(redistools, "MySentinel")
    def test_sentinel_defaults(self, sentinel_mock):
        master_for_mock = sentinel_mock.return_value.master_for
        slave_for_mock = sentinel_mock.return_value.slave_for
        master = redistools.get_redis()
        sentinel_mock.assert_called_once_with([("sentinel1", 26379)], socket_timeout=0.2)

        master_for_mock.assert_called_once_with("redis", socket_timeout=0.2)
        slave_for_mock.assert_called_once_with("redis", socket_timeout=0.2)
        sentinel_mock.reset_mock()
        slave = redistools.get_redis(master=False)
        sentinel_mock.assert_not_called()  # Uses cached slave

        assert master != slave

    @mock.patch.dict(os.environ, {"REDIS_URL": "sentinel://sentinel2:2222?timeout=3.0&master=mymaster1"})
    @mock.patch.object(redistools, "MySentinel")
    def test_sentinel_non_standard(self, sentinel_mock):
        master_for_mock = sentinel_mock.return_value.master_for
        slave_for_mock = sentinel_mock.return_value.slave_for
        master = redistools.get_redis()
        sentinel_mock.assert_called_once_with([("sentinel2", 2222)], socket_timeout=3.0)
        master_for_mock.assert_called_once_with("mymaster1", socket_timeout=3.0)
        slave_for_mock.assert_called_once_with("mymaster1", socket_timeout=3.0)
        sentinel_mock.reset_mock()
        slave = redistools.get_redis(master=False)
        sentinel_mock.assert_not_called()  # Uses cached slave

        assert master != slave


class TestRedisTools(TestCase):
    def setUp(self):
        redistools._redis_master = redistools._redis_slave = redislite.Redis()

    def test_getset_json(self):
        redistools.set_json("mykey", {"foo": "bar"})
        assert {"foo": "bar"} == redistools.get_json("mykey")
        assert redistools.get_json("otherkey") is None

        master = redistools.get_redis()
        assert b'{"foo": "bar"}' == master.get("mykey")

    def test_one_at_a_time(self):

        @redistools.one_at_a_time()
        def run_count(name, count, log=print, period=0.1, sleep=time.sleep):
            for i in range(count):
                sleep(period)
                log("Progress {}: {}".format(name, i))

        print_mock = mock.MagicMock(wraps=print)
        t1 = threading.Thread(target=run_count, args=("t1", 10, print_mock))
        t2 = threading.Thread(target=run_count, args=("t2", 3, print_mock))

        t1.start()
        t2.start()
        t1.join()
        t2.join()
        assert print_mock.call_count == 13
        assert print_mock.call_args_list == [
            mock.call('Progress t1: %d' % i) for i in range(10)
        ] + [mock.call('Progress t2: %d' % i) for i in range(3)]

    def test_one_at_a_time_no_wait(self):

        @redistools.one_at_a_time(lock_timeout=0, key_prefix="foobar:",
                                  on_lock_error=lambda *args, **kargs: None)
        def run_count(name, count, log=print, period=0.1, sleep=time.sleep):
            for i in range(count):
                sleep(period)
                log("Progress {}: {}".format(name, i))

        print_mock = mock.MagicMock(wraps=print)
        t1 = threading.Thread(target=run_count, args=("t1", 10, print_mock))
        t2 = threading.Thread(target=run_count, args=("t2", 3, print_mock))

        t1.start()
        t2.start()
        t1.join()
        t2.join()
        assert print_mock.call_count == 10
        assert print_mock.call_args_list == [
            mock.call('Progress t1: %d' % i) for i in range(10)
        ]

    def test_rate_limit_gcra(self):

        @redistools.rate_limit(10, 10)
        def do_print(message, print_function):
            return print_function(message)

        print_mock = mock.MagicMock(wraps=print)

        for i in range(10):
            do_print("OK %d" % i, print_mock)

        assert print_mock.call_count == 10

        with self.assertRaises(redistools.TooManyRequests):
            do_print("Wrong 11", print_mock)

        assert print_mock.call_count == 10

        # Wait 1/10 of the period and should allow one new call (only one)
        time.sleep(1.0)

        do_print("OK 11", print_mock)
        assert print_mock.call_count == 11

        with self.assertRaises(redistools.TooManyRequests):
            do_print("Wrong 12", print_mock)

    def test_rate_limit_timebucket(self):
        @redistools.rate_limit(10, 5, algorithm="timebucket")
        def do_print(message, print_function):
            return print_function(message)

        print_mock = mock.MagicMock(wraps=print)

        for i in range(10):
            do_print("OK %d" % i, print_mock)

        assert print_mock.call_count == 10

        with self.assertRaises(redistools.TooManyRequests):
            do_print("Wrong 11", print_mock)

        assert print_mock.call_count == 10

        # Wait 1/10 of the period and still blocking
        time.sleep(1.0)
        with self.assertRaises(redistools.TooManyRequests):
            do_print("Wrong 11", print_mock)

        time.sleep(4.0)

        # When unlocked, another 10 calls can be made
        for i in range(10):
            do_print("OK %d" % (i + 10), print_mock)

        assert print_mock.call_count == 20

        with self.assertRaises(redistools.TooManyRequests):
            do_print("Wrong 21", print_mock)
