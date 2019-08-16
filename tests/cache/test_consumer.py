from __future__ import absolute_import

import pytest

from nicoshttp.cachereader import KafkaCache
from nicoshttp.cachereader import NicosCacheReader as Cache


class MockKafkaCache(KafkaCache):

    def _connect(self, brokers):
        try:
            if not isinstance(brokers, list):
                raise TypeError('bootstrap_servers must be a list')
        finally:
            pass

    def _assign(self, topic):
        pass

    def _initial_db(self):
        self.cache_db = {'device1': {'status': '', 'value': 1},
                         'device2': {'status': '', 'value': 2}, }


class TestBaseCache(object):

    def test_constructor_raise(self):
        with pytest.raises(NotImplementedError):
            Cache()


class TestKafkaCache(object):

    def test_missing_broker_in_constructor_throws(self):
        with pytest.raises(KeyError):
            KafkaCache(topics='anytopic')

    def test_missing_topic_in_constructor_throws(self):
        with pytest.raises(KeyError):
            KafkaCache(brokers='localhost')

    def test_constructor_succeed_with_string_arguments(self):
        self.cache = MockKafkaCache(brokers='localhost', topics='anytopic')

    def test_constructor_succeed_with_brokers_list(self):
        self.cache = MockKafkaCache(brokers=['localhost'], topics='anytopic')

    def test_constructor_fail_with_topics_list(self):
        with pytest.raises(TypeError):
            self.cache = MockKafkaCache(brokers=['localhost'],
                                        topics=['anytopic'])

    def test_message_is_interesting(self):
        self.cache = MockKafkaCache(brokers='localhost', topics='anytopic')

        interestings = ['nicos/device/value', 'nicos/device/status']
        for key in interestings:
            assert self.cache._message_is_interesting(key.split('/'))

    def test_message_is_not_interesting(self):
        self.cache = MockKafkaCache(brokers='localhost', topics='anytopic')

        interestings = ['nicos/device/target', 'nicos/device/error']
        for key in interestings:
            assert not self.cache._message_is_interesting(key.split('/'))

    def test_add_device_to_db(self):
        self.cache = MockKafkaCache(brokers='localhost', topics='anytopic')

        # add device value
        self.cache._update_db('nicos/device/value'.split('/'))
        assert self.cache.cache_db.get('device')
        assert self.cache.cache_db['device'] == {'value': 'found'}

        # add device status
        self.cache._update_db('nicos/device/status'.split('/'))
        assert self.cache.cache_db['device'] == {'value': 'found',
                                                 'status': 'found'}

    def test_add_device_to_db_with_specific_values(self):
        self.cache = MockKafkaCache(brokers='localhost', topics='anytopic')

        self.cache._update_db('nicos/otherdevice/value'.split('/'),10)
        self.cache._update_db('nicos/otherdevice/status'.split('/'),'ok')

        assert self.cache.cache_db['otherdevice'] == {'value': 10,
                                                 'status': 'ok'}

    def test_update_device_in_db(self):
        self.cache = MockKafkaCache(brokers='localhost', topics='anytopic')
        self.cache._update_db('nicos/device/value'.split('/'))
        self.cache._update_db('nicos/device/status'.split('/'))

        # update
        self.cache._update_db('nicos/device/value'.split('/'), 'new_value')
        self.cache._update_db('nicos/device/status'.split('/'), 'ok')

        assert self.cache.cache_db['device'] == {'value': 'new_value',
                                                  'status': 'ok'}
