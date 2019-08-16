from kafka import KafkaConsumer, TopicPartition


class NicosCacheReader(object):
    cache_db = {}
    _stop = False
    _deserializer = None

    def __init__(self, **kwargs):
        raise NotImplementedError

    def disconnect(self):
        raise NotImplementedError

    def run(self):
        raise NotImplementedError

    def add_deserializer(self, deserializer):
        self._deserializer = deserializer

    @staticmethod
    def _message_is_interesting(key):
        if key[0] != 'nicos' or len(key) != 3:
            return None
        if key[2] in ['status', 'value']:
            return True

    def _update_db(self, key, value = 'found'):
        if not self.cache_db.get(key[1]):
            self.cache_db.update({key[1] : { key[2] : value}})
        else:
            self.cache_db[key[1]].update({ key[2] : value})


class KafkaCache(NicosCacheReader):
    _consumer = None
    _topic = ""

    def __init__(self, **kwargs):
        brokers = kwargs['brokers']
        topic = kwargs['topics']
        if not isinstance(brokers, list):
            brokers = [brokers]
        if not isinstance(topic, str):
            raise TypeError('topic must be a string')
        self._connect(brokers)
        self._assign(topic)
        self._initial_db()

    def _connect(self, brokers):
        self._consumer = KafkaConsumer(bootstrap_servers=brokers,
                                       auto_offset_reset='earliest')

    def _assign(self, topic):
        consumer = self._consumer
        alltopics = consumer.topics()
        if not topic in alltopics:
            raise ValueError('topic: %s is not present' % topic)
        partitions = consumer.partitions_for_topic(topic)
        consumer.assign([TopicPartition(topic, partition) for partition in
                         partitions])
        self._topic = topic

    def _initial_db(self):
        consumer = self._consumer
        assignment = consumer.assignment()
        end = consumer.end_offsets(list(assignment))

        for partition in assignment:
            while consumer.position(partition) < end[partition]:
                message = next(consumer)
                key = message.key.decode().split('/')
                if self._message_is_interesting(key):
                    self._update_db(key)

    def _log(self):
        consumer = self._consumer
        assignment = consumer.assignment()
        beginning = self._consumer.beginning_offsets(list(assignment))
        end = self._consumer.end_offsets(list(assignment))
        print('beginning: %r\tend: %r' % (beginning, end))
        for partition in assignment:
            print('> partition %r: offset: %d' % (partition, consumer.position(
                partition)))

    def disconnect(self):
        self._consumer.unsubscribe()
        self._consumer.close()

    def run(self):
        consumer = self._consumer
        while not self._stop:
            message = next(consumer)
            key = message.key.decode().split('/')
            if self._message_is_interesting(key):
                self._update_db(key)