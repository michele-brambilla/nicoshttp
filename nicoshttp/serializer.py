

class NicosCacheEntry(object):
    __slots__ = ('time', 'ttl', 'value', 'expired')

    def __init__(self, time, ttl, value):
        self.time = time
        self.ttl = ttl
        self.value = value
        self.expired = False

    def __repr__(self):
        if self.expired:
            return '(%s+%s@%s)' % (self.time, self.ttl, self.value)
        return '%s+%s@%s' % (self.time, self.ttl, self.value)

    def asDict(self):
        return {x: getattr(self, x) for x in self.__slots__}


class CacheSerializer(object):

    def encode(self, key, entry, **params):
        raise NotImplementedError('Encoder not implemented')

    def decode(self, encoded):
        raise NotImplementedError('Decoder not implemented')


