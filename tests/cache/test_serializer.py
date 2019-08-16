from __future__ import absolute_import

import pytest

from nicoshttp.flatbuffers import FlatbuffersCacheEntrySerializer as \
    FBSSerializer
from nicoshttp.serializer import CacheSerializer, NicosCacheEntry


class TestBaseDeserializer(object):

    def test_encode_raise(self):
        serializer = CacheSerializer()
        with pytest.raises(NotImplementedError):
            serializer.encode('', None)

        with pytest.raises(NotImplementedError):
            serializer.decode(None)


class TestFlatbuffersDeserializer(object):

    @pytest.fixture(autouse=True)
    def create_encoder(self):
        self.serializer = FBSSerializer()

    def test_decode_none_value_raise(self):
        with pytest.raises(TypeError):
            self.serializer.decode(None)

    def test_encode_entry_succeed(self):
        entry = NicosCacheEntry(1234, None, 'myvalue')
        buff = self.serializer.encode('nicos/device/status', entry)
        assert buff

    def test_encode_decode(self):
        key = 'nicos/device/status'
        entry = NicosCacheEntry(1234.0, None, 'myvalue')
        buff = self.serializer.encode(key, entry)

        try:
            out = self.serializer.decode(buff)
        except Exception as error:
            assert False, error

        assert out[0] == key
        assert out[1].value == entry.value
