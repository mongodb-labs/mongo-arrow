from bson import encode

from unittest import TestCase

from pymongoarrow.bson_util import docs_from_bson_stream


class TestBSONUtil(TestCase):
    @classmethod
    def setUpClass(cls):
        docs = [{'_id': 1, 'data': 10},
                {'_id': 2, 'data': 20},
                {'_id': 3, 'data': 30},
                {'_id': 4, 'data': 40}]

        payload = b''
        for doc in docs:
            payload += encode(doc)

        cls.docs = docs
        cls.bson_stream = payload

    def test_simple(self):
        returned_documents = docs_from_bson_stream(self.bson_stream)
        for doc in self.docs:
            self.assertIn(doc, returned_documents)
