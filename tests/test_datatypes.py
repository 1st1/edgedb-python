import unittest


import edgedb
from edgedb.protocol.protocol import RecordDescriptor


class TestRecordDesc(unittest.TestCase):

    def test_recorddesc_1(self):
        with self.assertRaisesRegex(TypeError, 'one to two positional'):
            RecordDescriptor()

        with self.assertRaisesRegex(TypeError, 'one to two positional'):
            RecordDescriptor(t=1)

        with self.assertRaisesRegex(TypeError, 'requires a tuple'):
            RecordDescriptor(1)

        with self.assertRaisesRegex(TypeError, 'requires a frozenset'):
            RecordDescriptor(('a',), 1)

        RecordDescriptor(('a', 'b'))

        with self.assertRaisesRegex(TypeError, f'more than {0x4000-1}'):
            RecordDescriptor(('a',) * 20000)

    def test_recorddesc_2(self):
        rd = RecordDescriptor(('a', 'b'), frozenset({'a'}))

        self.assertEqual(rd.get_pos('a'), 0)
        self.assertEqual(rd.get_pos('b'), 1)

        self.assertTrue(rd.is_linkprop('a'))
        self.assertFalse(rd.is_linkprop('b'))

        with self.assertRaises(LookupError):
            rd.get_pos('z')

        with self.assertRaises(LookupError):
            rd.is_linkprop('z')


class TestTuple(unittest.TestCase):

    def test_tuple_empty_1(self):
        t = edgedb.Tuple()
        self.assertEqual(len(t), 0)
        self.assertEqual(hash(t), hash(()))
        with self.assertRaisesRegex(IndexError, 'out of range'):
            t[0]

    def test_tuple_2(self):
        t = edgedb.Tuple((1, 'a'))
        self.assertEqual(len(t), 2)
        self.assertEqual(hash(t), hash((1, 'a')))

        self.assertEqual(t[0], 1)
        self.assertEqual(t[1], 'a')
        with self.assertRaisesRegex(IndexError, 'out of range'):
            t[2]


class TestNamedTuple(unittest.TestCase):

    def test_namedtuple_empty_1(self):
        with self.assertRaisesRegex(ValueError, 'at least one field'):
            edgedb.NamedTuple()

    def test_namedtuple_2(self):
        t = edgedb.NamedTuple(a=1, b='a')

        self.assertEqual(t[0], 1)
        self.assertEqual(t[1], 'a')
        with self.assertRaisesRegex(IndexError, 'out of range'):
            t[2]

        self.assertEqual(len(t), 2)
        self.assertEqual(hash(t), hash((1, 'a')))
