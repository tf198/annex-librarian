import unittest
import librarian

TEST_DATA = '''
1501483673.556634875s date +2014-01-18T14:08:44 device +Canon +!Q2Fub24gRU9TIDYwMEQ= extension +jpg indexers +exif +file mimetype +image +jpeg orientation +landscape size +6774188
1501483677.235289932s tag +foo +bar
1501483690.375289235s tag -foo +nar gallery my_things
1501483693.763823542s blah -dar tag -nar -bar
1501483695.573489579s description !BADDATA state +ok
'''.split('\n')

INDEXED_META = {
    'indexers': ['file', 'exif'],
    'mimetype': ['image', 'jpeg'],
    'date': ['2014-01-18T14:08:44'],
    'extension': ['jpg'],
    'orientation': ['landscape'],
    'device': ['Canon', 'Canon EOS 600D'],
    'size': ['6774188'],
}

class MetaTestCase(unittest.TestCase):

    def test_parse_empty(self):
        
        meta = librarian.parse_meta_log(TEST_DATA[:1])
        self.assertDictEqual(meta, {})

    def test_parse_indexed(self):
        meta = librarian.parse_meta_log(TEST_DATA[:2])
        self.assertDictEqual(meta, INDEXED_META)

    def test_parse_added_tags(self):
        meta = librarian.parse_meta_log(TEST_DATA[:3])
        expected = dict(INDEXED_META)
        expected['tag'] = ['foo', 'bar']
        self.assertDictEqual(meta, expected)

    def test_parse_add_remove_tags(self):
        meta = librarian.parse_meta_log(TEST_DATA[:4])
        expected = dict(INDEXED_META)
        expected['tag'] = ['nar', 'bar']
        self.assertDictEqual(meta, expected)

    def test_parse_remove_empty(self):
        meta = librarian.parse_meta_log(TEST_DATA[:5])
        expected = dict(INDEXED_META)
        self.assertDictEqual(meta, expected)

    def test_parse_bad_base64(self):
        meta = librarian.parse_meta_log(TEST_DATA[:6])
        expected = dict(INDEXED_META)
        expected['state'] = ['ok']
        self.assertDictEqual(meta, expected)
