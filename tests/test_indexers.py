import unittest
import os
from librarian.indexers import Indexer

class IndexerTestCase(unittest.TestCase):

    def test_none(self):
        i = Indexer()
        self.assertEqual(i._indexers, {})

    def test_file(self):
        i = Indexer('file')

        meta = i.index_file('requirements.txt')

        s = os.stat('requirements.txt')

        self.assertDictEqual(meta, {
            'date': meta['date'],
            'mimetype': ['text', 'plain'],
            'indexers': ['file'],
            'extension': 'txt', 
            'size': '0kB',
        })

    def test_nomatch(self):
        i = Indexer('file', 'exif')

        meta = i.index_file('requirements.txt')
        self.assertEqual(meta['indexers'], ['file'])

    def test_error(self):
        i = Indexer('file')

        def bad_indexer(filename, meta):
            raise IOError()

        i.add_indexer('bad', bad_indexer, ['txt'])

        meta = i.index_file('requirements.txt')
        self.assertEqual(meta['indexers'], ['file'])


    def test_exif(self):
        i = Indexer('exif')

        meta = i.index_file('tests/files/boat.jpg')
        self.assertDictEqual(meta, {
            'date': ['2009-05-09T16:18:55'],
            'device': ['Nokia', 'E51'],
            'indexers': ['exif'],
        })

    def test_image(self):
        i = Indexer('image')

        meta = i.index_file('tests/files/boat.jpg')
        self.assertDictEqual(meta, {
            'indexers': ['image'],
            'props': ['landscape', '4:3', 'medium', 'RGB']
        })

    def test_date_handling(self):
        i = Indexer('file', 'exif')

        meta = i.index_file('tests/files/boat.jpg')
        self.assertEqual(meta['date'], ['2009-05-09T16:18:55'])
