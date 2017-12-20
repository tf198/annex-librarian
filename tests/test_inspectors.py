import unittest
import os
import logging
from librarian.inspectors import Inspector

class InspectorTestCase(unittest.TestCase):

    def test_none(self):
        i = Inspector()
        self.assertEqual(i._inspectors, {})

    def test_add(self):
        i = Inspector()
        i.add_inspector('foo', 1, ['.foo'])
        self.assertDictEqual(i._inspectors, {'foo': 1})
        self.assertListEqual(i._extensions, [('.foo', 'foo')])

        i.add_inspector('foo', 2, ['.bar'])
        self.assertDictEqual(i._inspectors, {'foo': 1})
        self.assertListEqual(i._extensions, [('.foo', 'foo')])

    def test_file(self):
        i = Inspector('file')

        data = i.inspect_file('requirements.txt')

        s = os.stat('requirements.txt')

        self.assertDictEqual(data['file'], {
            'mimetype': ['text', 'plain'],
            'extension': ['txt'], 
            'size': ['0kB'],
        })

        self.assertDictEqual(data['librarian'], {'inspector': ['file-1.0.0']})

    def test_nomatch(self):
        i = Inspector('file', 'image')

        data = i.inspect_file('requirements.txt')
        self.assertEqual(data['librarian']['inspector'], ['file-1.0.0'])

    def test_error(self):
        i = Inspector('file')

        def bad_indexer(filename):
            raise IOError()

        i.add_inspector('bad', bad_indexer, ['txt'])

        logging.disable(logging.CRITICAL)
        data = i.inspect_file('requirements.txt')
        logging.disable(logging.NOTSET)
        self.assertEqual(data['librarian']['inspector'], ['file-1.0.0'])


    def test_image(self):
        i = Inspector('image')

        info = i.inspect_file('tests/files/boat.jpg')
        self.assertDictEqual(info['image'], {
            'created': ['2009-05-09T16:18:55'],
            'device': ['Nokia', 'E51'],
            'props': ['300dpi', 'landscape', '4:3', 'medres', 'RGB']
        })

    def test_pdf(self):
        self.skipTest("Not yet implemented")
        i = Inspector('pdf')

        info = i.inspect_file('tests/files/monty_quotes.pdf')
        print(info)
