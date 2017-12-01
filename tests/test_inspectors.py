import unittest
import os
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
            'created': data['file']['created'],
            'mimetype': ['text', 'plain'],
            'extension': ['txt'], 
            'size': ['0kB'],
        })

        self.assertDictEqual(data['info'], {'inspectors': ['file']})

    def test_nomatch(self):
        i = Inspector('file', 'exif')

        data = i.inspect_file('requirements.txt')
        self.assertEqual(data['info']['inspectors'], ['file'])

    def test_error(self):
        i = Inspector('file')

        def bad_indexer(filename, meta):
            raise IOError()

        i.add_inspector('bad', bad_indexer, ['txt'])

        data = i.inspect_file('requirements.txt')
        self.assertEqual(data['info']['inspectors'], ['file'])


    def test_image(self):
        i = Inspector('image')

        info = i.inspect_file('tests/files/boat.jpg')
        self.assertDictEqual(info['image'], {
            'created': ['2009-05-09T16:18:55'],
            'device': ['Nokia', 'E51'],
            'props': ['300dpi', 'landscape', '4:3', 'medres', 'RGB']
        })

