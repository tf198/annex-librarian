import unittest
import tempfile
import shutil
import json
import os
from librarian.backends.xapian_indexer import XapianIndexer

class IndexingTestCase(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.d = tempfile.mkdtemp()
        cls.indexer = XapianIndexer(cls.d)

        with open('%s/files/index.json' % os.path.dirname(__file__)) as f:
            data = json.load(f)

        cls.indexer.set_writable()
        for i, record in enumerate(data):
            cls.indexer.put_data('R%d' % i, record)
        cls.indexer.unset_writable()

        #print(cls.indexer.get_data('R0', True))


    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.d)

    def assertSearch(self, query, records):
        r = self.indexer.search(query)
        result = [ m['key'] for m in r['matches'] ]
        self.assertListEqual(result, records, "Search failed: %s => %r" % (query, result))

    def test_state(self):
        self.assertSearch('state:untagged', ['R0'])

    def test_dates(self):
        self.assertSearch('date:2008*', ['R0'])
        self.assertSearch('date:2017*', ['R1'])
        self.assertSearch('date:2016*', ['R2'])
        self.assertSearch('', ['R1', 'R2', 'R0']) # order newest to oldest
        self.assertSearch('added:20171202', ['R1', 'R2', 'R0'])
        self.assertSearch('date:20081025', ['R0'])
        self.assertSearch('year:2008', ['R0'])
        
        self.skipTest("Need to implement date ranges")
        self.assertSearch('date:20160101..20180101', [])
        self.assertSearch('year:2008..2010')

    def test_props(self):
        self.assertSearch('props:landscape', ['R1'])
        self.assertSearch('props:3:4', ['R2', 'R0'])
        self.assertSearch('props:300dpi', ['R1'])

    def test_paths(self):
        self.assertSearch('path:boats', ['R2', 'R0'])
        self.assertSearch('path:boat', [])
        self.assertSearch('path:can*', []) # cant do wildcard on boolean
        self.assertSearch('filename:boating', ['R0'])

    def test_tags(self):
        self.assertSearch('tag:boat', ['R1', 'R2'])

    def test_free_search(self):
        self.assertSearch('blue', ['R2', 'R1'])
        self.assertSearch('blue boat', ['R2', 'R1', 'R0']) # record 2 is better match
        self.assertSearch('boat', ['R0', 'R2', 'R1'])
        self.assertSearch('boating', ['R0', 'R2', 'R1']) # record 1 has a filename match as well
        self.assertSearch('canoing', ['R1'])
        self.assertSearch('paddle', ['R1'])

    def test_love_hate(self):
        self.assertSearch('tag:boat -tag:blue', ['R1'])
        self.assertSearch('boat +props:landscape', ['R1'])

    def test_combinations(self):
        self.assertSearch('boat AND NOT blue', ['R0'])
        self.assertSearch('boat AND paddle', ['R1'])
        self.assertSearch('boat paddle', ['R1', 'R0', 'R2'])

    def test_inspectors(self):
        self.assertSearch('inspector:file-1.0.0', ['R2', 'R0'])
        self.assertSearch('inspector:file-*', []) # cant do wildcard on boolean

    def test_dropped(self):
        self.assertSearch('state:dropped', ['R3'])
        self.assertSearch('mimetype:image', ['R1', 'R2', 'R0'])
