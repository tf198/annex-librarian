import unittest
import tempfile
import shutil
import os, os.path
import stat
from librarian import Librarian
from datetime import datetime
import subprocess
import logging

#logging.basicConfig(level=logging.INFO)

ALL_DOCS = [
    'SHA256E-s7--6a85a2eca1195e5201b6118281f27a581ac9c34e0caa849e40331bbbfbba3f7e.txt', # test_0.txt
    'SHA256E-s7--724c531a3bc130eb46fbc4600064779552682ef4f351976fe75d876d94e8088c.txt', # test_1.txt
    'SHA256E-s7--e31ee1d0324634d01318e9631c4e7691f5e6f3df483b4a2c15c610f8055ff13e.txt', # test_2.txt
]

NOW = datetime.now().isoformat()[:19]

class MockBackend:

    def __init__(self):
        self.items = {}
        self.values = {}

    def update(self, key, info, idterm=None):
        if idterm is None:
            idterm = 'K' + key
        self.items[idterm] = info

    def set_writable(self, clear=False):
        if clear:
            self.items = {}

    def unset_writable(self):
        pass

    def get_value(self, name):
        return self.values.get(name)

    def set_value(self, name, value):
        self.values[name] = value

def create_repo(repo):
    subprocess.check_output(['git', '-C', repo, 'init'])
    subprocess.check_output(['git', '-C', repo, 'annex', 'init', 'testing'])
    l = Librarian(repo)

    for i in range(3):
        d = os.path.join(repo, 'dir_{0}'.format(i))
        os.mkdir(d)
        filename = os.path.join(d, 'test_%s.txt' % i)
        with open(filename, 'w') as f:
            f.write("Hello %d" % i)
        l.git_raw('annex', 'add', filename)
        l.git_raw('commit', '-m', 'Added %d' % i)

    return l

class IntegrationTestCase(unittest.TestCase):

    def setUp(self):
        self.repo = tempfile.mkdtemp()

    def tearDown(self):

        # annex protects itself well!
        objects = os.path.join(self.repo, '.git', 'annex', 'objects')
        if os.path.exists(objects):
            for root, dirs, files in os.walk(objects):
                for d in dirs: 
                    os.chmod(os.path.join(root, d), 0755)

        shutil.rmtree(self.repo)

    def test_required_paths(self):

        with self.assertRaisesRegexp(IOError, 'No such directory'):
            Librarian('bar')

        with self.assertRaisesRegexp(IOError, 'not an annexed repo'):
            Librarian(self.repo)

    def test_from_scratch(self):

        l = create_repo(self.repo)
        head = l.sync()

        self.assertSearchResult(l.db.alldocs(), ALL_DOCS)

        self.assertSearchResult(l.search('path:dir_2'),
                ['SHA256E-s7--e31ee1d0324634d01318e9631c4e7691f5e6f3df483b4a2c15c610f8055ff13e.txt'])

        self.assertSearchResult(l.search('state:new'), ALL_DOCS, True)

        meta = l.db.get_data('SHA256E-s7--e31ee1d0324634d01318e9631c4e7691f5e6f3df483b4a2c15c610f8055ff13e.txt')
        self.assertDictEqual(meta, {
            'indexers': ['none'],
            'date': meta['date'],
            'state': ['new'],
            'extension': ['txt'],
            'docid': 3,
        });
        self.assertEqual(meta['date'][0][:10], NOW[:10])

        self.assertEqual(l.sync(), head)

        # manually tag test_2
        l.git_lines('annex', 'metadata', '-t', 'animals', '-t', 'cat', '-s', 'date=2001-01-01T12:00:00', 'dir_2/test_2.txt')
        self.assertNotEqual(l.sync(), head)

        r = l.db.search('state:new')
        self.assertSearchResult(l.db.search('state:new'), ALL_DOCS[:2], True)

        r = l.search('tag:animals')
        self.assertEqual(r['total'], 1)
        match = r['matches'][0]
        self.assertEqual(match['info'], '2001-01-01T12:00:00')
        self.assertEqual(match['key'], 
                'SHA256E-s7--e31ee1d0324634d01318e9631c4e7691f5e6f3df483b4a2c15c610f8055ff13e.txt')

        meta = l.db.get_data('SHA256E-s7--e31ee1d0324634d01318e9631c4e7691f5e6f3df483b4a2c15c610f8055ff13e.txt')
        self.assertDictEqual(meta, {
            'indexers': ['none'],
            'docid': 3,
            'extension': ['txt'],
            'state': ['tagged'],
            'tag': ['animals', 'cat'],
            'date': ['2001-01-01T12:00:00']
        });

        r = l.run_indexer(ALL_DOCS[:2], True) # test_0 and test_1
        self.assertEqual(r['indexed'], 2)
        self.assertEqual(r['total'], 2)

        self.assertSearchResult(l.db.search('state:new'), [])

        r = l.db.search('path:dir_1')
        self.assertEqual(r['total'], 1)
        self.assertEqual(r['matches'][0]['key'], 
                'SHA256E-s7--724c531a3bc130eb46fbc4600064779552682ef4f351976fe75d876d94e8088c.txt')
        self.assertEqual(r['matches'][0]['info'][:10], 'test_1.txt')

        meta = l.db.get_data('SHA256E-s7--724c531a3bc130eb46fbc4600064779552682ef4f351976fe75d876d94e8088c.txt')
        self.assertDictEqual(meta, {
            'date': meta['date'],
            'docid': 2,
            'extension': ['txt'],
            'indexers': ['file'],
            'mimetype': ['plain', 'text'],
            'size': ['0kB'],
            'state': ['untagged'],
        })
        self.assertEqual(meta['date'][0][:10], NOW[:10])

        self.assertSearchResult(l.db.search('state:untagged'), ALL_DOCS[:2], True)

        # check the terms generated for test_1
        self.assertDocTerms(l.db.db.get_document(2), [
            'D' + NOW[:10].replace('-', ''),
            'Etxt',
            'M' + NOW[:7].replace('-', ''),
            'QKSHA256E-s7--724c531a3bc130eb46fbc4600064779552682ef4f351976fe75d876d94e8088c.txt',
            'Tplain', 'Ttext',
            'XIfile',
            'XK0kb',
            'XSuntagged',
            'Y' + NOW[:4],
        ])

        # check the terms generated for test_3
        self.assertDocTerms(l.db.db.get_document(3), [
            'D20010101',
            'Etxt',
            'Kanimals',
            'Kcat',
            'M200101',
            'QKSHA256E-s7--e31ee1d0324634d01318e9631c4e7691f5e6f3df483b4a2c15c610f8055ff13e.txt',
            'XInone',
            'XStagged',
            'Y2001',
            'Zanim',
            'Zcat',
            'animals',
            'cat',
        ])

        # check the terms for the master listing
        self.assertDocTerms(l.db.db.get_document(4), [
            'D' + NOW[:10].replace('-', ''),
            'Etxt',
            'M' + NOW[:7].replace('-', ''),
            'Pdir_0', 'Ptest_0',
            'QFmaster:dir_0/test_0.txt',
            'Y' + NOW[:4]
        ])

        self.assertSearchResult(l.db.search('animal'),
                ['SHA256E-s7--e31ee1d0324634d01318e9631c4e7691f5e6f3df483b4a2c15c610f8055ff13e.txt'])

        self.assertSearchResult(l.db.search('"animal"'), [])


    @unittest.skip("haven't found a use for this yet") 
    def test_git_handling(self):

        l = create_repo(self.repo)
        l.db = MockBackend()
        l.sync()

        #print(l.db.items)

    def assertSearchResult(self, result, keys, sort=True):
        self.assertEqual(result['total'], len(keys))

        matches = [ x['key'] for x in result['matches'] ]
        if sort:
            matches.sort()

        self.assertListEqual(matches, keys)

    def assertDocTerms(self, doc, terms):
        result = [ x.term for x in doc.termlist() ]
        self.assertListEqual(result, terms)
