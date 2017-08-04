import unittest
import tempfile
import shutil
import os, os.path
import stat
from librarian import Librarian
from librarian.annex import AnnexError
from datetime import datetime
import subprocess
import logging

#logging.basicConfig(level=logging.INFO)

DOCS = (
    ('test_0', 'SHA256E-s7--6a85a2eca1195e5201b6118281f27a581ac9c34e0caa849e40331bbbfbba3f7e.txt'),
    ('test_1', 'SHA256E-s7--724c531a3bc130eb46fbc4600064779552682ef4f351976fe75d876d94e8088c.txt'),
    ('test_2', 'SHA256E-s7--e31ee1d0324634d01318e9631c4e7691f5e6f3df483b4a2c15c610f8055ff13e.txt'),
)

ALL_DOCS = [ x[1] for x in DOCS ]

DOC_KEYS = { k: v for (k, v) in DOCS }

NOW = datetime.now().isoformat()[:19]

def now(chars):
    return NOW[:chars].replace('-', '')

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
    l = Librarian(repo, progress=None)

    for i in range(3):
        d = os.path.join(repo, 'dir_{0}'.format(i))
        os.mkdir(d)
        filename = os.path.join(d, 'test_%s.txt' % i)
        with open(filename, 'w') as f:
            f.write("Hello %d" % i)
        l.annex.git_raw('annex', 'add', filename)
        l.annex.git_raw('commit', '-m', 'Added %d' % i)

    return l

def clone_repo(origin, repo):
    subprocess.check_output(['git', 'clone', origin, repo], stderr=subprocess.STDOUT)
    subprocess.check_output(['git', '-C', repo, 'annex', 'init', 'testing'])
    l = Librarian(repo, progress=None)
    return l

def destroy_repo(repo):
    # annex protects itself well!
    objects = os.path.join(repo, '.git', 'annex', 'objects')
    if os.path.exists(objects):
        for root, dirs, files in os.walk(objects):
            for d in dirs: 
                os.chmod(os.path.join(root, d), 0755)

    shutil.rmtree(repo)

class IntegrationTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.origin = tempfile.mkdtemp()
        create_repo(cls.origin)

    @classmethod
    def tearDownClass(cls):
       destroy_repo(cls.origin) 

    def setUp(self):
        self.repo = tempfile.mkdtemp()

    def tearDown(self):
        destroy_repo(self.repo)

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

        self.assertSearchResult(l.search('state:nometa'), ALL_DOCS, True)

        data = l.db.get_data('SHA256E-s7--e31ee1d0324634d01318e9631c4e7691f5e6f3df483b4a2c15c610f8055ff13e.txt')
        self.assertEqual(data['_docid'], 3)
        meta = data['meta']
        self.assertDictEqual(meta, {
            'indexers': ['none'],
            'date': meta['date'],
            'state': ['nometa'],
            'extension': ['txt'],
        });
        self.assertEqual(meta['date'][0][:10], NOW[:10])

        self.assertEqual(l.sync(), head)

        # manually tag test_2
        l.annex.git_lines('annex', 'metadata', '-t', 'animals', '-t', 'cat', '-s', 'date=2001-01-01T12:00:00', 'dir_2/test_2.txt')
        self.assertNotEqual(l.sync(), head)

        r = l.db.search('state:nometa')
        self.assertSearchResult(l.db.search('state:nometa'), ALL_DOCS[:2], True)

        r = l.search('tag:animals')
        self.assertEqual(r['total'], 1)
        match = r['matches'][0]
        self.assertEqual(match['date'], '2001-01-01T12:00:00')
        self.assertEqual(match['key'], 
                'SHA256E-s7--e31ee1d0324634d01318e9631c4e7691f5e6f3df483b4a2c15c610f8055ff13e.txt')

        data = l.db.get_data('SHA256E-s7--e31ee1d0324634d01318e9631c4e7691f5e6f3df483b4a2c15c610f8055ff13e.txt')
        self.assertEqual(data['_docid'], 3)
        self.assertDictEqual(data['meta'], {
            'indexers': ['none'],
            'extension': ['txt'],
            'state': ['tagged'],
            'tag': ['animals', 'cat'],
            'date': ['2001-01-01T12:00:00'],
        });
        r = l.run_indexer(ALL_DOCS[:2], True) # test_0 and test_1
        self.assertEqual(r['indexed'], 2)
        self.assertEqual(r['total'], 2)

        self.assertSearchResult(l.db.search('state:nometa'), [])

        #print([ t.term for t in l.db.db.get_document(2).termlist() ])

        r = l.db.search('path:dir_1')
        self.assertEqual(r['total'], 1)
        self.assertEqual(r['matches'][0]['key'], 
                'SHA256E-s7--724c531a3bc130eb46fbc4600064779552682ef4f351976fe75d876d94e8088c.txt')
        #self.assertEqual(r['matches'][0]['info'][:10], 'test_1.txt')

        data = l.db.get_data('SHA256E-s7--724c531a3bc130eb46fbc4600064779552682ef4f351976fe75d876d94e8088c.txt')
        self.assertEqual(data['_docid'], 2)
        meta = data['meta']
        self.assertDictEqual(meta, {
            'date': meta['date'],
            'extension': ['txt'],
            'indexers': ['file'],
            'mimetype': ['plain', 'text'],
            'size': ['0kB'],
            'state': ['untagged']
        })
        self.assertEqual(meta['date'][0][:10], NOW[:10])

        self.assertSearchResult(l.db.search('state:untagged'), ALL_DOCS[:2], True)

        # check the terms generated for test_1
        self.assertDocTerms(l.db.db.get_document(2), [
            'Bmaster',
            'D' + NOW[:10].replace('-', ''),
            'Etxt',
            'M' + NOW[:7].replace('-', ''),
            'Pdir_1',
            'Ptest_1',
            'QKSHA256E-s7--724c531a3bc130eb46fbc4600064779552682ef4f351976fe75d876d94e8088c.txt',
            'Tplain', 'Ttext',
            'XIfile',
            'XK0kb',
            'XSok',
            'XSuntagged',
            'Y' + NOW[:4],
        ])

        # check the terms generated for test_3
        self.assertDocTerms(l.db.db.get_document(3), [
            'Bmaster',
            'D20010101',
            'Etxt',
            'Kanimals',
            'Kcat',
            'M200101',
            'Pdir_2',
            'Ptest_2',
            'QKSHA256E-s7--e31ee1d0324634d01318e9631c4e7691f5e6f3df483b4a2c15c610f8055ff13e.txt',
            'XInone',
            'XSok',
            'XStagged',
            'Y2001',
            'Zanim',
            'Zcat',
            'animals',
            'cat',
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

    def test_resolve_keys(self):
        l = clone_repo(self.origin, self.repo)

        test_0 = os.path.realpath(l.relative_path('dir_0/test_0.txt'))
        self.assertFalse(os.path.exists(test_0))

        items = l.annex.resolve_keys(ALL_DOCS)
        self.assertListEqual(items, [
            ('SHA256E-s7--6a85a2eca1195e5201b6118281f27a581ac9c34e0caa849e40331bbbfbba3f7e.txt',
                self.repo + '/.git/annex/objects/3W/6G/SHA256E-s7--6a85a2eca1195e5201b6118281f27a581ac9c34e0caa849e40331bbbfbba3f7e.txt/SHA256E-s7--6a85a2eca1195e5201b6118281f27a581ac9c34e0caa849e40331bbbfbba3f7e.txt'),
            ('SHA256E-s7--724c531a3bc130eb46fbc4600064779552682ef4f351976fe75d876d94e8088c.txt', 
                self.repo + '/.git/annex/objects/9Z/jj/SHA256E-s7--724c531a3bc130eb46fbc4600064779552682ef4f351976fe75d876d94e8088c.txt/SHA256E-s7--724c531a3bc130eb46fbc4600064779552682ef4f351976fe75d876d94e8088c.txt'),
            ('SHA256E-s7--e31ee1d0324634d01318e9631c4e7691f5e6f3df483b4a2c15c610f8055ff13e.txt', 
                self.repo + '/.git/annex/objects/Qv/k5/SHA256E-s7--e31ee1d0324634d01318e9631c4e7691f5e6f3df483b4a2c15c610f8055ff13e.txt/SHA256E-s7--e31ee1d0324634d01318e9631c4e7691f5e6f3df483b4a2c15c610f8055ff13e.txt')
        ])

        self.assertTrue(os.path.exists(test_0))

        with self.assertRaisesRegexp(AnnexError, "Invalid key: foo"):
            l.annex.resolve_keys(['foo'])

        bad_key = 'SHA256E-s7--f31ee1d0324634d01318e9631c4e7691f5e6f3df483b4a2c15c610f8055ff13e.txt'
        with self.assertRaisesRegexp(AnnexError, "Unable to locate key: " + bad_key):
            l.annex.resolve_keys([bad_key])

    def test_resolve_links(self):
        l = clone_repo(self.origin, self.repo)

        files = [ 'dir_0/test_0.txt', 'dir_1/test_1.txt']
        test_0 = l.relative_path(files[0])
        self.assertFalse(os.path.exists(test_0))

        items = l.annex.resolve_links(files)
        self.assertListEqual([ x[0] for x in items ], files)

        self.assertTrue(os.path.exists(test_0))

        with self.assertRaisesRegexp(AnnexError, "Not an annexed file: foo"):
            l.annex.resolve_links(['foo'])

        p = l.annex.resolve_link('dir_1/test_1.txt')
        self.assertEqual(p, self.repo + "/.git/annex/objects/9Z/jj/SHA256E-s7--724c531a3bc130eb46fbc4600064779552682ef4f351976fe75d876d94e8088c.txt/SHA256E-s7--724c531a3bc130eb46fbc4600064779552682ef4f351976fe75d876d94e8088c.txt")

    def test_branch_ops(self):
        l = clone_repo(self.origin, self.repo)
        l.sync()

        data = l.db.get_data(DOC_KEYS['test_1'])
        self.assertDictEqual(data['paths'], {'master': 'dir_1/test_1.txt'})

        l.annex.git_raw('rm', 'dir_1/test_1.txt')
        l.sync()

        # not yet committed - still present
        data = l.db.get_data(DOC_KEYS['test_1'])
        self.assertDictEqual(data['paths'], {'master': 'dir_1/test_1.txt'})

        l.annex.git_raw('commit', '-m', 'Removed test_1')
        l.sync()

        # content still exists for key
        data = l.db.get_data(DOC_KEYS['test_1'])
        self.assertDictEqual(data['paths'], {})
        self.assertListEqual(data['meta']['state'], ['nometa'])

        # but is not searchable
        self.assertDocTerms(l.db.db.get_document(2), [
            'QK' + DOC_KEYS['test_1'],
            'XSdropped',
        ])

    def test_unannex(self):
        l = create_repo(self.repo)
        l.sync()
    
        data = l.db.get_data(DOC_KEYS['test_1'])
        self.assertDictEqual(data['paths'], {'master': 'dir_1/test_1.txt'})
        
        # un annex one of the files
        l.annex.git_raw('annex', 'unannex', 'dir_1/test_1.txt')
        l.sync()
       
        # branch file deleted but content still present
        data = l.db.get_data(DOC_KEYS['test_1'])
        self.assertEqual(data['paths'], {})
        self.assertEqual(data['meta']['state'], ['nometa'])

        # but is not searchable
        doc = l.db.db.get_document(2)
        self.assertDocTerms(doc, [
            'QK' + DOC_KEYS['test_1'],
            'XSdropped'
        ])



    def assertSearchResult(self, result, keys, sort=True):
        self.assertEqual(result['total'], len(keys))

        matches = [ x['key'] for x in result['matches'] ]
        if sort:
            matches.sort()

        self.assertListEqual(matches, keys)

    def assertDocTerms(self, doc, terms):
        result = [ x.term for x in doc.termlist() ]
        self.assertListEqual(result, terms)

