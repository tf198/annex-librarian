import unittest
import os, os.path
import stat
from librarian import Librarian
from librarian.inspectors import Inspector
from librarian.annex import AnnexError
from datetime import datetime
import logging
from tests import RepoBase, create_repo, clone_repo

#logging.basicConfig(level=logging.INFO)

DOCS = (
    (u'test_0', u'SHA256E-s7--6a85a2eca1195e5201b6118281f27a581ac9c34e0caa849e40331bbbfbba3f7e.txt'),
    (u'test_1', u'SHA256E-s7--724c531a3bc130eb46fbc4600064779552682ef4f351976fe75d876d94e8088c.txt'),
    (u'test_2', u'SHA256E-s7--e31ee1d0324634d01318e9631c4e7691f5e6f3df483b4a2c15c610f8055ff13e.txt'),
)

ALL_DOCS = [ x[1] for x in DOCS ]

DOC_KEYS = { k: v for (k, v) in DOCS }

NOW = datetime.now().isoformat()[:19]
DNOW = ''.join(NOW.split('T')[0].split('-'))

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


class IntegrationTestCase(RepoBase, unittest.TestCase):

    def test_required_paths(self):

        with self.assertRaisesRegex(IOError, 'No such directory'):
            Librarian('bar')

        with self.assertRaisesRegex(IOError, 'not an annexed repo'):
            Librarian(self.repo)

    def test_from_scratch(self):

        l = create_repo(self.repo)
        head = l.sync()
        
        self.assertSearchResult(l.db.alldocs(), ALL_DOCS)

        self.assertSearchResult(l.search('path:dir_2'),
                ['SHA256E-s7--e31ee1d0324634d01318e9631c4e7691f5e6f3df483b4a2c15c610f8055ff13e.txt'])

        self.assertSearchResult(l.search('state:untagged'), ALL_DOCS, True)

        data = l.db.get_data('SHA256E-s7--e31ee1d0324634d01318e9631c4e7691f5e6f3df483b4a2c15c610f8055ff13e.txt')
        self.assertEqual(data['_docid'], 3)
        self.assertDictEqual(data['meta'], {'state': ['new', 'untagged']})
        self.assertEqual(data['_date'][:10], NOW[:10])

        self.assertEqual(l.sync(), head)

        # manually tag test_2
        l.annex.git_lines('annex', 'metadata', '-t', 'animals', '-t', 'cat', '-s', 'date=2001-01-01T12:00:00', 'dir_2/test_2.txt')
        self.assertNotEqual(l.sync(), head)

        self.assertSearchResult(l.db.search('state:untagged'), ALL_DOCS[:2], True)

        r = l.search('tag:animals')
        self.assertEqual(r['total'], 1)
        match = r['matches'][0]
        self.assertEqual(match['date'], '2001-01-01T12:00:00')
        self.assertEqual(match['key'], 
                'SHA256E-s7--e31ee1d0324634d01318e9631c4e7691f5e6f3df483b4a2c15c610f8055ff13e.txt')

        data = l.db.get_data('SHA256E-s7--e31ee1d0324634d01318e9631c4e7691f5e6f3df483b4a2c15c610f8055ff13e.txt')
        self.assertEqual(data['_docid'], 3)
        self.assertDictEqual(data['meta'], {
            'state': ['tagged'],
            'tag': ['animals', 'cat'],
            'date': ['2001-01-01T12:00:00'],
        });

        #self.skipTest("Need to finish updating this...")

        inspector = Inspector('file')
        r = inspector.inspect_items(l.annex, ALL_DOCS[:2], True)
        #r = l.run_indexer(ALL_DOCS[:2], True) # test_0 and test_1
        self.assertEqual(r['inspected'], 2)
        self.assertEqual(r['total'], 2)
        l.sync()

        #self.assertSearchResult(l.db.search('state:noinfo'), [])

        #print([ t.term for t in l.db.db.get_document(2).termlist() ])

        r = l.db.search('path:dir_1')
        self.assertEqual(r['total'], 1)
        self.assertEqual(r['matches'][0]['key'], 
                'SHA256E-s7--724c531a3bc130eb46fbc4600064779552682ef4f351976fe75d876d94e8088c.txt')
        #self.assertEqual(r['matches'][0]['info'][:10], 'test_1.txt')

        data = l.db.get_data('SHA256E-s7--724c531a3bc130eb46fbc4600064779552682ef4f351976fe75d876d94e8088c.txt')
        self.assertEqual(data['_docid'], 2)
        self.assertEqual(data['_date'], data['annex']['added'])
        self.assertDictEqual(data['meta'], {'state': ['new', 'untagged']})
        self.assertDictEqual(data['file'], {
            'extension': ['txt'],
            'mimetype': ['text', 'plain'],
            'size': ['0kB']
        })

        self.assertSearchResult(l.db.search('state:untagged'), ALL_DOCS[:2], True)

        # check the terms generated for test_1
        self.assertDocTerms(l.db.db.get_document(2), [
            DNOW[:4],
            'Bmaster',
            'D' + DNOW,
            'DA' + DNOW,
            'Etxt',
            'Ftest_1',
            'Pdir_1',
            'QKSHA256E-s7--724c531a3bc130eb46fbc4600064779552682ef4f351976fe75d876d94e8088c.txt',
            'Tplain', 'Ttext',
            'XIfile-1.0.0',
            'XK0kb',
            'XSnew',
            'XSok',
            'XSuntagged',
            'Y' + DNOW[:4],
            'ZFtest_1',
            'Ztest_1',
            'test_1'
        ])

        # check the terms generated for test_3
        self.assertDocTerms(l.db.db.get_document(3), [
            '2001',
            'Bmaster',
            'D20010101',
            'DA' + DNOW,
            'Etxt',
            'Ftest_2',
            'Kanimals',
            'Kcat',
            'Pdir_2',
            'QKSHA256E-s7--e31ee1d0324634d01318e9631c4e7691f5e6f3df483b4a2c15c610f8055ff13e.txt',
            'XInone',
            'XSok',
            'XStagged',
            'Y2001',
            'ZFtest_2',
            'Zanim',
            'Zcat',
            'Ztest_2',
            'animals',
            'cat',
            'test_2'
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

        with self.assertRaisesRegex(AnnexError, "Invalid key: foo"):
            l.annex.resolve_keys(['foo'])

        bad_key = 'SHA256E-s7--f31ee1d0324634d01318e9631c4e7691f5e6f3df483b4a2c15c610f8055ff13e.txt'
        with self.assertRaisesRegex(AnnexError, "Unable to locate key: " + bad_key):
            l.annex.resolve_keys([bad_key])

    def test_resolve_links(self):
        l = clone_repo(self.origin, self.repo)

        files = [ 'dir_0/test_0.txt', 'dir_1/test_1.txt']
        test_0 = l.relative_path(files[0])
        self.assertFalse(os.path.exists(test_0))

        items = l.annex.resolve_links(files)
        self.assertListEqual([ x[0] for x in items ], files)

        self.assertTrue(os.path.exists(test_0))

        with self.assertRaisesRegex(AnnexError, "Not an annexed file: foo"):
            l.annex.resolve_links(['foo'])

        p = l.annex.resolve_link('dir_1/test_1.txt')
        self.assertEqual(p, self.repo + "/.git/annex/objects/9Z/jj/SHA256E-s7--724c531a3bc130eb46fbc4600064779552682ef4f351976fe75d876d94e8088c.txt/SHA256E-s7--724c531a3bc130eb46fbc4600064779552682ef4f351976fe75d876d94e8088c.txt")

    def test_branch_ops(self):
        l = clone_repo(self.origin, self.repo)
        l.sync()

        data = l.db.get_data(DOC_KEYS['test_1'])
        self.assertDictEqual(data['git']['branch'], {'master': 'dir_1/test_1.txt'})

        l.annex.git_raw('rm', 'dir_1/test_1.txt')
        l.sync()

        # not yet committed - still present
        data = l.db.get_data(DOC_KEYS['test_1'])
        self.assertDictEqual(data['git']['branch'], {'master': 'dir_1/test_1.txt'})

        l.annex.git_raw('commit', '-m', 'Removed test_1')
        l.sync()

        # content still exists for key
        data = l.db.get_data(DOC_KEYS['test_1'])
        self.assertDictEqual(data['git'], {'branch': {}})

        self.assertEqual(data['meta']['state'], ['new', 'untagged'])

        # but is not searchable
        self.assertDocTerms(l.db.db.get_document(2), [
            'QK' + DOC_KEYS['test_1'],
            'XSdropped',
        ])

    def test_sync(self):
        l = clone_repo(self.origin, self.repo)
        l.sync()

        data = l.db.get_data(DOC_KEYS['test_1'])
        self.assertEqual(data['_docid'], 2)

    def test_unannex(self):
        l = create_repo(self.repo)
        l.sync()
    
        data = l.db.get_data(DOC_KEYS['test_1'])
        self.assertDictEqual(data['git']['branch'], {'master': 'dir_1/test_1.txt'})
        
        # un annex one of the files
        l.annex.git_raw('annex', 'unannex', 'dir_1/test_1.txt')
        l.sync()
       
        # branch file deleted but content still present
        data = l.db.get_data(DOC_KEYS['test_1'])
        self.assertEqual(data['git']['branch'], {})
        self.assertEqual(data['meta']['state'], ['new', 'untagged'])

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
        result = [ x.term.decode('utf-8') for x in doc.termlist() ]
        self.assertListEqual(result, terms)

