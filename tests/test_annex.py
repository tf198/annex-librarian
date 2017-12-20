import unittest
from tests import RepoBase
from librarian import annex
from subprocess import CalledProcessError

#import logging
#logging.basicConfig(level=logging.DEBUG)

class AnnexTestCase(RepoBase, unittest.TestCase):

    def test_key_for_content(self):
        self.assertEqual(annex.key_for_content('/foo/bar/sha-xxx.key'), 'sha-xxx.key')

    def test_batch(self):
        l = self.clone_repo()

        with l.annex.git_batch(['annex', 'info', '--json']) as batch:
            info = batch.execute('dir_1/test_1.txt', True)
            self.assertEqual(info['key'], "SHA256E-s7--724c531a3bc130eb46fbc4600064779552682ef4f351976fe75d876d94e8088c.txt")


    def test_bad_batch(self):
        l = self.clone_repo()

        with self.assertRaisesRegex(CalledProcessError, 'non-zero exit status 1'):
            with l.annex.git_batch(['foo']) as batch:
                pass
