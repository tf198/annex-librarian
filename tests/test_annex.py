import unittest
from tests import RepoBase
from librarian import annex
from subprocess import CalledProcessError

class AnnexTestCase(RepoBase, unittest.TestCase):

    def test_key_for_content(self):
        self.assertEqual(annex.key_for_content('/foo/bar/sha-xxx.key'), 'sha-xxx.key')

    def test_batch(self):
        l = self.clone_repo()

        batch = l.annex.git_batch(['annex', 'info', '--json'])

        info = batch.execute('dir_1/test_1.txt', True)
        self.assertEqual(info['key'], "SHA256E-s7--724c531a3bc130eb46fbc4600064779552682ef4f351976fe75d876d94e8088c.txt")

        batch.close()

    def test_bad_batch(self):
        l = self.clone_repo()

        batch = l.annex.git_batch(['foo'])

        with self.assertRaisesRegexp(CalledProcessError, 'non-zero exit status 1'):
            batch.close()
