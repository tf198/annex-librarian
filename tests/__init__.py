import os
import logging
import shutil
import subprocess
import tempfile
import sys
import unittest
from librarian import Librarian, progress

progress.ENABLED = False

PYTHON_VERSION = int(sys.version[0])

#from . import trace_resources
#trace_resources.enable()

debug = os.environ.get('DEBUG')
if debug is not None:
    logging.basicConfig(level=getattr(logging, debug.upper(), logging.INFO))
    logging.info("Enabled logging")

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
        l.annex.git_raw('annex', 'add', filename)
        l.annex.git_raw('commit', '-m', 'Added %d' % i)

    return l

def clone_repo(origin, repo):
    subprocess.check_output(['git', 'clone', origin, repo], stderr=subprocess.STDOUT)
    subprocess.check_output(['git', '-C', repo, 'annex', 'init', 'testing'])
    l = Librarian(repo)
    return l

def destroy_repo(repo):
    # annex protects itself well!
    objects = os.path.join(repo, '.git', 'annex', 'objects')
    if os.path.exists(objects):
        for root, dirs, files in os.walk(objects):
            for d in dirs: 
                os.chmod(os.path.join(root, d), 0o755)

    shutil.rmtree(repo)

class RepoBase(object):
    
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

    def create_repo(self):
        return create_repo(self.repo)

    def clone_repo(self):
        return clone_repo(self.origin, self.repo)


if PYTHON_VERSION == 2:

    def assertRaisesRegex(self, *args, **kwargs):
        return self.assertRaisesRegexp(*args, **kwargs)
    RepoBase.assertRaisesRegex = assertRaisesRegex
