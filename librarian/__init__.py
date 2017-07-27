import os.path
#import subprocess
import logging
from indexers import xapian_indexer as indexer
from gevent import subprocess

logger = logging.getLogger(__name__);

def parse_meta_log(lines):
    result = {}
    field = None

    for line in lines:
        parts = line.split()
        
        for token in parts[1:]:
            try:
                if token[0] == '+':
                    result[field].add(token[1:])
                elif token[0] == '-':
                    result[field].remove(token[1:])
                else:
                    field = token
                    if not field in result:
                        result[field] = set()
            except KeyError:
                pass

    return { k: list(v) for k, v in result.items() }

class Librarian:

    _head = None

    def __init__(self, path):
        self.base_path = os.path.abspath(path)
        if not os.path.exists(self.base_path):
            raise RuntimeError("No such path: {}".format(this.base_path))

        # check it is an annexed repo
        if not os.path.exists(os.path.join(self.base_path, '.git', 'annex')):
            raise RuntimeError("{} is not an annexed repo".format(self.base_path))

        librarian_path = os.path.join(self.base_path, '.git', 'librarian')
        if not os.path.exists(librarian_path):
            os.mkdir(librarian_path, 0700)

        self.cache_dir = os.path.join(librarian_path, 'cache')
        if not os.path.exists(self.cache_dir):
            os.mkdir(self.cache_dir, 0700)

        self.index = indexer.XapianIndexer(os.path.join(librarian_path, 'db'))
        self.git_cmd = ('git', '-C', self.base_path);
    
    def relative_path(self, p):
        return os.path.join(self.base_path, p);

    @property
    def head(self):
        if self._head is None:
            logger.debug("Loading current head")
            self._head = self.index.get_latest()
        return self._head

    def git_raw(self, *args):
        cmd = self.git_cmd + args
        logger.debug("Executing %r", cmd)
        return subprocess.check_output(cmd)

    def git_lines(self, *args):
        return self.git_raw(*args).strip().split("\n")

    def git_line(self, *args):
        r = self.git_lines(*args)

        if len(r) != 1: raise IndexError("Expected one line, got {0}".format(len(r)))
        return r[0]

    def sync(self, start=None):

        self.index.set_writable()

        # get the most recent commit
        latest = self.git_line('show-ref', 'refs/heads/git-annex').split()[0]
        
        if start is None:
            start = self.head
       
        if latest == start:
            logger.info("Already up to date")
            return

        # get a list of commits to bring us up to date
        if start:
            commit_range = "{0}...{1}".format(latest, start)
        else:
            commit_range = latest
        logger.info("Finding new commits...")
        commits = self.git_lines('rev-list', commit_range, '--reverse')


        for i, commit in enumerate(commits):
            commit_date = self.git_line('show', '-s', '--format=%cI', commit)
            logger.info("Commit %s (%s) [%d/%d]", commit[:8], commit_date[:10], i+1, len(commits))

            tree = self.git_lines('diff-tree', '--root', '-r', commit)

            for item in tree:
                parts = item.split("\t")
                if len(parts) == 2:
                    filename = os.path.basename(parts[1])

                    meta = {
                        'date': [commit_date[:19]],
                    }

                    if not filename.startswith('uuid'):
                        stat = dict(zip(['_mode', 'mode', 'parent', 'blob', 'action'], parts[0].split(" ")))
                        if filename.endswith('.log.met'):
                            key = filename[:-8]
                            _, ext = os.path.splitext(key)
                            meta['extension'] = [ext[1:].lower()]
                            self._process_meta_log(key, stat, meta)

                        if filename.endswith('.log'):
                            key = filename[:-4]
                            _, ext = os.path.splitext(key)
                            meta['extension'] = [ext[1:].lower()]
                            self._process_log(key, stat, meta)

            self.index.set_latest(commit)
            self._head = commit

        self.index.unset_writable()


    def _process_meta_log(self, key, stat, meta):
        logger.debug("Metafile: %s", key)

        meta.update(parse_meta_log(self.git_lines('cat-file', 'blob', stat['blob'])))

        self.index.update(key, meta)

    def _process_log(self, key, stat, meta):
        logger.debug("Logfile: %s", key)

        if stat['action'] == 'A':
            meta['state'] = ['new']
            self.index.update(key, meta)

    def search(self, terms, offset=0, pagesize=20):
        return self.index.search(terms, offset, pagesize)

    def get_meta(self, key):
        return self.index.get_data(key)

    def thumb_for_key(self, key):
        filepath = os.path.join(self.cache_dir, key + "-thumb.jpg");

        if os.path.exists(filepath):
            return filepath

        original = self.file_for_key(key)
        subprocess.check_call([
            'convert', 
            '-format', 'jpg', 
            '-thumbnail', '150x150',
            '-unsharp', '0x.5',
            '-auto-orient',
            original + "[0]",
            filepath
        ])


        return filepath

    def preview_for_key(self, key):
        filepath = os.path.join(self.cache_dir, key + "-preview.jpg");

        if os.path.exists(filepath):
            return filepath

        original = self.file_for_key(key)
        subprocess.check_call([
            'convert', 
            '-format', 'jpg', 
            '-thumbnail', '640x640',
            '-unsharp', '0x.5',
            '-auto-orient',
            original + "[0]",
            filepath
        ])

        return filepath

    def file_for_key(self, key):

        try:
            self.git_raw('annex', 'get', '--key', key)
            p = self.git_line('annex', 'contentlocation', key)
        except:
            logger.exception("Failed to get file for %s", key)
            raise KeyError("Not an annexed file");
        
        return self.relative_path(p)

    def __repr__(self):
        return "<Annex Librarian: {0}>".format(self.base_path)
