import os.path
import logging
import sys
import json
from gevent import subprocess

from backends import xapian_indexer as backend
from indexers import Indexer
from annex import Annex
from progress import Progress
from meta import parse_meta_log

logger = logging.getLogger(__name__);

DEFAULT_CONFIG = {
    'BRANCHES': ['master'],
    'INDEXERS': ['file', 'image'],
}

class Librarian:
    '''
    Curator of annex metadata
    '''

    def __init__(self, path, config=None, progress=sys.stderr):
        self.base_path = os.path.abspath(path)
        if not os.path.exists(self.base_path):
            raise IOError("No such directory: {}".format(self.base_path))

        self.config = dict(DEFAULT_CONFIG);
        if config: 
            self.config.update(config)

        self.annex = Annex(self.base_path)

        librarian_path = os.path.join(self.base_path, '.git', 'librarian')
        if not os.path.exists(librarian_path):
            os.mkdir(librarian_path, 0700)

        self.cache_dir = os.path.join(librarian_path, 'cache')
        if not os.path.exists(self.cache_dir):
            os.mkdir(self.cache_dir, 0700)

        self.db = backend.XapianIndexer(os.path.join(librarian_path, 'db'))

        self.indexer = Indexer(*self.config['INDEXERS'])
        self.progress = Progress(stream=progress)


    def relative_path(self, p):
        return os.path.join(self.base_path, p)

    def get_head(self, branch):
        return self.db.get_value('head:{0}'.format(branch))

    def set_head(self, branch, commit):
        self.db.set_value('head:{0}'.format(branch), commit);
    
    def sync(self, start=None, fresh=False):

        self.db.set_writable(fresh)

        for filename, stat in self.file_modifications('git-annex', start):
                    
            filename=os.path.basename(filename)
            if filename == 'uuid.log':
                continue

            meta = None

            if filename.endswith('.log.met'):
                key = filename[:-8]
                _, ext = os.path.splitext(key)
                stat['ext'] = ext[1:]
                meta = self._process_meta_log(key, stat)

            if filename.endswith('.log'):
                key = filename[:-4]
                _, ext = os.path.splitext(key)
                stat['ext'] = ext[1:]
                meta = self._process_log(key, stat)

            if meta is not None:
                data = self._get_current(key)
                
                if data.get('meta', {}) != meta:
                    data['meta'] = meta
                    self.db.put_data(key, data)
        
        for branch in self.config['BRANCHES']:
            for filename, stat in self.file_modifications('master', start):
                key, p = self._process_branch_file('master', filename, stat)

                if p is not None:
                    data = self._get_current(key)
                    paths = data.setdefault('paths', {})
                    if paths.get(branch) != p:
                        if p:
                            paths[branch] = p
                        else:
                            del(paths[branch])
                    self.db.put_data(key, data)

        self.db.unset_writable()
        return self.get_head('git-annex')

    def _get_current(self, key):
        try:
            return self.db.get_data(key)
        except KeyError:
            return {}

    def run_indexer(self, items, keys=False, batchsize=100):
        c = 0
        
        if not items:
            total = 0
            while True:
                items = self.search('state:new', pagesize=batchsize)['matches']

                if len(items) == 0:
                    return {'total': total, 'indexed': c}

                total += len(items)

                result = self.run_indexer([ x['key'] for x in items ], True, batchsize+1)
                c += result['indexed']
            return
        
        self.progress.init(len(items), "Resolving files...")
        if keys:
            items = self.annex.resolve_keys(items, self.progress.tick)
        else:
            items = self.annex.resolve_links(items, self.progress.tick)

        indexer = Indexer('file', 'image')

        self.progress.init(len(items), 'Indexing...')
        
        batch = self.annex.git_batch(['annex', 'metadata', '--json']) # doing our own json
        try:
            for i, f in items:
                payload = {'fields': indexer.index_file(f)}
                if keys:
                    payload['key'] = i
                else:
                    payload['file'] = i

                result = batch.execute(json.dumps(payload))
                if not result:
                    logger.error(result.get('message', "Error indexing file"))
                self.progress.tick()
                c += 1

                if c % batchsize == 0:
                    logger.debug("Flushing batch")
                    batch.close()
                    batch = self.annex.git_batch(['annex', 'metadata', '--json']) # doing our own json

        finally:
            batch.close()
        
        return {'total': len(items), 'indexed': c, 'commit': self.sync()}

    def file_modifications(self, branch, start=None, update_head=True):

        # get the most recent commit
        latest = self.annex.git_line('show-ref', 'refs/heads/{0}'.format(branch)).split()[0]
        
        if start is None:
            start = self.get_head(branch)
       
        if latest == start:
            logger.info("Already up to date")
            return

        # get a list of commits to bring us up to date
        if start:
            commit_range = "{0}...{1}".format(latest, start)
        else:
            commit_range = latest
        logger.debug("Finding new commits on %s...", branch)
        self.progress.log("Finding new commits on %s...", branch)

        commits = self.annex.git_lines('rev-list', commit_range, '--reverse')
        self.progress.init(len(commits))

        for i, commit in enumerate(commits):
            commit_date = self.annex.git_line('show', '-s', '--format=%cI', commit)
            logger.debug("Commit %s (%s) [%d/%d]", commit[:8], commit_date[:10], i+1, len(commits))

            tree = self.annex.git_lines('diff-tree', '--root', '-r', commit)

            for item in tree:
                parts = item.split("\t")
                if len(parts) == 2:
                    stat = dict(zip(['_mode', 'mode', 'parent', 'blob', 'action'], parts[0].split(" ")))
                    stat['date'] = commit_date
                    stat['commit'] = commit
                    filename = parts[1]

                    yield parts[1], stat

            if update_head:
                self.set_head(branch, commit)

            self.progress.tick(commit[:8])

    def _process_meta_log(self, key, stat):
        if stat['action'] == 'D':
            logger.warning("Deleted meta for %s", key)
            return

        #logger.debug("Metafile: %s", key)

        meta = parse_meta_log(self.annex.git_lines('cat-file', 'blob', stat['blob']))
        
        meta['state'] = ['tagged'] if len(meta.get('tag', [])) > 0 else ['untagged']
        meta['extension'] = [stat['ext']]
        meta.setdefault('indexers', ['none'])
        meta.setdefault('date', [stat['date'][:19]])

        return meta

    def _process_log(self, key, stat):

        #logger.debug("Logfile: %s [%d]", key, stat['action'])

        if stat['action'] == 'A':
            meta = {
                'state': ['nometa'],
                'indexers': ['none'],
                'date': [stat['date'][:19]],
                'extension': [stat['ext']],
            }
            return meta

        if stat['action'] == 'D':
            logger.warning("Deleted logfile for %s:", key)

        return None

    def _process_branch_file(self, branch, filename, stat):

        if stat['mode'] == "120000" and stat['action'] == 'A':
            key = self.annex.key_for_link(filename)
            return key, filename
            b, c = os.path.split(filename)
            f, e = os.path.splitext(c)

            p = b.split(os.sep) if b else []
            p.append(f)

            info = {
                'path': p,
                #'date': [stat['date'][:19]],
                'extension': [e[1:]],
            }
            logger.debug("%s: %r", filename, info)

            return key, info

        if stat['_mode'] == ':120000' and stat['action'] == 'D':
            logger.debug("Deleted branch file: %s", filename)
            key = os.path.basename(self.annex.git_line('cat-file', 'blob', stat['parent']))

            return key, ''


    def search(self, terms, offset=0, pagesize=20):
        return self.db.search(terms, offset, pagesize)

    def alldocs(self, offset=0, pagesize=20):
        return self.db.alldocs(offset, pagesize)

    def get_meta(self, key):
        return self.db.get_data(key)


    def thumb_for_key(self, key):
        filepath = os.path.join(self.cache_dir, key + "-thumb.jpg");

        if os.path.exists(filepath):
            return filepath

        original = self.annex.resolve_key(key)
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

        original = self.annex.resolve_key(key)
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

    def __repr__(self):
        return "<Annex Librarian: {0}>".format(self.base_path)
