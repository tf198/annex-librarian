import os.path
import logging
import base64
import sys
from backends import xapian_indexer as backend
from librarian.indexers import Indexer
from librarian.annex import Annex
from librarian.progress import Progress
from backends.xapian_indexer import encode_sortable_date, decode_sortable_date
from gevent import subprocess
import time
import json

logger = logging.getLogger(__name__);

DEFAULT_CONFIG = {
    'BRANCHES': ['master'],
    'INDEXERS': ['file', 'image'],
}

def parse_meta_log(lines):
    result = {}
    field = None

    for line in lines:
        parts = line.split()

        for token in parts[1:]:
            if token[0] in '+-':
                op = token[0]
                token = token[1:]
                if token[0] == '!':
                    token = base64.b64decode(token[1:])

                if op == '+':
                    result[field].add(token)
                else:
                    try:
                        result[field].remove(token)
                    except KeyError:
                        pass
            else:
                field = token
                if not field in result:
                    result[field] = set()

    return { k: list(v) for k, v in result.items() if v }

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

            if filename.endswith('.log.met'):
                key = filename[:-8]
                _, ext = os.path.splitext(key)
                stat['ext'] = ext[1:]
                self._process_meta_log(key, stat)

            if filename.endswith('.log'):
                key = filename[:-4]
                _, ext = os.path.splitext(key)
                stat['ext'] = ext[1:]
                self._process_log(key, stat)
        
        for branch in self.config['BRANCHES']:
            for filename, stat in self.file_modifications('master', start):
                self._process_branch_file('master', filename, stat) 
            

        self.db.unset_writable()
        return self.get_head('git-annex')

    def run_indexer(self, items, keys=False, batch=100):
        c = 0
        
        if not items:
            total = 0
            while True:
                items = self.search('state:new', pagesize=batch)['matches']

                if len(items) == 0:
                    return {'total': total, 'indexed': c}

                total += len(items)

                result = self.run_indexer([ x['key'] for x in items ], True)
                c += result['indexed']
            return
        
        self.progress.init(len(items), "Resolving files...")
        if keys:
            items = self.annex.resolve_keys(items, self.progress.tick)
        else:
            items = self.annex.resolve_links(items, self.progress.tick)

        indexer = Indexer('file', 'image')

        batch = self.annex.git_batch(['annex', 'metadata', '--json']) # doing our own json

        self.progress.init(len(items), 'Indexing...')
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

            self.progress.tick(commit)

    def _process_meta_log(self, key, stat):
        logger.debug("Metafile: %s", key)

        meta = parse_meta_log(self.annex.git_lines('cat-file', 'blob', stat['blob']))
        
        meta['state'] = ['tagged'] if len(meta.get('tag', [])) > 0 else ['untagged']
        meta['extension'] = [stat['ext']]
        if meta.get('indexers', []) == []:
            meta['indexers'] = ['none']

        if not meta.get('date'):
            meta['date'] = [stat['date'][:19]]

        self.db.update(key, meta, 'K', key, encode_sortable_date(meta.get('date', [None])[0]))

    def _process_log(self, key, stat):
        logger.debug("Logfile: %s", key)

        if stat['action'] == 'A':
            meta = {
                'state': ['new'],
                'indexers': ['none'],
                'date': [stat['date'][:19]],
                'extension': [stat['ext']],
            }
            self.db.update(key, meta, 'K', key, encode_sortable_date(meta.get('date', [None])[0]))

    def _process_branch_file(self, branch, filename, stat):

        if stat['mode'] == "120000":
            content = os.path.realpath(self.relative_path(filename))
            key = os.path.basename(content)

            b, c = os.path.split(filename)
            f, e = os.path.splitext(c)

            p = b.split(os.sep) if b else []
            p.append(f)

            info = {
                'path': p,
                'date': [stat['date'][:19]],
                'extension': [e[1:]],
            }
            logger.debug("%s: %r", filename, info)

            self.db.update(key, info, 'F', "{0}:{1}".format(branch, filename), c)


    def search(self, terms, offset=0, pagesize=20):
        result = self.db.search(terms, offset, pagesize)
        
        for match in result['matches']:
            if match['type'] == 'K':
                match['info'] = decode_sortable_date(match['info'])
        return result

    def alldocs(self, offset=0, pagesize=20):
        return self.db.alldocs(offset, pagesize)

    def get_meta(self, key):
        return self.db.get_data(key)

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

    def __repr__(self):
        return "<Annex Librarian: {0}>".format(self.base_path)
