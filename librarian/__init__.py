import os.path
import logging
import base64
import sys
from backends import xapian_indexer as backend
from librarian.indexers import Indexer
from backends.xapian_indexer import encode_sortable_date, decode_sortable_date
from gevent import subprocess
import time

logger = logging.getLogger(__name__);

DEFAULT_CONFIG = {
    'BRANCHES': ['master']
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

    def __init__(self, path, config=None):
        self.base_path = os.path.abspath(path)
        if not os.path.exists(self.base_path):
            raise IOError("No such directory: {}".format(self.base_path))

        self.config = dict(DEFAULT_CONFIG);
        if config: 
            self.config.update(config)

        # check it is an annexed repo
        if not os.path.exists(os.path.join(self.base_path, '.git', 'annex')):
            raise IOError("{} is not an annexed repo".format(self.base_path))

        librarian_path = os.path.join(self.base_path, '.git', 'librarian')
        if not os.path.exists(librarian_path):
            os.mkdir(librarian_path, 0700)

        self.cache_dir = os.path.join(librarian_path, 'cache')
        if not os.path.exists(self.cache_dir):
            os.mkdir(self.cache_dir, 0700)

        self.db = backend.XapianIndexer(os.path.join(librarian_path, 'db'))
        self.git_cmd = ('git', '-C', self.base_path);

        self.indexer = Indexer('file', 'image', 'exif')

        self.heads = {}
    
    def relative_path(self, p):
        return os.path.join(self.base_path, p);

    def get_head(self, branch):
        try:
            return self.heads[branch]
        except KeyError:
            logger.debug("Loading current commit for %s", branch)
            self.heads[branch] = self.db.get_value('head:{0}'.format(branch))
            return self.heads[branch]

    def set_head(self, branch, commit):
        self.heads[branch] = commit
        self.db.set_value('head:{0}'.format(branch), commit);

    def git_raw(self, *args):
        cmd = self.git_cmd + args
        #logger.debug("Executing %r", cmd)

        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        sout, serr = p.communicate()

        if p.returncode != 0:
            sys.stderr.write(serr)
            lines = sout.strip().split('\n')
            e = RuntimeError("Command failed: " + lines[0])
            e.stderr = lines
            raise e

        return sout

    def git_lines(self, *args):
        return self.git_raw(*args).strip().split("\n")

    def git_line(self, *args):
        r = self.git_lines(*args)

        if len(r) != 1: raise IndexError("Expected one line, got {0}".format(len(r)))
        return r[0]

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

    def run_indexer(self, items, keys=False, batch=50):
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


        for i, item in enumerate(items):
            logger.info("Indexing %d of %d", i+1, len(items))
            try:
                f = item

                args = ['annex', 'metadata']

                if keys:
                    f = self.file_for_key(item)
                    args.append('--key')
                else:
                    self.git_raw('annex', 'get', f)

                args.append(item)

                meta = self.indexer.index_file(f)

                for f, v in meta.items():
                    if isinstance(v, (list, tuple)):
                        for kw in v:
                            args.append('-s')
                            args.append('{0}+={1}'.format(f, kw))
                    else:
                        if isinstance(v, (int, long)):
                            v = str(v)
                        args.append('-s')
                        args.append('{0}={1}'.format(f, v))

                self.git_lines(*args)
                c += 1
            except Exception, e:
                logger.error(e)
                logger.error("Failed to process %s", item)

        return {'total': len(items), 'indexed': c, 'commit': self.sync()}

    def file_modifications(self, branch, start=None, update_head=True):

        # get the most recent commit
        latest = self.git_line('show-ref', 'refs/heads/{0}'.format(branch)).split()[0]
        
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
        logger.info("Finding new commits on %s...", branch)
        commits = self.git_lines('rev-list', commit_range, '--reverse')


        for i, commit in enumerate(commits):
            commit_date = self.git_line('show', '-s', '--format=%cI', commit)
            logger.info("Commit %s (%s) [%d/%d]", commit[:8], commit_date[:10], i+1, len(commits))

            tree = self.git_lines('diff-tree', '--root', '-r', commit)

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

    def _process_meta_log(self, key, stat):
        logger.debug("Metafile: %s", key)

        meta = parse_meta_log(self.git_lines('cat-file', 'blob', stat['blob']))
        
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

    #def alldocs(self, offset=0, pagesize=20):
    #    return self.db.search(None, offset, pagesize)

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

    def file_for_key(self, key):

        self.git_raw('annex', 'get', '--key', key)
        p = self.git_line('annex', 'contentlocation', key)
        
        return self.relative_path(p)

    def __repr__(self):
        return "<Annex Librarian: {0}>".format(self.base_path)
