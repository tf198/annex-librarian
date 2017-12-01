import os.path
import logging
import sys
import json
from gevent import subprocess

from backends import xapian_indexer as backend
from annex import Annex, parse_meta_log

from librarian import progress

logger = logging.getLogger(__name__);

DEFAULT_CONFIG = {
    'BRANCHES': ['master'],
    'INDEXERS': ['file', 'image'],
}

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

        self.annex = Annex(self.base_path)

        librarian_path = os.path.join(self.base_path, '.git', 'librarian')
        if not os.path.exists(librarian_path):
            os.mkdir(librarian_path, 0700)

        self.cache_dir = os.path.join(librarian_path, 'cache')
        if not os.path.exists(self.cache_dir):
            os.mkdir(self.cache_dir, 0700)

        self.db = backend.XapianIndexer(os.path.join(librarian_path, 'db'))

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

            data = None

            if filename.endswith('.log.met'):
                key = filename[:-8]
                data = {'meta': self._process_meta_log(key, stat)}

            if filename.endswith('.log'):
                key = filename[:-4]
                _, ext = os.path.splitext(key)
                stat['ext'] = ext[1:]
                data = {'log': self._process_log(key, stat)}

            if filename.endswith('.info'):
                key = filename[:-5]
                section = 'info'
                data = self._process_info(key, stat)

            if data is not None:
                logger.debug("updating %r", data.keys())
                self.db.update_data(key, data)
        
        for branch in self.config['BRANCHES']:
            for filename, stat in self.file_modifications('master', start):
                key, p = self._process_branch_file('master', filename, stat)

                data = self._get_current(key)
                paths = data.setdefault('paths', {})

                if p is None:
                    del(paths[branch])
                else:
                    paths[branch] = p
                self.db.put_data(key, data)

        self.db.unset_writable()
        return self.get_head('git-annex')

    def _get_current(self, key):
        try:
            return self.db.get_data(key)
        except KeyError:
            return {}

    def get_details(self, filename, commit):
        result = {}

        key = self.annex.key_for_link(filename)
        p = self.annex.git_line('annex', 'examinekey', '--format', "${hashdirlower}${key}", key)

        data = self.db.get_data(key, include_terms=True)

        return data

        if commit == 'HEAD': commit = 'git-annex'

        try:
            for line in self.annex.git_lines('show', "{0}:{1}.log".format(commit, p)):
                locations = set()
                parts = line.split()
                if parts[1] == '1': 
                    locations.add(parts[2])
                else:
                    locations.discard(parts[2])
                result['locations'] = list(locations)

        except:
            logger.exception("Failed to read locations");

        try:
            data = json.loads(self.annex.git_raw('show', "{0}:{1}.info".format(commit, p)))
            result.update(data)
        except:
            pass

        return result

    '''
    def run_inspector(self, items, keys=False, batchsize=100):
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
       
        # prepare workspace
        worktree = self.annex.relative_path('.git/librarian/git-librarian')
        if not os.path.exists(worktree):
            self.annex.git_raw('worktree', 'add', worktree, 'git-annex')
        else:
            self.annex.git_raw('checkout', work_dir=worktree)

        context.progress.init(len(items), "Resolving files...")
        if keys:
            items = self.annex.resolve_keys(items, context.progress.tick)
        else:
            items = self.annex.resolve_links(items, context.progress.tick)

        inspector = Inspector('file', 'image')

        context.progress.init(len(items), 'Inspecting...')

        for i, f in items:
            key = self.annex.key_for_link(f);
            doc = inspector.inspect_file(f)
            annex_location = self.annex.git_line('annex', 'examinekey', '--format', worktree + '/${hashdirlower}${key}.json', key)
            logger.debug("Writing to %s", annex_location)

            with open(annex_location, 'w') as f:
                json.dump(doc, f)

            self.annex.git_raw('add', annex_location, work_dir=worktree)
            c += 1
            context.progress.tick()

        changes = self.annex.git_lines('status', '--porcelain', work_dir=worktree)
        if len(changes):
            self.annex.git_raw('commit', '-m', 'Inspected {0} items'.format(c), work_dir=worktree)
                
        return {'total': len(items), 'inspected': c, 'commit': self.sync()}
    '''
    def file_modifications(self, branch, start=None, update_head=True):

        pbar = progress.getProgress()

        # get the most recent commit
        try:
            latest = self.annex.git_line('show-ref', 'refs/heads/{0}'.format(branch)).split()[0]
        except:
            logger.debug("No commits for branch " + branch)
            return 
        
        if start is None:
            start = self.get_head(branch)
       
        if latest == start:
            logger.debug("Already up to date")
            return

        # get a list of commits to bring us up to date
        if start:
            commit_range = "{0}...{1}".format(latest, start)
        else:
            commit_range = latest
        logger.debug("Finding new commits on %s...", branch)
        pbar.log("Finding new commits on %s...", branch)

        commits = self.annex.git_lines('rev-list', commit_range, '--reverse')
        pbar.init(len(commits))

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

            pbar.tick(commit[:8])

    def _process_meta_log(self, key, stat):
        if stat['action'] == 'D':
            logger.warning("Deleted meta for %s", key)
            return

        meta = parse_meta_log(self.annex.git_lines('cat-file', 'blob', stat['blob']))
        
        meta['state'] = ['tagged'] if len(meta.get('tag', [])) > 0 else ['untagged']
        #meta['updated'] = stat['date'][:19]

        return meta

    def _process_log(self, key, stat):

        # TODO: add content location
        if stat['action'] == 'A':
            log = {
                'added': stat['date'][:19],
                'extension': stat['ext'],
            }
            return log

        if stat['action'] == 'D':
            logger.warning("Deleted logfile for %s:", key)

        return None

    def _process_info(self, key, stat):
        info = json.loads(self.annex.git_raw('cat-file', 'blob', stat['blob']))
        return info

    def _process_branch_file(self, branch, filename, stat):

        if stat['mode'] == "120000" and stat['action'] == 'A':
            key = self.annex.key_for_link(filename)
            return key, filename

        if stat['_mode'] == ':120000' and stat['action'] == 'D':
            logger.debug("Deleted branch file: %s", filename)
            key = os.path.basename(self.annex.git_line('cat-file', 'blob', stat['parent']))

            return key, None


    def search(self, terms, offset=0, pagesize=20):
        return self.db.search(terms, offset, pagesize)

    def alldocs(self, offset=0, pagesize=20):
        return self.db.alldocs(offset, pagesize)

    def get_data(self, key):
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
