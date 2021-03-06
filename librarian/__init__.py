'''
Librarian reads the changes from the git log and updates the
index based on that data

'''
from __future__ import absolute_import, division, print_function

import os.path
import logging
import sys
import json
#from gevent import subprocess
import subprocess

from .backends import xapian_indexer as backend
from .annex import Annex, parse_meta_log, parse_location_log

from . import progress

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
            os.mkdir(librarian_path, 0o700)

        self.cache_dir = os.path.join(librarian_path, 'cache')
        if not os.path.exists(self.cache_dir):
            os.mkdir(self.cache_dir, 0o700)

        self.db = backend.XapianIndexer(os.path.join(librarian_path, 'db'))

    def relative_path(self, p):
        return os.path.join(self.base_path, p)

    def get_head(self, branch):
        return self.db.get_value('head:{0}'.format(branch))

    def set_head(self, branch, commit):
        self.db.set_value('head:{0}'.format(branch), commit);
    
    def sync(self, start=None, fresh=False):

        self.db.set_writable(fresh)
        pbar = progress.getProgress()

        pbar.log("Syncing annex...")
        commits = self.annex.get_commit_list('git-annex', start or self.get_head('git-annex'))
        pbar.init(len(commits))

        for commit in commits:
            for filename, stat in self.annex.file_modifications(commit):

                filename=os.path.basename(filename)
                if filename in ('uuid.log', 'group.log', 'numcopies.log', 'transitions.log'):
                    continue

                data = None

                if filename.endswith('.log.met'):
                    key = filename[:-8]
                    data = self._process_meta_log(key, stat)

                if filename.endswith('.log'):
                    key = filename[:-4]
                    _, ext = os.path.splitext(key)
                    stat['ext'] = ext[1:]
                    data = self._process_log(key, stat)

                if filename.endswith('.info'):
                    key = filename[:-5]
                    data = self._process_info(key, stat)

                if data is not None:
                    logger.debug("updating %r", data.keys())
                    self.db.update_data(key, data)

            self.set_head('git-annex', commit)
            pbar.tick(commit)


        for branch in self.config['BRANCHES']:

            pbar.log("Syncing {0}...".format(branch))
            commits = self.annex.get_commit_list(branch, start or self.get_head(branch))
            pbar.init(len(commits))

            for commit in commits:
                for filename, stat in self.annex.file_modifications(commit):
                    key, p = self._key_for_branch_file('master', filename, stat)

                    data = self.db.get_or_create_data(key)
                    branches = data.setdefault('git', {}).setdefault('branch', {})

                    if p is None:
                        del(branches[branch])
                    else:
                        branches[branch] = p
                    self.db.put_data(key, data)

                self.set_head(branch, commit)
                pbar.tick(commit)

        self.db.unset_writable()
        return self.get_head('git-annex')

    def get_details(self, filename, include_terms=False):
        result = {}

        key = self.annex.key_for_link(filename)
        p = self.annex.git_line('annex', 'examinekey', '--format', "${hashdirlower}${key}", key)

        data = self.db.get_data(key, include_terms)

        return data

    def _process_meta_log(self, key, stat):
        if stat['action'] == 'D':
            logger.warning("Deleted meta for %s", key)
            return {"meta": {"state": ["untagged"]}}

        meta = parse_meta_log(self.annex.git_lines('cat-file', 'blob', stat['blob']))
        
        meta['state'] = ['tagged'] if len(meta.get('tag', [])) > 0 else ['untagged']
        return {"meta": meta}

    def _process_log(self, key, stat):


        # TODO: add content location?
        if stat['action'] == 'A':
            return {"annex": {
                'added': stat['date'][:19],
                'extension': stat['ext'],
            }}

        if stat['action'] == 'D':
            logger.warning("Deleted logfile for %s:", key)
            return {"annex": {"state": "deleted"}}

    def _process_info(self, key, stat):
        data = self.annex.git_json('cat-file', 'blob', stat['blob'])
        return data

    def _key_for_branch_file(self, branch, filename, stat):

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
