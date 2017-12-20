'''
Helpers for interacting with an annexed git repo
'''
from __future__ import absolute_import, division, print_function
from builtins import bytes

#from gevent import subprocess
import subprocess
import os.path
import logging
import json
import sys
import base64
import io
from collections import OrderedDict

try:
    subprocess.DEVNULL
except AttributeError:
    subprocess.DEVNULL = open('/dev/null', 'wb')

logger = logging.getLogger(__name__)

def noop(*args):
    pass

class AnnexError(Exception):
    pass

def key_for_content(content):
    _, key = os.path.split(content)
    return key

DEBUG = logger.isEnabledFor(logging.INFO)

class GitBatch:

    def __init__(self, cmd, is_json=False):
        self.is_json = is_json
        self.cmd = cmd

    def __enter__(self):
        err = None if DEBUG else subprocess.DEVNULL
        self.p = subprocess.Popen(self.cmd, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=err, bufsize=0)
        logger.debug(u"Spawned %r", self.cmd)
        return self

    def __exit__(self, *args):
        self.p.stdin.close()
        self.p.stdout.close()
        self.p.wait()

        if self.p.returncode != 0:
            raise subprocess.CalledProcessError(self.p.returncode, " ".join(self.cmd), u"Batch failed")
        logger.debug(u"Finished batch")

    def execute(self, line, is_json=False):
        if self.is_json:
            line = json.dumps(line)
        logger.debug(u"Executing %r", line)
        self.p.stdin.write(bytes(line, 'utf-8'))
        self.p.stdin.write(b'\n')

        result = self.p.stdout.readline().rstrip()
        if self.is_json or is_json:
            result = json.loads(result)
        logger.log(5, u"Received %r", result)
        return result

    def close(self):
        raise RuntimeError("Depreciated call to close()")

class Annex:

    def __init__(self, path):
        self.repo = os.path.abspath(path)
        
        # check it is an annexed repo
        if not os.path.exists(os.path.join(self.repo, u'.git', u'annex')):
            raise IOError(u"{} is not an annexed repo".format(self.repo))

        self.git_options = {'work_dir': self.repo};

    def relative_path(self, p):
        return os.path.join(self.repo, p);

    def git_cmd(self, args, options=None):
        opts = self.git_options.copy();
        if options: opts.update(options);

        cmd = ('git',)
        if 'work_dir' in opts: cmd += ('-C', opts['work_dir'])

        return cmd + args

    def git_raw(self, *args, **kwargs):
        #cmd = self.git_cmd + args
        cmd = self.git_cmd(args, kwargs)
        logger.debug("Executing %r", cmd)

        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        sout, serr = p.communicate()

        if p.returncode != 0:
            if DEBUG:
                sys.stderr.write(u"-- ERROR: %s\n" % repr(cmd))
                sys.stderr.write(u"-- STDOUT\n")
                sys.stderr.write(sout)
                sys.stderr.write(u"-- STDERR\n")
                sys.stderr.write(serr)
                sys.stderr.write(u"-- END\n")
            raise subprocess.CalledProcessError(p.returncode, " ".join(cmd), sout + serr)

        logger.log(5, "Received %r", sout)

        return sout

    def git_json(self, *args, **kwargs):
        return json.loads(self.git_raw(*args, **kwargs))

    def git_lines(self, *args, **kwargs):
        s = self.git_raw(*args, **kwargs).decode('utf-8').strip()
        if s == '': return []
        return s.split("\n")

    def git_line(self, *args, **kwargs):
        r = self.git_lines(*args, **kwargs)

        if len(r) != 1: raise GitError(u"Expected one line, got {0}".format(len(r)), "\n".join(r))
        return r[0]

    def git_batch(self, args, is_json=False):
        extra = (u'--json', u'--batch') if is_json else (u'--batch', )
        #cmd = self.git_cmd + tuple(args) + extra
        cmd = self.git_cmd(tuple(args) + extra)
        return GitBatch(cmd, is_json)

    def content_for_link(self, link):
        l = self.relative_path(link)
        if not os.path.islink(l):
            raise AnnexError(u"Not an annexed file: " + link)
        p = os.path.realpath(l)
        if not p.startswith(self.repo):
            raise AnnexError(u"Not an annexed file: " + link)
        return p

    def key_for_link(self, link):
        f = os.path.realpath(self.relative_path(link))
        return os.path.basename(f)

    def resolve_key(self, key):
        '''
        Convert key to annexed file path.
        Retrieves content from remotes if required
        Returns <string> path to content.
        '''
        try:
            p = self.git_line(u'annex', u'examinekey', u'--format', u'.git/annex/objects/${hashdirmixed}${key}/${key}', key)
            p = self.relative_path(p)
        except subprocess.CalledProcessError:
            raise AnnexError("Invalid key: " + key)

        try:
            if not os.path.exists(p):
                self.git_raw('annex', 'get', '--key', key)
            return p
        except subprocess.CalledProcessError:
            raise AnnexError("Unable to locate key: " + key)

    def resolve_keys(self, keys, tick=noop):
        '''
        Resolve a list keys to the annexed files they refer to.
        Retrieves content from remotes if required.
        Returns a list of (key, filename) tuples.
        '''
        
        result = []
        for key in keys:
            result.append((key, self.resolve_key(key)))
            tick()
        return result

    def resolve_link(self, link):
        '''
        Resolves a branch symlink to its annexed file.
        Retrieves content from remotes if required.
        Returns <string> path to content.
        '''
        return self.resolve_links([link])[0][1]

    def resolve_links(self, links, tick=noop):
        '''
        Resolve a list of symlinks to the annexed files they refer to.
        Retrieves content from remotes if required.
        Returns a list of (key, filename) tuples.
        '''

        items = [ (link, self.content_for_link(link)) for link in links ]

        missing = [ link for link, f in items if not os.path.exists(f) ]

        for i in range(len(items) - len(missing)): tick()

        if missing:
            e = None
            with self.git_batch(['annex', 'get', '--json']) as batch:
                for link in missing:
                    result = batch.execute(link)
                    if not result:
                        raise AnnexError("Unable to locate file: " + link)
                    tick(os.path.basename(link))

        
        return items
    
    def get_commit_list(self, branch, start, end=None):
        '''
        Returns a list of commits 
        '''
        # get the most recent commit
        if end is None:
            try:
                end = self.git_line('show-ref', 'refs/heads/{0}'.format(branch)).split()[0]
            except IOError:
                logger.debug("No commits for branch " + branch)
                return [] 
       
        if end == start:
            logger.debug("Already up to date")
            return []

        # get a list of commits to bring us up to date
        if start:
            commit_range = "{0}...{1}".format(end, start)
        else:
            commit_range = end

        logger.debug("Finding new commits on %s...", branch)

        commits = self.git_lines('rev-list', commit_range, '--reverse')
        return commits

    def file_modifications(self, commit):
        '''
        Returns an iterator of (filename, stat) objects for a given commit
        '''

        commit_date = self.git_line('show', '-s', '--format=%cI', commit)
        logger.debug("Commit %s (%s)", commit[:8], commit_date[:10])

        tree = self.git_lines('diff-tree', '--root', '-r', commit)

        for item in tree:
            parts = item.split("\t")
            if len(parts) == 2:
                stat = dict(zip(['_mode', 'mode', 'parent', 'blob', 'action'], parts[0].split(" ")))
                stat['date'] = commit_date
                stat['commit'] = commit
                filename = parts[1]

                yield filename, stat

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
                    token = base64.b64decode(token[1:]).decode('utf-8')

                if op == '+':
                    #result[field].add(token)
                    result[field][token] = True
                else:
                    try:
                        #result[field].remove(token)
                        result[field].pop(token)
                    except KeyError:
                        pass
            else:
                field = token
                if not field in result:
                    result[field] = OrderedDict()

    return { k: list(v.keys()) for k, v in result.items() if v }

def parse_location_log(lines):
    locations = set()
    
    for line in lines:
        parts = line.split()
        if parts[1] == '1': 
            locations.add(parts[2])
        else:
            locations.discard(parts[2])
    
    return list(locations)
