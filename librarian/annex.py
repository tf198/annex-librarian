'''
Helpers for interacting with an annexed git repo
'''

from gevent import subprocess
import os.path
import logging
import json
import sys
import base64

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
        devnull = None if DEBUG else open(os.devnull, 'w')
        self.p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=devnull)
        self.is_json = is_json
        self.cmd = " ".join(cmd)
        logger.debug("Spawned %r", self.cmd)

    def execute(self, line, is_json=False):
        if self.is_json:
            line = json.dumps(line)
        self.p.stdin.write(line)
        self.p.stdin.write('\n')

        result = self.p.stdout.readline().rstrip()
        if self.is_json or is_json:
            result = json.loads(result)
        return result

    def close(self):
        self.p.stdin.close()
        self.p.wait()

        if self.p.returncode != 0:
            raise subprocess.CalledProcessError(self.p.returncode, self.cmd, "Batch failed")
        logger.debug("Finished batch")

class Annex:

    def __init__(self, path):
        self.repo = os.path.abspath(path)
        
        # check it is an annexed repo
        if not os.path.exists(os.path.join(self.repo, '.git', 'annex')):
            raise IOError("{} is not an annexed repo".format(self.repo))

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
                sys.stderr.write("-- ERROR: %s\n" % repr(cmd))
                sys.stderr.write("-- STDOUT\n")
                sys.stderr.write(sout)
                sys.stderr.write("-- STDERR\n")
                sys.stderr.write(serr)
                sys.stderr.write("-- END\n")
            raise subprocess.CalledProcessError(p.returncode, " ".join(cmd), sout + serr)

        return sout

    def git_lines(self, *args, **kwargs):
        s = self.git_raw(*args, **kwargs).strip()
        if s == '': return []
        return s.split("\n")

    def git_line(self, *args, **kwargs):
        r = self.git_lines(*args, **kwargs)

        if len(r) != 1: raise GitError("Expected one line, got {0}".format(len(r)), "\n".join(r))
        return r[0]

    def git_batch(self, args, is_json=False):
        extra = ('--json', '--batch') if is_json else ('--batch', )
        #cmd = self.git_cmd + tuple(args) + extra
        cmd = self.git_cmd(tuple(args) + extra)
        return GitBatch(cmd, is_json)

    def content_for_link(self, link):
        l = self.relative_path(link)
        if not os.path.islink(l):
            raise AnnexError("Not an annexed file: " + link)
        p = os.path.realpath(l)
        if not p.startswith(self.repo):
            raise AnnexError("Not an annexed file: " + link)
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
            p = self.git_line('annex', 'examinekey', '--format', '.git/annex/objects/${hashdirmixed}${key}/${key}', key)
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
            batch = self.git_batch(['annex', 'get', '--json'])
            for link in missing:
                result = batch.execute(link)
                if not result:
                    e = AnnexError("Unable to locate file: " + link)
                    break
                tick(os.path.basename(link))
            batch.close()
            if e: raise e

        
        return items

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
