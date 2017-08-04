from gevent import subprocess
import os.path
import logging
import json
import sys

logger = logging.getLogger(__name__)

def noop(*args):
    pass

class AnnexError(Exception):
    pass

def key_for_content(content):
    _, key = os.path.split(content)
    return key

class GitBatch:

    def __init__(self, cmd, is_json=False):
        self.p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
        self.is_json = is_json
        self.cmd = " ".join(cmd)
        logger.debug("Spawned %r", self.cmd)

    def execute(self, line):
        if self.is_json:
            line = json.dumps(line)
        self.p.stdin.write(line)
        self.p.stdin.write('\n')

        result = self.p.stdout.readline().rstrip()
        if self.is_json:
            result = json.loads(line)
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


        self.git_cmd = ('git', '-C', self.repo)

    def relative_path(self, p):
        return os.path.join(self.repo, p);

    def git_raw(self, *args):
        cmd = self.git_cmd + args
        #logger.debug("Executing %r", cmd)

        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        sout, serr = p.communicate()

        if p.returncode != 0:
            if logger.isEnabledFor(logging.INFO):
                sys.stderr.write("-- ERROR: %s\n" % repr(cmd))
                sys.stderr.write("-- STDOUT\n")
                sys.stderr.write(sout)
                sys.stderr.write("-- STDERR\n")
                sys.stderr.write(serr)
                sys.stderr.write("-- END\n")
            raise subprocess.CalledProcessError(p.returncode, " ".join(cmd), sout + serr)

        return sout

    def git_lines(self, *args):
        return self.git_raw(*args).strip().split("\n")

    def git_line(self, *args):
        r = self.git_lines(*args)

        if len(r) != 1: raise GitError("Expected one line, got {0}".format(len(r)), "\n".join(r))
        return r[0]

    def git_batch(self, args, is_json=False):
        extra = ('--json', '--batch') if is_json else ('--batch', )
        cmd = self.git_cmd + tuple(args) + extra
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
        p = os.path.realpath(self.relative_path(link))
        if not os.path.exists(p):
            try:
                self.git_raw('annex', 'get', link)
            except subprocess.CalledProcessError:
                raise AnnexError("Failed to retrieve file: " + link)

        return p

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
