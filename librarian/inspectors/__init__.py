import os.path
import logging
import time
import mimetypes
import json
import sys
import subprocess
from librarian.progress import getProgress

logger = logging.getLogger(__name__)

def file_inspector(filename):
    'Reports posix filesystem attributes for a file'

    _, ext = os.path.splitext(filename)

    s = os.stat(filename)
    content_type, encoding = mimetypes.guess_type(filename)

    return {
        'created': [time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime(s.st_ctime))],
        'extension': [ext[1:].lower()],
        'mimetype': content_type.split('/'),
        'size': ["{0:d}kB".format(s.st_size/1000)]
    }
file_inspector.extensions = []

def inspect_files(annex, items, keys=False, inspectors=['file', 'image']):
    '''Runs inspector on annexed files (or keys)
        
    '''
    c = 0

    pbar = getProgress()

    if not items:
        total = 0
        while True:
            items = annex.search('state:new', pagesize=batchsize)['matches']

            if len(items) == 0:
                return {'total': total, 'indexed': c}

            total += len(items)

            result = self.run_indexer([ x['key'] for x in items ], True, batchsize+1)
            c += result['indexed']
        return

    head = annex.git_line('rev-list', '--max-count=1', 'git-annex')
    user = annex.git_line('config', 'user.name')
    email = annex.git_line('config', 'user.email')
    message = "Inspecting files."

    logger.debug("HEAD: %s", head)

    cmd = annex.git_cmd(('fast-import', '--date-format=now', '--quiet'))
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE)
    s = p.stdin

    s.write("commit refs/heads/git-annex\n")
    s.write("committer {0} <{1}> now\n".format(user, email))
    s.write("data {0}\n{1}\n".format(len(message), message))
    s.write("from {0}\n".format(head))

    pbar.init(len(items), "Resolving files...")
    if keys:
        items = annex.resolve_keys(items, pbar.tick)
    else:
        items = annex.resolve_links(items, pbar.tick)

    inspector = Inspector(*inspectors)

    pbar.init(len(items), 'Inspecting...')

    for i, f in items:
        key = annex.key_for_link(f);
        doc = inspector.inspect_file(f)
        annex_location = annex.git_line('annex', 'examinekey', '--format', '${hashdirlower}${key}.info', key)
        logger.debug("Writing to %s", annex_location)

        s.write("M 100644 inline {0}\n".format(annex_location))
        s.write("data <<EOT\n")
        json.dump(doc, s)
        s.write("\nEOT\n")

        c += 1
        pbar.tick()

    s.close()
    p.wait()

    if p.returncode != 0:
        raise subprocess.CalledProcessError(p.returncode, cmd, "fast-import failed")

    return {'total': len(items), 'inspected': c}

class Inspector(object):
    '''Collection of inspection methods

    Registers file extensions against the inspectors
    '''

    def __init__(self, *enable):
        self._inspectors = {}
        self._extensions = []

        for e in enable:
            getattr(self, 'enable_%s' % e)()

    def enable_image(self):
        from librarian.inspectors.image import image_inspector
        self.add_inspector('image', image_inspector, ['.jpg', '.tif', '.png', '.gif'])

    def enable_exif(self):
        from librarian.inspectors.exif import exif_inspector
        self.add_inspector('exif', exif_inspector, ['.jpg', '.jpeg', '.tiff', '.tif'])

    def enable_file(self):
        self.add_inspector('file', file_inspector, [''])

    def add_inspector(self, name, inspector, extensions):

        if name in self._inspectors:
            logger.debug("Indexer %s already added", name)
            return

        logger.debug("Adding inspector: %s", name)
        self._inspectors[name] = inspector

        for ext in extensions:
            ext = ext.lower()
            self._extensions.append((ext, name))

    def inspect_file(self, filename):

        f = filename.lower()

        data = {'info': {}}

        for ext, inspector in self._extensions:
            if f.endswith(ext):
                logger.debug("Running %s inspector...", inspector)
                try:
                    data[inspector] = self._inspectors[inspector](filename)
                    data['info'].setdefault('inspectors', []).append(inspector)
                except:
                    logger.exception("Inspection failed")

        return data

    def inspect_items(self, annex, items, keys=False):
        '''Runs inspector on annexed files (or keys) and updates the .info files
            
        '''
        c = 0

        pbar = getProgress()

        head = annex.git_line('rev-list', '--max-count=1', 'git-annex')
        user = annex.git_line('config', 'user.name')
        email = annex.git_line('config', 'user.email')
        message = "Inspecting files."

        logger.debug("HEAD: %s", head)

        cmd = annex.git_cmd(('fast-import', '--date-format=now', '--quiet'))
        p = subprocess.Popen(cmd, stdin=subprocess.PIPE)
        s = p.stdin

        s.write("commit refs/heads/git-annex\n")
        s.write("committer {0} <{1}> now\n".format(user, email))
        s.write("data {0}\n{1}\n".format(len(message), message))
        s.write("from {0}\n".format(head))

        pbar.init(len(items), "Resolving files...")
        if keys:
            items = annex.resolve_keys(items, pbar.tick)
        else:
            items = annex.resolve_links(items, pbar.tick)

        pbar.init(len(items), 'Inspecting...')

        for i, f in items:
            key = annex.key_for_link(f);
            doc = self.inspect_file(f)
            annex_location = annex.git_line('annex', 'examinekey', '--format', '${hashdirlower}${key}.info', key)
            logger.debug("Writing to %s", annex_location)

            s.write("M 100644 inline {0}\n".format(annex_location))
            s.write("data <<EOT\n")
            json.dump(doc, s)
            s.write("\nEOT\n")

            c += 1
            pbar.tick()

        s.close()
        p.wait()

        if p.returncode != 0:
            raise subprocess.CalledProcessError(p.returncode, cmd, "fast-import failed")

        return {'total': len(items), 'inspected': c}
