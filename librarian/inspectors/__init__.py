from __future__ import absolute_import, division, print_function

import os.path
import logging
import time
import json
import sys
import subprocess
import codecs
import importlib
from librarian.progress import getProgress

logger = logging.getLogger(__name__)


class Inspector(object):
    '''Collection of inspection methods

    Registers file extensions against the inspectors
    '''

    def __init__(self, *enable):
        self._inspectors = {}
        self._extensions = []

        for e in enable:
            #getattr(self, 'enable_%s' % e)()
            self.enable(e)

    def enable(self, inspector):
        m = importlib.import_module('librarian.inspectors.%s' % inspector)
        i = getattr(m, '{}_inspector'.format(inspector))
        self.add_inspector(inspector, i, i.extensions)
    '''
    def enable_image(self):
        from librarian.inspectors.image import image_inspector
        self.add_inspector('image', image_inspector, ['.jpg', '.tif', '.png', '.gif'])

    def enable_exif(self):
        from librarian.inspectors.exif import exif_inspector
        self.add_inspector('exif', exif_inspector, ['.jpg', '.jpeg', '.tiff', '.tif'])

    def enable_file(self):
        self.add_inspector('file', file_inspector, [''])
    '''
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

        data = {'librarian': {'inspector': []}}

        for ext, inspector in self._extensions:
            if ext == ".*" or f.endswith(ext):
                logger.debug("Running %s inspector...", inspector)
                try:
                    m = self._inspectors[inspector]
                    data[inspector] = m(filename) or {}
                    data['librarian']['inspector'].append("{0}-{1}".format(inspector, getattr(m, 'version', '0.0.1')))
                except:
                    data[inspector] = {}
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
        #s = io.TextIOWrapper(p.stdin, "utf-8")
        s = codecs.getwriter('utf-8')(p.stdin)

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
