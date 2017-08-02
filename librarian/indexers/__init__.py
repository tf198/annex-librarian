import os.path
import logging
import time
import mimetypes

logger = logging.getLogger(__name__)

def file_indexer(filename, meta):
    _, ext = os.path.splitext(filename)

    s = os.stat(filename)
    content_type, encoding = mimetypes.guess_type(filename)

    meta.update({
        'date': time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime(s.st_ctime)),
        'extension': ext[1:].lower(),
        'mimetype': content_type.split('/'),
        'size': "{0:d}kB".format(s.st_size/1000)
    })

class Indexer(object):

    def __init__(self, *enable):
        self._indexers = {}
        self._extensions = []

        for e in enable:
            getattr(self, 'enable_%s' % e)()

    def enable(self, name):
        pass # TODO

    def enable_image(self):
        from librarian.indexers.image import image_indexer
        self.add_indexer('image', image_indexer, ['.jpg', '.tif', '.png', '.gif'])

    def enable_exif(self):
        from librarian.indexers.exif import exif_indexer
        self.add_indexer('exif', exif_indexer, ['.jpg', '.jpeg', '.tiff', '.tif'])

    def enable_file(self):
        self.add_indexer('file', file_indexer, [''])

    def add_indexer(self, name, indexer, extensions):

        if name in self._indexers:
            logger.debug("Indexer %s already added", name)
            return

        logger.debug("Adding indexer: %s", name)
        self._indexers[name] = indexer

        for ext in extensions:
            ext = ext.lower()
            self._extensions.append((ext, name))

    def index_file(self, filename):

        f = filename.lower()

        meta = {}

        for ext, indexer in self._extensions:
            if f.endswith(ext):
                logger.debug("Running %s indexer...", indexer)
                try:
                    self._indexers[indexer](filename, meta)
                    meta.setdefault('indexers', []).append(indexer)
                except:
                    logger.exception("Indexing failed")

        return meta
