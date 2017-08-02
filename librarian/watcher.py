from gevent import Greenlet, GreenletExit, sleep
import os
import logging

logger = logging.getLogger(__name__)

class FileWatcher(Greenlet):

    mtime = None

    def __init__(self, f, cb, seconds=10):
        Greenlet.__init__(self)
        self.f = f
        self.cb = cb
        self.seconds = seconds

    def _run(self):
        logger.info("Watching %s", self.f)
        while True:
            s = os.stat(self.f)
            if s.st_mtime != self.mtime:
                logger.debug("Change detected on %s", self.f)
                self.mtime = s.st_mtime
                try:
                    self.cb()
                except:
                    logger.exception("Failed to execute callaback")
            try:
                sleep(self.seconds)
            except GreenletExit:
                break
        logger.info("Finished watching %s", self.f)
        
