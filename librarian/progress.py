'''
Simple progress meter

Uses the current log level to decided whether to render or not
'''
import sys

import logging
logger = logging.getLogger(__name__)

ENABLED = True

class Progress:

    step = 0
    total = 100
    message = ""

    def __init__(self, width=30, stream=None):
        self.width = 30
        self.stream = stream

    def log(self, message, *args):
        if self.stream is not None:
            self.stream.write(message % args)
            self.stream.write("\n")


    def update(self, step, total=100, message=""):
        if self.stream is None: return


        if step == total:
            message = ": OK"

        p = float(step) / total
        self.stream.write('\r[{0:30s}] {1}/{2} {3:30s}'.format('#'*int(p*30), step, total, message[:30]))
        if step == total:
            self.stream.write("\n")

    def init(self, total, message=None):
        self.total = total
        self.step = 0
        if message:
            self.log(message)
       

    def tick(self, message=""):
        if self.step < self.total:
            self.step += 1
        self.update(self.step, self.total, message)

    def error(self, message):
        ' Write error on new line '
        if self.stream is not None:
            self.stream.write("\nERROR: {0}\n".format(message))

def getProgress():

    if ENABLED == False or logger.isEnabledFor(logging.DEBUG):
        s = None
    else:
        s = sys.stderr

    return Progress(stream=s)
