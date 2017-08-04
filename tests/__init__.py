import os
import logging

debug = os.environ.get('DEBUG')
if debug is not None:
    logging.basicConfig(level=getattr(logging, debug.upper(), logging.INFO))
    logging.info("Enabled logging")


