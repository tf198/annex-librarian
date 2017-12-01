import exifread
import logging
from fractions import Fraction

logger = logging.getLogger(__name__)

def date_to_iso(d):
    return d[:10].replace(':', '-') + "T" + d[11:]

def exif_inspector(filename, meta):
    with open(filename, 'rb') as f:
        tags = exifread.process_file(f, details=False)

    try:
        created = date_to_iso(str(tags['EXIF DateTimeOriginal']))
        meta['date'] = [created]
    except:
        logger.debug("Failed to parse create time")

    try:
        meta['device'] = [str(tags['Image Make']), str(tags['Image Model'])]
    except KeyError:
        logger.debug("Failed to read device info")
