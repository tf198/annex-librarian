import logging
from PIL import Image
from fractions import Fraction

logger = logging.getLogger(__name__)


try:
    import exifread
    HAS_EXIF = True
except ImportError:
    HAS_EXIF = False

def exif_date_to_iso(d):
    return d[:10].replace(':', '-') + "T" + d[11:]



def image_indexer(filename, meta):
    
    im = Image.open(filename)
    
    props = meta.setdefault('props', [])

    width, height = im.size

    if HAS_EXIF:
        with open(filename, 'rb') as f:
            exif = exifread.process_file(f, details=False)

        created = exif.get('Exif DateTimeOriginal')
        if created:
            meta['date'] = [ exif_date_to_iso(created.values[0]) ]

        o = exif.get('Image Orientation')
        if o:
            if o.values[0] in [6, 8]:
                width, height = height, width

        res = exif.get('Image XResolution')
        if res:
            props.append("{0}dpi".format(res.values[0]))

    orientation = 'landscape' if width > height else 'portrait'
    props.append(orientation)

    aspect = Fraction(width, height)
    props.append('{0}:{1}'.format(aspect.numerator, aspect.denominator))

    if aspect > 2:
        props.append('pano')

    if width > 1920:
        size = 'highres'
    elif width > 800:
        size = 'medres'
    else:
        size = 'lowres'
    props.append(size)

    mode = im.mode
    if mode == '1':
        mode = 'BW'
    props.append(mode)
