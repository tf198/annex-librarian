import logging
from PIL import Image
from fractions import Fraction

logger = logging.getLogger(__name__)

HIGH_RES = 1920
MED_RES = 1024

try:
    import exifread
    HAS_EXIF = True
except ImportError:
    HAS_EXIF = False

def exif_date_to_iso(d):
    return d[:10].replace(':', '-') + "T" + d[11:]


def image_inspector(filename):
    
    im = Image.open(filename)
   
    info = {}

    props = info.setdefault('props', [])

    width, height = im.size

    if HAS_EXIF:
        with open(filename, 'rb') as f:
            exif = exifread.process_file(f, details=False)

        created = exif.get('EXIF DateTimeOriginal')
        if created:
            info['created'] = [ exif_date_to_iso(str(created)) ]

        o = exif.get('Image Orientation')
        if o:
            if o.values[0] in [6, 8]:
                width, height = height, width

        res = exif.get('Image XResolution')
        if res:
            props.append("{0}dpi".format(res.values[0]))

        device = info.setdefault('device', [])
        make = exif.get('Image Make')
        if make:
            device.append(str(make))
        model = exif.get('Image Model')
        if model:
            device.append(str(model))

    orientation = 'landscape' if width > height else 'portrait'
    props.append(orientation)

    aspect = Fraction(width, height)
    props.append('{0}:{1}'.format(aspect.numerator, aspect.denominator))

    if aspect > 2:
        props.append('pano')

    if max(width, height) > HIGH_RES:
        size = 'highres'
    elif max(width, height) > MED_RES:
        size = 'medres'
    else:
        size = 'lowres'
    props.append(size)

    mode = im.mode
    if mode == '1':
        mode = 'BW'
    props.append(mode)

    logger.debug("Image info: %r", info)

    return info

image_inspector.extensions = ['.jpg', 'jpeg', '.tif', '.png', '.gif']
image_inspector.version = '1.0.0'
