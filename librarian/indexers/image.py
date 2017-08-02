import logging
from PIL import Image
from fractions import Fraction

def image_indexer(filename, meta):
    
    im = Image.open(filename)
    
    props = meta.setdefault('props', [])

    width, height = im.size

    orientation = 'landscape' if width > height else 'portrait'
    props.append(orientation)

    aspect = Fraction(width, height)
    props.append('{0}:{1}'.format(aspect.numerator, aspect.denominator))

    if aspect > 2:
        props.append('pano')

    if width > 1920:
        size = 'large'
    elif width > 800:
        size = 'medium'
    else:
        size = 'small'
    props.append(size)

    mode = im.mode
    if mode == '1':
        mode = 'BW'
    props.append(mode)
