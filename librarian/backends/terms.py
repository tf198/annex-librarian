
SCHEMA_VERSION = '0.2'

'''
Boolean terms will be indexed prefixed without any position information.
They will also be indexed stemmed.
'''
BOOLEAN_PREFIXES = (
    ('topic', 'B'),
    ('branch', 'B'),
    ('extension', 'E'),
    ('ext', 'E'),
    ('include', 'I'),
    ('keyword', 'K'),
    ('tag', 'K'), # Keyword
    ('language', 'L'),
    ('month', 'M'),
    ('path', 'P'),
    ('folder', 'P'),
    ('id', 'Q'), # uniQue
    ('mimetype', 'T'),
    ('type', 'T'),
    ('exclude', 'V'),
    ('year', 'Y'),

    ('device', 'XD'),
    ('size', 'XK'),
    ('inspectors', 'XI'),
    ('props', 'XP'),
    ('properties', 'XP'),
    ('state', 'XS'),
)

'''
Free terms will be indexed stemmed with positional information.
'''
FREE_PREFIXES = (
    ('author', 'A'),
    ('date', 'D'),
    ('created', 'DC'),
    ('added', 'DA'),
    ('filename', 'F'),
    ('raw', 'R'),
    ('subject', 'S'), # or title
    ('description', 'S'),
)

BOOLEAN_TERMS = dict(BOOLEAN_PREFIXES)
FREE_TERMS = dict(FREE_PREFIXES)

'''
These prefixes will be skipped for stemming
'''
# extension, path, mimetype, size
BOOLEAN_ONLY = ('E', 'T', 'P', 'XK', 'XI', 'XS')
