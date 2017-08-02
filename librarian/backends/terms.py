
SCHEMA_VERSION = '0.1'

BOOLEAN_PREFIXES = (
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
    ('indexers', 'XI'),
    ('props', 'XP'),
    ('properties', 'XP'),
    ('state', 'XS'),
)


FREE_PREFIXES = (
    ('author', 'A'),
    ('topic', 'B'), # aBout
    ('date', 'D'),
    ('filename', 'F'),
    ('raw', 'R'),
    ('subject', 'S'), # or title
    ('description', 'S'),
)

BOOLEAN_TERMS = dict(BOOLEAN_PREFIXES)
FREE_TERMS = dict(FREE_PREFIXES)

# extension, path, mimetype, size
SKIP_FREE = ('E', 'T', 'P', 'XK', 'XI', 'XS')
