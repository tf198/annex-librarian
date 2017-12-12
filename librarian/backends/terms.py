
SCHEMA_VERSION = '0.2'

'''
Will be indexed prefixed and unstemmed.
Will be registered as boolean terms with query parser.
'''
PREFIXED_UNSTEMMED_BOOLEAN = (
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

    ('state', 'XS'),
    ('inspector', 'XI'),
    ('device', 'XD'),
    ('props', 'XP'),
    ('prop', 'XP'),
    ('properties', 'XP'),
    ('property', 'XP'),
)

'''
Will be indexed prefixed and unstemmed
'''
PREFIXED_UNSTEMMED = (
    ('date', 'D'),

    ('created', 'DC'),
    ('added', 'DA'),
    
    ('size', 'XK'),
)

'''
Will be indexed stemmed with positional information.
'''
STEMMED = (
    ('author', 'A'),
    ('filename', 'F'),
    ('raw', 'R'),
    ('subject', 'S'), # or title
    ('description', 'S'),
)


PREFIXED_UNSTEMMED_BOOLEAN_TERMS = dict(PREFIXED_UNSTEMMED_BOOLEAN)
PREFIXED_UNSTEMMED_TERMS = dict(PREFIXED_UNSTEMMED)
STEMMED_TERMS = dict(STEMMED)

'''
Will also be indexed unprefixed and unstemmed: tag
'''
BOOLEAN_UNPREFIXED_STEMMED = ('K', )
