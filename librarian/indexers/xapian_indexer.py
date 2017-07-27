import xapian
import time
import logging
import json

logger = logging.getLogger(__name__)

ISO_8601="%Y-%m-%dT%H:%M:%S"

PREFIXES = (
    ('author', 'A'),
    ('topic', 'B'), # aBout
    ('date', 'D'),
    ('extension', 'E'),
    ('filename', 'F'),
    ('include', 'I'),
    ('tag', 'K'), # Keyword
    ('language', 'L'),
    ('month', 'M'),
    ('path', 'P'),
    ('id', 'Q'), # uniQue
    ('raw', 'R'),
    ('subject', 'S'), # or title
    ('mimetype', 'T'),
    ('exclude', 'V'),
    ('year', 'Y'),
    ('stemmed', 'Z'),

    ('device', 'XD'),
    ('orientation', 'XO'),
    ('width', 'XW'),
    ('height', 'XH'),
    ('state', 'XS'),
)
TERM_PREFIXES = dict(PREFIXES)

def encode_sortable_date(d):
    try:
        t = time.strptime(d, ISO_8601)
        n = -int(time.mktime(t))
    except ValueError:
        n = 0;

    return xapian.sortable_serialise(n)

def decode_sortable_date(r):
    t = -xapian.sortable_unserialise(r)
    if t == 0: return None
    return time.strftime(ISO_8601, time.localtime(t))

class XapianIndexer:

    _db = None

    def __init__(self, path):
        self.path = path

    @property
    def db(self):
        if self._db is None:
            self._db = xapian.Database(self.path)
        return self._db

    def set_writable(self):
        if self._db is not None:
            self._db.close()

        self._db = xapian.WritableDatabase(self.path, xapian.DB_CREATE_OR_OPEN)

    def unset_writable(self):

        if self._db is not None:
            self._db.close()

        self._db = None

    def get_latest(self):
        return self.db.get_metadata('latest')

    def set_latest(self, commit):
        return self.db.set_metadata('latest', commit)

    def update(self, key, meta):
        try:
            date = encode_sortable_date(meta['date'][0]);
        except KeyError:
            date = encode_sortable_date('');
    
        doc = xapian.Document()

        for field, values in meta.items():
            field = TERM_PREFIXES.get(field, field.upper())
            for value in values:
                if value:
                    doc.add_boolean_term(field + value.lower())
                    doc.add_boolean_term(value.lower())

        doc.set_data(json.dumps(meta))
        doc.add_value(0, key);
        doc.add_value(1, date), 

        idterm = u"Q" + key.lower()
        doc.add_boolean_term(idterm)

        self.db.replace_document(idterm, doc)

    def get_data(self, key):
        term = u"Q" + key.lower()

        matches = list(self.db.postlist(term))
        if len(matches) > 1: raise KeyError("Key is not unique!");
        if len(matches) == 0: raise KeyError("Key not found");
        
        docid = matches[0].docid
        return json.loads(self.db.get_document(docid).get_data())



    def search(self, querystring, offset=0, pagesize=10):

        queryparser = xapian.QueryParser()
    
        # Start of prefix configuration.

        for field, prefix in PREFIXES:
            queryparser.add_prefix(field, prefix)
        # End of prefix configuration.

        # And parse the query
        logger.debug("Query string: %s", querystring)
        query = queryparser.parse_query(querystring,
                xapian.QueryParser.FLAG_WILDCARD | xapian.QueryParser.FLAG_BOOLEAN | xapian.QueryParser.FLAG_LOVEHATE)
        logger.debug(query);

        # Use an Enquire object on the database to run the query
        enquire = xapian.Enquire(self.db)
        #enquire.set_sort_by_relevance_then_value(1, False)
        #enquire.set_sort_by_relevance()
        enquire.set_query(query)

        # And print out something about each match
        matches = []
        mset = enquire.get_mset(offset, pagesize)
        for match in mset:
            doc = match.document
            meta = json.loads(doc.get_data())
            matches.append({
                'rank': match.rank + 1,
                'key': doc.get_value(0),
                'created': decode_sortable_date(doc.get_value(1)),
                'tags': meta.get('tag', [])
            })

        result = {
            'matches': matches,
            'total': mset.get_matches_estimated()
        }

        # Finally, make sure we log the query and displayed results
        return result
