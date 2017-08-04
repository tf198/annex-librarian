import xapian
import time
import logging
import json
from librarian.backends import terms

logger = logging.getLogger(__name__)

ISO_8601 = "%Y-%m-%dT%H:%M:%S"

DB_VERSION = "{0}.{1}".format(terms.SCHEMA_VERSION, 1)

STEMMING = xapian.QueryParser.STEM_SOME

def encode_sortable_date(d):
    try:
        t = time.strptime(d, ISO_8601)
        n = -int(time.mktime(t))
    except:
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

        self.term_generator = xapian.TermGenerator()
        self.term_generator.set_stemmer(xapian.Stem("en"))
        self.term_generator.set_stemming_strategy(STEMMING)

        self.query_parser = xapian.QueryParser()
        self.query_parser.set_stemmer(xapian.Stem("en"))
        self.query_parser.set_stemming_strategy(STEMMING)

        for field, prefix in terms.FREE_PREFIXES:
            self.query_parser.add_prefix(field, prefix)
        for field, prefix in terms.BOOLEAN_PREFIXES:
            self.query_parser.add_boolean_prefix(field, prefix)

    def _check_version(self):
        current = self._db.get_metadata('db:version')
        logger.debug("Database version: %s", current)
        if current and current != DB_VERSION:
            raise RuntimeError("Need to upgrade database to " + DB_VERSION)

    @property
    def db(self):
        if self._db is None:
            self._db = xapian.Database(self.path)
            self._check_version()

        return self._db

    def set_writable(self, clear=False):
        if self._db is not None:
            self._db.close()

        flags = xapian.DB_CREATE_OR_OVERWRITE if clear else xapian.DB_CREATE_OR_OPEN

        self._db = xapian.WritableDatabase(self.path, flags)
        self._check_version()

        
    def unset_writable(self):

        if self._db is not None:
            self._db.set_metadata('db:version', DB_VERSION)
            self._db.close()

        self._db = None

    def close(self):
        self.unset_writable()

    def get_value(self, key):
        return self.db.get_metadata(key)

    def set_value(self, key, value):
        return self.db.set_metadata(key, value)

    def update_data(self, key, section, info):

        try:
            data = self.get_data(key)
        except KeyError:
            data = {}

        # dont re-index if no changes
        if data.get(section, {}) == info:
            logger.debug("No changes")
            return

        if info:
            data[section] = info
        else:
            del(data[section])

        self.put_data(key, data)

    def put_data(self, key, data):

        try:
            sortvalue = encode_sortable_date(data['git-annex']['date'][0]);
        except KeyError:
            sortvalue = encode_sortable_date('');

        logger.debug("%r", data)

        doc = xapian.Document()
        self.term_generator.set_document(doc)

        boolean_terms = set()

        for section, info in data.items():
            if section[0] == '_': continue

            boolean_terms.add('B{0}'.format(section))
            for field, values in info.items():

                if not isinstance(values, (list, tuple)):
                    values = [values]

                if field == 'date':
                    try:
                        parts = values[0].split('T')[0].split('-')
                        boolean_terms.add('Y{0}'.format(parts[0]))
                        boolean_terms.add('M{0}{1}'.format(parts[0], parts[1]))
                        boolean_terms.add('D{0}{1}{2}'.format(parts[0], parts[1], parts[2]))
                        continue
                    except IndexError:
                        pass

                # prepare boolean prefixed terms
                if field in terms.BOOLEAN_TERMS:
                    field = terms.BOOLEAN_TERMS[field]
                    for value in values:
                        if value: boolean_terms.add(field + value.lower())
                elif field in terms.FREE_TERMS:
                    field = terms.FREE_TERMS[field]

                if field in terms.SKIP_FREE:
                    continue

                for value in values:
                    for word in value.split():
                        self.term_generator.index_text(word)
                    self.term_generator.increase_termpos()

        # add the boolean terms after the 
        for t in boolean_terms:
            doc.add_boolean_term(t)

        doc.set_data(json.dumps(data))
        doc.add_value(0, key)
        doc.add_value(1, sortvalue)

        idterm = "QK{0}".format(key)
        doc.add_boolean_term(idterm)

        #print([ x.term for x in doc.termlist() ])

        self.db.replace_document(idterm, doc)

    def get_data(self, key):
        term = "QK{0}".format(key)

        matches = list(self.db.postlist(term))
        if len(matches) > 1: raise KeyError("Key is not unique!");
        if len(matches) == 0: raise KeyError("Key not found");

        docid = matches[0].docid
        data = json.loads(self.db.get_document(docid).get_data())
        data['_docid'] = docid
        return data

    def alldocs(self, offset=0, pagesize=10):

        return self.search(None, offset, pagesize)

        items = self.db.allterms('QK')

        matches = [ {'key': i.term[2:]} for i in items ]
        total = len(matches)

        if offset: matches = matches[offset:]
        if pagesize: matches = matches[:pagesize]
        
        return {'total': total, 'matches': matches} 

    def search(self, querystring, offset=0, pagesize=10):

        if querystring:
            query = self.query_parser.parse_query(querystring,
                    xapian.QueryParser.FLAG_PURE_NOT | xapian.QueryParser.FLAG_WILDCARD | xapian.QueryParser.FLAG_BOOLEAN | xapian.QueryParser.FLAG_LOVEHATE)
        else:
            query = xapian.Query.MatchAll


        # allow for re-open
        retries = 2
        while retries:
        
            # Use an Enquire object on the database to run the query
            enquire = xapian.Enquire(self.db)
            enquire.set_sort_by_relevance_then_value(1, False)
            enquire.set_collapse_key(0)
            enquire.set_query(query)

            try:
                mset = enquire.get_mset(offset, pagesize)
                break
            except xapian.DatabaseModifiedError:
                logger.debug("Database error - retrying")
                self._db = None

        matches = []
        for match in mset:
            doc = match.document
            matches.append({
                'rank': match.rank + 1,
                'key': doc.get_value(0),
                'date': decode_sortable_date(doc.get_value(1)),
            })

        result = {
            'matches': matches,
            'total': mset.get_matches_estimated(),
        }

        logger.info("%r => %s [%d]", querystring, query, result['total'])
        # Finally, make sure we log the query and displayed results
        return result

    def field_cloud(self, field):


        prefix = terms.BOOLEAN_TERMS.get(field, terms.FREE_TERMS.get(field))
        if prefix is None:
            raise KeyError("Unknown field: %s" % field)
        c = len(prefix)

        result = [ (x.term[c:], x.termfreq) for x in self.db.allterms(prefix) ]

        return result
