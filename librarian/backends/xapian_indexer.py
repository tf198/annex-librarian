import xapian
import time
import logging
import json
import os
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

def term_date(d):
    try:
        return ''.join(d.split('T')[0].split('-'))
    except:
        raise ValueError("Failed to parse date {0}".format(d))

def get_dotted(d, key):
    try:
        parts = key.split('.')
        for p in parts[:-1]:
            d = d[p]
        return d[parts[-1]]
    except:
        raise KeyError("No such key: " + key)

def first_of(d, *args):
    for a in args:
        try:
            return get_dotted(d, a)
        except KeyError:
            pass
    raise KeyError("No provided key in dict")

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

    def update_data(self, key, info):

        try:
            data = self.get_data(key)
        except KeyError:
            data = {
                'meta': {'state': 'untagged'},
                'info': {'state': 'noinfo'}
            }

        # dont re-index if no changes
        if data == info:
            logger.debug("No changes")
            return

        data.update(info)

        self.put_data(key, data)

    def put_data(self, key, data):

        boolean_terms = set()
        
        try:
            data['_date'] = first_of(data,
                    'meta.date',
                    'image.created',
                    'log.added')
            if isinstance(data['_date'], (list, tuple)):
                data['_date'] = data['_date'][0]
            boolean_terms.add('D{0}'.format(term_date(data['_date'])))
        except KeyError:
            data['_date'] = ''
        
        logger.debug("Sort key: %r", data['_date'])
        sortvalue = encode_sortable_date(data['_date'])

        doc = xapian.Document()
        self.term_generator.set_document(doc)


        paths = data.get('paths')

        if data.get('paths'):

            for section in data:
           
                if section[0] == '_': continue

                if data[section] is None: continue

                for field, values in data[section].items():

                    # handle paths section
                    if section == 'paths':
                        boolean_terms.add('{0}{1}'.format(terms.BOOLEAN_TERMS['branch'], field.lower()))
                        p, _ = os.path.splitext(values)
                        for t in p.split(os.sep):
                            boolean_terms.add('{0}{1}'.format(terms.BOOLEAN_TERMS['path'], t.lower()))
                        continue

                    # handle arrays and straight values
                    if not isinstance(values, (list, tuple)):
                        values = [values]

                    # prepare boolean prefixed terms
                    if field in terms.BOOLEAN_TERMS:
                        field = terms.BOOLEAN_TERMS[field]

                        for value in values:
                            if value: boolean_terms.add(field + value.lower())
                    elif field in terms.FREE_TERMS:
                        field = terms.FREE_TERMS[field]

                    # some fields shouldn't be added to full text index
                    if field in terms.BOOLEAN_ONLY:
                        continue

                    # handle dates
                    if field[0] == 'D':
                        boolean_terms.add('{0}{1}'.format(field, term_date(values[0])))
                        continue

                    # full-text indexing
                    for value in values:
                        for word in value.split():
                            self.term_generator.index_text(word)
                        self.term_generator.increase_termpos()
       
            boolean_terms.add('XSok')
        else:
            boolean_terms.add('XSdropped')

        # add the boolean terms after the free terms 
        for t in boolean_terms:
            doc.add_boolean_term(t)

        doc.set_data(json.dumps(data))
        doc.add_value(0, key)
        doc.add_value(1, sortvalue)

        idterm = "QK{0}".format(key)
        doc.add_boolean_term(idterm)

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Data: %r", data)
            logger.debug("Terms: %r", [ x.term for x in doc.termlist() ])

        self.db.replace_document(idterm, doc)

    def get_data(self, key, include_terms=False):
        term = "QK{0}".format(key)

        matches = list(self.db.postlist(term))
        if len(matches) > 1: raise KeyError("Key is not unique!");
        if len(matches) == 0: raise KeyError("Key not found");

        docid = matches[0].docid
        doc = self.db.get_document(docid)
        data = json.loads(doc.get_data())
        data['_docid'] = docid

        if include_terms:
            data['_terms'] = [ x.term for x in doc.termlist() ]

        return data

    def alldocs(self, offset=0, pagesize=10):

        return self.search("state:ok", offset, pagesize)


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
            'start': offset+1,
            'end': offset + len(matches),
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
