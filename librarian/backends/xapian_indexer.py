import xapian
import time
import logging
import json
import os
import itertools
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

        for field, prefix in terms.STEMMED:
            self.query_parser.add_prefix(field, prefix)
        for field, prefix in terms.PREFIXED_UNSTEMMED:
            self.query_parser.add_prefix(field, prefix)
        for field, prefix in terms.PREFIXED_UNSTEMMED_BOOLEAN:
            self.query_parser.add_boolean_prefix(field, prefix)

    def _check_version(self):
        current = self._db.get_metadata('db:version').decode('utf-8')
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

    def exists(self, key):
        term = "QK{0}".format(key)

        c = len(list(self.db.postlist(term)))
        if c > 1: raise KeyError("Key is not unique!")
        return c == 1

    def get_value(self, key):
        return self.db.get_metadata(key).decode('utf-8')

    def set_value(self, key, value):
        return self.db.set_metadata(key, value)

    def get_data(self, key, include_terms=False):
        term = u"QK{0}".format(key)
        matches = list(self.db.postlist(term))
        if len(matches) > 1: raise KeyError("Key is not unique!");
        if len(matches) == 0: raise KeyError("Key not found");

        docid = matches[0].docid
        doc = self.db.get_document(docid)
        data = json.loads(doc.get_data())
        data['_docid'] = docid

        if include_terms:
            data['_terms'] = [ x.term.decode('utf-8') for x in doc.termlist() ]

        return data

    def get_or_create_data(self, key, include_terms=False):
        try:
            return self.get_data(key, include_terms)
        except KeyError:
            return {"meta": {"state": ["new", "untagged"]},
                    "librarian": {"inspector": ["none"]}}

    def update_data(self, key, info):

        data = self.get_or_create_data(key)
        data.update(info)
        self.put_data(key, data)

    def put_data(self, key, data):

        try:
            data['_date'] = first_of(data,
                    'meta.date',
                    'image.created',
                    'annex.added')
            if isinstance(data['_date'], (list, tuple)):
                data['_date'] = data['_date'][0]
        except KeyError:
            data['_date'] = ''
        
        logger.debug("Sort key: %r", data['_date'])
        sortvalue = encode_sortable_date(data['_date'])

        doc = xapian.Document()
        self.term_generator.set_document(doc)

        git = data.get('git', {})


        if git.get('branch'):

            # add the sort date
            d = term_date(data['_date'])
            doc.add_term('D' + d, 0)
            doc.add_term('Y' + d[:4], 0)
            doc.add_term(d[:4], 0)

            for branch, p in git.get('branch', {}).items():
                folder, filename = os.path.split(p)
                name, _ = os.path.splitext(filename)
                self.term_generator.index_text(name, 0, 'F')
                self.term_generator.index_text(name)
                self.term_generator.increase_termpos()
                for t in folder.split(os.sep):
                    if t:
                        doc.add_term("P" + t.lower(), 0)

            for section in data:
           
                if section[0] == '_': continue

                if data[section] is None: continue

                for field, values in data[section].items():

                    prefix = None

                    # handle arrays and straight values
                    if isinstance(values, (dict, )):
                        values = list(values)
                    if not isinstance(values, (list, tuple)):
                        values = [values]

                    # handle prefixed unstemmed boolean terms
                    if field in terms.PREFIXED_UNSTEMMED_BOOLEAN_TERMS:
                        field = terms.PREFIXED_UNSTEMMED_BOOLEAN_TERMS[field]

                        for value in values:
                            doc.add_term(field + value.lower(), 0)

                            # some terms should be added to the full text index
                            if field in terms.BOOLEAN_UNPREFIXED_STEMMED:
                                self.term_generator.index_text(value)
                                self.term_generator.increase_termpos()
                        continue

                    # handle prefixed unstemmed terms
                    if field in terms.PREFIXED_UNSTEMMED_TERMS:
                        field = terms.PREFIXED_UNSTEMMED_TERMS[field]

                        for value in values:
                            if field[0] == 'D':
                                value = term_date(value)
                            doc.add_term(field + value.lower(), 0)
                        continue

                    # handle free terms
                    if field in terms.STEMMED_TERMS:

                        for value in values:
                            self.term_generator.index_text(value, 1, terms.STEMMED_TERMS[field])
                            self.term_generator.index_text(value)
                            self.term_generator.increase_termpos()
            doc.add_term('XSok')
        else:
            doc.add_term('XSdropped')
       
        doc.set_data(json.dumps(data))
        doc.add_value(0, key)
        doc.add_value(1, sortvalue)

        idterm = "QK{0}".format(key)
        doc.add_boolean_term(idterm)

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Data: %r", data)
            logger.debug("Terms: %r", [ x.term for x in doc.termlist() ])

        self.db.replace_document(idterm, doc)

    def search(self, querystring, offset=0, pagesize=10, raw=False):

        if not querystring:
            querystring = "state:ok"

        logger.debug("QUERY: %s", querystring)
        if raw:
            query = xapian.Query(querystring)
        else:
            query = self.query_parser.parse_query(querystring,
                        xapian.QueryParser.FLAG_PURE_NOT | xapian.QueryParser.FLAG_WILDCARD | xapian.QueryParser.FLAG_BOOLEAN | xapian.QueryParser.FLAG_LOVEHATE)

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
                'key': doc.get_value(0).decode('utf-8'),
                'date': decode_sortable_date(doc.get_value(1)),
            })

        result = {
            'matches': matches,
            'start': offset+1 if len(matches) else 0,
            'end': offset + len(matches),
            'total': mset.get_matches_estimated(),
        }

        logger.info("%r => %s [%d]", querystring, query, result['total'])
        # Finally, make sure we log the query and displayed results
        return result

    def alldocs(self, offset=0, pagesize=10):

        return self.search(None, offset, pagesize)
    
    def field_cloud(self, field):

        prefix = terms.BOOLEAN_TERMS.get(field, terms.FREE_TERMS.get(field))
        if prefix is None:
            raise KeyError("Unknown field: %s" % field)
        c = len(prefix)

        result = [ (x.term[c:], x.termfreq) for x in self.db.allterms(prefix) ]

        return result
