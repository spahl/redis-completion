try:
    import simplejson as json
except ImportError:
    import json

from redis_completion.stop_words import STOP_WORDS as _STOP_WORDS


# aggressive stop words will be better when the length of the document is longer
AGGRESSIVE_STOP_WORDS = _STOP_WORDS

# default stop words should work fine for titles and things like that
DEFAULT_STOP_WORDS = set(['a', 'an', 'of', 'the'])


class BaseEngine(object):
    def __init__(self, min_length=2, prefix='ac', stop_words=None):
        self.min_length = min_length
        self.prefix = prefix
        self.stop_words = (stop_words is None) and DEFAULT_STOP_WORDS or stop_words

    def flush(self):
        raise NotImplementedError

    def store(self, obj_id, title=None, data=None):
        raise NotImplementedError

    def store_json(self, obj_id, title, data_dict):
        return self.store(obj_id, title, json.dumps(data_dict))

    def remove(self, obj_id):
        raise NotImplementedError

    def search(self, phrase, limit=None, filters=None, mappers=None):
        raise NotImplementedError

    def search_json(self, phrase, limit=None, filters=None, mappers=None):
        if not mappers:
            mappers = []
        mappers.insert(0, json.loads)
        return self.search(phrase, limit, filters, mappers)
