import urllib2
from redis_completion import RedisEngine

engine = RedisEngine(prefix='stocks')

def load_data():
    url = 'http://media.charlesleifer.com/downloads/misc/NYSE.txt'
    contents = urllib2.urlopen(url).read()
    for row in contents.splitlines()[1:]:
        ticker, company = row.split('\t')
        engine.store_json(ticker, company, {'ticker': ticker, 'company': company}) # id, search phrase, data

def search(p, **kwargs):
    return engine.search_json(p, **kwargs)

if __name__ == '__main__':
    engine.flush()
    print 'Loading data (may take a few seconds...)'
    load_data()

    print 'Search data by typing a partial phrase, like "uni sta"'
    print 'Type "q" at any time to quit'

    while 1:
        cmd = raw_input('? ')
        if cmd == 'q':
            break
        results = search(cmd)
        print 'Found %s matches' % len(results)
        for result in results:
            print '%s: %s' % (result['ticker'], result['company'])
