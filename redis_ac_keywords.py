import redis

class RedisACKeywords(object):
    '''
    (1) Efficient String Matching: An Aid to Bibliographic Search
    (2) Construction of Aho Corasick Automaton in Linear Time for Integer Alphabets
    '''
    # %s is name
    KEYWORD_KEY='{}:keyword'
    PREFIX_KEY='{}:prefix'
    SUFFIX_KEY='{}:suffix'

    # %s is keyword
    OUTPIUT_KEY='{}:output'
    NODE_KEY='{}:node'

    def __init__(self, host='localhost', port=6379, db=12, name='RedisACKeywords', encoding='utf8'):
        '''
        db: 7+5 because 1975
        '''
        self.encoding = encoding

        self.client = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        self.client.ping()

        self.name = name

        # Init trie root
        self.client.zadd(self.PREFIX_KEY.format(self.name), {'': 1.0})


    def add(self, keyword):
        keyword = keyword.strip().lower()

        # Add keyword in keyword set
        self.client.sadd(self.KEYWORD_KEY.format(self.name), keyword)

        self._build_trie(keyword)

        num = self.client.scard(self.KEYWORD_KEY.format(self.name))
        return num

    def remove(self, keyword):
        keyword = keyword.strip().lower()

        self._remove(keyword)

        self.client.srem(self.KEYWORD_KEY.format(self.name), keyword)
        num = self.client.scard(self.KEYWORD_KEY.format(self.name))
        return num

    def find(self, text):
        ret = []
        i = 0
        state = ''
        while i < len(text):
            c = text[i]
            next_state = self._go(state, c)
            if next_state is None:
                next_state = self._fail(state)
                _next_state = self._go(next_state, c)
                if _next_state is None:
                    _next_state = self._fail(next_state + c)
                next_state = _next_state

            pos = i - 1
            outputs = self._output(state)
            ret.extend(outputs)

            state = next_state
            i += 1

        # check last state
        pos = i - 1
        outputs = self._output(state)
        ret.extend(outputs)
        return ret

    def flush(self):
        keywords = self.client.smembers(self.KEYWORD_KEY.format(self.name))
        for keyword in keywords:
            self.client.delete(self.OUTPIUT_KEY.format(keyword))
            self.client.delete(self.NODE_KEY.format(keyword))
        self.client.delete(self.PREFIX_KEY.format(self.name))
        self.client.delete(self.SUFFIX_KEY.format(self.name))
        self.client.delete(self.KEYWORD_KEY.format(self.name))

    def info(self):
        return {
            'keywords':self.client.scard(self.KEYWORD_KEY.format(self.name)),
            'nodes':self.client.zcard(self.PREFIX_KEY.format(self.name)),
        }

    def suggest(self, input_):
        ret = []
        rank = self.client.zrank(self.PREFIX_KEY.format(self.name), input_)
        a = self.client.zrange(self.PREFIX_KEY.format(self.name), rank, rank)
        while a:
            node = a[0]
            if node.startswith(input_) and self.client.sismember(self.KEYWORD_KEY.format(self.name), node):
                ret.append(node)
            rank += 1
            a = self.client.zrange(self.PREFIX_KEY.format(self.name), rank, rank)
        return ret

    def _go(self, state, c):
        next_state = state + c
        i = self.client.zscore(self.PREFIX_KEY.format(self.name), next_state)
        if i is None:
            return None
        return next_state

    def _build_trie(self, keyword):
        l = len(keyword)
        for i in range(l): # trie depth increase
            prefix = keyword[:i+1] # every prefix is a node
            _suffix = ''.join(reversed(prefix))
            if self.client.zscore(self.PREFIX_KEY.format(self.name), prefix) is None: # node does not exist
                self.client.zadd(self.PREFIX_KEY.format(self.name), {prefix: 1.0})
                self.client.zadd(self.SUFFIX_KEY.format(self.name), {_suffix: 1.0}) # reversed suffix node

                self._rebuild_output(_suffix)
            else:
                if (self.client.sismember(self.KEYWORD_KEY.format(self.name), prefix)): # node may change, rebuild affected nodes
                    self._rebuild_output(_suffix)

    def _rebuild_output(self, _suffix):
        rank = self.client.zrank(self.SUFFIX_KEY.format(self.name), _suffix)
        a = self.client.zrange(self.SUFFIX_KEY.format(self.name), rank, rank)
        while a:
            suffix_ = a[0]
            if suffix_.startswith(_suffix):
                state = ''.join(reversed(suffix_))
                self._build_output(state)
            else:
                break
            rank += 1 # TODO: Binary search?
            a = self.client.zrange(self.SUFFIX_KEY.format(self.name), rank, rank)

    def _build_output(self, state):
        outputs = []
        if self.client.sismember(self.KEYWORD_KEY.format(self.name), state):
            outputs.append(state)
        fail_state = self._fail(state)
        fail_output = self._output(fail_state)
        if fail_output:
            outputs.extend(fail_output)
        if outputs:
            self.client.sadd(self.OUTPIUT_KEY.format(state), *outputs)
            for k in outputs:
                self.client.sadd(self.NODE_KEY.format(k), state) # ref node for delete keywords in output

    def _fail(self, state):
        # max suffix node will be the failed node
        next_state = ''
        for i in range(1, len(state)+1): # depth increase
            next_state = state[i:]
            if self.client.zscore(self.PREFIX_KEY.format(self.name), next_state):
                break
        return next_state

    def _output(self, state):
        return [k for k in self.client.smembers(self.OUTPIUT_KEY.format(state))]

    def debug_print(self):
        keywords = self.client.smembers(self.KEYWORD_KEY.format(self.name))
        if keywords:
            print('-' + self.KEYWORD_KEY.format(self.name) + ' '.join(keywords))
        prefix = self.client.zrange(self.PREFIX_KEY.format(self.name), 0, -1)
        if prefix:
            prefix[0] = '.'
            print ('-' + self.PREFIX_KEY.format(self.name) + ' '.join(prefix))
        suffix = self.client.zrange(self.SUFFIX_KEY.format(self.name), 0, -1)
        if suffix:
            print('-' + self.SUFFIX_KEY.format(self.name) + ' '.join(suffix))

        outputs = []
        for node in prefix:
            output = self._output(node)
            outputs.append(output)
        if outputs:
            print('-' + 'outputs' + outputs)

        nodes = []
        for keyword in keywords:
            keyword_nodes = self.client.smembers(self.NODE_KEY.format(keyword))
            nodes.append(keyword_nodes)
        if nodes:
            print('-' + 'nodes' + nodes)

    def _remove(self, keyword):
        nodes = self.client.smembers(self.NODE_KEY.format(keyword))
        for node in nodes:
            self.client.srem(self.OUTPIUT_KEY.format(node), keyword)
        self.client.delete(self.NODE_KEY.format(keyword))

        # remove nodes if need
        l = len(keyword)
        for i in range(l, 0, -1): # depth decrease
            prefix = keyword[:i]
            if self.client.sismember(self.KEYWORD_KEY.format(self.name), prefix) and i!=l:
                break
            _suffix = ''.join(reversed(prefix))

            rank = self.client.zrank(self.PREFIX_KEY.format(self.name), prefix)
            if rank is None:
                break
            a = self.client.zrange(self.PREFIX_KEY.format(self.name), rank+1, rank+1)
            if a:
                prefix_ = a[0]
                if not prefix_.startswith(prefix):
                    self.client.zrem(self.PREFIX_KEY.format(self.name), prefix)
                    self.client.zrem(self.SUFFIX_KEY.format(self.name), _suffix)
                else:
                    break
            else:
                self.client.zrem(self.PREFIX_KEY.format(self.name), prefix)
                self.client.zrem(self.SUFFIX_KEY.format(self.name), _suffix)

if __name__ == '__main__':
    keywords = RedisACKeywords(name='test')

    ks = ['her', 'he', 'his']
    for k in ks:
        keywords.add(k)

    input_ = 'he'
    print('suggest {}: {}'.format(input_, keywords.suggest(input_))) # her, he

    text = 'ushers'
    print('text: {}'.format(text))
    print('keywords: {} added. {}'.format(' '.join(ks), keywords.find(text))) # her, he

    ks2 = ['she', 'hers']
    for k in ks2:
        keywords.add(k)
    print('keywords: {} added. {}'.format(' '.join(ks2), keywords.find(text))) # her, he, she, hers

    keywords.add('h')
    print('h added. {}'.format(keywords.find(text))) # her, he, she, hers, h

    keywords.remove('h')
    print('h removed. {}'.format(keywords.find(text))) # her, he, she, hers

    keywords.flush()
    print('flushed. {}'.format(keywords.find(text))) # []