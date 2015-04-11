import json
from multiprocessing import Pool


def index_json(filename):
    index, reversed_index = {}, {}

    with open(filename) as fp:
        for line in fp:
            _ = json.loads(line)

            if _['user_id'] not in index:
                index[_['user_id']] = set()
            index[_['user_id']].add(_['product_id'])

            if _['product_id'] not in reversed_index:
                reversed_index[_['product_id']] = set()
            reversed_index[_['product_id']].add(_['user_id'])

    return index, reversed_index


def map_index(data):
    results = []
    for user in data[1]:
        results.extend([v for v in index[user] if v != data[0]])
    return (data[0], results)


if __name__ == '__main__':
    index_json('user-product_map.json')

    pool = Pool()
