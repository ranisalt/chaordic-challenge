import json
from multiprocessing import Pool


index = None


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


def write_json(filename, output):
    with open(filename, 'w') as fp:
        json.dump(output, fp, indent=2)


def map_index(data):
    results = []
    for user in data[1]:
        results.extend([v for v in index[user] if v != data[0]])
    return (data[0], results)


def reduce_index(data):
    results = {}
    for product in data[1]:
        results[product] = results[product] + 1 if product in results else 1
    total = float(len(data[1]))
    return (data[0], {product: results[product] / total for product in results})


if __name__ == '__main__':
    index, reversed_index = index_json('user-product_map.min.json')

    pool = Pool()

    products_by_same_users = pool.map(map_index, reversed_index.items())
    similarity_index = pool.map(reduce_index, products_by_same_users)
