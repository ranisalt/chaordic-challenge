#!/usr/bin/env python
""" Chaordic Challenge by Ranieri Althoff
This is a very simplified and lightweight item-based collaborative filtering.
What it does is to look on every user that is connected to a product (it is not
specified which is this connection, but it may be "has bought" or "has rated")
and look at other products that this user is also related to. By crossing this
information and then calculating a ratio based on "total times users are related
to this" / "total other items all users are also related" I get a similarity
value, then all dumped to a file.
"""

import argparse
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


def format_output(data):
    object = {
        'reference_product_id': data[0],
        'recommendations': [],
    }
    for product in data[1]:
        object['recommendations'].append({
            'product_id': product,
            'similarity': data[1][product]
        })
    return object


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help='input (user-product map) file')
    parser.add_argument('output', help='output file')
    args = parser.parse_args()

    index, reversed_index = index_json(args.input)

    pool = Pool()

    products_by_same_users = pool.map(map_index, reversed_index.items())
    similarity_index = pool.map(reduce_index, products_by_same_users)

    write_json(args.output, pool.map(format_output, similarity_index))
