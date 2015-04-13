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
            try:
               _ = json.loads(line)
            except ValueError:
                continue

            # Create index of users->products
            if _['user_id'] not in index:
                index[_['user_id']] = set()
            index[_['user_id']].add(_['product_id'])

            # Create reversed index of products->users
            if _['product_id'] not in reversed_index:
                reversed_index[_['product_id']] = set()
            reversed_index[_['product_id']].add(_['user_id'])

    return index, reversed_index


def write_json(filename, output):
    # Perform sorting before dump
    ordered = sorted(output, key=lambda obj: obj['reference_product_id'])

    with open(filename, 'w') as fp:
        json.dump(ordered, fp, indent=2)


def map_index(data):
    """
    Map a product to every other product that is related to the same users
    :param data: tuple (product, users)
    :return: tuple of (products, other products from the same users)
    """
    results = []
    for user in data[1]:
        # This smarty stuff should filter out a product mapping to itself
        results.extend([v for v in index[user] if v != data[0]])
    return (data[0], results)


def reduce_index(data):
    """
    Reduce a product related products to a ratio of similarity by repetition
    :param data: tuple (product, other products)
    :return: tuple of (product, dict of {other product, ratio})
    """
    results = {}
    for product in data[1]:
        # Just check that the index exists before incrementing
        results[product] = results[product] + 1 if product in results else 1

    # Casted to float since Python does not cast integer division to float
    total = float(len(data[1]))

    # I LOVE COMPREHENSIONS
    return (data[0], {product: results[product] / total for product in results})


def format_output(data):
    """
    Format the result according to the requirements by recruitment
    :param data: the stuff returned by the function above
    :return: a new dict with formatted stuff
    """
    object = {
        'reference_product_id': data[0],
        'recommendations': [],
    }
    for product in data[1]:
        object['recommendations'].append({
            'product_id': product,
            'similarity': data[1][product]
        })

    # Sort recommendations by product id. It wasn't really clear if it should be
    # sorted by id or similarity level, in latter case you should just change
    # the sorting key to be "obj['similarity']". Easy peasy!
    object['recommendations'].sort(key=lambda obj: obj['product_id'])
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
