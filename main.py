import json


def index_json(filename):
    index, reverse_index = {}, {}

    with open(filename) as fp:
        for line in fp:
            _ = json.loads(line)

            if _['user_id'] not in index:
                index[_['user_id']] = set()
            index[_['user_id']].add(_['product_id'])

            if _['product_id'] not in reverse_index:
                reverse_index[_['product_id']] = set()
            reverse_index[_['product_id']].add(_['user_id'])

    return index, reverse_index


if __name__ == '__main__':
    index_json('user-product_map.json')
