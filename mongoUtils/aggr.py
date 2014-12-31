'''
Created on Dec 12, 2014

@author: nickmilon
'''


def group_counts(
        collection,
        field,
        match=None,  # match expression i.e. {'lang':'en'}
        sort={'count': -1}
        ):
    pl = []
    if match is not None:
        pl.append({'$match': match})

    pl.append({
              '$group': {'_id': '$'+field,
                         'count': {'$sum': 1}}
              })
    if sort is not None:
        pl.append({'$sort': sort})
    return collection.aggregate(pl)
