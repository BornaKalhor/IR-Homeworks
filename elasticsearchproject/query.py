import os
import json

import config
from indexer import get_es_client

def log(query, res):
    print(f"""
        Query:
            {query}
        
        Result:
            {res}
    """)

def save(res, name):
    with open(os.path.join('results', name + '.json'), 'w') as f:
        json.dump(res, f)

if __name__ == '__main__':
    client = get_es_client()
    """        q1         """
    query = {
        'query': {
            'match_all': {}
        }
    }
    r = client.count(
        index=config.INDEX_PREFIX + 'platform*',
        body=query,
    )
    log(query, r)
    save(r, 'q1')
    save(query, 'q1.query')
    """        q2         """
    query = {
        'query': {
            'match': {
                'platform': 'edX',
            }
        },
        'from': 0,
        'size': 1000,
    }
    r = client.search(
        index=config.INDEX_PREFIX + 'category-science',
        body=query,
    )
    log(query, r)
    save(r, 'q2')
    save(query, 'q2.query')
    """         q3          """
    query = {
        'size': 1,
        'sort': [
            {'certificate_price': 'desc'}
        ],
        'query': {
            'bool': {
                'must': [
                    {
                        'match': {
                            'level': 'Intermediate',
                        }
                    },{
                        'match': {
                            'language': 'English'
                        }
                    }
                ]
            }
        }
    }
    r = client.search(
        index=config.INDEX_PREFIX + 'platform*',
        body=query,
    )
    log(query, r)
    save(r, 'q3')
    save(query, 'q3.query')
    """         q4          """
    query = {
        'size': 0,
        'aggs': {
            'byindex': {
                'terms': {
                    'field': '_index',
                    'size': 100000
                },
                'aggs': {
                    'avg_cert': {'avg': {'field': 'certificate_price'}}
                }
            },
        }
    }
    r = client.search(
        index=config.INDEX_PREFIX + 'category-*',
        body=query,
    )
    log(query, r)
    save(r, 'q4')
    save(query, 'q4.query')
    """         q5          """
    query = {
        'size': 0,
        'aggs': {
            'byindex': {
                'terms': {
                    'field': '_index',
                    'size': 1
                },
            },
        }
    }
    r = client.search(
        index=config.INDEX_PREFIX + 'subtitle-*',
        body=query,
    )
    log(query, r)
    save(r, 'q5')
    save(query, 'q5.query')
    """         q6          """
    query = {
        'size': 0,
        'aggs': {
            'byindex': {
                'terms': {
                    'field': '_index',
                    'size': 1
                },
            },
        }
    }
    r = client.search(
        index=config.INDEX_PREFIX + 'platform-*',
        body=query,
    )
    log(query, r)
    save(r, 'q6')
    save(query, 'q6.query')
    """         q7          """
    query = {
        'size': 0,
        'aggs': {
            'byindex': {
                'terms': {
                    'field': '_index',
                    'size': 1
                },
            },
        }
    }
    r = client.search(
        index=config.INDEX_PREFIX + 'presenter-*',
        body=query,
    )
    log(query, r)
    save(r, 'q7')
    save(query, 'q7.query')
    """        q8       """
    query = {
        'query': {
            'range': {
                'certificate_price': {
                    'gte': 60,
                    'lte': 90,
                }
            }
        }
    }
    r = client.count(
        index=config.INDEX_PREFIX + 'platform*',
        body=query,
    )
    log(query, r)
    save(r, 'q8')
    save(query, 'q8.query')
    """        q9       """
    query = {
        'size': 0,
        'aggs': {
            'platform_grouping': {
                'terms': {
                    'field': '_index',
                    'size': 100000
                },
                'aggs': {
                    'subs_grouping': {
                        'top_hits': {
                            'sort': [
                                {
                                    'subtitles_count': {
                                        'order': 'desc'
                                    }
                                }
                            ],
                            # '_source': {
                            #     'includes': []
                            # },
                            'size': 3,
                        }
                        # 'terms': {
                        #     'field': 'subtitles_count',
                        #     'size': 3,
                        # }
                    }
                }
            },
        }
    }
    r = client.search(
        index=config.INDEX_PREFIX + 'category-*',
        body=query,
    )
    log(query, r)
    save(query, 'q9.query')
    save(r, 'q9')