from ast import literal_eval
import pandas
from pandas.core.frame import DataFrame
from elasticsearch import Elasticsearch
import config

def preprocess(df: DataFrame) -> DataFrame:
    df['certificate_price'] = df['certificate_price'].fillna(df['certificate_price'].mean())
    df = df.fillna('NaN')
    return df

def load_data() -> DataFrame:
    data = pandas.read_csv(config.DATA_PATH, sep=';')
    return preprocess(data.loc[:, data.columns[1]:])

def get_es_client() -> Elasticsearch:
    return Elasticsearch(
        hosts=config.ES_HOSTS,
        port=config.ES_PORT
    )

def index_data(df: DataFrame) -> None:
    client = get_es_client()
    """
        we want to index data in multiple indexes:
            platform-<platform_name>:
                main indexer
            presenter-<presenter_name>:
                revert index for presenter
            category-<category_name>:
                revert index for category
                has all of the courses of an category
                may have duplicate data in each index
            subtitle-<subtitle-name>:
                revert index for subtitle
                may have duplicate data

        course data structure:
            {
                name
                presenter
                platform
                language
                level
                duration
                url
                certificate_price
                categories
                subtitles
                subtitles_count
            }

    """
    for i, course in df.iterrows():
        print('*** inserting course in es ***')
        doc = {
            'presenters':literal_eval(course['presenter'])\
                if course['presenter'] else [],
            'platform':course['platform'],
            'subtitles':literal_eval(course['subtitles']),
            'categories':literal_eval(course['categories']),
            'subtitles_count':len(literal_eval(course['subtitles'])),
        }
        data = {
            **course.to_dict(), 
            'subtitles_count': doc['subtitles_count']
        }
        # index on platform
        r = client.index(
            index=config.INDEX_PREFIX + 'platform-' + 
                    doc['platform'].replace(' ', '_').replace('/', '_').replace('#', '_').lower(),
            document=data,
        )
        # index on presenter
        for presenter in doc['presenters']:
            r = client.index(
                index=config.INDEX_PREFIX + 'presenter-'
                    +presenter.replace(' ', '_').replace('/', '_').replace('#', '_').lower(),
                document=data,
            )
        # index on category
        for category in doc['categories']:
            r = client.index(
                index=config.INDEX_PREFIX + 'category-'
                    +category.replace(' ', '_').replace('/', '_').replace('#', '_').lower(),
                document=data
            )
        # index on subtitle 
        for sub in doc['subtitles']:
            r = client.index(
                index=config.INDEX_PREFIX+'subtitle-'
                    +sub.replace(' ', '_').replace('/', '_').replace('#', '_').lower(),
                document=data
            )

if __name__ == '__main__':
    df = load_data()
    index_data(df)