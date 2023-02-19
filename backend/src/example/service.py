from src.common.mapreduce_util import mapreduce
from src.common.mutation_util import flatten
from duckdb import DuckDBPyConnection
from src.common.spark_util import create_spark_session
from typing import Dict
from src.resources.conf import SPARK_MASTER

from src.example.exception import spark_execution_exception


def compute_word_count(conn: DuckDBPyConnection) -> Dict:
    def wordcount_f_map(doc_id, text):
        output_tuples = []
        for word in text.split(" "):
            output_tuples.append((word, 1))
        return output_tuples

    def wordcount_f_reduce(word, counts):
        total_count = 0
        for count in counts:
            total_count += count
        return [(word, total_count)]

    df_documents = conn.execute("select * from documents").df()
    # turn dataframe to list of tuples
    partition = list(df_documents.itertuples(index=False, name=None))
    partitioned_documents = [partition]
    # mapreduce output list of tuples: key=word, value=count
    wordcount_result = mapreduce(partitioned_documents, wordcount_f_map, wordcount_f_reduce, print_debug_text=False)
    # sort tuples reversely based on value (2nd element) and transform to dict
    wordcount_dict = dict(sorted(
        flatten(wordcount_result),
        key=lambda x: x[1],
        reverse=True
    ))
    return wordcount_dict


def compute_word_count_rdd(conn: DuckDBPyConnection) -> Dict:
    # load data from db
    df_documents = conn.execute("select * from documents").df()
    partition = list(df_documents.itertuples(index=False, name=None))

    # create spark session
    try:
        with create_spark_session("word_count", spark_master=SPARK_MASTER) as sc:
            # spark computation
            documents_rdd = sc.sparkContext.parallelize(partition, 2)
            wordcount_result = documents_rdd \
                .flatMap(lambda docid_and_title: [(word, 1) for word in docid_and_title[1].split(" ")]) \
                .reduceByKey(lambda count1, count2: count1 + count2) \
                .collect()
    except Exception:
        raise spark_execution_exception

    # sort list of tuples reversely based on value (2nd element) and transform to dict
    wordcount_dict = dict(sorted(wordcount_result, key=lambda x: x[1], reverse=True))
    return wordcount_dict
