from src.common.mapreduce_util import mapreduce
from src.common.mutation_util import flatten
from duckdb import DuckDBPyConnection
from src.common.spark_util import create_spark_session
from typing import Dict, List, Tuple
from src.resources.conf import SPARK_MASTER, RECORDS_EXAMPLE_DEDUPE_SETTING_PATH, RECORDS_EXAMPLE_CSV_PATH
from src.example.exception import spark_execution_exception
from src.common.load_data import read_data, to_abs_fname
import dedupe
import os
import logging

logger = logging.getLogger(__name__)


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
        with create_spark_session("word_count", spark_master=SPARK_MASTER) as spark:
            # spark computation
            documents_rdd = spark.sparkContext.parallelize(partition, 2)
            wordcount_result = documents_rdd \
                .flatMap(lambda docid_and_title: [(word, 1) for word in docid_and_title[1].split(" ")]) \
                .reduceByKey(lambda count1, count2: count1 + count2) \
                .collect()
    except Exception as e:
        logger.error(e, exc_info=True)
        raise spark_execution_exception

    # sort list of tuples reversely based on value (2nd element) and transform to dict
    wordcount_dict = dict(sorted(wordcount_result, key=lambda x: x[1], reverse=True))
    return wordcount_dict


def blocking_records(conn: DuckDBPyConnection) -> Dict:
    # load data
    input_file = RECORDS_EXAMPLE_CSV_PATH
    data_d = read_data(to_abs_fname(input_file))

    # load dedupe setting
    settings_file = RECORDS_EXAMPLE_DEDUPE_SETTING_PATH
    with open(to_abs_fname(settings_file), 'rb') as f:
        deduper = dedupe.StaticDedupe(f)

    # apply blocking rules on spark
    try:
        with create_spark_session("dedupe_blocking", spark_master=SPARK_MASTER) as spark:
            sc = spark.sparkContext

            # broadcast pretrained deduper
            broadcast_predicates = sc.broadcast(deduper.predicates)

            # define map function
            def f_map(id_record) -> Tuple:
                """
                apply predicates to incoming record, each record can have multiple block_key
                """
                record_id, instance = id_record
                predicates = [
                    (":" + str(i), predicate) for i, predicate in enumerate(broadcast_predicates.value)
                ]
                for pred_id, predicate in predicates:
                    block_keys = predicate(instance, target=False)
                    for block_key in block_keys:
                        yield record_id, block_key

            # blocking_list (record_id, block_key)
            original_records_list = list(data_d.items())
            records_rdd = sc.parallelize(original_records_list, 2)
            blocking_list = records_rdd \
                .flatMap(f_map) \
                .collect()

            # compute pair table based on block_key
            # we make sure that each pair only appear once
            columns = ["record_id", "block_key"]
            df_blocking = spark.createDataFrame(blocking_list, columns)
            df_blocking.createOrReplaceTempView("blocking_map")
            rdd_pairs = spark.sql("""
                SELECT DISTINCT a.record_id, b.record_id
                                   FROM blocking_map a
                                   INNER JOIN blocking_map b
                                   USING (block_key)
                                   WHERE a.record_id < b.record_id
            """).collect()
            list_pairs = [(row[0], row[1]) for row in rdd_pairs]
            # TODO store pairs in duckdb for later processing
    except Exception as e:
        logger.error(e, exc_info=True)
        raise spark_execution_exception
    response = {
        "nb_original_pairs": int(len(original_records_list) * (len(original_records_list) - 1) / 2),
        "nb_block_pairs": len(list_pairs),
        "predicates": [str(predicate) for predicate in deduper.predicates],
        "record_pairs": list_pairs[:50]
    }
    return response
