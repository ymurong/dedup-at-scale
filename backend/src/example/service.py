from src.common.mapreduce_util import mapreduce
from src.common.mutation_util import flatten
from duckdb import DuckDBPyConnection


def compute_word_count(conn: DuckDBPyConnection) -> dict:
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
