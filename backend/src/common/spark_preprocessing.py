from pyspark.sql import functions as F
from pyspark.sql.types import NullType
import pyspark


def lower_case(sdf: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """ lower case string type columns """
    col_type_dict = dict(sdf.dtypes)
    return sdf.select(
        [F.lower(col_name).alias(col_name) if (col_type_dict[col_name] == "string") else col_name
         for col_name in sdf.columns])


def abs_year(sdf: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """ year column sometimes have negative values """
    return sdf.select(
        [F.abs(col_name).alias(col_name) if col_name == "pyear" else col_name
         for col_name in sdf.columns])


def inversed_pauthor_ptitle(spark: pyspark.sql.SparkSession, sdf: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    sdf.createOrReplaceTempView("collection")
    sdf_results = spark.sql(
        """
        WITH t_wrong_pid as
        (
            (
            SELECT pid
            FROM collection c
            WHERE c.pauthor RLIKE '(?<!Jr)\.$'
            )
            UNION
            (
            SELECT pid
            FROM collection c
            WHERE c.pauthor RLIKE '^[^\|]+[^.]$'
            AND c.ptitle RLIKE '\|'
            )
            UNION
            (
            SELECT pid
            FROM collection c
            WHERE c.pauthor RLIKE '^[^\|]+[^.]$'
            AND c.ptitle RLIKE '^[^\|]+[^.]$'
            AND c.pauthor RLIKE ' is | and | a | in '
            )
        )
        (
        SELECT c.pid, pkey, ptitle as pauthor, peditor, pauthor as ptitle, pyear, paddress, ppublisher, pseries, pjournal,
        pbooktitle, ptype
        FROM collection c, t_wrong_pid
        WHERE c.pid in (select pid from t_wrong_pid)
        )
        UNION
        (
        SELECT c.pid, pkey, pauthor, peditor, ptitle, pyear, paddress, ppublisher, pseries, pjournal,
        pbooktitle, ptype
        FROM collection c, t_wrong_pid
        WHERE c.pid not in (select pid from t_wrong_pid)
        )
        """
    )
    return sdf_results


def null_type_fix(sdf: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    return sdf.select([
        F.lit(None).cast('string').alias(i.name)
        if isinstance(i.dataType, NullType)
        else i.name
        for i in sdf.schema
    ])
