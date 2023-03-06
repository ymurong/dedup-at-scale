from pyspark.sql import functions as F


def lower_case(input_df):
    """ lower case string type columns """
    col_type_dict = dict(input_df.dtypes)
    return input_df.select(
        [F.lower(col_name).alias(col_name) if (col_type_dict[col_name] == "string") else col_name
         for col_name in input_df.columns])


def year(input_df):
    """ year column sometimes have negative values """
    return input_df.select(
        [F.abs(col_name).alias(col_name) if col_name == "pyear" else col_name
         for col_name in input_df.columns])