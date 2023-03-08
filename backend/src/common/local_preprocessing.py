import pandas as pd
from unidecode import unidecode


def string_normalize(input_df: pd.DataFrame) -> pd.DataFrame:
    # 1.Remove whitespace around the string
    columns_to_normalize = ["pauthor", "ptitle", "pjournal", "pbooktitle", "ptype"]
    input_df.loc(axis=1)[columns_to_normalize] = input_df.loc(axis=1)[columns_to_normalize].apply(
        lambda col: col.str.strip())

    # 2.Lowercase the string
    input_df = lower_case(input_df)

    # 3. Remove all punctuation and control characters
    input_df.loc(axis=1)[columns_to_normalize] = input_df.loc(axis=1)[columns_to_normalize] \
        .apply(lambda col: col.str.replace('[^A-Za-z\s|]+', '', regex=True))

    # 4. ascii transformation
    input_df.loc(axis=1)[columns_to_normalize] = input_df.loc(axis=1)[columns_to_normalize] \
        .applymap(lambda cell: unidecode(cell) if cell else cell)

    # 5. tokenize
    def tokenize_pauthor(cell):
        sorted_tokens = sorted(cell.split("|"))
        return "|".join(sorted_tokens)

    input_df.loc(axis=1)[["pauthor"]] = input_df.loc(axis=1)[["pauthor"]] \
        .applymap(tokenize_pauthor)

    return input_df


def lower_case(input_df: pd.DataFrame) -> pd.DataFrame:
    """ lower case string type columns """
    col_type_dict = dict(input_df.dtypes)
    string_columns = [column for column in input_df.columns if col_type_dict[column] == "object"]
    input_df.loc(axis=1)[string_columns] = input_df.loc(axis=1)[string_columns].apply(lambda col: col.str.lower())
    return input_df


def abs_year(input_df: pd.DataFrame) -> pd.DataFrame:
    """ year column sometimes have negative values """
    input_df["pyear"] = input_df["pyear"].abs()
    return input_df


def inversed_pauthor_ptitle(input_df: pd.DataFrame) -> pd.DataFrame:
    stop_words_catcher_regex = " is | and | a | in "
    # pauthor ends with . (except Jr.) must be wrong
    df_must_wrong1 = input_df[input_df["pauthor"].str.contains("(?<!Jr)\.$")]
    # pauthor not ends with . and not contain | but ptitle contains | must be wrong
    df_must_wrong2 = input_df[
        (input_df["pauthor"].str.contains("^[^\|]+[^.]$")) & (input_df["ptitle"].str.contains("\|"))]
    # pauthor and ptitle both does not contain | or endswith . could be wrong,
    # in this case, if pauthor contains stop words must be wrong
    df_possible_wrong1 = input_df[(input_df["pauthor"].str.contains("^[^\|]+[^.]$"))
                                  & (input_df["ptitle"].str.contains("^[^\|]+[^.]$"))]
    df_must_wrong3 = df_possible_wrong1[df_possible_wrong1["pauthor"].str.contains(stop_words_catcher_regex)]
    wrong_row_index = list({*df_must_wrong1.index.values, *df_must_wrong2.index.values, *df_must_wrong3.index.values})
    input_df.loc[wrong_row_index, ["pauthor", "ptitle"]] = input_df.loc[wrong_row_index, ["ptitle", "pauthor"]].values
    return input_df
