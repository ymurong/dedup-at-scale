import pandas as pd
from unidecode import unidecode
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import make_pipeline


def pauthor_to_set(input_df: pd.DataFrame) -> pd.DataFrame:
    def tokenize_pauthor(cell):
        author_set = tuple([author for author in cell.split("|")])
        return author_set

    input_df.loc(axis=1)[["pauthor"]] = input_df.loc(axis=1)[["pauthor"]] \
        .applymap(tokenize_pauthor)
    return input_df


def string_normalize(input_df: pd.DataFrame) -> pd.DataFrame:
    # 1.Remove whitespace around the string
    columns_to_normalize = ["pauthor", "ptitle", "pjournal", "pbooktitle", "pjournalfull", "pbooktitlefull", "ptype"]
    input_df.loc(axis=1)[columns_to_normalize] = input_df.loc(axis=1)[columns_to_normalize].apply(
        lambda col: col.str.strip())

    # 2.Lowercase the string
    input_df = lower_case(input_df)

    # 3. ascii transformation
    input_df.loc(axis=1)[columns_to_normalize] = input_df.loc(axis=1)[columns_to_normalize] \
        .applymap(lambda cell: unidecode(cell) if cell else cell)

    # 4. Remove all punctuation and control characters
    input_df.loc(axis=1)[columns_to_normalize] = input_df.loc(axis=1)[columns_to_normalize] \
        .apply(lambda col: col.str.replace('[^A-Za-z\s|]+', '', regex=True))

    # 5. tokenize
    def tokenize_pauthor(cell):
        sorted_tokens = sorted(cell.split("|"))
        return "|".join(sorted_tokens)

    input_df.loc(axis=1)[["pauthor"]] = input_df.loc(axis=1)[["pauthor"]] \
        .applymap(tokenize_pauthor)

    # 6. nan string to None
    input_df = input_df.replace({"nan": None})

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


def impute(input_df: pd.DataFrame):
    columns_to_impute = ["pjournalfull", "pbooktitlefull"]

    def _impute(df, to_impute: str):
        '''
        Imputing to_impute column, based on ptitle
        '''

        pipeline = make_pipeline(
            TfidfVectorizer(lowercase=True
                            , analyzer='word'
                            , ngram_range=(1, 2)  # include ngrams (e.g. climate change)
                            # ,stop_words = stopwords.words("english") # remove stopwords
                            # ,max_df=0.9 # appearing in max 90%
                            # ,min_df=10 # appearing in min 10 docs)
                            # ,max_features=2000 # the 2000 most common words overall
                            ),
            RandomForestClassifier(n_estimators=100, random_state=42)
        )

        # Removing messung if we impute pbooktitle
        if to_impute == 'pbooktitlefull':
            df.loc[:, to_impute] = df[to_impute].replace({'messung': None})

        # Split the data into training and test sets
        train_data = df[df[to_impute].notna()].copy()
        test_data = df[df[to_impute].isna()].copy()

        # Fit the pipeline on the training data
        pipeline.fit(train_data['ptitle'], train_data[to_impute])

        # Use the pipeline to predict the missing values in the label column
        test_data.loc[test_data.index, to_impute] = pipeline.predict(test_data['ptitle'])

        # Combine the training and test data
        df.loc[:, to_impute] = pd.concat([train_data, test_data], axis=0)[to_impute]

        return df

    for column in columns_to_impute:
        input_df = _impute(input_df, to_impute=column)
    return input_df
