from pathlib import Path
import duckdb
import pandas as pd
import json
import numpy as np

project_root = Path(__file__).parent.parent


class Dataset:
    """This class is in charge of loading all data to duckdb"""

    def __init__(self, conn: duckdb.DuckDBPyConnection, data_path: Path):
        self.data_path = data_path
        self.conn = conn
        self._load()

    def _load(self):
        data_files = project_root / self.data_path / 'dblp-*.csv'
        pbooktitlefull = project_root / self.data_path / 'pbooktitlefull.json'
        pjournalfull = project_root / self.data_path / 'pjournalfull.json'
        ptype = project_root / self.data_path / 'ptype.json'
        train = project_root / self.data_path / 'train.csv'
        validation = project_root / self.data_path / 'validation_hidden.csv'
        test = project_root / self.data_path / 'test_hidden.csv'

        self.conn.execute(f"""
        CREATE OR REPLACE TABLE dblp AS SELECT * FROM read_csv_auto('{data_files}', header=True);
        """)
        self.conn.execute(f"""
        CREATE OR REPLACE TABLE pbooktitlefull AS SELECT * FROM read_json_auto('{pbooktitlefull}');
        """)
        self.conn.execute(f"""
        CREATE OR REPLACE TABLE pjournalfull AS SELECT * FROM read_json_auto('{pjournalfull}');
        """)
        with open(ptype) as file:
            df_ptype = pd.DataFrame.from_dict(json.load(file))
        self.conn.execute(f"""
        CREATE OR REPLACE TABLE ptype AS SELECT * FROM df_ptype;
        """)
        self.conn.execute(f"""
        CREATE OR REPLACE TABLE train AS SELECT * FROM read_csv_auto('{train}', header=True);
        """)
        self.conn.execute(f"""
        CREATE OR REPLACE TABLE validation AS SELECT * FROM read_csv_auto('{validation}', header=True);
        """)
        self.conn.execute(f"""
        CREATE OR REPLACE TABLE test AS SELECT * FROM read_csv_auto('{test}', header=True);
        """)

    def get_total_references(self):
        return self.conn.execute(f"""
            select count(1) from dblp;
        """).fetchone()

    def get_total_ptypes(self):
        return self.conn.execute(f"""
            select count(1) from ptype;
        """).fetchone()

    def get_total_train(self):
        return self.conn.execute(f"""
               select count(1) from train;
           """).fetchone()

    def get_total_validation(self):
        return self.conn.execute(f"""
               select count(1) from validation;
           """).fetchone()

    def get_total_test(self):
        return self.conn.execute(f"""
               select count(1) from test;
           """).fetchone()

    def get_collection(self) -> pd.DataFrame:
        """ return the whole collection of bibliography"""
        df_collection = self.conn.execute(f"""
            select d.pid, d.pkey, d.pauthor,d.peditor,d.ptitle,d.pyear,d.paddress,d.ppublisher,d.pseries, 
            pj.name as pjournal, pb.name as pbooktitle, pt.name as ptype
            from dblp d 
            left join pbooktitlefull pb on d.pbooktitlefull_id = pb.id
            left join pjournalfull pj on d.pjournalfull_id = pj.id
            inner join ptype pt on d.ptype_id = pt.id
        """).df()
        df_collection = df_collection.replace({np.nan: None})
        return df_collection

    def execute(self, sql):
        return self.conn.execute(sql).df()
