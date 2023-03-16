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
        pbooktitle = project_root / self.data_path / 'pbooktitle.json'
        pbooktitlefull = project_root / self.data_path / 'pbooktitlefull.json'
        pjournal = project_root / self.data_path / 'pjournal.json'
        pjournalfull = project_root / self.data_path / 'pjournalfull.json'
        ptype = project_root / self.data_path / 'ptype.json'
        train = project_root / self.data_path / 'train.csv'
        validation = project_root / self.data_path / 'validation_hidden.csv'
        test = project_root / self.data_path / 'test_hidden.csv'

        with open(pbooktitle, "r") as f:
            df_booktitle = pd.DataFrame(data=json.load(f))

        self.conn.execute(f"""
        CREATE OR REPLACE TABLE dblp AS SELECT * FROM read_csv_auto('{data_files}', header=True);
        """)
        self.conn.execute(f"""
        CREATE OR REPLACE TABLE pbooktitle AS SELECT * FROM df_booktitle;
        """)
        self.conn.execute(f"""
        CREATE OR REPLACE TABLE pbooktitlefull AS SELECT * FROM read_json_auto('{pbooktitlefull}');
        """)
        self.conn.execute(f"""
        CREATE OR REPLACE TABLE pjournal AS SELECT * FROM read_json_auto('{pjournal}');
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
            pj.name as pjournal, pjf.name as pjournalfull, pb.name as pbooktitle, pbf.name as pbooktitlefull, pt.name as ptype
            from dblp d 
            left join pbooktitle pb on d.pbooktitle_id = pb.id
            left join pbooktitlefull pbf on d.pbooktitlefull_id = pbf.id
            left join pjournal pj on d.pjournal_id = pj.id
            left join pjournalfull pjf on d.pjournalfull_id = pjf.id
            inner join ptype pt on d.ptype_id = pt.id
        """).df()
        df_collection = df_collection.replace({np.nan: None})
        return df_collection

    def execute(self, sql):
        return self.conn.execute(sql).df()
