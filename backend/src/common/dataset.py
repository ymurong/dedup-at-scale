from pathlib import Path
import duckdb
import pandas as pd
import json

project_root = Path(__file__).parent.parent


class Dataset:
    def __init__(self, conn: duckdb.DuckDBPyConnection, data_path: Path):
        self.data_path = data_path
        self.conn = conn
        self.__load__()

    def __load__(self):
        data_files = project_root / self.data_path / 'dblp-*.csv'
        pbooktitlefull = project_root / self.data_path / 'pbooktitlefull.json'
        pjournalfull = project_root / self.data_path / 'pjournalfull.json'
        ptype = project_root / self.data_path / 'ptype.json'
        train = project_root / self.data_path / 'train.csv'

        self.conn.execute(f"""
        CREATE OR REPLACE TABLE dblp AS SELECT * FROM read_csv_auto('{data_files}');
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
        CREATE OR REPLACE TABLE train AS SELECT * FROM read_csv_auto('{train}');
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

    def get_input(self):
        return self.conn.execute(f"""
            select d.pid, d.pkey, d.pauthor,d.peditor,d.ptitle,d.pyear,d.paddress,d.ppublisher,d.pseries, pj.name, pb.name, pt.name
            from dblp d 
            left join pbooktitlefull pb on d.pbooktitlefull_id = pb.id
            left join pjournalfull pj on d.pjournalfull_id = pj.id
            inner join ptype pt on d.ptype_id = pt.id
        """).df()

    def get_training_pairs(self):
        return self.conn.execute(f"""
                    select distinct on (t.column0) t.column0, 
                    d.pid, d.pkey, d.pauthor,d.peditor,d.ptitle,d.pyear,d.paddress,d.ppublisher,d.pseries, pj.name as pjournal, pb.name as pbooktitle, pt.name as ptype,
                    d2.pid, d2.pkey, d2.pauthor,d2.peditor,d2.ptitle,d2.pyear,d2.paddress,d2.ppublisher,d2.pseries, pj2.name as pjournal2, pb2.name as pbooktitle2, pt2.name  as ptype2,
                    t.label
                    from train t 
                    inner join dblp d on t.key1 = d.pkey
                    inner join ptype pt on d.ptype_id = pt.id
                    left join pbooktitlefull pb on d.pbooktitlefull_id = pb.id 
                    left join pjournalfull pj on d.pjournalfull_id = pj.id
                    inner join dblp d2 on t.key2 = d2.pkey
                    inner join ptype pt2 on d2.ptype_id = pt2.id
                    left join pbooktitlefull pb2 on d2.pbooktitlefull_id = pb2.id
                    left join pjournalfull pj2 on d2.pjournalfull_id = pj2.id
                """).df()

    def execute(self, sql):
        return self.conn.execute(sql).df()
