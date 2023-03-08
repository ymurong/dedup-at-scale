

import pandas as pd
import dedupe

data = pd.read_json('dblp/pjournal.json')

fields = [
    {'field': 'name', 'type': 'String'}]



deduper = dedupe.Dedupe(fields)

deduper.predicate

deduper.prepare_training(data.T.to_dict())
dedupe.console_label(deduper)

deduper.train()

with open('training_file.json', 'w') as tf:
            deduper.write_training(tf)

#cluster

print("hola")