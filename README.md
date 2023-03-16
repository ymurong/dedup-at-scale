# How to Develop Locally

## 1. VirtualENV Setup
The following example use python 3.10. Run the following commands from PROJECT ROOT.
```bash
pip3.10 install virtualenv
virtualenv venv --python=python3.10
```

Install Global Dependencies
```bash
source venv/bin/activate
pip install -r requirements.txt
```

## 2. installation with setuptools
This is useful for backend modules to locate packages
```bash
cd backend
pip install -r requirements.txt -e .
```

## 3. Dedupe Main
### 3.1 Train Dedupe
```bash
source venv/bin/activate
python main_train_dedupe.py
```

### 3.2 Use Dedupe to do Scoring
```bash
source venv/bin/activate
python main_score_dedupe.py
```

# Results

## No Data Imputation
LogisticRegression (ptitle as string, pauthor as set, pbooktitle as string)

|FIELD1|accuracy|precision|recall  |f1      |auc     |false_neg|false_pos|
|------|--------|---------|--------|--------|--------|---------|---------|
|0     |0.766809|0.875991 |0.61597 |0.723322|0.765273|1515     |344      |


LogisticRegression (ptitle as text, pauthor as set)

|FIELD1|accuracy|precision|recall  |f1      |auc     |false_neg|false_pos|
|------|--------|---------|--------|--------|--------|---------|---------|
|0     |0.771074|0.872191 |0.629658|0.731341|0.769634|1461     |364      |

LogisticRegression (ptitle as text, ptitle as string, pauthor as set)

|FIELD1|accuracy|precision|recall  |f1      |auc     |false_neg|false_pos|
|------|--------|---------|--------|--------|--------|---------|---------|
|0     |0.771952|0.873551 |0.630418|0.732332|0.770511|1458     |360      |

SVC (ptitle, pauthor)

|FIELD1|accuracy|precision|recall  |f1      |auc     |false_neg|false_pos|
|------|--------|---------|--------|--------|--------|---------|---------|
|0     |0.762168|0.863169 |0.617237|0.719775|0.760692|1510     |386      |

## With Data Imputation