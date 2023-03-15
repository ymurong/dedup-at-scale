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