# How to Run Application Locally
If you don't need to develop but only need to start and showcase the application. You could simply just run the following script. It will build the necessary images needed and start a docker compose app.

```bash
./start.sh
```

You can also list all your local containers by the following command:
```bash
docker container ls
```
The example output will look like:
```bash
CONTAINER ID   IMAGE                                  COMMAND                  CREATED          STATUS          PORTS                                                      NAMES
f951a4f7ed54   dedup-api:1.0.0                        "uvicorn backend.src…"   33 seconds ago   Up 31 seconds   0.0.0.0:8000->8000/tcp                                     docker-dedup-api-1
19d2b215b148   ymurong/spark-master:3.3.0-hadoop3.3   "/bin/bash /master.sh"   9 minutes ago    Up 31 seconds   0.0.0.0:7077->7077/tcp, 6066/tcp, 0.0.0.0:8080->8080/tcp   spark-master
```

The UI Address and Service Address are exposed as the following:
```bash
# OpenAPI3 Playground UI
http://localhost:8000/docs
# Spark Application UI
http://localhost:8080
# Spark Master Address
http://localhost:7077
```

You can stop and remove Spark and API containers by running the following script:
```bash
./stop.sh
```


# How to Develop Locally

## 1. VirtualENV Setup
You must use python 3.10 as remote spark cluster worker only support python 3.10. 
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


## 2. Backend Setup

### 2.1 installation with setuptools
This is useful for backend modules to locate packages
```bash
cd backend
pip install -r requirements.txt -e .
```

### 2.2 start spark standalone cluster
The spark UI is exposed on localhost:8080. Spark Master is exposed on 7077.
```bash
./docker/start-spark.sh
```
When you want to stop and remove the cluster
```bash
./docker/stop-spark.sh
```

### 2.3 start up the api server
```bash
source venv/bin/activate
python backend/src/run.py
```

## 3. Swagger Playground
The docs page gives access to API interface and allow us to directly test API through UI.
```bash
http://localhost:8000/docs
```