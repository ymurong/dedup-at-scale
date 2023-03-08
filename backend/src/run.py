from fastapi import FastAPI, Request
import uvicorn
from fastapi.middleware.cors import CORSMiddleware
from src.example import example_app
from src.dedupe_api import dedupe_app
from src.resources.logging_conf import logging_config
import time
import logging.config

logging.config.dictConfig(logging_config)
logger = logging.getLogger(__name__)

# create FastAPI application
app = FastAPI(
    title='DeDup API',
    version='1.0.0',
    docs_url='/docs',
    redoc_url='/redoc',
)


@app.middleware('http')
async def add_process_time_header(request: Request, call_next):  # call_next将接收request请求做为参数
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers['X-Process-Time'] = str(process_time)  # 添加自定义的以“X-”开头的请求头
    return response


app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1",
        "http://127.0.0.1:8000",
        "http://localhost:8000",
        "http://localhost:4200"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(example_app, prefix='/example', tags=['example'])
app.include_router(dedupe_app, prefix='/dedupe', tags=['dedupe'])

if __name__ == '__main__':
    uvicorn.run('run:app', host='0.0.0.0', port=8000, reload=False)
