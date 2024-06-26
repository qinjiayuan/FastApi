from fastapi import FastAPI, Body
import uvicorn
from pydantic import BaseModel
from fastapi import FastAPI,Body
from typing import Optional
from pydantic import BaseModel
from derivative.dailyjob import derivative
from review.review import review
from newapp.params import check
from optionjob.new_flow_job import option

app = FastAPI(title="测试常用接口汇总",description="测试过程中经常使用的接口文档",debug=True)

#通过include_router路由映射，来让所有接口都映射到一个进程使用

app.include_router(review,prefix='',tags=['回访流程'])
app.include_router(option,prefix='',tags=['期权产品监测流程'])
app.include_router(derivative,prefix='',tags=['台账接口'])



if __name__ == '__main__':
    uvicorn.run(app, host="10.50.72.116", port=8090)