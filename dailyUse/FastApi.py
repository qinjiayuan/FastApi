import datetime
from uuid import uuid4

import cx_Oracle
from fastapi import FastAPI,HTTPException,Query,Depends,openapi
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel,ValidationError
from typing import List, Optional
import uvicorn
from enum import Enum
from datetime import datetime,date
app = FastAPI()

class Client(BaseModel):
    client_id : str




# @app.get("/select", response_model=List[Client])
# def select_client():
#     host = "10.62.146.18"
#     port = "1521"
#     user = "gf_otc"
#     passwd = "otc1qazXSW@"
#     service_name = 'jgjtest'
#     db = cx_Oracle.connect(user, passwd, f"{host}:{port}/{service_name}")
#     cursor = db.cursor()
#     cursor.execute(f"SELECT CLIENT_ID FROM OTC_DERIVATIVE_COUNTERPARTY WHERE CORPORATE_NAME = '测试产品关注类'")
#     # clients = [Client(client_id=x[0]) for x in cursor.fetchall()]
#     clients = [Client(client_id=x[0] )for x in cursor.fetchall()]

    # return clients

class Item(BaseModel):
    name: str
    description: Optional[str] = None

class SelectCounterparty(BaseModel):
    corporate_name :str
    aml_monitor_flag : str
    client_id : str

class Item(BaseModel):
    name: str
    description: str = None
    price: float
    tax: float = None
#这里使用的是依赖
def select(corporate_name:str = Query(None,max_length=10),client_id:str=Query(None),num:int =Query(...,ge=0,le=100)):
    return {"corporate_name":corporate_name,
            "client_id":client_id,
            "num":num}
@app.get("/select")
def getnum(params : dict = Depends(select)):
    return params

class ClientReviewBuffer(BaseModel):

    client_id : List[str]
    review_start_date : str
    review_buffer_start : str
    review_buffer_end : str

def input_data(data:ClientReviewBuffer):
    data_ba = data.dict()
    data_ba["id"] = uuid4()
    return data_ba

@app.post("/insert",tags=["回访相关"],description="插入回访缓冲期的数据")
def insert_job(data : dict = Depends(input_data)):
    host = "10.62.146.18"
    port = "1521"
    username = 'gf_otc'
    passwd = "otc1qazXSW@"
    service_name = 'jgjtest'

    db = cx_Oracle.connect(username,passwd,f"{host}:{port}/{service_name}")
    cursor = db.cursor()
    client_id_list = data["client_id"]
    params_list = []
    for i in range(len(client_id_list)):
        params = []
        params.append(str(uuid4()))
        params.append(client_id_list[i])
        params.append(datetime.datetime.strptime(data["review_start_date"],"%Y-%m-%d").date())
        params.append(datetime.datetime.strptime(data["review_buffer_start"],"%Y-%m-%d").date())
        params.append(datetime.datetime.strptime(data["review_buffer_end"],"%Y-%m-%d").date())
        params_list.append(params)
    # insert_sql = f"""insert into client_review_buffer values(:id,:client_id,:review_start_date,:review_buffer_start,:review_buffer_end)"""
    # cursor.executemany(insert_sql,params_list)
    #
    select_sql = "select *from client_review_buffer "
    cursor.execute(select_sql)


    while True:
        rows = cursor.fetchmany(300)
        if not rows:
            break
        print(rows)
        print(len(rows))

    cursor.close()
    db.close()
    print("Succeefully")
    return "OK！"

class User(BaseModel):
    id : int
    name : str = "John Snow"
    signup_ts : Optional[datetime] = None #该字段选填,并且默认值为空
    friends : List[int] = [] #列表中的元素是int类型，且默认值为空

@app.post("/")
def user_test( params : ClientReviewBuffer):
    try:
        external_data = {"id":"123",
                         "signup_ts":"2022-12-22 12:22",
                         "friends":[1,2,"4"]}
        user = User(**external_data)
        print(user.dict())
        print(user.json())
        print(user.copy()) #拷贝


    except ValidationError as e :
        print(e.json())


if __name__ == '__main__':
    uvicorn.run(app , host="10.50.72.116",port=8000)
