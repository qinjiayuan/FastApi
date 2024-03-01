import time

import requests
from fastapi import FastAPI , APIRouter,Query,Form,Request
from pydantic import BaseModel, validator
from datetime import date, datetime
from typing import List, Union,Optional

from requests import Response

check = APIRouter()

class addr(BaseModel):
    city : str
    province : str

class User(BaseModel):
    name : str
    age : int = Query(...,ge=0,le=100)
    birth : Union[date,str] = None
    firends : Optional[List[int]] =[1]
    add : addr #使用嵌套来实现


#使用validator来实现验证器的功能
    @validator("name")
    def name_must_bealpha(cls,value):
        assert value.isalpha() , 'name一定是英文字母'
        return value

    @validator("age")
    def judge_age(cls,value):
        assert value>0 and value <100 ,'年龄必须在0到120之间'
        return value



@check.post("/user/{id}")
def get_user(id:int):
    print(f"id:{id}")
    return {"user_id":id}

@check.post("/data")
async def data(data:User):
    print(data.firends,type(data.firends))
    return data


#form表单数据
@check.post('/regin')
async def regin(username : str = Form(),passwd : str = Form()):
    print(f"username:{username},passwd:{passwd}")
    return username


#通过request来进行一些获取的操作
@check.get("/item")
async def item(request:Request):
    print({"url":request.url,
           "host":request.client.host,
           "cookie":request.cookies})
    return {"url":request.url,
           "host":request.client.host,
           "cookie":request.cookies}

#响应模型

class student(BaseModel): #输入响应
    id : Optional[str] = None
    user : str
    passwd : str
    age : int
    Class : str
    no : int
class info(BaseModel):
    user : str
    Class : str
    no : str
    id : str


@check.post("/createuser",response_model=info)
def createuser(student:student):
    return student



import asyncio

async def async_operation(duration):
    print(f"Starting async operation for {duration} seconds")
    await asyncio.sleep(duration*10)
    print(f"Async operation completed after {duration} seconds")
#
# async def dealwithData(left:int  ,right:int):
#     data_num = []
#     for i in range(left,right):
#         data_num.append(i)
#         print(i)
#     return data_num
#
# async def main_test():
#     start = time.time()
#     tasks = [
#         dealwithData(0,1250000),
#         dealwithData(1250000,2500000),
#         dealwithData(2500000,3750000),
#         dealwithData(3750000,5000000)
#     ]
#     result  = await asyncio.gather(*tasks)
#     print(len(result))
#     end = time.time()
#     print(f"耗时{end-start}s")


async def main():
    start = time.time()
    task1 = asyncio.create_task(async_operation(1))
    task2 = asyncio.create_task(async_operation(2))
    task3 = asyncio.create_task(async_operation(3))

    await task1
    print("现在已经开始执行task1")
    await task2
    print("现在已经开始执行task1")
    await task3
    print("现在已经开始执行task1")
    end = time.time()
    print(f"耗时总共为{end-start}s")

# 使用 asyncio.run() 运行协程
# asyncio.run(main())
async def dealwithData(left: int, right: int):
    data_num = []
    for i in range(left, right):
        data_num.append(i)
        print(i)
    return data_num

async def clientreview(**kwargs):
    url = "http://10.50.72.116:8080/clientreview/processjob"
    response : Response = requests.post(url=url,data=kwargs)
    return response.json() if response.status_code ==200 else f"调用接口失败{response.json()}"


async def main_test1():
    start = time.time()
    params1 = {"corporateName":"测试产品关注类",
              "customermanager":"孙滨",
              "isnew":"1"}
    params2 = {"corporateName":"准入同步测试",
              "customermanager":"古建国",
              "isnew":"1"}


    result = await asyncio.gather(asyncio.create_task(clientreview(**params1)),
                                  asyncio.create_task(clientreview(**params2)))
    end = time.time()
    print(f"耗时{end-start}")

async def main_test():
    start = time.time()
    tasks = [
        dealwithData(0, 1250000),
        dealwithData(1250000, 2500000),
        dealwithData(2500000, 3750000),
        dealwithData(3750000, 5000000),
    ]
    results = await asyncio.gather(*tasks)
    end = time.time()
    print("Time elapsed:", end - start)
    return results

async def sleep_test(delay : int):
    print(f"启动：{delay}s job ")
    await asyncio.sleep(delay)
    print(f"完成 {delay}s job")

async def many_test_sleep():
    await asyncio.gather(asyncio.create_task(sleep_test(3)),asyncio.create_task(sleep_test(5)))

if __name__ == '__main__':
    start = time.time()
    asyncio.run(many_test_sleep())
    # time.sleep(3)
    # time.sleep(5)
    end = time.time()
    print("总耗时%.fs"%(end-start))



