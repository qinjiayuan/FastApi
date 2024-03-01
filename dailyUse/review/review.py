import asyncio
import time
from datetime import date, datetime, timedelta
from typing import List, Optional
from uuid import uuid4
from enum import Enum
import aiomysql
import cx_Oracle
import jsonpath
import pymysql
import requests
import uvicorn as uvicorn
from fastapi import FastAPI, APIRouter, Query, Depends
from pydantic import BaseModel, Field
import logging
import aiohttp
import json
from aiohttp import FormData

review = APIRouter()
# log = logging.getLogger("counterpartyjob")
#固收信创mysql
config = {"host": "10.128.12.222",
          "user": "otcmtst",
          "password": "Gf_otcmtst_test",
          "port": 15006,
          "db": "gf_ficc"}
#股衍
config_otc = {"host": "10.62.146.18",
              "user": "gf_otc",
              "password": "otc1qazXSW@",
              "port": 1521,
              "db": "jgjtest"}


#环境枚举类
class Enviroment(Enum):
    FICC = "http://10.128.12.40:59754/otcoms-tst/fc-kyc-server"
    OTC = "http://10.2.145.216:9090"

#发起回访流程的响应模型
class ReviewFlowResponse(BaseModel):
    title : str
    corporateName : str
    documentId : str
    recordId:str
    currentOperator : str

#新旧版本枚举类
class ReviewIsNew(Enum):
     new :str = "1"
     old :str = "0"

#回访流程record表
class ReviewRecordInfo(BaseModel):
    '''
    流程记录表
    '''
    ID : Optional[str] =Field(None,description="id")
    DOC_ID : Optional[str]  =Field(None,description="文档号")
    TITLE : Optional[str] =Field(None,description="标题")
    CLIENT_NAME : Optional[str] =Field(None,description="公司名称")
    UNIFIEDSOCIAL_CODE : Optional[str] =Field(None,description="社会信用代码")
    REVIEW_DATE : Optional[date] =Field(None,description="回访日期")
    REVIEW_USER : Optional[str] =Field(None,description="回访人员oa账号")
    REVIEW_NAME : Optional[str] =Field(None,description="")
    CURRENT_STATUS : Optional[str] =Field(None,description="当前状态")
    CURRENT_OPERATOR : Optional[str] =Field(None,description="当前处理人")
    CURRENT_ACTIVITY_NAME : Optional[str] =Field(None,description="当前节点名称")
    RECORD_ID : Optional[str] =Field(None,description="CLIENT_REVIEW_DETAIL外键")
    CREATED_DATETIME : Optional[datetime] =Field(None,description="创建日期")
    WORK_PHONE : Optional[str] =Field(None,description="工作电话")
    PHONE: Optional[str] =Field(None,description="电话")
    SECURITY_LEVEL : Optional[str] =Field(None,description="安全等级")
    SECURITY_LEVEL_DETAIL : Optional[str] =Field(None,description="安全等级有效时间")
    URGENCY_LEVEL : Optional[str] =Field(None,description="紧急程度")
    URGENCY_LEVEL_REASON : Optional[str] =Field(None,description="紧急原因")
    SALE_PERSON : Optional[str] =Field(None,description="对应总部销售")
    REVIEW_TERM : Optional[date] =Field(None,description="回访期限")
    REVIEW_PROCESS_TYPE : Optional[str] =Field(None,description="回访流程发起类型")
    SPECIAL_MENTIONED_CUSTOMER : Optional[str] =Field(None,description="是否为关注类客户")
    VERSION : Optional[str] =Field(None,description="标识存量增量数据，增量为202210，存量为空")
    NO_MORE_REVIEW : Optional[str] =Field(None,description="不再自动回访")
    ACCOUNTING_FIRM_NAME : Optional[str] =Field(None,description="会计师事务所")
    SUPPLEMENTARY_MATERIALS_TIME : Optional[date] =Field(None,description="另行补充材料完成日期")
    SUPPLEMENTARY_MATERIALS : Optional[str] =Field(None,description="需要另行补充材料，'true'/'false'")
    REACH_TO_03_DATETIME : Optional[datetime] =Field(None,description="到达03节点的时间")
    SERIAL_NUMBER : Optional[str] =Field(None,description="流程文档编号")

#回访流程counterparty表
class ReviewCounterpartyInfo(BaseModel):
    '''
    回访对象表
    '''
    ID : Optional[str] =Field(None,description="id")
    RECORD_ID : Optional[str] =Field(None,description="记录id")
    PRODUCT_NAME : Optional[str] =Field(None,description="产品名称")
    CREATED_DATETIME : Optional[datetime] =Field(None,description="创建时间")
    CLIENT_ID : Optional[str] =Field(None,description="客户编号")
    IGNORE : Optional[str] =Field(None,description="是否本次不回访")
    BENEFIT_OVER_FLAG : Optional[str] =Field(None,description="是否存在单一委托人占比超过20%的情况（初始值来源台账，待流程结束回填）")
    AGREE_INFO : Optional[str] =Field(None,description="以下信息是否一致，增量'Y'或'N'，存量为空")
    ALLOW_BUSI_TYPE : Optional[str] =Field(None,description="拟参与的衍生品业务类型")
    CLIENT_QUALIFY_REVIEW : Optional[str] =Field(None,description="客户资质复核结果")
    SEQ : Optional[int] =Field(None,description="回访对象的序号")
    REVIEW_BUFFER_START : Optional[datetime] =Field(None,description="拟限制交易日（取自beffer）")
    SUPPLEMENTARY_MATERIALS_NOTE : Optional[str] =Field(None,description="补充材料说明")
    SHOW_NOTE : Optional[str] =Field(None,description="展示补充材料说明，'true'/'false'")
    ALLOW_BUSI_TYPE_HIS : Optional[str] =Field(None,description="拟参与的衍生品业务类型历史")
    MANUAL_DEL_ALLOW_BUSI_TYPE : Optional[str] =Field(None,description="回访中手动减少的衍生品业务类型")
    PRODUCT_ASSET : Optional[int]  =Field(None,description="产品资产净值（万元）")

#回访流程受益人表
class ReviewAmlBeneficiaryInfo(BaseModel):
    '''
    回访受益人信息
    '''
    ID : Optional[str] = Field(None,description='主键')
    ENTITY_TYPE : Optional[str]
    CATEGORY : Optional[str] = Field(None,description='受益所有人的身份')
    NAME : Optional[str]
    ID_KIND : Optional[str] = Field(None,description='证件类型')
    ID_NO : Optional[str] = Field(None,description='证件号码')
    BIRTH : Optional[str] = Field(None,description='出生日期')
    GENDER : Optional[str] = Field(None,description='性别')
    COUNTRY : Optional[str] = Field(None,description='国家')
    ID_VALIDDATE_START : Optional[str] = Field(None,description='证件有效期开始日')
    ID_VALIDDATE_END : Optional[str] =Field(None,description='证件有效期结束日')
    PHONE : Optional[str] = Field(None,description='电话')
    MOBILE : Optional[str]  = Field(None,description='手机')
    EMAIL : Optional[str]  = Field(None,description='邮箱')
    HOLD_RATE : Optional[str] = Field(None,description='持股比例')
    SPECIAL_TYPE : Optional[str]
    POSITION : Optional[str]
    HOLD_TYPE : Optional[str] = Field(None,description='持股类型')
    BENEFICIARY_TYPE : Optional[str]
    LOCKED : Optional[str]
    COUNTERPARTY_ID : Optional[str]
    ADDRESS : Optional[str]
    VERSION : Optional[int]
    CLIENT_KIND : Optional[str]
    CLIENT_ID : Optional[str]
    RECORD_ID : Optional[str]
    BUSINESS_TYPE : Optional[str]

#回访流程投资者明细表
class ReviewBenefitOver(BaseModel):
    CLIENT_ID : Optional[str] = Field(None , description='交易对手编号')
    NAME : Optional[str] = Field(None , description='姓名或机构名称')
    ID_NO : Optional[str] = Field(None , description='身份证号或统一信用代码')
    PROPORTION : Optional[int] = Field(None , description='认购份额比例')
    FIID  : Optional[int] = Field(None , description='流程编号')
    PROFESSIONAL_INVESTOR_FLAG  : Optional[str] = Field(None , description='专业投资者标准')
    FINANCIAL_ASSETS_OF_LASTYEAR : Optional[float] = Field(None , description='上年末金融资产')
    INVEST_3YEAR_EXP_FLAG : Optional[str] = Field(None , description='3年以上投资经验')
    PROD_ID : Optional[str] = Field(None , description='产品ID')
    ASSETS_20MILLION_FLAG : Optional[str] = Field(None , description='最近一年末金融资产是否不低于2000万')
    RECORD_ID : Optional[str] = Field(None , description='回访流程ID')
    ID : Optional[str] = Field(None , description='主键')







# 单产品回访接口
async def single_review(corporatename: str,env : str):
    try:
        async with aiomysql.create_pool(**config) as pool:
            async with pool.acquire() as db:
                async with db.cursor() as cursor:
                    select_unicode = f"select unifiedsocial_code from otc_derivative_counterparty where corporate_name = '{corporatename}'"
                    print(f"execute_sql:{select_unicode}")
                    await cursor.execute(select_unicode)
                    unicode = await cursor.fetchone()
    except Exception as e:
        print(e)
    finally:
        await cursor.close()
        await pool.wait_closed()

    async with aiohttp.ClientSession() as session:
        async with session.post(url=env + "/clientreview/checkSingleClient",
                                data={"checkDateEnd": date.today(), "checkDateStart": date.today(),
                                      "uniCodeList": unicode[0]}) as response:
            print(await response.json())
            return None


#多产品回访接口
async def multiple_review(corporatename: str,env : str):
    try:
        async with aiomysql.create_pool(**config) as pool:
            async with pool.acquire() as db:
                async with db.cursor() as cursor:
                    select_unicode = f"select unifiedsocial_code from otc_derivative_counterparty where corporate_name = '{corporatename}'"
                    print(f"execute_sql:{select_unicode}")
                    await cursor.execute(select_unicode)
                    unicode = await cursor.fetchone()
    except Exception as e:
        print(e)
    finally:
        await cursor.close()
        await pool.wait_closed()

    async with aiohttp.ClientSession() as session:
        async with session.post(url=env + "/clientreview/checkMultipleClient",
                                data={"checkDateEnd": date.today(), "checkDateStart": date.today(),
                                      "uniCodeList": unicode[0]}) as response:
            print(await response.json())
            return None

            # async with aiohttp.ClientSession() as session:

#回访上传附件接口
async def uploadFile(env : str):
    file_name = ['主体/管理人文件', '32', 'CSRC', 'QCC_CREDIT_RECORD', 'CEIDN', 'QCC_ARBITRATION', 'QCC_AUDIT_INSTITUTION',
                 'CCPAIMIS', 'CC', 'P2P', 'OTHERS', 'NECIPS', 'CJO','场外衍生品交易授权书']
    S3filed = []
    headers = {"name": "sunbin"}
    file_path = r"D:\dailyUse\data.xlsx"

    async with aiohttp.ClientSession() as session:
        for i in range(28):
            json_data = {"fileBelong": file_name[i % 13], "productName": file_name[i % 13]}
            form_data = FormData()
            form_data.add_field("files", open(file_path, "rb"), filename="data.xlsx",
                                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            form_data.add_field("fileBelong", file_name[i % 14])
            form_data.add_field("productName", file_name[i % 14])

            async with session.post(url=env + "/clientreview/file/upload", data=form_data, headers=headers) as response:
                if response.status == 200:
                    print(await response.json())
                    s3id = jsonpath.jsonpath(await response.json(), "$..s3FileId")
                    S3filed.append(s3id[0])

    print(f"s3fileid:{S3filed}")
    return S3filed


'''
#同时请求26次,后台服务器报错（暂不使用）
async def uploadFile_as(env: str):
    file_name = ['主体/管理人文件', '32', 'CSRC', 'QCC_CREDIT_RECORD', 'CEIDN', 'QCC_ARBITRATION', 'QCC_AUDIT_INSTITUTION',
                 'CCPAIMIS', 'CC', 'P2P', 'OTHERS', 'NECIPS', 'CJO']
    S3filed = []
    headers = {"name": "sunbin"}
    file_path = r"D:\dailyUse\data.xlsx"

    async def upload_file(i : int):
        form_data = FormData()
        form_data.add_field("files", open(file_path, "rb"), filename="data.xlsx",
                            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        form_data.add_field("fileBelong", file_name[i % 13])
        form_data.add_field("productName", file_name[i % 13])

        async with aiohttp.ClientSession() as session :
            async with session.post(url=env + "/clientreview/file/upload", data=form_data, headers=headers) as response:
                if response.status == 200:
                    print(await response.json())
                    s3id = jsonpath.jsonpath(await response.json(), "$..s3FileId")
                    # S3filed.append(s3id[0])
                    return s3id
    task = [upload_file(i) for i in range(26)]
    result = await asyncio.gather(*task)
    print(result)
    return result


    # print(f"s3fileid:{S3filed}")
    # return S3filed
'''
@review.post("/clientreview/create",summary="发起回访流程")
async def reviewjob(corporatename: str , customerManager: str, isnew: ReviewIsNew,enviroment:Enviroment):
    '''
    :param corporatename: 公司名称
    :param customerManager:客户经理（中文）
    :param isnew: 0-旧流程 1-新流程
    :param enviroment:40-固收测试环境 216-股衍测试环境
    '''
    start = time.time()
    print(f"公司名称 ：{corporatename},客户经理：{customerManager},isnew : {isnew},enviroment : {enviroment}")
    date_1 = date.today()
    date_str = date.strftime(date_1, "%Y-%m-%d")
    env = enviroment.value
    isnew = isnew.value
    print(f"isnew:{isnew}")
    #固收的流程
    if enviroment.name == "FICC":
        try:
            async with aiomysql.create_pool(**config) as pool:
                async with pool.acquire() as db:
                    async with db.cursor() as cursor:

                        select_client = f"select client_id from otc_derivative_counterparty where corporate_name = '{corporatename}'"
                        await cursor.execute(select_client)
                        if not await cursor.fetchall():
                            raise ValueError("交易对手不存在")
                        # 删除存量流程
                        delete_flow = f"delete from client_review_record where client_name = '{corporatename}' and current_status !='CLOSED' "
                        print(f"execute_sql:{delete_flow}")
                        await cursor.execute(delete_flow)
                        # 查找客户经理
                        select_customermanager = f"SELECT a2.userid , a.dept_code FROM AORG a LEFT JOIN Auser a2 ON a.ORGID  = a2.ORGID WHERE a2.USERNAME = '{customerManager}' "
                        print(f"execute_sql:{select_customermanager}")
                        await cursor.execute(select_customermanager)
                        customer = await cursor.fetchall()
                        if customer:
                            user, dept_code = customer[0][0], customer[0][1]
                            print(f"user:{customer[0][0]},dept_code:{customer[0][1]}")
                        else:
                            raise ValueError("请输入正确的客户经理")
                        # 直接设置发起回访流程的条件
                        set_derivative = f"""update otc_derivative_counterparty set
                                                aml_monitor_flag = 'true',
                                                no_auto_visit = 'false',
                                                return_visit_date=date_format('{date_str}','%Y-%m-%d'),
                                                customer_manager='{user}',
                                                introduction_department = '{dept_code}',
                                                allow_busi_type = 'OPTION,PRODUCT'
                                                where corporate_name='{corporatename}'"""
                        print(f"execute_sql:{set_derivative}")
                        await cursor.execute(set_derivative)

                        set_counterpartyOrg = f"update counterparty_org set aml_monitor_flag = 'true' where corporate_name = '{corporatename}'"
                        print(f"execute_sql:{set_counterpartyOrg}")
                        await cursor.execute(set_counterpartyOrg)

                        # 查询受益人
                        select_beneficiary = f"""select count(*) from aml_beneficiary ab
                                                    left join aml_counterparty ac on ac.id = ab.counterparty_id
                                                    left join otc_derivative_counterparty odc on odc.client_id = ac.client_id
                                                    where ab.category = '1' and odc.corporate_name ='{corporatename}'"""
                        print(f"execute_sql:{select_beneficiary}")
                        await cursor.execute(select_beneficiary)
                        if not await cursor.fetchall():
                            raise ValueError("缺少受益人或者被授权代表人")

                        # 设置投资者明细条件
                        select_prod_client = f"select client_id from otc_derivative_counterparty where corporate_name = '{corporatename}' and is_prod_holder = '03'"
                        await cursor.execute(select_prod_client)
                        prod_clients = [item[0] for item in await cursor.fetchall()]
                        if prod_clients:
                            for client_id in prod_clients:
                                benefit_over_flag = f"select * from COUNTERPARTY_BENEFIT_OVER_LIST where client_id = '{client_id}'"
                                await cursor.execute(benefit_over_flag)
                                data = await cursor.fetchall()
                                if not data:
                                    insert_benefit = f"""INSERT INTO COUNTERPARTY_BENEFIT_OVER_LIST
                                                        (CLIENT_ID, NAME, ID_NO, PROPORTION, FIID, PROFESSIONAL_INVESTOR_FLAG, FINANCIAL_ASSETS_OF_LASTYEAR, INVEST_3YEAR_EXP_FLAG, PROD_ID, ASSETS_20MILLION_FLAG)
                                                        VALUES('{client_id}', '测试', '81UB1HOGR7K8497CLS7PIM0C82P30F2S', 0.0321, NULL, '1', 123, '1', NULL, '1')"""
                                    print(f"execute_sql:{insert_benefit}")
                                    await cursor.execute(insert_benefit)
                                else:
                                    update_benefit = f"""UPDATE COUNTERPARTY_BENEFIT_OVER_LIST
                                                        SET NAME='测试', PROPORTION=0.0321, FIID=NULL, PROFESSIONAL_INVESTOR_FLAG='1', FINANCIAL_ASSETS_OF_LASTYEAR=123, INVEST_3YEAR_EXP_FLAG='1', PROD_ID=NULL, ASSETS_20MILLION_FLAG='1'
                                                        WHERE CLIENT_ID='{client_id}'"""
                                    print(f"execute_sql:{update_benefit}")
                                    await cursor.execute(update_benefit)

                        await db.commit()

                        result = await asyncio.gather(asyncio.create_task(single_review(corporatename,env)),
                                                      asyncio.create_task(multiple_review(corporatename,env)),
                                                      asyncio.create_task(uploadFile(env)))
                        print(result)
                        await cursor.execute(
                            f"select record_id from client_review_record where client_name ='{corporatename}' and current_status !='CLOSED'")
                        record_id = await cursor.fetchall()
                        if not record_id:
                            raise Exception("发起流程失败")
                        print(f"record_id 是 {record_id}")
                        data_tuple = []
                        if len(record_id) == 1:
                            for i in range(13):
                                data_list = []
                                data_list.append(record_id[0][0])
                                data_list.append(result[2][i])
                                data_tuple.append(data_list)
                            print(data_tuple)

                        elif len(record_id) == 2:
                            _location = 0
                            while _location < 26:
                                data_list = []
                                if _location >= 0 and _location < 13:
                                    data_list.append(record_id[0][0])
                                    data_list.append(result[2][_location])
                                    data_tuple.append(data_list)

                                elif _location >= 13:
                                    data_list.append(record_id[1][0])
                                    data_list.append(result[2][_location])
                                    data_tuple.append(data_list)
                                _location += 1
                        # update_file = f"""update client_review_file_record set record_id=%s where s3_file_id=%s"""
                        # await  cursor.executemany(update_file, data_tuple)
                        for i in data_tuple:
                            update_file = f"update client_review_file_record set record_id = '{i[0]}' where s3_file_id = '{i[1]}'"
                            print(f"execute_sql:{update_file}")
                            await cursor.execute(update_file)

                        print(f"update_file : {update_file}")
                        for i in range(len(record_id)):
                            update_detail = f"""insert into CLIENT_REVIEW_DETAIL(
                            id,record_id,client_name,client_position,email,phone,review_log,suitability,suitability_log,created_datetime) 
                            values('{str(uuid4())}','{record_id[i][0]}','11','老师','123@qq.com','13112345678','123','N','123',date_format('{date_str}','%Y-%m-%d')) """
                            print(f"execute_sql:{update_detail}")
                            await cursor.execute(update_detail)

                            update_counterparty = f"""update client_review_counterparty set agree_info = 'Y',benefit_over_flag = '1' where record_id = '{record_id[i][0]}'"""
                            print(f"execute_sql:{update_counterparty}")
                            await cursor.execute(update_counterparty)

                            update_record = f"""update client_review_record set accounting_firm_name='测试专用',sale_person = 'zhuliejin'  where record_id = '{record_id[i][0]}'"""
                            print(f"execute_sql:{update_record}")

                            await cursor.execute(update_record)
                        await db.commit()
                        end = time.time()
                        print(f"回访流程已成功发起，耗时:%.2f s" % (end - start))

                        select_flow = f"select title, doc_id ,record_id ,CURRENT_OPERATOR from client_review_record where client_name = '{corporatename}' and current_status !='CLOSED'"
                        await cursor.execute(select_flow)
                        flow_title = await cursor.fetchall()
                        response_list = []
                        for i in flow_title:
                            flow_dict = {}
                            flow_dict["corporateName"] = corporatename
                            flow_dict["title"] = i[0]
                            flow_dict["documentId"] = i[1]
                            flow_dict["recordId"]=i[2]
                            flow_dict["currentOperator"] = i[3]
                            response_list.append(flow_dict)
                        return {"code": 200,
                                "env": enviroment.name,
                                "data": [ReviewFlowResponse(**item) for item in response_list],
                                "status": "Successfully"}

        except Exception as e:
            print("{'error':%s}" % e)
            return {"code": 200,
                    "env": enviroment.name,
                    "data": [ReviewFlowResponse(**item) for item in response_list],
                    "status": "Successfully"}
    #股衍的流程
    else :
        try:
            with cx_Oracle.connect("gf_otc", "otc1qazXSW@", "10.62.146.18:1521/jgjtest") as db:
                with db.cursor() as cursor:
                    select_client = f"select client_id from otc_derivative_counterparty where corporate_name = '{corporatename}'"
                    cursor.execute(select_client)
                    if not cursor.fetchall():
                        raise ValueError("交易对手不存在")
                    # 删除存量流程
                    delete_flow = f"delete from client_review_record where client_name = '{corporatename}' and current_status !='CLOSED' "
                    print(f"execute_sql:{delete_flow}")
                    cursor.execute(delete_flow)
                    # 查找客户经理
                    select_customermanager = f"SELECT a2.userid , a.dept_code FROM AORG a LEFT JOIN Auser a2 ON a.ORGID  = a2.ORGID WHERE a2.USERNAME = '{customerManager}' "
                    print(f"execute_sql:{select_customermanager}")
                    cursor.execute(select_customermanager)
                    customer = cursor.fetchall()
                    if customer:
                        user, dept_code = customer[0][0], customer[0][1]
                        print(f"user:{customer[0][0]},dept_code:{customer[0][1]}")
                    else:
                        raise Exception("请输入正确的客户经理")
                    # 直接设置发起回访流程的条件
                    set_derivative = f"""update otc_derivative_counterparty set
                                            aml_monitor_flag = 'true',
                                            no_auto_visit = 'false',
                                            return_visit_date=to_date('{date_str}','yyyy-mm-dd'),
                                            customer_manager='{user}',
                                            introduction_department = '{dept_code}',
                                            allow_busi_type = 'OPTION,PRODUCT'
                                            where corporate_name='{corporatename}'"""
                    print(f"execute_sql:{set_derivative}")
                    cursor.execute(set_derivative)

                    set_counterpartyOrg = f"update counterparty_org set aml_monitor_flag = 'true' where corporate_name = '{corporatename}'"
                    print(f"execute_sql:{set_counterpartyOrg}")
                    cursor.execute(set_counterpartyOrg)

                    # 查询受益人
                    select_beneficiary = f"""select count(*) from aml_beneficiary ab
                                                left join aml_counterparty ac on ac.id = ab.counterparty_id
                                                left join otc_derivative_counterparty odc on odc.client_id = ac.client_id
                                                where ab.category = '1' and odc.corporate_name ='{corporatename}'"""
                    print(f"execute_sql:{select_beneficiary}")
                    cursor.execute(select_beneficiary)
                    if not cursor.fetchall():
                        raise ValueError("缺少受益人或者被授权代表人")

                    # 设置投资者明细条件
                    select_prod_client = f"select client_id from otc_derivative_counterparty where corporate_name = '{corporatename}' and is_prod_holder = '03'"
                    cursor.execute(select_prod_client)
                    prod_clients = [item[0] for item in cursor.fetchall()]
                    if prod_clients:
                        for client_id in prod_clients:
                            benefit_over_flag = f"select * from COUNTERPARTY_BENEFIT_OVER_LIST where client_id = '{client_id}'"
                            cursor.execute(benefit_over_flag)
                            data = cursor.fetchall()
                            if not data:
                                insert_benefit = f"""INSERT INTO COUNTERPARTY_BENEFIT_OVER_LIST
                                                    (CLIENT_ID, NAME, ID_NO, PROPORTION, FIID, PROFESSIONAL_INVESTOR_FLAG, FINANCIAL_ASSETS_OF_LASTYEAR, INVEST_3YEAR_EXP_FLAG, PROD_ID, ASSETS_20MILLION_FLAG)
                                                    VALUES('{client_id}', '测试', '81UB1HOGR7K8497CLS7PIM0C82P30F2S', 0.0321, NULL, '1', 123, '1', NULL, '1')"""
                                print(f"execute_sql:{insert_benefit}")
                                cursor.execute(insert_benefit)
                            else:
                                update_benefit = f"""UPDATE COUNTERPARTY_BENEFIT_OVER_LIST
                                                    SET NAME='测试', PROPORTION=0.0321, FIID=NULL, PROFESSIONAL_INVESTOR_FLAG='1', FINANCIAL_ASSETS_OF_LASTYEAR=123, INVEST_3YEAR_EXP_FLAG='1', PROD_ID=NULL, ASSETS_20MILLION_FLAG='1'
                                                    WHERE CLIENT_ID='{client_id}'"""
                                print(f"execute_sql:{update_benefit}")
                                cursor.execute(update_benefit)

                    db.commit()

                    result = await asyncio.gather(asyncio.create_task(single_review(corporatename,env)),
                                                  asyncio.create_task(multiple_review(corporatename,env)),
                                                  asyncio.create_task(uploadFile(env)))
                    print(result)
                    cursor.execute(
                        f"select record_id from client_review_record where client_name ='{corporatename}' and current_status !='CLOSED'")
                    record_id = cursor.fetchall()
                    if not record_id:
                        raise Exception("发起流程失败")
                    print(f"record_id 是 {record_id}")
                    data_tuple = []
                    if len(record_id) == 1:
                        for i in range(14):
                            data_list = []
                            data_list.append(record_id[0][0])
                            data_list.append(result[2][i])
                            data_tuple.append(data_list)
                        print(data_tuple)

                    elif len(record_id) == 2:
                        _location = 0
                        while _location < 28:
                            data_list = []
                            if _location >= 0 and _location < 14:
                                data_list.append(record_id[0][0])
                                data_list.append(result[2][_location])
                                data_tuple.append(data_list)

                            elif _location >= 14:
                                data_list.append(record_id[1][0])
                                data_list.append(result[2][_location])
                                data_tuple.append(data_list)
                            _location += 1
                    # update_file = f"""update client_review_file_record set record_id=%s where s3_file_id=%s"""
                    # await  cursor.executemany(update_file, data_tuple)
                    for i in data_tuple:
                        update_file = f"update client_review_file_record set record_id = '{i[0]}' where s3_file_id = '{i[1]}'"
                        print(f"execute_sql:{update_file}")
                        cursor.execute(update_file)
                    db.commit()

                    print(f"update_file : {update_file}")
                    for i in range(len(record_id)):
                        update_detail = f"""insert into CLIENT_REVIEW_DETAIL(
                        id,record_id,client_name,client_position,email,phone,review_log,suitability,suitability_log,created_datetime) 
                        values('{str(uuid4())}','{record_id[i][0]}','11','老师','123@qq.com','13112345678','123','N','123',to_date('{date_str}','yyyy-mm-dd')) """
                        print(f"execute_sql:{update_detail}")
                        cursor.execute(update_detail)

                        update_counterparty = f"""update client_review_counterparty set agree_info = 'Y',benefit_over_flag = '1',product_asset = 100 where record_id = '{record_id[i][0]}'"""
                        print(f"execute_sql:{update_counterparty}")
                        cursor.execute(update_counterparty)

                        indata = ({"accounting_firm_name":"测试专用","sale_person":"renyu","version":"202210" if isnew=="1" else None,"record_id":record_id[i][0]})
                        update_record = f"""update client_review_record set accounting_firm_name=:accounting_firm_name ,sale_person =:sale_person , version =:version where record_id =:record_id"""

                        print(f"execute_sql:{update_record}")

                        cursor.execute(update_record,indata)
                    db.commit()
                    end = time.time()


                    select_flow = f"select title, doc_id ,record_id ,CURRENT_OPERATOR from client_review_record where client_name = '{corporatename}' and current_status !='CLOSED'"
                    cursor.execute(select_flow)
                    flow_title = cursor.fetchall()
                    response_list = []
                    for i in flow_title:
                        flow_dict = {}
                        flow_dict["corporateName"] = corporatename
                        flow_dict["title"] = i[0]
                        flow_dict["documentId"] = i[1]
                        flow_dict["recordId"] = i[2]
                        flow_dict["currentOperator"] = i[3]
                        response_list.append(flow_dict)
                    print(f"回访流程已成功发起，耗时:%.2f s" % (end - start))
                    return {"code": 200,
                            "env": enviroment.name,
                            "data": [ReviewFlowResponse(**item) for item in response_list],
                            "status": "Successfully"}

        except Exception as e :
            print(e)
            return {"code": 500,
                            "env": enviroment.name,
                            "error":str(e),
                            "status": "Successfully"}

@review.post("/clientreview/buffer",summary="触发回访缓冲期",description="本接口会自动将资质改成满足,且先取消回访超期原因，然后将今天设置为缓冲期到期日并且触发回访缓冲期")
async def bufferjob(corporatename: str , enviroment:Enviroment):
    '''
    :param corporatename: 公司名称
    :param enviroment:40-固收测试环境 216-股衍测试环境
    '''
    print(f"公司名称 ：{corporatename},enviroment : {enviroment}")
    date_1 = date.today()
    date_str = date.strftime(date_1, "%Y-%m-%d")
    env = enviroment.value
    if enviroment.name == "FICC":
        try:
            async with aiomysql.create_pool(**config) as pool:
                async with pool.acquire() as db:
                    async with db.cursor() as cursor:
                        select_clientId = "select client_id from otc_derivative_counterparty where corporate_name =%s"
                        print(f"execute_sql:{select_clientId}")
                        await cursor.execute(select_clientId, corporatename)
                        client_id = [clientId[0] for clientId in await cursor.fetchall()]
                        print(client_id)

                        for i in range(len(client_id)):
                            select_buffer = "select client_id from client_review_buffer where client_id =%s"
                            await cursor.execute(select_buffer,client_id[i])
                            if not await cursor.fetchall():
                                print(f"{client_id[i]} 不存在")
                                raise Exception(f"{client_id[i]} 不存在")
                            update_buffer_sql = f"update client_review_buffer set review_buffer_start =date_format('{date_str}','%Y-%m-%d') where client_id ='{client_id[i]}'"
                            print(f"update_buffer_sql:{update_buffer_sql}")
                            await cursor.execute(update_buffer_sql)
                        update_qualify = "update otc_derivative_counterparty set client_qualify_review_reason=NULL,client_qualify_review='true' where corporate_name=%s"
                        print(f"update_buffer_sql:{update_qualify}")
                        await cursor.execute(update_qualify,corporatename)
                        await db.commit()


                        # response = requests.get(env + "/api/test/countdownReviewBuffer")
                        # print(response.json())
                        with requests.Session() as session :
                            with session.post(env + "/api/test/countdownReviewBuffer") as response:
                                print(response.json())
                                return {"code": 200,
                                "env": enviroment.name,
                                "data": response.json(),
                                "status": "Successfully"}



        except Exception as e :
            return {"code": 500,
                    "env": enviroment.name,
                    "data": "失败",
                    "status": "Failed"}
    try:
        with cx_Oracle.connect("gf_otc", "otc1qazXSW@", "10.62.146.18:1521/jgjtest") as db:
            with db.cursor() as cursor:

                    select_clientId = f"select client_id from otc_derivative_counterparty where corporate_name ='{corporatename}'"
                    print(f"execute_sql:{select_clientId}")
                    cursor.execute(select_clientId)
                    client_id = [clientId[0] for clientId in cursor.fetchall()]
                    print(client_id)

                    for i in range(len(client_id)):
                        select_buffer = f"select client_id from client_review_buffer where client_id ='{client_id[i]}'"
                        cursor.execute(select_buffer)
                        if not cursor.fetchall():
                            print(f"{client_id[i]} 不存在")
                            raise Exception(f"{client_id[i]} 不存在")
                        update_buffer_sql = f"update client_review_buffer set review_buffer_start =to_date('{date_str}','yyyy-mm-dd') where client_id ='{client_id[i]}'"
                        print(f"update_buffer_sql:{update_buffer_sql}")
                        cursor.execute(update_buffer_sql)
                    update_qualify = f"update otc_derivative_counterparty set client_qualify_review_reason=NULL,client_qualify_review='true' where corporate_name='{corporatename}'"
                    print(f"update_buffer_sql:{update_qualify}")
                    cursor.execute(update_qualify)
                    db.commit()

                    # response = requests.get(env + "/api/test/countdownReviewBuffer")
                    # print(response.json())
                    with requests.Session() as session:
                        with session.post(env + "/api/test/countdownReviewBuffer") as response:
                            print(response.json())
                            return {"code": 200,
                                    "env": enviroment.name,
                                    "data": response.json(),
                                    "status": "Successfully"}
    except Exception as e :
        print(e)
        return {"code": 500,
                "env": enviroment.name,
                "data": "失败",
                "status": "Failed"}

@review.get("/checkReviewRecord",summary="查询回访流程record表的信息",description="查询流程状态，当前处理人和会计师事务所，页面信息等数据")
def check_review_record(recordId : str , enviroment : Enviroment):
    '''
    :param recordId: 流程ID
    :param enviroment: 40-固收测试环境 216-股衍测试环境

    '''
    print(recordId)
    if enviroment.name == "FICC" :
        try :
            db = pymysql.connect(**config)
            cursor = db.cursor()

            if cursor :
                print(f"成功连接{enviroment.name} 的数据库")
            else  :
                raise Exception(f"连接{enviroment.name}数据库异常")
        except Exception as e :
            print(e)
            return e

        try :
            check_record = f"""select 
                                    id ,doc_id ,title , client_name ,UNIFIEDSOCIAL_CODE , REVIEW_DATE ,
                                    REVIEW_USER ,REVIEW_NAME ,CURRENT_STATUS ,CURRENT_OPERATOR ,CURRENT_ACTIVITY_NAME ,
                                    RECORD_ID ,CREATED_DATETIME ,WORK_PHONE ,PHONE,SECURITY_LEVEL ,SECURITY_LEVEL_DETAIL ,
                                    URGENCY_LEVEL ,URGENCY_LEVEL_REASON ,SALE_PERSON ,REVIEW_TERM ,REVIEW_PROCESS_TYPE,SPECIAL_MENTIONED_CUSTOMER ,
                                    VERSION ,NO_MORE_REVIEW ,ACCOUNTING_FIRM_NAME ,SUPPLEMENTARY_MATERIALS_TIME ,SUPPLEMENTARY_MATERIALS ,REACH_TO_03_DATETIME ,
                                    SERIAL_NUMBER from client_review_record where record_id = %s"""
            cursor.execute(check_record, (recordId,))
            record_info = cursor.fetchone()
            print(record_info)
            result = ReviewRecordInfo(ID=record_info[0], DOC_ID=record_info[1], TITLE=record_info[2],
                                      CLIENT_NAME=record_info[3],
                                      UNIFIEDSOCIAL_CODE=record_info[4], REVIEW_DATE=record_info[5],
                                      REVIEW_USER=record_info[6], REVIEW_NAME=record_info[7],
                                      CURRENT_STATUS=record_info[8], CURRENT_OPERATOR=record_info[9],
                                      CURRENT_ACTIVITY_NAME=record_info[10], RECORD_ID=record_info[11],
                                      CREATED_DATETIME=record_info[12], WORK_PHONE=record_info[13],
                                      PHONE=record_info[14], SECURITY_LEVEL=record_info[15],
                                      SECURITY_LEVEL_DETAIL=record_info[16], URGENCY_LEVEL=record_info[17],
                                      URGENCY_LEVEL_REASON=record_info[18], SALE_PERSON=record_info[19],
                                      REVIEW_TERM=record_info[20], REVIEW_PROCESS_TYPE=record_info[21],
                                      SPECIAL_MENTIONED_CUSTOMER=record_info[22], VERSION=record_info[23],
                                      NO_MORE_REVIEW=record_info[24], ACCOUNTING_FIRM_NAME=record_info[25],
                                      SUPPLEMENTARY_MATERIALS_TIME=record_info[26],
                                      SUPPLEMENTARY_MATERIALS=record_info[27],
                                      REACH_TO_03_DATETIME=record_info[28], SERIAL_NUMBER=record_info[29])
            print(result)
            return {"code": 200,
                    "env": enviroment.name,
                    "data": result,
                    "status": "Successfully"}

        except Exception as e :
            db.rollback()
            print("error : %s"%e)
        finally:
            cursor.close()
            db.close()
    else :
        try:
            db = cx_Oracle.connect("gf_otc","otc1qazXSW@","10.62.146.18:1521/jgjtest")
            cursor = db.cursor()
            if cursor :
                print(f"成功连接{enviroment.name} 的数据库")
            else  :
                raise Exception(f"连接{enviroment.name}数据库异常")
        except Exception as e :
            print(e)
            return e

        try :
            check_record = f"""select 
                                    id ,doc_id ,title , client_name ,UNIFIEDSOCIAL_CODE , REVIEW_DATE ,
                                    REVIEW_USER ,REVIEW_NAME ,CURRENT_STATUS ,CURRENT_OPERATOR ,CURRENT_ACTIVITY_NAME ,
                                    RECORD_ID ,CREATED_DATETIME ,WORK_PHONE ,PHONE,SECURITY_LEVEL ,SECURITY_LEVEL_DETAIL ,
                                    URGENCY_LEVEL ,URGENCY_LEVEL_REASON ,SALE_PERSON ,REVIEW_TERM ,REVIEW_PROCESS_TYPE,SPECIAL_MENTIONED_CUSTOMER ,
                                    VERSION ,NO_MORE_REVIEW ,ACCOUNTING_FIRM_NAME ,SUPPLEMENTARY_MATERIALS_TIME ,SUPPLEMENTARY_MATERIALS ,REACH_TO_03_DATETIME ,
                                    SERIAL_NUMBER from client_review_record where record_id = :recordId"""
            cursor.execute(check_record, {"recordId":recordId})
            record_info = cursor.fetchone()
            print(record_info)
            result = ReviewRecordInfo(ID=record_info[0], DOC_ID =record_info[1],TITLE =record_info[2],CLIENT_NAME =record_info[3],
                                      UNIFIEDSOCIAL_CODE=record_info[4],REVIEW_DATE=record_info[5],REVIEW_USER =record_info[6],REVIEW_NAME =record_info[7],
                                      CURRENT_STATUS=record_info[8],CURRENT_OPERATOR =record_info[9],CURRENT_ACTIVITY_NAME =record_info[10],RECORD_ID=record_info[11],
                                      CREATED_DATETIME=record_info[12],WORK_PHONE =record_info[13],PHONE = record_info[14],SECURITY_LEVEL =record_info[15],
                                      SECURITY_LEVEL_DETAIL=record_info[16],URGENCY_LEVEL =record_info[17],URGENCY_LEVEL_REASON =record_info[18],SALE_PERSON =record_info[19],
                                      REVIEW_TERM=record_info[20],REVIEW_PROCESS_TYPE =record_info[21],SPECIAL_MENTIONED_CUSTOMER =record_info[22],VERSION =record_info[23],
                                      NO_MORE_REVIEW=record_info[24],ACCOUNTING_FIRM_NAME =record_info[25],SUPPLEMENTARY_MATERIALS_TIME =record_info[26],SUPPLEMENTARY_MATERIALS =record_info[27],
                                      REACH_TO_03_DATETIME=record_info[28],SERIAL_NUMBER =record_info[29])
            print(result)
            return {"code": 200,
                    "env": enviroment.name,
                    "data": result,
                    "status": "Successfully"}
        except Exception as e :
            print(e)
            return e
        finally:
            cursor.close()
            db.close()

@review.get("/checkReviewCounterparty",summary="查询回访流程回访对象的信息",description="查询对应各个回访对象的信息（如拟参与业务类型），不包括回访受益人")
def check_review_counterparty(recordId:str,enviroment : Enviroment):
    '''
        :param recordId: 流程ID
        :param enviroment: 40-固收测试环境 216-股衍测试环境

    '''
    print(recordId)
    if enviroment.name == "FICC":
        try:
            db = pymysql.connect(**config)
            cursor = db.cursor()

            if cursor:
                print(f"成功连接{enviroment.name} 的数据库")
            else:
                raise Exception(f"连接{enviroment.name}数据库异常")
        except Exception as e:
            print(e)
            return e

        try:
            counterparty_list = []
            check_record = f"""select 
                                    ID,RECORD_ID,PRODUCT_NAME,
                                    CREATED_DATETIME,CLIENT_ID,"IGNORE",
                                    BENEFIT_OVER_FLAG,AGREE_INFO,ALLOW_BUSI_TYPE,
                                    CLIENT_QUALIFY_REVIEW,SEQ,supplementary_materials_note,
                                    show_note,review_buffer_start,ALLOW_BUSI_TYPE_HIS,
                                    MANUAL_DEL_ALLOW_BUSI_TYPE from client_review_counterparty where record_id = %s"""
            cursor.execute(check_record, (recordId,))
            while True :
                rows = cursor.fetchmany(1)
                counterpartyList = {}
                if not rows :
                    break
                counterpartyList["ID"] = rows[0][0]
                counterpartyList["RECORD_ID"] = rows[0][1]
                counterpartyList["PRODUCT_NAME"] = rows[0][2]
                counterpartyList["CREATED_DATETIME"] = rows[0][3]
                counterpartyList["CLIENT_ID"] = rows[0][4]
                counterpartyList["IGNORE"] = rows[0][5]
                counterpartyList["BENEFIT_OVER_FLAG"] = rows[0][6]
                counterpartyList["AGREE_INFO"] = rows[0][7]
                counterpartyList["ALLOW_BUSI_TYPE"] = rows[0][8]
                counterpartyList["CLIENT_QUALIFY_REVIEW"] = rows[0][9]
                counterpartyList["SEQ"] = rows[0][10]
                counterpartyList["SUPPLEMENTARY_MATERIALS_NOTE"] = rows[0][11]
                counterpartyList["SHOW_NOTE"] = rows[0][12]
                counterpartyList["REVIEW_BUFFER_START"] = rows[0][13]
                counterpartyList["ALLOW_BUSI_TYPE_HIS"] = rows[0][14]
                counterpartyList["MANUAL_DEL_ALLOW_BUSI_TYPE"] = rows[0][15]
                counterparty_list.append(counterpartyList)
            return {"code": 200,
                    "env": enviroment.name,
                    "data":[ReviewCounterpartyInfo(**item) for item in counterparty_list],
                    "status": "Successfully"}
        except Exception as e:
            db.rollback()
            print("error : %s" % e)
        finally:
            cursor.close()
            db.close()
    else:
        try:
            db = cx_Oracle.connect("gf_otc", "otc1qazXSW@", "10.62.146.18:1521/jgjtest")
            cursor = db.cursor()
            if cursor:
                print(f"成功连接{enviroment.name} 的数据库")
            else:
                raise Exception(f"连接{enviroment.name}数据库异常")
        except Exception as e:
            print(e)
            return e

        try:
            counterparty_list = []
            check_record = f"""select 
                                    ID,RECORD_ID,PRODUCT_NAME,
                                    CREATED_DATETIME,CLIENT_ID,"IGNORE",
                                    BENEFIT_OVER_FLAG,AGREE_INFO,ALLOW_BUSI_TYPE,
                                    CLIENT_QUALIFY_REVIEW,SEQ,REVIEW_BUFFER_START,
                                    SUPPLEMENTARY_MATERIALS_NOTE,SHOW_NOTE,ALLOW_BUSI_TYPE_HIS,
                                    MANUAL_DEL_ALLOW_BUSI_TYPE ,PRODUCT_ASSET from client_review_counterparty where record_id = :recordId"""
            cursor.execute(check_record, {"recordId": recordId})
            while True :
                rows = cursor.fetchmany(1)
                counterpartyList = {}
                if not rows :
                    break
                counterpartyList["ID"] = rows[0][0]
                counterpartyList["RECORD_ID"] = rows[0][1]
                counterpartyList["PRODUCT_NAME"] = rows[0][2]
                counterpartyList["CREATED_DATETIME"] = rows[0][3]
                counterpartyList["CLIENT_ID"] = rows[0][4]
                counterpartyList["IGNORE"] = rows[0][5]
                counterpartyList["BENEFIT_OVER_FLAG"] = rows[0][6]
                counterpartyList["AGREE_INFO"] = rows[0][7]
                counterpartyList["ALLOW_BUSI_TYPE"] = rows[0][8]
                counterpartyList["CLIENT_QUALIFY_REVIEW"] = rows[0][9]
                counterpartyList["SEQ"] = rows[0][10]
                counterpartyList["REVIEW_BUFFER_START"] = rows[0][11]
                counterpartyList["SUPPLEMENTARY_MATERIALS_NOTE"] = rows[0][12]
                counterpartyList["SHOW_NOTE"] = rows[0][13]
                counterpartyList["ALLOW_BUSI_TYPE_HIS"] = rows[0][14]
                counterpartyList["MANUAL_DEL_ALLOW_BUSI_TYPE"] = rows[0][15]
                counterpartyList["PRODUCT_ASSET"] = rows[0][16]
                counterparty_list.append(counterpartyList)
            return {"code": 200,
                    "env": enviroment.name,
                    "data": [ReviewCounterpartyInfo(**item) for item in counterparty_list],
                    "status": "Successfully"}



        except Exception as e:
            print(e)
            return e
        finally:
            cursor.close()
            db.close()

@review.get("/checkReviewAmlBenificiary",summary="查询回访流程受益人的信息",description="查询回访流程中所有受益人的信息")
def check_review_aml_beneficiary(recordId:str,enviroment : Enviroment):
    '''
            :param recordId: 流程ID
            :param enviroment: 40-固收测试环境 216-股衍测试环境

    '''
    print(recordId)
    if enviroment.name == "FICC":
        try:
            db = pymysql.connect(**config)
            cursor = db.cursor()

            if cursor:
                print(f"成功连接{enviroment.name} 的数据库")
            else:
                raise Exception(f"连接{enviroment.name}数据库异常")
        except Exception as e:
            print(e)
            return e



        try:
            check_beneficiary = f"""select ID , ENTITY_TYPE ,CATEGORY ,  NAME,ID_KIND,
                                            ID_NO,BIRTH,GENDER,COUNTRY,ID_VALIDDATE_START,
                                            ID_VALIDDATE_END,PHONE,MOBILE,EMAIL,HOLD_RATE,
                                            SPECIAL_TYPE,POSITION,t.HOLD_TYPE,t.BENEFICIARY_TYPE,
                                            t.LOCKED,t.COUNTERPARTY_ID,t.ADDRESS,t.VERSION,t.CLIENT_KIND,
                                            t.CLIENT_ID,t.RECORD_ID,t.BUSINESS_TYPE from client_review_aml_beneficiary t where record_id = %s and CATEGORY in ('1','6')"""
            cursor.execute(check_beneficiary,(recordId,))
            beneficiary = []
            while True :
                rows = cursor.fetchmany(1)
                if not rows :
                    break
                print(rows)
                beneficiary_dict = {}
                beneficiary_dict["ID"] = rows[0][0]
                beneficiary_dict["ENTITY_TYPE"] = rows[0][1]
                beneficiary_dict["CATEGORY"] = rows[0][2]
                beneficiary_dict["NAME"] = rows[0][3]
                beneficiary_dict["ID_KIND"] = rows[0][4]
                beneficiary_dict["ID_NO"] = rows[0][5]
                beneficiary_dict["BIRTH"] = rows[0][6]
                beneficiary_dict["GENDER"] = rows[0][7]
                beneficiary_dict["COUNTRY"] = rows[0][8]
                beneficiary_dict["ID_VALIDDATE_START"] = rows[0][9]
                beneficiary_dict["id_validdate_end".upper()] = rows[0][10]
                beneficiary_dict["phone".upper()] = rows[0][11]
                beneficiary_dict["mobile".upper()] = rows[0][12]
                beneficiary_dict["email".upper()] = rows[0][13]
                beneficiary_dict["hold_rate".upper()] = rows[0][14]
                beneficiary_dict["special_type".upper()] = rows[0][15]
                beneficiary_dict["position".upper()] = rows[0][16]
                beneficiary_dict["hold_type".upper()] = rows[0][17]
                beneficiary_dict["beneficiary_type".upper()] = rows[0][18]
                beneficiary_dict["locked".upper()] = rows[0][19]
                beneficiary_dict["counterparty_id".upper()] = rows[0][20]
                beneficiary_dict["address".upper()] = rows[0][21]
                beneficiary_dict["version".upper()] = rows[0][22]
                beneficiary_dict["client_kind".upper()] = rows[0][23]
                beneficiary_dict["client_id".upper()] = rows[0][24]
                beneficiary_dict["record_id".upper()] = rows[0][25]
                beneficiary_dict["business_type".upper()] = rows[0][26]
                beneficiary.append(beneficiary_dict)


            return {"code": 200,
                    "env": enviroment.name,
                    "data": [ReviewAmlBeneficiaryInfo(**item) for item in beneficiary],
                    "status": "Successfully"}
        except Exception as e:
            db.rollback()
            print("error : %s" % e)
        finally:
            cursor.close()
            db.close()
    else:
        try:
            db = cx_Oracle.connect("gf_otc", "otc1qazXSW@", "10.62.146.18:1521/jgjtest")
            cursor = db.cursor()
            if cursor:
                print(f"成功连接{enviroment.name} 的数据库")
            else:
                raise Exception(f"连接{enviroment.name}数据库异常")
        except Exception as e:
            print(e)
            return e

        try:
            check_beneficiary = f"""select ID , ENTITY_TYPE ,CATEGORY ,  NAME,ID_KIND,
                                            ID_NO,BIRTH,GENDER,COUNTRY,ID_VALIDDATE_START,
                                            ID_VALIDDATE_END,PHONE,MOBILE,EMAIL,HOLD_RATE,
                                            SPECIAL_TYPE,POSITION,t.HOLD_TYPE,t.BENEFICIARY_TYPE,
                                            t.LOCKED,t.COUNTERPARTY_ID,t.ADDRESS,t.VERSION,t.CLIENT_KIND,
                                            t.CLIENT_ID,t.RECORD_ID,t.BUSINESS_TYPE from client_review_aml_beneficiary t where record_id = :recordId and CATEGORY in ('1','6')"""
            cursor.execute(check_beneficiary,{"recordId":recordId})
            beneficiary = []
            while True:
                rows = cursor.fetchmany(1)
                if not rows:
                    break
                print(rows)
                beneficiary_dict = {}
                beneficiary_dict["ID"] = rows[0][0]
                beneficiary_dict["ENTITY_TYPE"] = rows[0][1]
                beneficiary_dict["CATEGORY"] = rows[0][2]
                beneficiary_dict["NAME"] = rows[0][3]
                beneficiary_dict["ID_KIND"] = rows[0][4]
                beneficiary_dict["ID_NO"] = rows[0][5]
                beneficiary_dict["BIRTH"] = rows[0][6]
                beneficiary_dict["GENDER"] = rows[0][7]
                beneficiary_dict["COUNTRY"] = rows[0][8]
                beneficiary_dict["ID_VALIDDATE_START"] = rows[0][9]
                beneficiary_dict["id_validdate_end".upper()] = rows[0][10]
                beneficiary_dict["phone".upper()] = rows[0][11]
                beneficiary_dict["mobile".upper()] = rows[0][12]
                beneficiary_dict["email".upper()] = rows[0][13]
                beneficiary_dict["hold_rate".upper()] = rows[0][14]
                beneficiary_dict["special_type".upper()] = rows[0][15]
                beneficiary_dict["position".upper()] = rows[0][16]
                beneficiary_dict["hold_type".upper()] = rows[0][17]
                beneficiary_dict["beneficiary_type".upper()] = rows[0][18]
                beneficiary_dict["locked".upper()] = rows[0][19]
                beneficiary_dict["counterparty_id".upper()] = rows[0][20]
                beneficiary_dict["address".upper()] = rows[0][21]
                beneficiary_dict["version".upper()] = rows[0][22]
                beneficiary_dict["client_kind".upper()] = rows[0][23]
                beneficiary_dict["client_id".upper()] = rows[0][24]
                beneficiary_dict["record_id".upper()] = rows[0][25]
                beneficiary_dict["business_type".upper()] = rows[0][26]
                beneficiary.append(beneficiary_dict)

            return {"code": 200,
                    "env": enviroment.name,
                    "data": [ReviewAmlBeneficiaryInfo(**item) for item in beneficiary],
                    "status": "Successfully"}
        except Exception as e:
            db.rollback()
            print("error : %s" % e)

        finally:
            cursor.close()
            db.close()

@review.get("/checkReviewBenefitOver",summary="查询投资者明细信息",description="查询回访流程中的投资者明细")
def checkt_review_counterparty_benefit(recordId : str ,enviroment : Enviroment):
    '''
        :param recordId: 流程ID
        :param enviroment: 40-固收测试环境 216-股衍测试环境
    '''
    print(recordId)
    if enviroment.name == "FICC":
        try:
            db = pymysql.connect(**config)
            cursor = db.cursor()

            if cursor:
                print(f"成功连接{enviroment.name} 的数据库")
            else:
                raise Exception(f"连接{enviroment.name}数据库异常")
        except Exception as e:
            print(e)
            return e

        try:
            check_beneficiary = f"""select t.client_id , t.name , t.id_no , 
                                            t.proportion , t.fiid , t.professional_investor_flag ,
                                            t.financial_assets_of_lastyear , t.invest_3year_exp_flag , t.prod_id,
                                            t.assets_20million_flag , t.record_id , t.id from client_review_benefit_over t where record_id = %s"""
            cursor.execute(check_beneficiary, (recordId,))
            info = []
            while True:
                rows = cursor.fetchmany(1)
                if not rows:
                    break
                print(rows)
                info_dict = {}
                info_dict["CLIENT_ID"] = rows[0][0]
                info_dict["NAME"] = rows[0][1]
                info_dict["ID_NO"] = rows[0][2]
                info_dict["PROPORTION"] = rows[0][3]
                info_dict["FIID"] = rows[0][4]
                info_dict["PROFESSIONAL_INVESTOR_FLAG"] = rows[0][5]
                info_dict["FINANCIAL_ASSETS_OF_LASTYEAR"] = rows[0][6]
                info_dict["INVEST_3YEAR_EXP_FLAG"] = rows[0][7]
                info_dict["PROD_ID"] = rows[0][7]
                info_dict["ASSETS_20MILLION_FLAG"] = rows[0][7]
                info_dict["RECORD_ID"] = rows[0][7]
                info_dict["ID"] = rows[0][8]
                info.append(info_dict)
            return {"code": 200,
                    "env": enviroment.name,
                    "data": [ReviewBenefitOver(**item) for item in info],
                    "status": "Successfully"}
        except Exception as e:
            db.rollback()
            print("error : %s" % e)
        finally:
            cursor.close()
            db.close()
    else:
        try:
            db = cx_Oracle.connect("gf_otc", "otc1qazXSW@", "10.62.146.18:1521/jgjtest")
            cursor = db.cursor()
            if cursor:
                print(f"成功连接{enviroment.name} 的数据库")
            else:
                raise Exception(f"连接{enviroment.name}数据库异常")
        except Exception as e:
            print(e)
            return e

        try:
            check_beneficiary = f"""SELECT 
                                        CLIENT_ID,NAME,ID_NO,
                                        PROPORTION,FIID,PROFESSIONAL_INVESTOR_FLAG,
                                        FINANCIAL_ASSETS_OF_LASTYEAR,INVEST_3YEAR_EXP_FLAG,
                                        PROD_ID,ASSETS_20MILLION_FLAG,RECORD_ID,ID FROM CLIENT_REVIEW_BENEFIT_OVER WHERE RECORD_ID = :recordId"""
            cursor.execute(check_beneficiary, {"recordId": recordId})
            info = []
            while True:
                rows = cursor.fetchmany(1)
                if not rows:
                    break
                print(rows)
                info_dict = {}
                info_dict["CLIENT_ID"] = rows[0][0]
                info_dict["NAME"] = rows[0][1]
                info_dict["ID_NO"] = rows[0][2]
                info_dict["PROPORTION"] = rows[0][3]
                info_dict["FIID"] = rows[0][4]
                info_dict["PROFESSIONAL_INVESTOR_FLAG"] = rows[0][5]
                info_dict["FINANCIAL_ASSETS_OF_LASTYEAR"] = rows[0][6]
                info_dict["INVEST_3YEAR_EXP_FLAG"] = rows[0][7]
                info_dict["PROD_ID"] = rows[0][7]
                info_dict["ASSETS_20MILLION_FLAG"] = rows[0][7]
                info_dict["RECORD_ID"] = rows[0][7]
                info_dict["ID"] = rows[0][8]
                info.append(info_dict)

            return {"code": 200,
                    "env": enviroment.name,
                    "data": [ReviewBenefitOver(**item) for item in info],
                    "status": "Successfully"}
        except Exception as e:
            db.rollback()
            print("error : %s" % e)

        finally:
            cursor.close()
            db.close()
    print(recordId)
    if enviroment.name == "FICC":
        try:
            db = pymysql.connect(**config)
            cursor = db.cursor()

            if cursor:
                print(f"成功连接{enviroment.name} 的数据库")
            else:
                raise Exception(f"连接{enviroment.name}数据库异常")
        except Exception as e:
            print(e)
            return e

        try:
            check_beneficiary = f"""select ID , ENTITY_TYPE ,CATEGORY ,  NAME,ID_KIND,
                                                ID_NO,BIRTH,GENDER,COUNTRY,ID_VALIDDATE_START,
                                                ID_VALIDDATE_END,PHONE,MOBILE,EMAIL,HOLD_RATE,
                                                SPECIAL_TYPE,POSITION,t.HOLD_TYPE,t.BENEFICIARY_TYPE,
                                                t.LOCKED,t.COUNTERPARTY_ID,t.ADDRESS,t.VERSION,t.CLIENT_KIND,
                                                t.CLIENT_ID,t.RECORD_ID,t.BUSINESS_TYPE from client_review_aml_beneficiary t where record_id = %s and CATEGORY in ('1','6')"""
            cursor.execute(check_beneficiary, (recordId,))
            beneficiary = []
            while True:
                rows = cursor.fetchmany(1)
                if not rows:
                    break
                print(rows)
                beneficiary_dict = {}
                beneficiary_dict["ID"] = rows[0][0]
                beneficiary_dict["ENTITY_TYPE"] = rows[0][1]
                beneficiary_dict["CATEGORY"] = rows[0][2]
                beneficiary_dict["NAME"] = rows[0][3]
                beneficiary_dict["ID_KIND"] = rows[0][4]
                beneficiary_dict["ID_NO"] = rows[0][5]
                beneficiary_dict["BIRTH"] = rows[0][6]
                beneficiary_dict["GENDER"] = rows[0][7]
                beneficiary_dict["COUNTRY"] = rows[0][8]
                beneficiary_dict["ID_VALIDDATE_START"] = rows[0][9]
                beneficiary_dict["id_validdate_end".upper()] = rows[0][10]
                beneficiary_dict["phone".upper()] = rows[0][11]
                beneficiary_dict["mobile".upper()] = rows[0][12]
                beneficiary_dict["email".upper()] = rows[0][13]
                beneficiary_dict["hold_rate".upper()] = rows[0][14]
                beneficiary_dict["special_type".upper()] = rows[0][15]
                beneficiary_dict["position".upper()] = rows[0][16]
                beneficiary_dict["hold_type".upper()] = rows[0][17]
                beneficiary_dict["beneficiary_type".upper()] = rows[0][18]
                beneficiary_dict["locked".upper()] = rows[0][19]
                beneficiary_dict["counterparty_id".upper()] = rows[0][20]
                beneficiary_dict["address".upper()] = rows[0][21]
                beneficiary_dict["version".upper()] = rows[0][22]
                beneficiary_dict["client_kind".upper()] = rows[0][23]
                beneficiary_dict["client_id".upper()] = rows[0][24]
                beneficiary_dict["record_id".upper()] = rows[0][25]
                beneficiary_dict["business_type".upper()] = rows[0][26]
                beneficiary.append(beneficiary_dict)

            return {"code": 200,
                    "env": enviroment.name,
                    "data": [ReviewAmlBeneficiaryInfo(**item) for item in beneficiary],
                    "status": "Successfully"}
        except Exception as e:
            db.rollback()
            print("error : %s" % e)
        finally:
            cursor.close()
            db.close()
    else:
        try:
            db = cx_Oracle.connect("gf_otc", "otc1qazXSW@", "10.62.146.18:1521/jgjtest")
            cursor = db.cursor()
            if cursor:
                print(f"成功连接{enviroment.name} 的数据库")
            else:
                raise Exception(f"连接{enviroment.name}数据库异常")
        except Exception as e:
            print(e)
            return e

        try:
            check_beneficiary = f"""select ID , ENTITY_TYPE ,CATEGORY ,  NAME,ID_KIND,
                                                ID_NO,BIRTH,GENDER,COUNTRY,ID_VALIDDATE_START,
                                                ID_VALIDDATE_END,PHONE,MOBILE,EMAIL,HOLD_RATE,
                                                SPECIAL_TYPE,POSITION,t.HOLD_TYPE,t.BENEFICIARY_TYPE,
                                                t.LOCKED,t.COUNTERPARTY_ID,t.ADDRESS,t.VERSION,t.CLIENT_KIND,
                                                t.CLIENT_ID,t.RECORD_ID,t.BUSINESS_TYPE from client_review_aml_beneficiary t where record_id = :recordId and CATEGORY in ('1','6')"""
            cursor.execute(check_beneficiary, {"recordId": recordId})
            beneficiary = []
            while True:
                rows = cursor.fetchmany(1)
                if not rows:
                    break
                print(rows)
                beneficiary_dict = {}
                beneficiary_dict["ID"] = rows[0][0]
                beneficiary_dict["ENTITY_TYPE"] = rows[0][1]
                beneficiary_dict["CATEGORY"] = rows[0][2]
                beneficiary_dict["NAME"] = rows[0][3]
                beneficiary_dict["ID_KIND"] = rows[0][4]
                beneficiary_dict["ID_NO"] = rows[0][5]
                beneficiary_dict["BIRTH"] = rows[0][6]
                beneficiary_dict["GENDER"] = rows[0][7]
                beneficiary_dict["COUNTRY"] = rows[0][8]
                beneficiary_dict["ID_VALIDDATE_START"] = rows[0][9]
                beneficiary_dict["id_validdate_end".upper()] = rows[0][10]
                beneficiary_dict["phone".upper()] = rows[0][11]
                beneficiary_dict["mobile".upper()] = rows[0][12]
                beneficiary_dict["email".upper()] = rows[0][13]
                beneficiary_dict["hold_rate".upper()] = rows[0][14]
                beneficiary_dict["special_type".upper()] = rows[0][15]
                beneficiary_dict["position".upper()] = rows[0][16]
                beneficiary_dict["hold_type".upper()] = rows[0][17]
                beneficiary_dict["beneficiary_type".upper()] = rows[0][18]
                beneficiary_dict["locked".upper()] = rows[0][19]
                beneficiary_dict["counterparty_id".upper()] = rows[0][20]
                beneficiary_dict["address".upper()] = rows[0][21]
                beneficiary_dict["version".upper()] = rows[0][22]
                beneficiary_dict["client_kind".upper()] = rows[0][23]
                beneficiary_dict["client_id".upper()] = rows[0][24]
                beneficiary_dict["record_id".upper()] = rows[0][25]
                beneficiary_dict["business_type".upper()] = rows[0][26]
                beneficiary.append(beneficiary_dict)

            return {"code": 200,
                    "env": enviroment.name,
                    "data": [ReviewAmlBeneficiaryInfo(**item) for item in beneficiary],
                    "status": "Successfully"}
        except Exception as e:
            db.rollback()
            print("error : %s" % e)

        finally:
            cursor.close()
            db.close()

@review.delete("/clientreview/deleteByClientName",summary="根据机构名称来删除在途回访流程",operation_id='delete_review_flow_by_clientName')
def deleteFlow(enviroment : Enviroment , clientNameList : List[str]):

    '''
    :param enviroment: 40-固收测试环境 216-股衍测试环境
    :param clientNameList: 机构名称列表，会删除机构下所有交易对手的在途流程
    :return:
    '''
    if enviroment.name == "FICC":
        try :
            db = pymysql.connect(**config)
            cursor = db.cursor()
            if cursor:
                print(f"{enviroment.name}连接成功")
            else :
                raise EnvironmentError("连接数据库错误")
        except EnvironmentError as e :
            print(e)
            return "连接数据库异常"

        try :
            print(clientNameList)
            client_name_list = []
            for i in clientNameList:
                demo_list = []
                demo_list.append(i)
                client_name_list.append(demo_list)
            cursor.executemany("delete from client_review_record where client_name = %s and current_status !='CLOSED'",client_name_list)
        except Exception as e :
            print(e)
            return e
        else :
            db.commit()
            print("已成功删除")
            return {"code": 200,
                    "env": enviroment.name,
                    "data": "已成功删除所有在途流程",
                    "status": "Successfully"}
        finally :
            cursor.close()
            db.close()
    else :
        try :
            db = cx_Oracle.connect("gf_otc", "otc1qazXSW@", "10.62.146.18:1521/jgjtest")
            cursor = db.cursor()
            if cursor:
                print(f"{enviroment.name}连接成功")
            else :
                raise EnvironmentError("连接数据库错误")
        except EnvironmentError as e :
            print(e)
            return "连接数据库异常"

        try :
            print(clientNameList)
            client_name_list = []
            for i in clientNameList:
                demo_list = {}
                demo_list["clientName"] = i
                client_name_list.append(demo_list)
            cursor.executemany("delete from client_review_record where client_name = :clientName and current_status !='CLOSED'",client_name_list)
        except Exception as e :
            print(e)
            return e
        else :
            db.commit()
            print("已成功删除")
            return {"code": 200,
                    "env": enviroment.name,
                    "data": "已成功删除所有在途流程",
                    "status": "Successfully"}
        finally :
            cursor.close()
            db.close()

# @review.delete("/clientreview/deleteByRecordId",summary="根据recordId来删除一条在途回访流程",operation_id='delete_review_flow_by_recordId')
@review.delete("/clientreview/deleteByRecordId",operation_id='delete_review_flow_by_recordId')
def deleteFlow(enviroment : Enviroment , recordList : List[str]):
    '''
    :param recordList:  传record_id列表，可删除一个或者多条流程
    '''
    if enviroment.name == "FICC":
        try :
            db = pymysql.connect(**config)
            cursor = db.cursor()
            if cursor:
                print(f"{enviroment.name}连接成功")
            else :
                raise EnvironmentError("连接数据库错误")
        except EnvironmentError as e :
            print(e)
            return "连接数据库异常"

        try :
            print(recordList)
            client_name_list = []
            for i in recordList:
                demo_list = []
                demo_list.append(i)
                client_name_list.append(demo_list)
            cursor.executemany("delete from client_review_record where record_id = %s and current_status !='CLOSED'",client_name_list)
        except Exception as e :
            print(e)
            return e
        else :
            db.commit()
            print("已成功删除")
            return {"code": 200,
                    "env": enviroment.name,
                    "data": "已成功删除所有在途流程",
                    "status": "Successfully"}
        finally :
            cursor.close()
            db.close()
    else :
        try :
            db = cx_Oracle.connect("gf_otc", "otc1qazXSW@", "10.62.146.18:1521/jgjtest")
            cursor = db.cursor()
            if cursor:
                print(f"{enviroment.name}连接成功")
            else :
                raise EnvironmentError("连接数据库错误")
        except EnvironmentError as e :
            print(e)
            return "连接数据库异常"

        try :
            print(recordList)
            client_name_list = []
            for i in recordList:
                demo_list = {}
                demo_list["recordId"] = i
                client_name_list.append(demo_list)
            cursor.executemany("delete from client_review_record where record_id = :recordId and current_status !='CLOSED'",client_name_list)
        except Exception as e :
            print(e)
            return e
        else :
            db.commit()
            print("已成功删除")
            return {"code": 200,
                    "env": enviroment.name,
                    "data": "已成功删除所有在途流程",
                    "status": "Successfully"}
        finally :
            cursor.close()
            db.close()













