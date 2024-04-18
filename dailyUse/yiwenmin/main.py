# import pandas as pd
import json
import random
import re
import time
import datetime

import cx_Oracle
import requests
import jsonpath

import pymysql
'''
#运管获取cookie

def get_code(user):
    url = 'https://uatoauth2.gf.com.cn/ws/pub/code'
    params = {"no302redirect": "1",
              "login_type": "oa",
              "client_id": "otcoms",
              "redirect_uri": "https://otcoms-test.gf.com.cn/spsrest/auth/user/login",
              "user_id": user,
              "password": "Gfte5tHw2022!"}
    response = requests.post(url=url, data=params)
    result = response.json()
    print(result)
    code = str(result["redirect"]).split("=")[1]
    print("code is %s"%code)
    getToken = response.cookies.get_dict()
    print("getToken is %s"%getToken)

    login_url = "https://otcoms-test.gf.com.cn/spsrest/auth/user/login"
    params = {"code":code}
    login_response = requests.get(url=login_url,params=params)
    getCookie = login_response.cookies.get_dict()
    print("getCookie is %s"%getCookie)

    # cookie = "LtpaToken2=%s"%getToken["LtpaToken2"] +";"+"OAUTH_TOKEN=%s"%getCookie["OAUTH_TOKEN"]+";appcmssid=%s"%getCookie["appcmssid"]
    cookie =  "OAUTH_TOKEN=%s" % getCookie[
        "OAUTH_TOKEN"] + ";appcmssid=%s" % getCookie["appcmssid"]
    print(cookie)

    select_url = "https://otcoms-test.gf.com.cn/spsrest/deriv_counterparty/queryList"
    select_data = {"pageNum":1,"pageSize":10,"query":{"clientId":"","abbreviation":"","nameAbbreviation":"","unifiedsocialCode":"","reportStatus":"","productReportStatus":"","operator":"","clientType":"","auditStatus":"","corporateName":"测试产品关注类","defaultData":"false"}}
    select_headers = {"cookie":cookie}
    select_response = requests.post(url=select_url,json=select_data,headers=select_headers)
    print(select_response.json())
'''
def selectFromTitans():
    user = "titans_query"
    password = "libra1234"
    host = "10.51.137.146"
    port = "1521"
    service_name = "testdb"
    try:
        # 使用正确的connect函数调用方式
        db = cx_Oracle.connect(user, password, f"{host}:{port}/{service_name}")
        cursor = db.cursor()

        test_sql1 = """
        SELECT CODE_VALUE from titans_refdata.ref_instrument i
left join titans_refdata.ref_instrument_code ic on ic.key_instrument_id=i.key_instrument_id and ic.sec_code='WIND'
where i.currency='CNY' and i.exch_market ='SZSE'  and (
ic.code_value like '001%' or
ic.code_value like '002%' or
ic.code_value like '159%' or
ic.code_value like '300%' or
ic.code_value like '301%' or
ic.code_value like '510%' or
ic.code_value like '511%' or
ic.code_value like '512%' or
ic.code_value like '513%' or
ic.code_value like '515%' or
ic.code_value like '516%' or
ic.code_value like '517%' or
ic.code_value like '518%' or
ic.code_value like '560%' or
ic.code_value like '563%' or
ic.code_value like '588%' or
ic.code_value like '600%' or
ic.code_value like '601%' or
ic.code_value like '603%' or
ic.code_value like '605%' or
ic.code_value like '688%' or
ic.code_value like '689%' or
ic.code_value like '562%' or
ic.code_value like '000%' or
ic.code_value like '003%' or
ic.code_value like '561%'
)
UNION  
SELECT CODE_VALUE from titans_refdata.ref_instrument i
left join titans_refdata.ref_instrument_code ic on ic.key_instrument_id=i.key_instrument_id and ic.sec_code='WIND'
where i.currency='CNY' and i.exch_market ='SSE'  and (
ic.code_value like '001%' or
ic.code_value like '002%' or
ic.code_value like '159%' or
ic.code_value like '300%' or
ic.code_value like '301%' or
ic.code_value like '510%' or
ic.code_value like '511%' or
ic.code_value like '512%' or
ic.code_value like '513%' or
ic.code_value like '515%' or
ic.code_value like '516%' or
ic.code_value like '517%' or
ic.code_value like '518%' or
ic.code_value like '560%' or
ic.code_value like '563%' or
ic.code_value like '588%' or
ic.code_value like '600%' or
ic.code_value like '601%' or
ic.code_value like '603%' or
ic.code_value like '605%' or
ic.code_value like '688%' or
ic.code_value like '689%' or
ic.code_value like '562%' or
ic.code_value like '000%' or
ic.code_value like '003%' or
ic.code_value like '561%'
)
        """
        # 执行SQL查询并获取结果
        code_value = []
        cursor.execute(test_sql1)
        results = cursor.fetchall()
        for row in results:
            code_value.append(row[0])
        # 或者你可以按照需要处理这些结果
        cursor.close()  # 关闭游标对象
        db.close()  # 关闭连接对象

        return code_value
    except Exception as e:
        print(str(e))


def updateToFttst():
            # 创建连接配置
        start_time = time.time()
        config = {
                'host': '10.51.135.19',
                'port': 3306,
                'user': 'ft_tst',
                'password': 'GFftpass1234_tst',
                'database': 'ft_tst'
            }
        conn = pymysql.connect(**config)
        cursor = conn.cursor()
        try:
                # 创建连接对象
                start_time = time.time()
                print("成功连接到MySQL数据库!")

                data = selectFromTitans()
                print(len(data))



                # print(type(data))
                 # 创建游标对象，用于执行SQL语句
                select_sql = "select count(corpcode) from ft_tst.entrust_detail_1229 abs WHERE trade_date =20231229 order by corpcode DESC "
                cursor.execute(select_sql)  # 示例SQL语句，获取版本信息
                select_result = cursor.fetchone()  # 获取查询结果
                print(f"总共有{select_result[0]}个数据")



                select_sql = """ select  corpcode  
                                    from ft_tst.entrust_detail_1229
                                    where trade_date =20231229
                                    """
                cursor.execute(select_sql)

                while True :
                    row = cursor.fetchmany(6540)
                    if not row:
                        break
                    data_list=[]
                    for i in range(len(row)):
                        params = []
                        params.append(data[i])
                        params.append(row[i][0])
                        data_list.append(params)
                    update_corpcode = """update ft_tst.entrust_detail_1229
                                                            set windcode =%s where corpcode = %s"""

                    cursor.executemany(update_corpcode, data_list)
                conn.commit()
                end_time = time.time()
                print(f"总耗时{end_time - start_time}")
                print(f"次数：{i}")




                data_params = []
                for i in range(select_result[0]):
                    params_list = []
                    params_list.append(data[i%len(data)])
                    params_list.append(result[i][0])

                    data_params.append(tuple(params_list))

                print(data_params)
                update_corpcode = """update ft_tst.entrust_detail_1229
                                        set windcode =%s where corpcode = %s"""

                cursor.executemany(update_corpcode,data_params)
                print("已经成功插入")
                conn.rollback()
                end_time = time.time()
                print("耗时:{}s".format(end_time-start_time))

        except Exception as e :
                print(str(e))
        finally:
            cursor.close()  # 关闭游标
            conn.close()








if __name__ == '__main__':
    updateToFttst()














