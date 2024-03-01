import datetime
import random
import time

import cx_Oracle
from flask import Flask, jsonify, make_response, Response, request

app = Flask(__name__)



def par(code,**kwargs) -> Response:

    content = jsonify(kwargs)
    resp = make_response(content, code)
    resp.headers["Content-Type"] = "application/json; charset=utf-8"
    resp.headers["Access-Control-Allow-Origin"] = '*'
    return resp

@app.route("/select",methods=['POST'])
def select():
    host = "10.62.146.18"
    port =  "1521"
    username = 'gf_otc'
    passwd = "otc1qazXSW@"
    service_name = 'jgjtest'



    try:
        data = request.get_data()
        if not data :
            return par(code=400,data="Invalid JSON data_option",succeed=False)
        print(data,type(data))
        db = cx_Oracle.connect(username,passwd,f"{host}:{port}/{service_name}")
        cursor = db.cursor()
        select_clientId = f"select client_id from otc_derivative_counterparty"
        cursor.execute(select_clientId)
        result = [x[0] for x in cursor.fetchall()]
        cursor.close()
        db.close()
        response_data = {"client_id":result[:10]}
        return par(code=200,data=response_data,succeed=True)
    except Exception as e:
        print(e)
        return par(code=500,data=f"{e}",succeed=False)
@app.route("/insertBuffer",methods=['POST'])
def insert_buffer() ->Response:
    host = "10.62.146.18"
    port = "1521"
    username = 'gf_otc'
    passwd = "otc1qazXSW@"
    service_name = 'jgjtest'
    try:
        start = time.time()
        data = request.get_json()
        print(data)

        data_list = []
        num = int(data["num"]) if data["num"] else 1
        review_start_date = datetime.datetime.now()
        while num :
            list1 = []
            list1.append(id())
            list1.append(None)
            list1.append(review_start_date)
            list1.append(review_start_date)
            list1.append(review_start_date)
            data_list.append(tuple(list1))
            num -= 1
        print(data_list)

        insert_sql = f"insert into client_review_buffer values(:id,:client_id,:review_start_date," \
                     f":review_buffer_start,:review_buffer_end)"

        db = cx_Oracle.connect(username,passwd,f"{host}:{port}/{service_name}")

        cursor = db.cursor()
        cursor.executemany(insert_sql,data_list)
        db.rollback()
        cursor.close()
        db.close()
        end = time.time()
        print(f"耗时{end-start}")
        return par(code=200,data=data_list,succeed=True,time=f"{end-start}")
    except Exception as e :
        return par(code=500,data=f"{e}",succeed=False)

def id():
    strList = '123456789QWERTYUIOPASDFGHJKLZXCVBNM'
    str = ''

    for i in range(32):
        str += random.choice(strList)
    return str






if __name__ == '__main__':
    app.run(debug=True)