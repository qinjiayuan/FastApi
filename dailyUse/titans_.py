import json

import cx_Oracle
import jsonpath
import requests
from flask import Flask, jsonify
from enum import Enum
from uuid import uuid4
import pandas as pd

app = Flask(__name__)


bundle_params = {
        "bundleType": "B_FEE_SWAP",
        "secCollateralBundle": False,
        "mtmCurrency": "CNY",
        "keyPlanId": None,
        "planCode": None,
        "markMarket": None,
        "optionDayCount": "ACT365",
        "trsFundIsIncludeShort": None,
        "trdBundlePaymentSchedules": [
            {"optionAnnualPaymentDate": "03-21"},
            {"optionAnnualPaymentDate": "06-21"},
            {"optionAnnualPaymentDate": "09-21"},
            {"optionAnnualPaymentDate": "12-21"}],
        "optionPaymentCalendar": "SSE",
        "optionDeferralType": "FOLLOWING",
        "supportMargin": "NONE",
        "keyCtptyId": 13760
}

class currency(Enum):
    # :币种类型

    CNY = "CNY"
    CNH = "CNH"
    HKD = "HKD"
    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"
    JPY = "JPY"
    NZD = "NZD"
    AUD = "AUD"






class TitansContract():
    """
        :user --登录用户
        :env  --环境（一般是tst）
        :purpose -- 业务类型（OPTION , TRS）
        :currency -- 币种
    """

    def __init__(self, **kwargs):
        if "user" in kwargs and "env" in kwargs:
            self.user = kwargs["user"]
            self.env = kwargs["env"]
            self.purpose = kwargs["purpose"]
            self.currency = kwargs["currency"]

            url = f"http://testoauth2.gf.com.cn/ws/pub/code?no302redirect=1&login_type=oa&client_id=titans&redirect_uri=" \
                  f"http://titans-{self.env}.gf.com.cn/login/callback&user_id={self.user}&password=Gfte5tHw2022!"
            url_response = requests.post(url=url)
            url1 = url_response.json()['redirect']
            token_response = requests.get(url=url1)
            self.token = token_response.json()["data"]["jwtToken"]
            self.header_otc = {
                "Content-Type": "application/json;charset=UTF-8",
                "Authorization": f"Bearer {self.token}"
            }

        else:
            raise ValueError("请求参数中缺少user或者env")
#删除合约的接口
    def delete_contract(self,*args):
        user = "titans_query"
        password = "libra1234"
        host = "10.51.137.146"
        port = "1521"
        service_name = "testdb"

        db = cx_Oracle.connect(user, password, f"{host}:{port}/{service_name}")
        cursor = db.cursor()
        url = "http://titans-tst.gf.com.cn/api/titans/tradeflow/1.0.0/tradeflow/statemachine/contract/option/transit"
        for i in args:
            select_id = f"SELECT KEY_OTC_TRADE_ID,CONTR_STATUS  FROM TITANS_TRADEFLOW.REF_OTC_OPTION_DEAL WHERE CONTRACT_CODE='{i}' "
            print(select_id)
            cursor.execute(select_id)
            result = cursor.fetchone()
            print(f"合约编号:{i},查出来的结果为{result}")
            if not result:continue
            if result[1] != "DRAFT" :
                continue
            id = result[0]

            playload = {"id": id, "event": "ACT_DELETE"}
            header_otc = {
                "Content-Type": "application/json;charset=UTF-8",
                "Authorization": f"Bearer {self.token}"
            }
            response = requests.post(url=url,json=playload,headers=header_otc)
            if response.status_code == 200:
                print(f"成功删除{id}")
            else :
                print(response.json())
                cursor.close()
                db.close()
                raise Exception(f"{response.json()}")
        cursor.close()
        db.close()


    def save_option_contract(self):

        url = "http://titans-tst.gf.com.cn/api/titans/tradeflow/1.0.0/otc/contract/ContractOption"
        try:
            with open("data_option", "r", encoding="utf-8") as data:
                paramas = json.load(data)

            result = requests.post(url=url, json=paramas, headers=self.header_otc)
            if result.status_code == 200:
                print(f"期权合约生成成功{result.json()} ")
            else :
                print(f"期权合约生成失败{result.json()}")

            short_name = jsonpath.jsonpath(result.json(), "$..ctptyName")
            return short_name[0]
        except Exception as e:
            print(f"error is {e}")


    def save_trs_contract(self):
        user = "titans_query"
        password = "libra1234"
        host = "10.51.137.146"
        port = "1521"
        service_name = "testdb"

        db = cx_Oracle.connect(user, password, f"{host}:{port}/{service_name}")
        cursor = db.cursor()



        save_trs_url = "http://titans-tst.gf.com.cn/api/titans/tradeflow/1.0.0/otc/contract?moduleType=ContractTrs"
        with open("data_trs","r",encoding="utf-8") as data :
            params = json.load(data)
        params["bundleId"] = self.get_bundle_id()
        print(params)
        contract_trs_url = "http://titans-tst.gf.com.cn/api/titans/tradeflow/1.0.0/otc/contract?moduleType=ContractTrs"
        responses = requests.post(url=contract_trs_url,json=params,headers=self.header_otc)
        if responses.status_code == 200:
            print("互换合约成功生成")
            id = params["keyCtptyId"]
            cursor.execute(f" SELECT short_name FROM TITANS_STATICDATA.REF_COUNTER_PARTY WHERE id = '{id}' ")

            cursor.close()
            db.close()
            return cursor.fetchone()[0]
        else :
            print(f"save接口error : {responses.json()}")
            cursor.close()
            db.close()
            raise ValueError("互换合约生成失败")


#交易对手写死德利远佳1
    def get_bundle_id(self):
        user = "titans_query"
        password = "libra1234"
        host = "10.51.137.146"
        port = "1521"
        service_name = "testdb"
        db = cx_Oracle.connect(user,password,f"{host}:{port}/{service_name}")
        cursor = db.cursor()
        cursor.execute("SELECT ID FROM TITANS_STATICDATA.REF_COUNTER_PARTY WHERE SHORT_NAME ='德远利佳1' ")
        id = cursor.fetchone()[0]
        budle_id_url = "http://titans-tst.gf.com.cn/api/titans/tradeflow/1.0.0/otc/bundle"

        response = requests.post(url=budle_id_url,json=bundle_params,headers=self.header_otc)
        if response.status_code == 200:
            bundleId = jsonpath.jsonpath(response.json(),"$..bundleId")[0]
            print(f"bundleId:{bundleId}")
            return bundleId
        else :
            print(f"Fail to get bundleId,errmsg{response.json()}")
            raise ValueError("e")













# 创建资金账户
    def create_contract(self):
        user = "titans_query"
        password = "libra1234"
        host = "10.51.137.146"
        port = "1521"
        service_name = "testdb"
        try:
            # 查出交易对手ID
            db = cx_Oracle.connect(user, password, f"{host}:{port}/{service_name}")
            cursor = db.cursor()
            print('数据库连接成功')
            short_name = self.save_option_contract() if self.purpose == "OPTION" else self.save_trs_contract()
            select_id = f"select ID from TITANS_STATICDATA.REF_COUNTER_PARTY where SHORT_NAME ='{short_name}'"
            print(select_id)
            cursor.execute(select_id)
            id = cursor.fetchone()[0]

            # 产出是否存在资金账户

            select_account_sql = f"select t.purpose from TITANS_MARGIN.capital_account t where t.key_ctpty_id = '{id}' and t.currency = '{self.currency}' and t.purpose = '{self.purpose}' "
            #
            print(select_account_sql)
            cursor.execute(select_account_sql)

            exists_flag = True if cursor.fetchall() else False
            if not exists_flag:
                account_url = 'http://titans-tst.gf.com.cn/api/titans/margin/1.0.0/capitalAccountManage/account'
                paramas = {
                    "capitalAcctDto": {
                        "keyCtptyId": "13760",
                        "currency": self.currency,
                        "accountNumber": "621233362254012",
                        "accountPurpose": self.purpose,
                        "description": "123312",
                        "capitalAccountName": "中国工商银行天河支行",
                        "bankOfDeposit": "中国工商银行天河支行",
                        "largePaymentAccount": "621233362254012"
                    },
                    "purposeCheckDto": {
                        "purposeName": self.purpose,
                        "currency": self.currency,
                        "checkDesc": None,
                        "enabled": True,
                        "toStopAcctIds": None,
                        "toRestartIds": None
                    }
                }
                print(paramas)
                response = requests.post(url=account_url, json=paramas, headers=self.header_otc)
                print(response.json())
                cursor.close()
                db.close()
            else:
                print(f"已存在{self.purpose}且币种为资金账户")
                cursor.close()
                db.close()




        except Exception as e:
            print(e)

def getmodel_head():
    #连接数据库
    host = "10.62.146.18"
    port = "1521"
    user = "gf_otc"
    passwd = "otc1qazXSW@"
    service_name = 'jgjtest'
    db = cx_Oracle.connect(user,passwd,f"{host}:{port}/{service_name}")
    cursor = db.cursor()
    cursor.execute("SELECT ABBREVIATION FROM HK_COUNTERPARTY hc")
    #查询出香港台账中所有简称
    # hk_client = [x[0] for x in cursor.fetchall()]
    hk_client = []
    while True:
        row = cursor.fetchmany(100)
        if not row:
            break
        for i in row:
            hk_client.append(i[0])
    # print(len(hk_client))

    # 导入excel
    models = pd.read_excel("香港客户交易数据统计20240123.xlsx")
    models = models.where(models.notnull(),None)
    abbravation = (x for x in models['交易对手简称'])
    notFoundclient = []

    #找出excle中的简称不在台账中的合约简称
    try:
        while True :
            hk_abr = next(abbravation)
            if hk_abr not in hk_client:
                notFoundclient.append(hk_abr)
    except StopIteration:
            print("已核对完毕")
    print(notFoundclient)
    cursor.close()
    db.close()

def compareModel():
    model1 = pd.read_excel("data.xlsx")
    model1 = model1.where(model1.notnull(),None)
    model1_column = [x for x in model1.columns]

    model2 = pd.read_excel("香港客户交易数据统计20240123 (2).xlsx")
    model2 = model2.where(model1.notnull(), None)
    model2_column = [x for x in model2.columns]

    print(model1_column)
    print(model2_column)








if __name__ == '__main__':
    # do = TitansContract(**{"user": "dongshengli",
    #                        "env": 'tst',
    #                        "purpose": "TRS",
    #                        "currency":currency.EUR.value})
    # do.create_contract()
    # getmodel_head()
    HK_url = "http://10.2.145.216:9090/api/test/manualSendHkTradeDataEmail"
    parmas = {"date":"2024-01-27"}
    response = requests.get(url=HK_url,params=parmas)
    print(response.json())
