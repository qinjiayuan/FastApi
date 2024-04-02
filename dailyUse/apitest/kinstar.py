import unittest
from time import sleep

import requests



class TestApiSave(unittest.TestCase):

    def setUp(self) -> None:
        self.url = "http://10.2.145.216:9090/api/kingstar/1.0.0/transmFlow/save"
        self.data = {
    "title": "Titans划款流程测试",
    "transmType": "2",
    "transmDate": "2023-12-20",
    "openingBank": "工行广州一支行",
    "transferAmount": "10000",
    "purposeOfTransfer": "收益凭证到期兑付至清算户（交资金部）",
    "dateOfReceipt": "2022-03-31",
    "gatherBank": "",
    "gatherPerson": "广发证券柜台交易市场TA清算专户123",
    "bankAccount": "3602000129201476888",
    "transferAmountCapitalize": "壹拾万零壹仟捌佰贰拾贰元伍角玖分",
    "disbursementAccount": "",
    "transactor": "广发证券股份有限公司",
    "disbursementAccountNumber": "3602000129201572270",
    "files": [
        {
            "file_name": "LICENSE.electron.txt",
            "new_file_name": "LICENSE.electron20231220161942.txt",
            "file_path": "/tmp/upload_5897ce8ea92144fe8c1dce17c5ccd783.txt",
            "file_size": "0.001",
            "file_type": "text/plain"
        }
    ],
    "pubProperty": 2,
    "urgency": 1,
    "user": "sunbin",
    "flowId":"dce17c5ccd783"
}
#验证正确的密钥和请求参数
    def test_api_success_1(self):
        #根据自己写好的方法（开发提供,测试封装成接口获取）来获取token
        secret = requests.get(url='http://10.50.72.116:8080/clientreview/getToken',
                              params={"secretKey":"ED348B57438271F9"}).json()["data"]
        header = {"auth":secret}
        response = requests.post(url=self.url,json=self.data,headers=header)
        print(f"正确的鉴权信息：{response.json()}")

        self.assertEquals(response.json()['errCode']['code'], 200)
#验证正确的auth ，但是错误的密钥
    def test_api_fail_value_2(self):
        secret = requests.get(url='http://10.50.72.116:8080/clientreview/getToken',
                              params={"secretKey": "ED348B57438271F1"}).json()["data"]
        header = {"auth":secret}
        response = requests.post(url=self.url,json=self.data,headers=header)
        print(f"value错误的鉴权信息：{response.json()}")

        self.assertEquals(response.json()['errCode']['code'], 401)

#验证错误的auth,但是密钥是对的
    def test_api_fail_key_3(self):
        secret = requests.get(url='http://10.50.72.116:8080/clientreview/getToken',
                              params={"secretKey": "ED348B57438271F9"}).json()["data"]
        header = {"auth1": secret}
        response = requests.post(url=self.url, json=self.data, headers=header)
        print(f"key错误的鉴权信息：{response.json()}")

        self.assertEquals(response.json()['errCode']['code'], 401)
#验证auth,key都是错的
    def test_api_fail_header_4(self):
        secret = requests.get(url='http://10.50.72.116:8080/clientreview/getToken',
                              params={"secretKey": "ED348B57438271F1"}).json()["data"]
        header = {"auth1": secret}
        response = requests.post(url=self.url, json=self.data, headers=header)
        print(f"key错误的鉴权信息：{response.json()}")

        self.assertEquals(response.json()['errCode']['code'], 401)
#验证不传鉴权信息
    def test_api_null_header_5(self):
        header = {}
        response = requests.post(url=self.url, json=self.data, headers=header)
        print(f"key错误的鉴权信息：{response.json()}")

        self.assertEquals(response.json()['errCode']['code'], 401)
#验证超过1分钟后密钥失效
    def test_api_over_time_6(self):
        secret = requests.get(url='http://10.50.72.116:8080/clientreview/getToken',
                              params={"secretKey": "ED348B57438271F9"}).json()["data"]
        header = {"auth": secret}
        sleep(61)
        response = requests.post(url=self.url, json=self.data, headers=header)
        print(f"正确的鉴权信息：{response.json()}")

        self.assertEquals(response.json()['errCode']['code'], 401)


if __name__ == '__main__':
    unittest.main()





