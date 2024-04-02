import unittest
from time import sleep

import requests



class TestApiSave(unittest.TestCase):

    def setUp(self) -> None:
        self.url = "http://10.2.145.216:9090/api/kingstar/1.0.0/transmFlow/detail"
        self.data = {"flowId":"dce17c5ccd783","user":"sunbin"}
        self.file = {"files":open(r'D:\FastApi\FastApi\dailyUse\data_trs','rb')}
#验证正确的密钥和请求参数
    def test_api_success_1(self):
        #根据自己写好的方法（开发提供,测试封装成接口获取）来获取token
        secret = requests.get(url='http://10.50.72.116:8080/clientreview/getToken',
                              params={"secretKey":"ED348B57438271F9"}).json()["data"]
        header = {"auth":secret}
        response = requests.get(url=self.url,params=self.data,headers=header)
        print(self.data)
        print(f"正确的鉴权信息：{response.json()}")

        self.assertEquals(response.json()['errCode']['code'], 200)
#验证正确的auth ，但是错误的密钥
    def test_api_fail_value_2(self):
        secret = requests.get(url='http://10.50.72.116:8080/clientreview/getToken',
                              params={"secretKey": "ED348B57438271F1"}).json()["data"]
        header = {"auth":secret}
        response = requests.get(url=self.url,params=self.data,headers=header)
        print(f"错误的鉴权信息：{response.json()}")

        self.assertEquals(response.json()['errCode']['code'], 401)
#验证错误的key，正确的VALUE
    def test_api_fail_key_3(self):
        secret = requests.get(url='http://10.50.72.116:8080/clientreview/getToken',
                              params={"secretKey": "ED348B57438271F9"}).json()["data"]
        header = {"auths": secret}
        response = requests.get(url=self.url,params=self.data,headers=header)
        print(f"错误的鉴权信息：{response.json()}")

        self.assertEquals(response.json()['errCode']['code'], 401)

    #验证错误的KEY和错误的VALUE

    def test_api_fail_keyvalue_4(self):
        secret = requests.get(url='http://10.50.72.116:8080/clientreview/getToken',
                              params={"secretKey": "ED348B57438271F1"}).json()["data"]
        header = {"auths": secret}
        response = requests.get(url=self.url,params=self.data,headers=header)
        print(f"错误的鉴权信息：{response.json()}")

        self.assertEquals(response.json()['errCode']['code'], 401)

    #验证不传鉴权消息
    def test_api_fail_null_5(self):
        secret = requests.get(url='http://10.50.72.116:8080/clientreview/getToken',
                              params={"secretKey": "ED348B57438271F1"}).json()["data"]
        header = {"auths": secret}
        response = requests.get(url=self.url,params=self.data,headers=header)
        print(f"错误的鉴权信息：{response.json()}")

        self.assertEquals(response.json()['errCode']['code'], 401)
#验证超过一分钟时效性
    def test_api_fail_null_4(self):
        secret = requests.get(url='http://10.50.72.116:8080/clientreview/getToken',
                              params={"secretKey": "ED348B57438271F9"}).json()["data"]
        header = {"auth": secret}
        sleep(61)
        response = requests.get(url=self.url,params=self.data,headers=header)
        print(f"错误的鉴权信息：{response.json()}")

        self.assertEquals(response.json()['errCode']['code'], 401)


if __name__ == '__main__':
    unittest.main()





