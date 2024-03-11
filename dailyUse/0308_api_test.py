import unittest

import requests

from review.review import config , config_tdsql_otc
import pymysql

class Test_api(unittest.TestCase):

    def setUp(self) -> None:
        self.db = pymysql.connect(**config_tdsql_otc)
        self.cursor = self.db.cursor()
        self.response = requests.get(url='http://geninfotest.gf.com.cn/secinfo/api/ioa/getUserForOtc',params={"appId":"otc"})
        print(self.response.json())

    def get_data_from_auser(self,userId : str):
        select_sql = f"select userid ,username , erpid, orgid from auser where userid = %s"
        self.cursor.execute(select_sql,(userId,))
        result = self.cursor.fetchall()
        if not result:
            return None
        elif len(result) == 1 :
            data_dict = {}
            data_dict["userid"] = result[0][0]
            data_dict["username"] = result[0][1]
            data_dict["erpid"] = result[0][2]
            data_dict["orgid"] = result[0][3]
            print(data_dict)
            return data_dict

    def test_1(self):
        if self.response.json() :
            for user in self.response.json()["data"]["data"]:
                user_id = user["userId"]
                data_dict = self.get_data_from_auser(user_id)
                self.assertEquals(data_dict["userid"],user["userId"])
                print(f"userId 符合预期")
                self.assertEquals(data_dict["username"],user["userName"])
                print(f"userName 符合预期")
                self.assertEquals(data_dict["erpid"], user["erpId"])
                print(f"userId 符合预期")
                self.assertEquals(data_dict["orgid"], user["orgId"])
                print(f"orgId 符合预期")
                i += 1
    def tearDown(self) -> None:
        self.cursor.close()
        self.db.close()
