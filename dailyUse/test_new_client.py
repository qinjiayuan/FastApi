import unittest

import cx_Oracle


class MyTestCase(unittest.TestCase):

    def setUp(self) -> None:
        host = "10.62.146.18"
        port = "1521"
        user = "gf_otc"
        passwd = "otc1qazXSW@"
        service_name = 'jgjtest'

        self.db = cx_Oracle.connect(user,passwd,f"{host}:{port}/{service_name}")
        self.cursor = self.db.cursor()
        if self.db :
            print("数据库连接成功")
        else:
            raise ValueError("数据库连接失败")

    def tearDown(self) -> None:
        pass




if __name__ == '__main__':
    unittest.main()
