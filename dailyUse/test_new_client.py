import unittest
from unittest.mock import Mock , patch
import pymysql
import cx_Oracle
import requests
from review.review import config
import mock
import unittest
from unittest.mock import Mock, MagicMock, patch

class cumulate:

    def add(self,num1,num2):
        return num1+num2
    def dele(self,num1,num2):
        return num1 - num2

def mock_test():
    job = cumulate()
    job.add = Mock.return_value(60)
    result = job.add(20,30)
    print(result)
if __name__ == '__main__':
    mock_test()