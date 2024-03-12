import unittest
from unittest.mock import Mock , patch
import pymysql
import cx_Oracle
import requests
from review.review import config
import mock
import unittest
from unittest.mock import Mock, MagicMock, patch

def api(num1:int,num2:int):
    return num1 + num2

def mock_():
    api_mock = MagicMock()
    api_mock.return_value(10)

    result = api(1,2)

if __name__ == '__main__':
    mock_()