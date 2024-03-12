from unittest.mock import MagicMock

def add(num1:int,num2:int):
    return num1+num2

def mock():
    api = MagicMock()
    api.return_value = 100
    result = api(1,2)
    print(result)
def isequal(pramas1:str , paramsa2 : str):
    return "equal" if pramas1 == paramsa2 else "not equal"

if __name__ == '__main__':
    print(isequal('00ef1ffa02f540468437ba16d1177094','13710F5174DCC8E2E106312923E0A6EFC'))



