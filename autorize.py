import json
import sys

from copy import deepcopy

def input_operation(line):
    return json.loads(line)



def autorize(data):
    account = {}
    output = []
    for line in data:
        match line:
            case {"account": account_data}:
                if not account:
                    account = deepcopy(account_data)
                    output.append({"account": account_data, "violations": []})
                else:
                    output.append({"account": account_data, "violations": ["account-already-initialized"]})
            case {"transaction": transaction_data}:
                if transaction_data["amount"] <= account["available-limit"]:
                    account["available-limit"] -= transaction_data["amount"]
                    output.append({"account": deepcopy(account), "violations": []})
                else:
                    output.append({"account": deepcopy(account), "violations": ["insufficient-limit"]})
                
    
    return output




            

    