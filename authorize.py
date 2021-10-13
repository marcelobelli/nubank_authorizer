import sys

from authorizer import AccountState, authorize_transaction, input_operation, output_operation

if __name__ == "__main__":
    account_state = AccountState()

    for line in sys.stdin:
        account_state, result = authorize_transaction(account_state, input_operation(line))
        sys.stdout.write(output_operation(result) + "\n")
