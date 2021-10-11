from copy import deepcopy


def process_account_already_initialized(account, violations):
    violations = deepcopy(violations)
    if not account:
        return violations
    violations.append("account-already-initialized")
    return violations