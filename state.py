from pydantic import BaseModel


class AccountState(BaseModel):
    processors_state = {}
