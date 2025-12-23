from collections import defaultdict
import pandas as pd

from account_map import ACCOUNT_FREQ_MAP

REQUIRED_PERCENTAGE = 0.7 # 70%
def get_code(company, description):
    code = ""
    if company not in ACCOUNT_FREQ_MAP:
        return ""
    code_map = ACCOUNT_FREQ_MAP[company]
    total = sum(code_map.values())
    first_code, first_count = next(iter(code_map.items()))
    if first_count / total >= REQUIRED_PERCENTAGE:
        code += first_code
    else:
        code += "ASK AI"
    return code
print(get_code("Tsui Chak Company Limited",1))