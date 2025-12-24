from company_map import COMPANY_MAP
from shopdept_category_map import DEPT_CATEGORY_FREQ_MAP

REQUIRED_PERCENTAGE = 0.7 # 70%
def get_code(shop_dept, category, company):
    code = ""
    code_map = COMPANY_MAP[company]
    total = sum(code_map.values())
    first_code, first_count = next(iter(code_map.items()))
    if first_count / total >= REQUIRED_PERCENTAGE:
        code += first_code
    else:
        code_map = DEPT_CATEGORY_FREQ_MAP[shop_dept + category]
        total = sum(code_map.values())
        first_code, first_count = next(iter(code_map.items()))
        if first_count / total >= REQUIRED_PERCENTAGE and first_code != code:
            code += first_code
    return code
print(get_code("YSB", "Salary","Ng Tsui Yi"))