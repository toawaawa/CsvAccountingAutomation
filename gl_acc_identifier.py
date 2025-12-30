from company_map import COMPANY_MAP
from constants import COLUMN_SHOP_DEPT, COLUMN_CATEGORY, COLUMN_COMPANY
from shopdept_category_map import DEPT_CATEGORY_FREQ_MAP

REQUIRED_PERCENTAGE = 0.7 # 70%
def get_code(data):
    shop_dept, category, company = data[COLUMN_SHOP_DEPT].strip(), data[COLUMN_CATEGORY].strip(), data[COLUMN_COMPANY].strip()
    code = ""
    if COMPANY_MAP.get(company) is None:
        return ""
    code_map = COMPANY_MAP[company]
    total = sum(code_map.values())
    first_code, first_count = next(iter(code_map.items()))
    if first_count / total >= REQUIRED_PERCENTAGE:
        code += first_code
    else:
        if DEPT_CATEGORY_FREQ_MAP.get(shop_dept + category) is None:
            return ""
        code_map = DEPT_CATEGORY_FREQ_MAP[shop_dept + category]
        total = sum(code_map.values())
        first_code, first_count = next(iter(code_map.items()))
        if first_count / total >= REQUIRED_PERCENTAGE and first_code != code:
            code += first_code
    return code