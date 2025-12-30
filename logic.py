from operator import truediv

import pandas as pd
from pathlib import Path
import re

from constants import (
    COLUMN_DESCRIPTION, COLUMN_AMOUNT, COLUMN_GL_ACCOUNT,
    COLUMN_NUMBER_OF_DISTRIBUTIONS, COLUMN_COMPANY, GL_CLOSING, COLUMN_BALANCE, COLUMN_DATE, COLUMN_REFERENCE
)

def data_not_processed(df, i):
    curr = df.loc[i, "Reference"]

    prev_diff = True if i == 0 else curr != df.loc[i - 1, "Reference"]
    next_diff = True if i == len(df) - 1 else curr != df.loc[i + 1, "Reference"]

    return prev_diff and next_diff

def line_count(description):
    return len(description.splitlines())

def find_amount(description):
    if pd.isna(description):
        return None

    description = str(description)

    matches = re.findall(r'\$([\d,]+(?:\.\d+)?)', description)

    if not matches:
        return None

    amounts = [float(m.replace(',', '')) for m in matches]

    return max(amounts)

def cleanse_header(header):
    return re.sub(r'[\s:\-]+$', '', header)

def add_company_name(company, description):
    if len(description) > len(company) and description[:len(company)+1] == company + ',':
        return ""
    return company + ', '

def parse_amount(s):
    if pd.isna(s):
        return 0.0
    s = s.replace(',', '')
    s = s.strip()
    s = s.replace('- ', '-')
    s = s.replace('$', '')
    return float(s)

# remove empty line and count total number of distribution (uncounted subheadings)
def remove_empty(lines):
    return [line for line in lines if line]
def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # Unify format of column name naming
    df.columns = (
        df.columns
        .astype(str)
        .str.strip()  # remove leading/trailing spaces
        .str.replace(r"\s+", " ", regex=True)  # collapse multiple spaces
    )

    df[COLUMN_DESCRIPTION] = df[COLUMN_DESCRIPTION].fillna("")
    df[COLUMN_COMPANY] = df[COLUMN_COMPANY].fillna("")
    # Initialize result DataFrame with same columns
    res = pd.DataFrame(columns=df.columns)

    for i in range(len(df)):
        data = df.loc[i].copy()

        if data_not_processed(df, i) and len(data[COLUMN_DESCRIPTION]) >= 1:
            description = data[COLUMN_DESCRIPTION]
            lines = remove_empty(description.splitlines())
            num_dist = len(lines) if len(lines) >= 1 else 1

            header = lines[0]
            header = cleanse_header(header)
            company = data[COLUMN_COMPANY]
            head = 1
            curr_balance = 0
            no_header = False
            if '$' in header or len(lines) == 1:
                no_header = True
                head = 0
                num_dist += 1
            # Process line 2 to end
            for line in lines[head:]:
                new_data = data.copy()
                if no_header:
                    new_description = add_company_name(company,description) + line
                else:
                    new_description = add_company_name(company,description) + header + ": " + line
                if find_amount(line):
                    new_amount = find_amount(line)
                else:
                    new_amount = data[COLUMN_AMOUNT]
                new_data[COLUMN_DESCRIPTION] = new_description
                new_data[COLUMN_AMOUNT] = new_amount
                new_data[COLUMN_NUMBER_OF_DISTRIBUTIONS] = num_dist
                res = pd.concat(
                    [res, new_data.to_frame().T],
                    ignore_index=True
                )
                curr_balance += parse_amount(str(new_amount))


            # Closing figure
            closing_description = add_company_name(company,description)
            if not no_header:
                closing_description += header + ' '

            if num_dist <= 3: # 2 items -> list all item on closing
                for line in lines[head:]:
                    closing_description += line + '; '
                closing_description = closing_description[:-2] # remove extra semicolon at the end
            else :
                closing_description += "Claim"

            data[COLUMN_GL_ACCOUNT] = GL_CLOSING
            data[COLUMN_DESCRIPTION] = closing_description
            data[COLUMN_AMOUNT] = '-' + data[COLUMN_AMOUNT]
            data[COLUMN_NUMBER_OF_DISTRIBUTIONS] = num_dist
            # Check if the amount equals
            data[COLUMN_BALANCE] = round(curr_balance + parse_amount(data[COLUMN_AMOUNT]),2)
            res = pd.concat(
                [res, data.to_frame().T],
                ignore_index=True
            )


        else: #if data already processed -> put the row from old to new file without amendment
            res = pd.concat(
                [res, data.to_frame().T],
                ignore_index=True
            )

    # Return result CSV
    return res
