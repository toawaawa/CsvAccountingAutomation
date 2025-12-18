from operator import truediv

import pandas as pd
from pathlib import Path
import re

COLUMN_REFERENCE = "Reference"
COLUMN_DESCRIPTION = "Description"
COLUMN_AMOUNT = "   Amount   "
COLUMN_GL_ACCOUNT = "G/L Account"
COLUMN_NUM_DISTRIBUTIONS = "Number of Distributions"
COLUMN_COMPANY = "Unnamed: 4"

GL_CLOSING = 10200

def data_not_processed(df, i):
    if i == len(df) - 1:
        return True
    return df.loc[i, COLUMN_REFERENCE] != df.loc[i + 1, COLUMN_REFERENCE] and df.loc[i, COLUMN_REFERENCE] != df.loc[i - 1, COLUMN_REFERENCE]

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
    return company + ','

def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df[COLUMN_DESCRIPTION] = df[COLUMN_DESCRIPTION].fillna("")
    df[COLUMN_COMPANY] = df[COLUMN_COMPANY].fillna("")

    # Initialize result DataFrame with same columns
    res = pd.DataFrame(columns=df.columns)

    for i in range(len(df)):
        data = df.loc[i].copy()

        if data_not_processed(df, i) and len(data[COLUMN_DESCRIPTION]) >= 1:
            description = data[COLUMN_DESCRIPTION]
            lines = description.splitlines()
            num_dist = len(lines) if len(lines) >= 1 else 1

            header = lines[0]
            header = cleanse_header(header)
            company = data[COLUMN_COMPANY]
            head = 1
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

                new_data[COLUMN_DESCRIPTION] = new_description
                new_data[COLUMN_AMOUNT
                ] = find_amount(line)
                new_data[COLUMN_NUM_DISTRIBUTIONS] = num_dist
                res = pd.concat(
                    [res, new_data.to_frame().T],
                    ignore_index=True
                )

            # Closing figure
            closing_description = add_company_name(company,description) + data[COLUMN_DESCRIPTION]
            data[COLUMN_GL_ACCOUNT] = GL_CLOSING
            data[COLUMN_DESCRIPTION] = closing_description
            data[COLUMN_AMOUNT
            ] = '-' + data[COLUMN_AMOUNT
            ]
            data[COLUMN_NUM_DISTRIBUTIONS] = num_dist
            res = pd.concat(
                [res, data.to_frame().T],
                ignore_index=True
            )


        else:
            res = pd.concat(
                [res, data.to_frame().T],
                ignore_index=True
            )

    # Return result CSV
    return res
