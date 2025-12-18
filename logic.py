from operator import truediv

import pandas as pd
from pathlib import Path
import re
def data_not_processed(df, i):
    if i == len(df) - 1:
        return True
    return df.loc[i, "Reference"] != df.loc[i + 1, "Reference"] and df.loc[i, "Reference"] != df.loc[i - 1, "Reference"]

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
    df["Description"] = df["Description"].fillna("")
    df["Unnamed: 4"] = df["Unnamed: 4"].fillna("")

    # Initialize result DataFrame with same columns
    res = pd.DataFrame(columns=df.columns)

    for i in range(len(df)):
        data = df.loc[i].copy()

        if data_not_processed(df, i) and len(data["Description"]) >= 1:
            description = data["Description"]
            lines = description.splitlines()
            num_dist = len(lines) if len(lines) >= 1 else 1

            header = lines[0]
            header = cleanse_header(header)
            company = data["Unnamed: 4"]
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

                new_data["Description"] = new_description
                new_data["Amount"] = find_amount(line)
                new_data['Number of Distributions'] = num_dist
                res = pd.concat(
                    [res, new_data.to_frame().T],
                    ignore_index=True
                )

            # Closing figure
            closing_description = add_company_name(company,description) + data["Description"]
            data["G/L Account"] = 10200
            data["Description"] = closing_description
            data["   Amount   "] = '-' + data["   Amount   "]
            data["Number of Distributions"] = num_dist
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
