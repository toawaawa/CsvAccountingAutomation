# not in use
import pandas as pd
from pathlib import Path
import re
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
    return re.sub(r'[\s:]+$', '', header)

def main():
    df = pd.read_csv("data/GENERAL 202511 999-1037.csv", encoding="utf-8")
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
            # if len(lines) == 1:
            #     new_data = data.copy()
            #     company = data["Unnamed: 4"]
            #     new_description = company + ', ' + description
            #
            #     new_data["Description"] = new_description
            #     res = pd.concat(
            #         [res, new_data.to_frame().T],
            #         ignore_index=True
            #     )
            #
            #     data["G/L Account"] = 10200
            #     data["Number of Distributions"] = num_dist
            #     res = pd.concat(
            #         [res, data.to_frame().T],
            #         ignore_index=True
            #     )


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
                    new_description = company + ', ' + header
                else:
                    new_description = company + ', ' + header + ": " + line

                new_data["Description"] = new_description
                new_data["Amount"] = find_amount(line)
                new_data['Number of Distributions'] = num_dist
                res = pd.concat(
                    [res, new_data.to_frame().T],
                    ignore_index=True
                )

            # Closing figure

            data["G/L Account"] = 10200
            data["Description"] = company + ', ' + data["Description"]
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

    # Output result CSV
    res.to_csv("output/GENERAL 202511 999-1037.csv", index=False, encoding="utf-8-sig")
if __name__ == "__main__":
    main()
