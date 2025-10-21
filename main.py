import pandas as pd
import json
import os
import re


def build_painless_script(rules_excel, output_json):
    """
    Reads an Excel file containing Numerator and Denominator rule definitions
    and dynamically generates a valid Elasticsearch painless script JSON.

    Expected Excel structure:
    -------------------------------------------------------
    | Numerator   | 1. Exclude line where column "A (ColA Name)" = "val1" ... |
    | Denominator | 1. Exclude line where column "A (ColA Name)" = "val1" ... |
    -------------------------------------------------------
    """

    # 1️⃣ Check file existence
    if not os.path.exists(rules_excel):
        raise FileNotFoundError(f"❌ Excel file not found: {rules_excel}")

    # 2️⃣ Read Excel
    df = pd.read_excel(rules_excel, header=None)
    df[0] = df[0].astype(str).str.strip().str.lower()

    # 3️⃣ Initialize placeholders
    numerator_text, denominator_text = None, None

    for i in range(len(df)):
        first_col = df.loc[i, 0]
        if "numerator" in first_col:
            numerator_text = str(df.loc[i, 1]).strip()
        elif "denominator" in first_col:
            denominator_text = str(df.loc[i, 1]).strip()

    if not numerator_text or not denominator_text:
        raise ValueError("❌ Missing Numerator or Denominator rules in Excel.")

    # 4️⃣ Parse the Numerator and Denominator rules into painless conditions
    def parse_rules(rule_text):
        """
        Convert textual rules into painless conditional snippets.
        Example:
        - Exclude line where column "A (ColA Name)" = "val1"
        - Filter column "B (ColB Name)" contain "val2"
        """
        lines = rule_text.split("\n")
        conditions = []

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Extract column name and value
            col_match = re.search(r'column\s+"([^"]+)"', line)
            val_match = re.search(r'"([^"]+)"\s*$', line)
            if not col_match or not val_match:
                continue

            col_name = col_match.group(1)
            val = val_match.group(1)

            if "exclude" in line.lower():
                conditions.append(f"params['_source']['{col_name}']!= '{val}'")
            elif "filter" in line.lower():
                if "contain" in line.lower():
                    conditions.append(f"params['_source']['{col_name}'] == '{val}'")
                else:
                    conditions.append(f"params['_source']['{col_name}'] == '{val}'")
            elif "unfilter" in line.lower():
                conditions.append(f"params['_source']['{col_name}']!= '{val}'")

        # Build nested painless condition chain
        painless_condition = ""
        if conditions:
            condition_chain = "if(" + " && ".join(conditions) + "){"
            painless_condition = (
                condition_chain
                + "state.map.var += (doc['ColL Name'].value);"
                + "state.map.var1 += (doc['ColJ Name'].value);"
                + "state.map.var2++;}"
            )

        return painless_condition

    # 5️⃣ Build map_script
    numerator_conditions = parse_rules(numerator_text)
    denominator_conditions = parse_rules(denominator_text)

    map_script = f"{numerator_conditions}  {denominator_conditions}"

    # 6️⃣ Construct painless JSON
    painless_script = {
        "aggs": {
            "group_by_sl_met": {
                "scripted_metric": {
                    "map_script": map_script,
                    "init_script": "state['map'] =['var': 0.0,'var1' : 0.0,'var2' : 0.0]",
                    "reduce_script": (
                        "def return_map = ['Final_Performance': 0.0,'Final_Numerator': 0.0, 'Final_Denominator': 0.0]; "
                        "def new_map = ['num1': 0.0, 'num2': 0.0, 'num3': 0.0, 'num': 0.0, 'den': 0.0]; "
                        "for(a in states){new_map.num1 += (a.map.var); new_map.num2 += (a.map.var1); new_map.num3 += (a.map.var2); "
                        "new_map.num = (new_map.num1*((new_map.num2/new_map.num3)*100))/100.00; new_map.den += a.map.var;} "
                        "if(new_map.den!=0){return_map.Final_Performance = Math.floor((float)(new_map.num)/(new_map.den)*10000.0)/100.0}"
                        "else{return_map.Final_Performance ='';return_map.SL_Met=5;}"
                        "return_map.Final_Denominator = new_map.den;"
                        "return_map.Final_Numerator = new_map.num; return return_map;"
                    ),
                    "combine_script": "return state",
                }
            }
        },
        "size": 0,
        "query": {
            "bool": {
                "must": [
                    {"match": {"childslaId": "childSLAId"}},
                    {"match": {"useInComputation": True}},
                ]
            }
        },
    }

    # 7️⃣ Save JSON
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(painless_script, f, indent=4)

    print(f"✅ Dynamic painless JSON generated successfully → {output_json}")


# 8️⃣ Run if executed directly
if __name__ == "__main__":
    build_painless_script(
        r"C:\Users\harsh.kumar\Downloads\Python to script\rules.xlsx",  # Input Excel
        r"C:\Users\harsh.kumar\Downloads\Python to script\generated_painless.json"  # Output JSON
    )
