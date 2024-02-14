import json

import pandas as pd

json_file = 'C:\\Users\\Martin_Marinov\\Downloads\\sample4.json'

with open(json_file, 'r') as f:
    data = json.loads(f.read())

df_nested_list = pd.json_normalize(data, record_path=['people'])
print(df_nested_list)



