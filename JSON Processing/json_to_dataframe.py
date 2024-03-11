import json

import pandas as pd

json_file = 'C:\\Users\\Martin_Marinov\\Downloads\\sample4.json'

def read_file(file, list_df=None):
    counter = 0
    if list_df is None:
        list_df = []
    for item in file:
        counter += 1
        with open(item, 'r') as f:
            data = json.loads(f.read())
            df = pd.DataFrame.from_dict(data["people"])
        list_df.append(df)
    return list_df


read_file(json_file)
