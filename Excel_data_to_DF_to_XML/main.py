import pandas as pd
from zeep import Client
from zeep.wsse.username import UsernameToken

WSDL = 'rpc_literal.wsdl'
soap_username = "username"
soap_password = "password"
soap_host = "https://rct12.enablon.com/Syngenta.UAT/"

config = {
    "id": {
      "Table": "/ims/incidents",
      "FolderId": "0",
      "Data": "{xml_body}",
      "CSVSeparator": "2",
      "FormatOptions": "38",
      "ImportType": "2",
      "MatchType": "0",
      "ImportOptions": "24576",
      "FormatType": "3"
    }}
soap_client = Client(f"{soap_host}{WSDL}", wsse=UsernameToken(soap_username, soap_password))
soap_client.settings.strict = False
soap_client.settings.xml_huge_tree = True
print(soap_client)

url = r'C:\Users\Martin_Marinov\Desktop\Enablon_Write_data_API\eHSE - Event Reporting Prototype.xlsx'
df = pd.read_excel(url, sheet_name='Sheet2', engine='openpyxl')
print(df.head())
df = df.fillna(" ")
df = df.loc[:, df.columns != 'Row ID']
body = df.to_xml(index=False, encoding="utf-16")

for key, props in config.items():
    props['Data'] = props['Data'].format(xml_body=body)
    print(body)
    response = soap_client.service.ImportData(**props)
    print(response)


