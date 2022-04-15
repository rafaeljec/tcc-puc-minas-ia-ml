from attr import attr
import pyodbc 
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import pandas as pd

class Banco():
  def conexao_sql_server(self):
    return pyodbc.connect("DRIVER={SQL Server};"
                      "Server=localhost;"
                      "Database=TCC_BASE;"
                      "uid=**********;pwd=*********")

  def buscar_sql_server(self, con, comando):
    return pd.read_sql_query(comando, con)

  def inserir_sql_server(self, con, comando):
    cursor = con.cursor()
    cursor.execute(comando)
    con.commit()

class Api():
  banco = Banco()
  con = banco.conexao_sql_server()
  cursor = con.cursor()
  url='http://api.worldweatheronline.com/premium/v1/past-weather.ashx?'
  key = '****************************'

  def BuscarDados(self, data):
    query = {'q':'Joinville', 'date':data, 'key': self.key}
    response = requests.get(self.url, params=query)
    root = ET.fromstring(response.content)
    
    return root[1][2].text

if __name__ == '__main__':
  api = Api()
  for dia in api.banco.buscar_sql_server(api.con, 'select * from chuva where temperatura is null').iterrows():
    data = datetime.strptime(dia[1][0], '%Y-%m-%d')
    com = "update chuva set temperatura = {0} where data = '{1}'".format(api.BuscarDados(data), str(data))
    print(com)
    api.banco.inserir_sql_server(api.con, com) 
    
  

