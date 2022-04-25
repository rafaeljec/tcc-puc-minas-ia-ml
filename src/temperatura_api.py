import pyodbc 
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import pandas as pd

class Banco():
  def conexao_sql_server(self):
    try:
      return pyodbc.connect("DRIVER={SQL Server};"
                        "Server=localhost;"
                        "Database=********************;"
                        "uid=sa;pwd=********************")
    except Exception as e:
      raise e

  def buscar_sql_server(self, con, comando):
    try:
      return pd.read_sql_query(comando, con)
    except Exception as e:
      raise e

  def inserir_sql_server(self, con, comando):
    try:
      cursor = con.cursor()
      cursor.execute(comando)
      con.commit()
    except Exception as e:
      raise e

class Api():
  banco = Banco()
  con = banco.conexao_sql_server()
  cursor = con.cursor()
  url='http://api.worldweatheronline.com/premium/v1/past-weather.ashx?'
  key = '*********************************************'

  def BuscarDados(self, data):
    try:
      query = {'q':'Joinville', 'date':data, 'key': self.key}
      response = requests.get(self.url, params=query)
      root = ET.fromstring(response.content)
      
      return root[1][2].text
    except Exception as e:
      raise e

if __name__ == '__main__':
  api = Api()

  try:

    for dia in api.banco.buscar_sql_server(api.con, 'SELECT * FROM DatasetRNA WHERE ValTemperaturaMaxima IS NULL').iterrows():
      data = datetime.strptime(dia[1][0], '%Y-%m-%d')
      com = "UPDATE DatasetRNA SET ValTemperaturaMaxima = {0} WHERE DtaEvento = '{1}'".format(api.BuscarDados(data), str(data))
      print(com)
      api.banco.inserir_sql_server(api.con, com) 

      api.cursor.close()
      api.con.close()
  except Exception as e:
    api.cursor.close()
    api.con.close()
    print('Houve um Erro: {0}'.format(e))

  

