import pyodbc 
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
import requests

class Banco():
  def conexao_sql_server(self):
    return pyodbc.connect("DRIVER={SQL Server};"
                      "Server=localhost;"
                      "Database=TCC_BASE;"
                      "uid=*******;pwd=*******")

  def buscar_sql_server(self, con, comando):
    return pd.read_sql_query(comando, con)

  def inserir_sql_server(self, con, comando):
    cursor = con.cursor()
    cursor.execute(comando)
    con.commit()

class Requests():
  
  def BuscarPagina(self, url):
    return requests.get(url).content

class Mare():
  banco = Banco()
  con = banco.conexao_sql_server()
  cursor = con.cursor()
  url='https://surfguru.com.br/previsao/mare/60220/m?mes={0}&ano={1}'


  def BuscarMare(self, html):
    soup = BeautifulSoup(html, 'html.parser')
    listaDias = soup.find('div', class_='arrudeia_item_previsao')
    topo_mare = -6

    for dia in listaDias.find_all(class_='linha_dia'):
      if dia.find(class_='celula_dia').contents[0].strip() == f'{data.day:02d}':
          for mares_altas in dia.find_all(class_='celulas_mares'):
             for mare in mares_altas.find_all(class_='celula_mare'):
               for valor in mare.find_all('b'):
                 if topo_mare < float(valor.text.replace('m','')):
                   topo_mare = float(valor.text.replace('m',''))
                 

    return topo_mare
  
  def MontarUrl(self, data):
    return self.url.format(str(data.month), str(data.year)[2:4])

if __name__ == '__main__':
  mare = Mare()
  request = Requests()
  last_month = ''
  last_year = ''
  html = ''

  for dia in mare.banco.buscar_sql_server(mare.con, 'select * from chuva where mare is null').iterrows():
    data = datetime.strptime(dia[1][0], '%Y-%m-%d')

    print(data)
    if last_month == '' or (last_month != str(data.month) and last_year != str(data.year)):
      html = request.BuscarPagina(mare.MontarUrl(data))

    print(str(mare.BuscarMare(html)))
    com = "update chuva set mare = {0} where data = '{1}'".format(str(mare.BuscarMare(html)), str(data))
    print(com)
    mare.banco.inserir_sql_server(mare.con, com) 
    
  

