import pyodbc 
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
import requests

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

class Requests():
  
  def BuscarPagina(self, url):
    try:
      return requests.get(url).content
    except Exception as e:
      raise e

class Mare():
  banco = Banco()
  con = banco.conexao_sql_server()
  cursor = con.cursor()
  url='https://surfguru.com.br/previsao/mare/60220/m?mes={0}&ano={1}'


  def BuscarMare(self, html):

    try:
      soup = BeautifulSoup(html, 'html.parser')
      listaDias = soup.find('div', class_='arrudeia_item_previsao')
      
      return self.BuscarDadosDia(listaDias, data)
    except Exception as e:
      raise e

  def BuscarDadosDia(self, listaDias, data):
    try:

      for dia in listaDias.find_all(class_='linha_dia'):
        if dia.find(class_='celula_dia').contents[0].strip() == f'{data.day:02d}':
          return self.BuscarDadosMare(dia.find_all(class_='celulas_mares'))
    except Exception as e:
      raise e

  def BuscarDadosMare(self, listaMare):
    try:
      for mares_altas in listaMare:
        return self.BuscarTopoDaMareNaData(mares_altas.find_all(class_='celula_mare'))
    except Exception as e:
      raise e
    

  def BuscarTopoDaMareNaData(self, listaCelulasMare):
    topo_mare = -6

    try:
      for mare in listaCelulasMare:
          for valor in mare.find_all('b'):
            topo_coletado = float(valor.text.replace('m',''))
            if topo_mare < topo_coletado:
              topo_mare = topo_coletado
      
      return topo_mare
    except Exception as e:
      raise e

  def MontarUrl(self, data):
    try:
      return self.url.format(str(data.month), str(data.year)[2:4])
    except:
      raise('Houve um erro ao montar a URL')

if __name__ == '__main__':
  
  mare = Mare()
  request = Requests()
    
  try:
    last_month = 0
    last_year = 0
    html = ''

    for dia in mare.banco.buscar_sql_server(mare.con, 'SELECT * FROM DatasetRNA WHERE ValMareMaxima IS NULL').iterrows():
      data = datetime.strptime(dia[1][0], '%Y-%m-%d')

      print('-----------------------\n{0}\n'.format(data))
      if last_month == 0 or last_month != data.month or last_year != data.year:
        html = request.BuscarPagina(mare.MontarUrl(data))
        last_month = data.month
        last_year = data.year

      comando = "UPDATE DatasetRNA SET ValMareMaxima = {0} WHERE DtaEvento = '{1}'".format(str(mare.BuscarMare(html)), str(data))
      print('{0}\n'.format(comando))
      mare.banco.inserir_sql_server(mare.con, comando) 
      
    mare.cursor.close()
    mare.con.close()
  except Exception as e:
    mare.cursor.close()
    mare.con.close()
    
    print('Houve um Erro: {0}'.format(e))

