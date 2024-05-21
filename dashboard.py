import pyspark.sql
import streamlit as st
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession


"""
# Dashboard Fifa Players

Banco de dados do jogadores da Fifa:
"""

data = pd.read_csv('data/fifa_players.csv')
data

"""
Nessa base de dados temos mais de 17.000 jogadores da Fifa

Vamos analisar as idades dos jogadores:
"""

st.subheader('Distribuição de Idade dos Jogadores')

# Seletores de Faixa Etária
#faixa_etaria_inicio = st.slider('Idade Inicial:', min_value=data['age'].min(), max_value=data['age'].max(), value=data['age'].min())
#faixa_etaria_fim = st.slider('Idade Final:', min_value=data['age'].min(), max_value=data['age'].max(), value=data['age'].max())

faixa_etaria_min = data['age'].min()
faixa_etaria_max = data['age'].max()

# Filtrando por Faixa Etária Selecionada
jogadores_filtrados = data[(data['age'] >= faixa_etaria_min) & (data['age'] <= faixa_etaria_max)]

# Contando Jogadores por Idade
quantidade_por_idade = jogadores_filtrados['age'].value_counts()

# Exibindo Gráfico de Barras
st.bar_chart(data=quantidade_por_idade, x=None, color="#00aaff")
st.write(f'Idade minima {faixa_etaria_min} e idade máxima {faixa_etaria_max}')

"""
Vemos que há uma grande quantidade de jogadores que tem 22 anos, enquanto há uns acima de 40 anos

Agora vamos analisar as posições distribuidas por jogadores jogadores
"""

posicoes = data['positions']

def separar_agrupar(coluna):
  # Dividir cada string por vírgula em uma lista
  dados_separados = coluna.str.split(',').explode()

  # Agrupar contagens por valor
  contagens_agrupadas = dados_separados.value_counts().reset_index(name='Contagem')

  # Renomear colunas
  contagens_agrupadas.columns = ['Posição', 'Contagem']

  return contagens_agrupadas

posicoes_jogador = separar_agrupar(posicoes)

st.write('Distribuição de posições por jogador:')
st.bar_chart(data=posicoes_jogador, x='Posição', y='Contagem', color="#00aaff")

#posicoes_jogador = data["positions"].value_counts()

#st.bar_chart(data=posicoes_jogador, x=None, color="#00aaff")