import pyspark.sql
import streamlit as st
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pandasql import sqldf

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


posicao = data["positions"].str.split(",", expand=True)
contagem_por_dado = posicao.explode(0).value_counts()
contagem_total = contagem_por_dado.groupby(0).sum()
print(contagem_total)



#posicoes_jogador = data["posicoes_lista"].value_counts()

#st.bar_chart(data=posicoes_jogador, x=None, color="#00aaff")