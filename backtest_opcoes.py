# -*- coding: utf-8 -*-
"""
Created on Tue Jun 11 14:58:42 2024

@author: JoãoWeckerle
"""

#!/usr/bin/env python
# coding: utf-8

# In[185]:


#!pip install MetaTrader5
#!pip install numpy==1.20
#!pip install vectorbt


# In[186]:


import vectorbt as vbt
from datetime import datetime
import MetaTrader5 as mt5
import pandas as pd
import pytz
from IPython.display import display, HTML, Markdown
from pandas.tseries.offsets import CustomBusinessDay
from pandas.tseries.holiday import AbstractHolidayCalendar, Holiday
import os
from tqdm import tqdm
import gc
import matplotlib.pyplot as plt

pd.set_option('display.expand_frame_repr', True)
pd.set_option('display.min_rows', 100)

login, password = open(r'C:\Users\JoãoWeckerle\Downloads\meta.txt').read().split()

timezone = pytz.timezone('Etc/GMT-3')


# In[187]:


# In[206]:
class feriados(AbstractHolidayCalendar):
    rules = [
        Holiday('New Years Day', month=1, day=1, year=2022),
        Holiday('May Day', month=5, day=1, year=2022),
        Holiday('14/04', month=4, day=17, year=2022),
        Holiday('Constitution Day', month=4, day=21, year=2022),
        Holiday('Pentecost Sunday', month=5, day=1, year=2022),
        Holiday('Corpus Christi', month=9, day=7, year=2022),
        Holiday('Assumption of the Blessed Virgin Mary', month=10, day=12, year=2022),
        Holiday('All Saints Day', month=11, day=2,  year=2022),
        Holiday('Independence Day', month=11, day=15, year=2022),
        Holiday('Christmas Day', month=12, day=25, year=2022),
        
        
        Holiday('New Years Day', month=1, day=1, year=2023),
        Holiday('Carnival', month=2, day=20, year=2023),
        Holiday('Carnival', month=2, day=21, year=2023),
        Holiday('Passion of Christ', month=4, day=7, year=2023),
        Holiday('TakeTooths', month=4, day=21, year=2023),
        Holiday('Labour Day', month=5, day=1, year=2023),
        Holiday('Corpus Christi', month=6, day=8, year=2023),
        Holiday('Independence Day', month=9, day=7, year=2023),
        Holiday('Nossa Sr.a Aparecida - Padroeira do Brasil', month=10, day=12, year=2023),
        Holiday('Finados', month=11, day=2, year=2023),
        Holiday('Proclamação da República', month=11, day=15, year=2023),
        Holiday('Christmas Day', month=12, day=25, year=2023),
        
        Holiday('New Years Day', month=1, day=1, year=2024),
        Holiday('Carnival', month=2, day=12, year=2024),
        Holiday('Carnival', month=2, day=13, year=2024),
        Holiday('Passion of Christ', month=3, day=29, year=2024),
        Holiday('TakeTooths', month=4, day=21, year=2024),
        Holiday('Labour Day', month=5, day=1, year=2024),
        Holiday('Corpus Christi', month=5, day=30, year=2024),
        Holiday('Independence Day', month=9, day=7, year=2024),
        Holiday('Nossa Sr.a Aparecida - Padroeira do Brasil', month=10, day=12, year=2024),
        Holiday('Finados', month=11, day=2, year=2024),
        Holiday('Proclamação da República', month=11, day=15, year=2024),
        Holiday('Black Consciousness', month=11, day=20, year=2024),
        Holiday('Christmas Day', month=12, day=25, year=2024)]
BUSINESS_DAY = CustomBusinessDay(
    calendar=feriados(),
    weekmask='Mon Tue Wed Thu Fri')

agora = pd.Timestamp.now()
ontem = (agora -  1*BUSINESS_DAY)
anteontem = (agora -  4*BUSINESS_DAY)


# In[188]:


mt5.shutdown()

if not mt5.initialize(login = 59307110, server= "XPMT5-DEMO", password = password):
    print("initialize() failed, error code =",mt5.last_error())
    mt5.shutdown()
 
# request connection status and parameters
#print(mt5.terminal_info())
# get data on MetaTrader 5 version
print(mt5.symbol_info('AZULF100'))


# In[189]:


inicio = datetime(2024,6,13)
fim = datetime(2024,6, 18)

#inicio = datetime(2024, 5, 4,  tzinfo = timezone)

# tickers = pd.read_csv(fr'C:\Users\JoãoWeckerle\OneDrive - B6 Capital\Dados B3\TradeInformationConsolidatedFile_{anteontem}_1.csv', low_memory=False,
#                      skiprows=1, delimiter=';')

# tickers = tickers['TckrSymb']
tickers = ['BOVA11', "BOVAF118", "BOVAR118", "BOVAF15", "BOVAR15"]


# In[ ]:

    # current contents of your for loop
for ticker in tqdm(tickers):
    ticks = mt5.copy_ticks_range(
        ticker, # Símbolo desejado
        inicio, # Data Inicial
        fim, # Data Final
        mt5.COPY_TICKS_ALL) # Flag da busca. Podemos passar também mt5.COPY_TICKS_TRADE e mt5.COPY_TICKS_INFO.


    ticks = pd.DataFrame(ticks)
    print(ticks)
    if len(ticks) > 0:
        ticks = ticks.drop('time', axis=1)
        ticks['time_msc'] = pd.to_datetime(ticks['time_msc'], unit='ms')
        ticks.index = ticks['time_msc']
        ticks = ticks.drop('time_msc', axis=1)
        

        nome_arquivo = f"Trades_{ticker}_{inicio.strftime('%Y-%m-%d')}_ate_{fim.strftime('%Y-%m-%d')}.txt"

        caminho = fr"C:\Users\JoãoWeckerle\B6 Capital\Gestão - General (1)\Fundos em estruturacao\Fundo Quant\Teste Opções\{ticker}"
        
    
    # Create the folder if it doesn't exist
        if os.path.exists(caminho) == False:
            os.mkdir(caminho)
    
    # Construct the full path for the new file
        new_file_path = os.path.join(caminho, nome_arquivo)
    
        arquivo = ticks.to_csv(new_file_path,  sep = ';', mode='a')
        del ticks
        del arquivo
        
        gc.collect()


       #%%
    else:
        with open(r"C:\Users\JoãoWeckerle\B6 Capital\Gestão - General (1)\Fundos em estruturacao\Fundo Quant\log.txt", mode = 'a') as f:
            l = ticker
            f.write(l + "; ")
            del l
            #print('empty DataFrame:', l)
        
mt5.shutdown()
#%%
bova11 = pd.read_csv(fr"C:\Users\JoãoWeckerle\B6 Capital\Gestão - General (1)\Fundos em estruturacao\Fundo Quant\Teste Opções\BOVA11\Trades_BOVA11_{inicio.strftime('%Y-%m-%d')}_ate_{fim.strftime('%Y-%m-%d')}.txt", sep = ";")
bovaf118 = pd.read_csv(fr"C:\Users\JoãoWeckerle\B6 Capital\Gestão - General (1)\Fundos em estruturacao\Fundo Quant\Teste Opções\BOVAF118\Trades_BOVAF118_{inicio.strftime('%Y-%m-%d')}_ate_{fim.strftime('%Y-%m-%d')}.txt", sep = ";")
bovar118 = pd.read_csv(fr"C:\Users\JoãoWeckerle\B6 Capital\Gestão - General (1)\Fundos em estruturacao\Fundo Quant\Teste Opções\BOVAR118\Trades_BOVAR118_{inicio.strftime('%Y-%m-%d')}_ate_{fim.strftime('%Y-%m-%d')}.txt", sep = ";")
bovaf15 = pd.read_csv(fr"C:\Users\JoãoWeckerle\B6 Capital\Gestão - General (1)\Fundos em estruturacao\Fundo Quant\Teste Opções\BOVAF15\Trades_BOVAF15_{inicio.strftime('%Y-%m-%d')}_ate_{fim.strftime('%Y-%m-%d')}.txt", sep = ";")
bovar15 = pd.read_csv(fr"C:\Users\JoãoWeckerle\B6 Capital\Gestão - General (1)\Fundos em estruturacao\Fundo Quant\Teste Opções\BOVAR15\Trades_BOVAR15_{inicio.strftime('%Y-%m-%d')}_ate_{fim.strftime('%Y-%m-%d')}.txt", sep = ";")

#%% 
######## Dropando os Bids e Asks de leilão e os iniciais onde bid ou ask iguais a zero

# BOVA 11
bova11 = bova11.drop(bova11[bova11['bid'] > bova11['ask']].index )
bova11 = bova11.drop(bova11[bova11['bid'] ==0].index )
bova11 = bova11.drop(bova11[bova11['ask'] ==0].index )


bova11['time_msc'] = pd.to_datetime(bova11['time_msc'])
bova11.set_index("time_msc", inplace = True)

# BOVAF118
bovaf118 = bovaf118.drop(bovaf118[bovaf118['bid'] > bovaf118['ask']].index )
bovaf118 = bovaf118.drop(bovaf118[bovaf118['bid'] ==0].index )
bovaf118 = bovaf118.drop(bovaf118[bovaf118['ask'] ==0].index )


bovaf118['time_msc'] = pd.to_datetime(bovaf118['time_msc'])
bovaf118.set_index("time_msc", inplace = True)

# BOVAR118
bovar118 = bovar118.drop(bovar118[bovar118['bid'] > bovar118['ask']].index )
bovar118 = bovar118.drop(bovar118[bovar118['bid'] ==0].index )
bovar118 = bovar118.drop(bovar118[bovar118['ask'] ==0].index )


bovar118['time_msc'] = pd.to_datetime(bovar118['time_msc'])
bovar118.set_index("time_msc", inplace = True)

# BOVAR15
bovar15 = bovar15.drop(bovar15[bovar15['bid'] > bovar15['ask']].index )
bovar15 = bovar15.drop(bovar15[bovar15['bid'] ==0].index )
bovar15 = bovar15.drop(bovar15[bovar15['ask'] ==0].index )


bovar15['time_msc'] = pd.to_datetime(bovar15['time_msc'])
bovar15.set_index("time_msc", inplace = True)

# BOVAF15
bovaf15 = bovaf15.drop(bovaf15[bovaf15['bid'] > bovaf15['ask']].index )
bovaf15 = bovaf15.drop(bovaf15[bovaf15['bid'] ==0].index )
bovaf15 = bovaf15.drop(bovaf15[bovaf15['ask'] ==0].index )


bovaf15['time_msc'] = pd.to_datetime(bovaf15['time_msc'])
bovaf15.set_index("time_msc", inplace = True)

print(bovaf15)
#%%
## Preenchendo os valores faltantes com o anterior
ativo_sintetico =  bovaf118.join(bovar118, lsuffix = '_BOVAF118', rsuffix = '_BOVAR118', how = 'outer').sort_index().fillna(method='ffill')
ativo_sintetico = ativo_sintetico.join(bova11, rsuffix = '_BOVA11', how = 'outer').sort_index().fillna(method='ffill')
ativo_sintetico = ativo_sintetico.join(bovar15, rsuffix = '_BOVAR15', how = 'outer').sort_index().fillna(method='ffill')
ativo_sintetico = ativo_sintetico.join(bovaf15, rsuffix = '_BOVAF15', how = 'outer').sort_index().fillna(method='ffill')

#Dropando valores vazios
ativo_sintetico = ativo_sintetico.dropna()

#teste = ativo_sintetico.to_csv('teste.csv', sep=';')
ativo_sintetico['Bid Sintético'] = ativo_sintetico['bid_BOVAR118'].astype(float) + ativo_sintetico['bid_BOVAF118'].astype(float) + ativo_sintetico['bid_BOVAR15'].astype(float) + ativo_sintetico['bid_BOVAF15'].astype(float)
ativo_sintetico['Ask Sintético'] = ativo_sintetico['ask_BOVAR118'].astype(float) + ativo_sintetico['ask_BOVAF118'].astype(float) + ativo_sintetico['ask_BOVAR15'].astype(float) + ativo_sintetico['ask_BOVAF15'].astype(float)

ativo_sintetico= ativo_sintetico[ativo_sintetico['Ask Sintético'] - ativo_sintetico['Bid Sintético'] < 2]
#%%
ativo_sintetico.to_csv(r'C:\Users\JoãoWeckerle\Downloads\backtest_bova11.csv', sep=';', decimal = ',',)

#%%
plt.figure(figsize=(10, 6))

# Plotar a Serie1
plt.plot(ativo_sintetico.index, ativo_sintetico['Bid Sintético'], label='Bid Sintético', color='blue')

# Plotar a Serie2
plt.plot(ativo_sintetico.index, ativo_sintetico['Ask Sintético'], label='Ask Sintético', color='red')

# Adicionar rótulos e título
plt.xlabel('Data')
plt.ylabel('Valores')
plt.title('Bid e Ask de Ativo Sintético composto por BOVAF118, BOVAR118, BOVAF15 e BOVAR15')
plt.legend()

# Exibir o gráfico
plt.show()

