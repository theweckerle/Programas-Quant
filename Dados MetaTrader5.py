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
anteontem = (agora -  2*BUSINESS_DAY).strftime('%Y%m%d')


# In[188]:


mt5.shutdown()

if not mt5.initialize(login = 59307110, server= "XPMT5-DEMO", password = password):
    print("initialize() failed, error code =",mt5.last_error())
    mt5.shutdown()
 
# request connection status and parameters
print(mt5.terminal_info())
# get data on MetaTrader 5 version


# In[189]:


inicio = datetime(ontem.year,ontem.month,ontem.day, hour=0,  tzinfo = timezone)
fim = datetime(ontem.year, ontem.month, ontem.month, hour=23, tzinfo = timezone)

inicio = datetime(2024, 5, 2, hour = 0, tzinfo = timezone)


tickers = pd.read_csv(fr'C:\Users\JoãoWeckerle\OneDrive - B6 Capital\Dados B3\TradeInformationConsolidatedFile_{anteontem}_1.csv', low_memory=False,
                     skiprows=1, delimiter=';')

tickers = tickers['TckrSymb']

# In[ ]:

    # current contents of your for loop
for ticker in tqdm(tickers):
    ticks = mt5.copy_ticks_range(
        ticker, # Símbolo desejado
        inicio, # Data Inicial
        fim, # Data Final
        mt5.COPY_TICKS_ALL) # Flag da busca. Podemos passar também mt5.COPY_TICKS_TRADE e mt5.COPY_TICKS_INFO.


    ticks = pd.DataFrame(ticks)
    
    if len(ticks) > 0:
        ticks = ticks.drop('time', axis=1)
        ticks['time_msc'] = pd.to_datetime(ticks['time_msc'], unit='ms')
        ticks.index = ticks['time_msc']
        ticks = ticks.drop('time_msc', axis=1)
                

        nome_arquivo = f"Trades_{ticker}_{ontem.strftime('%Y-%m-%d')}.txt"

        caminho = fr"C:\Users\JoãoWeckerle\B6 Capital\Gestão - General (1)\Fundos em estruturacao\Fundo Quant\{ticker}"
        
    
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

