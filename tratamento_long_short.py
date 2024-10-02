# -*- coding: utf-8 -*-
"""
Created on Sat Jul  6 16:50:23 2024

@author: JoãoWeckerle
"""

import vectorbt as vbt
from datetime import datetime
import pandas as pd

import os
from tqdm import tqdm
import matplotlib.pyplot as plt

import tkinter as tk
import customtkinter
from tkinter import messagebox
import plotly.express as px
import plotly.io as pio
import time
import numpy as np
from itertools import product

pio.renderers.default='browser'
pd.set_option('display.expand_frame_repr', True)
pd.set_option('display.max_rows', 100)
pd.options.mode.chained_assignment = None  # default='warn'


#%% CAIXA DE INPUT
customtkinter.set_appearance_mode('dark')
customtkinter.set_default_color_theme('dark-blue')

par =[]

# Criar a janela principal
root = customtkinter.CTk()
root.title("Entrada de Valores")


# Criar e posicionar os widgets
label1 = tk.Label(root, text="Ticker 1:")
label1.grid(row=0, column=0, padx=10, pady=10)

label2 = tk.Label(root, text="Ticker 2:")
label2.grid(row=1, column=0, padx=10, pady=10)

valor1 = tk.Entry(root)
valor1.grid(row=0, column=1, padx=10, pady=10)

valor2 = tk.Entry(root)
valor2.grid(row=1, column=1, padx=10, pady=10)


submit_button = tk.Button(
    root, text="Submit", 
    command=lambda: (par.extend([valor1.get(), valor2.get()]),  # Armazenar valores no vetor
        root.destroy()))

submit_button.grid(row=2, columnspan=2, pady=10)

# Iniciar o loop da interface gráfica
root.mainloop()


#%%
caminho = r'C:\Users\JoãoWeckerle\B6 Capital\Gestão - General (1)\Fundos em estruturacao\Fundo Quant\MARKET DATA\Teste'

#Definindo nomes
array_df = []
medias_desvios = pd.DataFrame()

## INPUTS DE ENTRADA
entradas = [0.002, 0.0025, 0.003]
medias = ['5', '15', '30']

## INPUTS DE SAIDA
gains = ['0.3', '0.4', '0.5']
losses = ['0.3', '0.4', '0.5']


pasta = os.listdir(caminho)

for arquivo in pasta:
    d = os.path.join(caminho, arquivo)
    df_chunk = pd.DataFrame()
    for chunk in pd.read_csv(fr'{d}',
                         encoding='Latin1', sep=';', chunksize = 10000, iterator=True, usecols= [1,3,4,5,8], decimal = ',',
                         names = ['Ticker', 'Preço', 'Quantidade', 'Hora','Data'],
                         dtype={"Ticker": "category"}):
    
            temp_df = chunk.loc[chunk['Ticker'].isin(par)]
            

            # MESCLANDO COLUNA DE DATA E HORA NUMA SO
            temp_df['Hora'] = temp_df['Hora'].astype(str)

            # Função para formatar o horário
            def format_horario(horario):
                # Formatar o horário para HHMMSS
                return f"{horario[:2]}:{horario[2:4]}:{horario[4:6]}.{horario[6:9]}"

            # Aplicar a formatação à coluna 'horario'
            temp_df['horario_formatado'] = temp_df['Hora'].apply(format_horario)

            # Combinar data e horário
            temp_df['Data Hora'] = pd.to_datetime(temp_df['Data'] + ' ' + temp_df['horario_formatado'], format='%Y-%m-%d %H:%M:%S')

            temp_df.set_index('Data Hora', inplace=True)

            temp_df = temp_df.between_time('10:00', '17:00')
            
            
            # COMEÇANDO A DIVIDIR OS TICKERS
            par1 = temp_df[temp_df['Ticker'] == par[0]]
            par2 = temp_df[temp_df['Ticker'] == par[1]]

            # DELETANDO VARIAVEIS 
            del temp_df
            
            ativo_sintetico =  par1.join(par2,  lsuffix = par[0], rsuffix = par[1], how = 'outer').sort_index().convert_dtypes().fillna(method='ffill').dropna() ##JUNTANDO OS TICKERS DANDO FFILL

            ativo_sintetico['Preço Sintetico Subtração'] = ativo_sintetico['Preço'+par[0]]  - ativo_sintetico['Preço'+par[1]]
            ativo_sintetico['Preço Sintetico Ratio'] = ativo_sintetico['Preço'+par[0]]  / ativo_sintetico['Preço'+par[1]]

            ativo_sintetico = ativo_sintetico[~ativo_sintetico.index.duplicated(keep='first') ] # DROPAR CASO HAJAM DOIS TICKS NO MESMO MS
            

            ativo_sintetico = ativo_sintetico[ativo_sintetico['Preço Sintetico Ratio'] != ativo_sintetico['Preço Sintetico Ratio'].shift()] # DROPAR CASO O PREÇO SINTETICO SE MANTENHA
              
            array_df.append(ativo_sintetico)


df_final = pd.concat(array_df, ignore_index=False)
del array_df
del chunk

#%%
#%%
# Função para aplicar os sinais de compra e venda no Df

def classify(current, previous, j2):
    if current > j2 and previous < j2:
        return "venda"
    elif current < -j2 and previous > -j2:
        return "compra"
    else:
        return ""

#%%
# for tempo in medias:
#     fig = px.scatter(df_final, x=df_final.index, y='Ativo - Media_' + tempo).update_traces(mode="lines+markers", )
    
#     fig.update_layout(
#         title="Ativo Sintetico Ratio - Media",
#         xaxis_title="Data", xaxis=dict(type = "category", ticklabelstep=5,tickangle=45, tickformat = '%d/%m/%Y %H:%M:%OS'),
#         font=dict(size=22,))
    
#     fig.add_hline(y=df_final['Ativo - Media_' + tempo].mean(), line=dict(color='blue', width=2, dash='dash'), annotation_text='Média', annotation_position='top right')
#     fig.add_hline(y=df_final['Ativo - Media_' + tempo].mean() - df_final['Ativo - Media_' + tempo].std(), line=dict(color='red', width=2, dash='dash'), annotation_text='+1 Desvio Padrão', annotation_position='top right')
#     fig.add_hline(y= df_final['Ativo - Media_' + tempo].mean() + df_final['Ativo - Media_' + tempo].std(), line=dict(color='red', width=2, dash='dash'), annotation_text='-1 Desvio Padrão', annotation_position='bottom right')
#     fig.add_hline(y=df_final['Ativo - Media_' + tempo].mean() + 2*df_final['Ativo - Media_' + tempo].std(), line=dict(color='green', width=2, dash='dash'), annotation_text='+2 Desvio Padrão', annotation_position='top right')
#     fig.add_hline(y=df_final['Ativo - Media_' + tempo].mean() - 2*df_final['Ativo - Media_' + tempo].std(), line=dict(color='green', width=2, dash='dash'), annotation_text='-2 Desvio Padrão', annotation_position='bottom right')
    
#     fig.show()

#%%
# df_final['Status'] = "Neutro"
# df_final['Resultado'] = 0
# df_final['Saida'] = ''
# df_final['teste'] = range(len(df_final))
# df_final['P/L'] = 0
# df_final['Preço Sintetico Subtração'] = round(df_final['Preço Sintetico Subtração'],2)

# for index, row in df_final.iterrows():
#     if index != df_final.index[0]:
        
#         ## ESTADO NEUTRO -> COMPRADO
#         if row['Entrada'] == 'compra' and df_final.shift(1).loc[index, 'Status'] == 'Neutro':
            
#             preço_entrada = df_final.loc[index,'Preço Sintetico Subtração']
#             df_final.loc[index,'Status'] = 'Comprado' ## MUDE O STATUS
#             stop_gain = df_final.loc[index,'Preço Sintetico Subtração'] + gain_loss ## DEFINA O GAIN
#             stop_loss = df_final.loc[index,'Preço Sintetico Subtração'] - gain_loss ## DEFINA O LOSS
#             df_final.loc[index,'Resultado'] = round(df_final.loc[index, 'Preço Sintetico Subtração'] - preço_entrada,2) 
            
#             #SE ESTIVER COMPRADO:
#         if df_final.shift(1).loc[index, 'Status'] == 'Comprado':
#             df_final.loc[index,'Resultado'] = round(df_final.loc[index, 'Preço Sintetico Subtração'] - preço_entrada,2) ##RESULTADO PARCIAL
            
#             # E NÃO TIVER BATIDO STOP GAIN OU STOP LOSS
#             if -gain_loss < df_final.loc[index,'Resultado'] and df_final.loc[index,'Resultado'] < gain_loss:
#                     df_final.loc[index,'Status'] = 'Comprado' #MANTENHA O STATUS
                    
#             ## CASO BATA O STOP GAIN        
#             if df_final.loc[index,'Resultado'] >= gain_loss:
#                 df_final.loc[index,'Saida'] = 'Stop Gain' ## MARQUE O GAIN
#                 df_final.loc[index,'Status'] = 'Neutro' ##MUDE O STATUS
#                 df_final.loc[index,'P/L'] = round(df_final.loc[index,'Resultado'],2) ## SALVE O VALOR DE GAIN
                
#             # CASO BATA O STOP LOSS        
#             if df_final.loc[index,'Resultado'] <= -gain_loss:
#                 df_final.loc[index,'Saida'] = 'Stop Loss' ## MARQUE O LOSS
#                 df_final.loc[index,'Status'] = 'Neutro' ##MUDE O STATUS
#                 df_final.loc[index,'P/L'] = round(df_final.loc[index,'Resultado'],2) ## SALVE O VALOR DE LOSS

#         ## ESTADO NEUTRO -> VENDIDO
#         if row['Entrada'] == 'venda' and df_final.shift(1).loc[index, 'Status'] == 'Neutro':
            
#             preço_entrada = df_final.loc[index,'Preço Sintetico Subtração']
#             df_final.loc[index,'Status'] = 'Vendido' ## MUDE O STATUS
#             stop_gain = df_final.loc[index,'Preço Sintetico Subtração'] - gain_loss  ## DEFINA O GAIN
#             stop_loss = df_final.loc[index,'Preço Sintetico Subtração'] + gain_loss  ## DEFINA O LOSS
#             df1.loc[index,'Resultado'] = round(preço_entrada - df1.loc[index, 'Preço Sintetico Subtração']       ,2) ##RESULTADO PARCIAL
            
#             #SE ESTIVER VENDIDO:
#         if df1.shift(1).loc[index, 'Status'] == 'Vendido':
#             df1.loc[index,'Resultado'] = round(preço_entrada - df1.loc[index, 'Preço Sintetico Subtração']   ,2)   ##RESULTADO PARCIAL
           
#             # E NÃO TIVER BATIDO STOP GAIN OU STOP LOSS
#             if -gain_loss < df1.loc[index,'Resultado'] and df1.loc[index,'Resultado'] < gain_loss:
#                     df1.loc[index,'Status'] = 'Vendido' #MANTENHA O STATUS
                                 
#             ## CASO BATA O STOP GAIN               
#             if df1.loc[index,'Resultado'] >= gain_loss:
#                 df1.loc[index,'Saida'] = 'Stop Gain' ## MARQUE O GAIN
#                 df1.loc[index,'Status'] = 'Neutro'##MUDE O STATUS
#                 df1.loc[index,'P/L'] = round(df1.loc[index,'Resultado'],2) ## SALVE O VALOR DE GAIN
                
#             # CASO BATA O STOP LOSS  
#             if df1.loc[index,'Resultado'] <= -gain_loss:
#                 df1.loc[index,'Saida'] = 'Stop Loss' ## MARQUE O LOSS
#                 df1.loc[index,'Status'] = 'Neutro' ##MUDE O STATUS
#                 df1.loc[index,'P/L'] = round(df1.loc[index,'Resultado'],2) ## SALVE O VALOR DE LOSS

# print("O resultado final foi de R$ ", df1['P/L'].sum())

#%%
# gains = ['0.2', '0.3']
# losses = ['0.1', '0.2']

# resultados = pd.DataFrame(columns = ['Hora Entrada', 'Gain', 'Loss', 'Gain/Loss', 'Resultado', 'Hora Saída'])


# for tempo, gain, loss in tqdm(product(medias, gains, losses), total=len(medias) * len(gains) * len(losses)):
# # for tempo in medias:
# #     for gain in gains:
# #         for loss in losses:
#             print(tempo, gain,loss)
#             for index, row in df_final.iterrows():
#                 if index != df_final.index[0]:
                    
#     ######################## COMPRAS
#                     if row['Entradas_' + tempo] == 'compra':
#                         preço_entrada = df_final.loc[index,'Preço Sintetico Subtração']
#                         # stop_gain = df_final.loc[index,'Preço Sintetico Subtração'] + float(gain) ## DEFINA O GAIN
#                         # stop_loss = df_final.loc[index,'Preço Sintetico Subtração'] - float(loss) ## DEFINA O LOSS
                                                
#                         for next_index in df_final.index[df_final.index.get_loc(index) + 1:]:
#                             preco_atual = df_final.loc[next_index, 'Preço Sintetico Subtração']
#                             retorno = preco_atual - preço_entrada
                        
#                         # Verifica se atingiu o stop loss
#                             if retorno <= -float(loss):
#                                 # df_final.loc[index, 'resultado_'+ tempo + '_' + gain + '_' + loss] = 'Stop Loss'
#                                 # df_final.loc[index, 'retorno_final_'+ tempo + '_' + gain + '_' + loss] = retorno
#                                 # df_final.loc[index, 'horario_saida_'+ tempo + '_' + gain + '_' + loss] = next_index  # Registra o horário da saída
                                
                                
#                                 resultados.loc[len(resultados)]=  pd.Series({'Hora Entrada': index,
#                                                                   'Media': tempo,
#                                                                   'Gain': gain,
#                                                                   'Loss':  loss,
#                                                                   'Gain/Loss': 'Stop Loss',
#                                                                   'Resultado': retorno,
#                                                                   'Hora Saída':  next_index})

#                                 break
                        
#                         # Verifica se atingiu o stop gain
#                             elif retorno >= float(gain):
#                                 # df_final.loc[index, 'resultado_'+ tempo + '_' + gain + '_' + loss] = 'Stop Gain'
#                                 # df_final.loc[index, 'retorno_final_'+ tempo + '_' + gain + '_' + loss] = retorno
#                                 # df_final.loc[index, 'horario_saida_'+ tempo + '_' + gain + '_' + loss] = next_index  # Registra o horário da saída
                                
#                                 resultados.loc[len(resultados)]=   pd.Series({'Hora Entrada': index,
#                                                                   'Media': tempo,
#                                                                   'Gain': gain,
#                                                                   'Loss':  loss,
#                                                                   'Gain/Loss': 'Stop Gain',
#                                                                   'Resultado': retorno,
#                                                                   'Hora Saída':  next_index})

#                                 break
                        
#                             # Se chegou ao final sem atingir stop loss ou stop gain
#                             if next_index.date() != index.date():
#                                 # df_final.loc[index, 'resultado_'+ tempo + '_' + gain + '_' + loss] = 'Hold'
#                                 # df_final.loc[index, 'retorno_final_'+ tempo + '_' + gain + '_' + loss] = retorno
#                                 # df_final.loc[index, 'horario_saida_'+ tempo + '_' + gain + '_' + loss] = next_index  # Registra o último horário
                               
#                                 resultados.loc[len(resultados)]=  pd.Series( {'Hora Entrada': index,
#                                                                   'Media': tempo,
#                                                                   'Gain': gain,
#                                                                   'Loss':  loss,
#                                                                   'Gain/Loss': 'Hold',
#                                                                   'Resultado': retorno,
#                                                                   'Hora Saída':  next_index})
                                
#             ######################## VENDAS
#                     if row['Entradas_'+ tempo] == 'venda':
#                         preço_entrada = df_final.loc[index,'Preço Sintetico Subtração']
#                         # stop_gain = df_final.loc[index,'Preço Sintetico Subtração'] - float(gain) ## DEFINA O GAIN
#                         # stop_loss = df_final.loc[index,'Preço Sintetico Subtração'] + float(loss) ## DEFINA O LOSS
                                                
#                         for next_index in df_final.index[df_final.index.get_loc(index) + 1:]:
#                             preco_atual = df_final.loc[next_index, 'Preço Sintetico Subtração']
#                             retorno = preco_atual - preço_entrada
                        
#                         # Verifica se atingiu o stop loss
#                             if retorno >= float(loss):
#                                 # df_final.loc[index, 'resultado_' + tempo + '_' + gain + '_' + loss] = 'Stop Loss'
#                                 # df_final.loc[index, 'retorno_final_'+ tempo + '_' + gain + '_' + loss] = -retorno
#                                 # df_final.loc[index, 'horario_saida_'+ tempo + '_' + gain + '_' + loss] = next_index  # Registra o horário da saída
            
#                                 resultados.loc[len(resultados)]=  pd.Series( {'Hora Entrada': index,
#                                                                   'Media': tempo,
#                                                                   'Gain': gain,
#                                                                   'Loss':  loss,
#                                                                   'Gain/Loss': 'Stop Loss',
#                                                                   'Resultado': -retorno,
#                                                                   'Hora Saída':  next_index})
            
#                                 break
                        
#                         # Verifica se atingiu o stop gain
#                             elif retorno <= -float(gain):
#                                 # df_final.loc[index, 'resultado_'+ tempo + '_' + gain + '_' + loss] = 'Stop Gain'
#                                 # df_final.loc[index, 'retorno_final_'+ tempo + '_' + gain + '_' + loss] = -retorno
#                                 # df_final.loc[index, 'horario_saida_'+ tempo + '_' + gain + '_' + loss] = next_index  # Registra o horário da saída
                                
                                
#                                 resultados.loc[len(resultados)]=  pd.Series( {'Hora Entrada': index,
#                                                                   'Media': tempo,
#                                                                   'Gain': gain,
#                                                                   'Loss':  loss,
#                                                                   'Gain/Loss': 'Stop Gain',
#                                                                   'Resultado': -retorno,
#                                                                   'Hora Saída':  next_index})
                                
#                                 break
                        
#                             # Se chegou ao final sem atingir stop loss ou stop gain
#                             if next_index.date() != index.date():
#                                 # df_final.loc[index, 'resultado_'+ tempo + '_' + gain + '_' + loss] = 'Hold'
#                                 # df_final.loc[index, 'retorno_final_'+ tempo + '_' + gain + '_' + loss] = -retorno
#                                 # df_final.loc[index, 'horario_saida_'+ tempo + '_' + gain + '_' + loss] = next_index  # Registra o último horário
                                
#                                 resultados.loc[len(resultados)]=   pd.Series({'Hora Entrada': index,
#                                                                   'Media': tempo,
#                                                                   'Gain': gain,
#                                                                   'Loss':  loss,
#                                                                   'Gain/Loss': 'Hold',
#                                                                   'Resultado': -retorno,
#                                                                   'Hora Saída':  next_index})
#%%
# Supondo que df_final seja o seu DataFrame e medias, gains, losses são as listas
start_time = time.time()

resultados_list = []

for tempo in medias:
    for entrada in entradas:
        # Calcular a média móvel com diferentes janelas
        df_final['Média_' + tempo] = df_final['Preço Sintetico Ratio'].rolling(window=int(tempo)*60, min_periods=1).mean()
        df_final['Ativo - Media_' + tempo] = df_final['Preço Sintetico Ratio'] - df_final['Média_' + tempo]
        
        # Aplicar a função ao DataFrame
        df_final['Entradas_' + tempo + '_' + str(entrada)] = df_final.apply(
            lambda row: classify(row['Ativo - Media_' + tempo],
                                 df_final['Ativo - Media_' + tempo].shift(1)[row.name],
                                 entrada), axis=1)
        
        for gain, loss in product(gains, losses):
            print(f"Processando: Média={tempo}, Entrada={entrada}, Gain={gain}, Loss={loss}")

            for index, row in df_final.iterrows():
                if index == df_final.index[0]:  # Pula o primeiro índice
                    continue

                preço_entrada = row['Preço Sintetico Subtração']
                next_indexes = df_final.index[df_final.index.get_loc(index) + 1:]
                next_index_same_day = next_indexes[next_indexes.date == index.date()]
                
                if row['Entradas_' + tempo + '_' + str(entrada)] == 'compra':
                    # Verifica se existem próximos índices no mesmo dia
                    if not next_index_same_day.empty:
                        preços_atuais = df_final.loc[next_index_same_day, 'Preço Sintetico Subtração']
                        retornos = preços_atuais - preço_entrada
                        stop_loss_hit = retornos <= -float(loss)
                        stop_gain_hit = retornos >= float(gain)

                        if stop_loss_hit.any():
                            idx_hit = stop_loss_hit.idxmax()
                            resultados_list.append({
                                'Hora Entrada': index, 'Compra/Venda': 'Compra', 'Preço Entrada': preço_entrada,
                                'Media': tempo, 'Gain': gain, 'Loss': loss, 'Entrada': entrada,
                                'Gain/Loss': 'Loss', 'Resultado': retornos[idx_hit],
                                'Preço Saída': df_final.loc[idx_hit, 'Preço Sintetico Subtração'],
                                'Hora Saída': idx_hit
                            })
                            continue
                        
                        elif stop_gain_hit.any():
                            idx_hit = stop_gain_hit.idxmax()
                            resultados_list.append({
                                'Hora Entrada': index, 'Compra/Venda': 'Compra', 'Preço Entrada': preço_entrada,
                                'Media': tempo, 'Gain': gain, 'Loss': loss, 'Entrada': entrada,
                                'Gain/Loss': 'Gain', 'Resultado': retornos[idx_hit],
                                'Preço Saída': df_final.loc[idx_hit, 'Preço Sintetico Subtração'],
                                'Hora Saída': idx_hit
                            })
                            continue
                        
                        else:
                            # Caso não haja hit de stop gain ou loss, fecha no final do dia
                            last_index_same_day = next_index_same_day[-1]
                            resultado = df_final.loc[last_index_same_day, 'Preço Sintetico Subtração'] - preço_entrada
                            resultados_list.append({
                                'Hora Entrada': index, 'Compra/Venda': 'Compra', 'Preço Entrada': preço_entrada,
                                'Media': tempo, 'Gain': gain, 'Loss': loss, 'Entrada': entrada,
                                'Gain/Loss': 'Hold', 'Resultado': resultado, 'Preço Saída': df_final.loc[last_index_same_day, 'Preço Sintetico Subtração'],
                                'Hora Saída': last_index_same_day
                            })

                elif row['Entradas_' + tempo + '_' + str(entrada)] == 'venda':
                    # Verifica se existem próximos índices no mesmo dia
                    if not next_index_same_day.empty:   
                        preços_atuais = df_final.loc[next_index_same_day, 'Preço Sintetico Subtração']
                        retornos = preço_entrada - preços_atuais
                        stop_loss_hit = retornos <= -float(loss)
                        stop_gain_hit = retornos >= float(gain)

                        if stop_gain_hit.any():
                            idx_hit = stop_gain_hit.idxmax()
                            resultados_list.append({
                                'Hora Entrada': index, 'Compra/Venda': 'Venda', 'Preço Entrada': preço_entrada,
                                'Media': tempo, 'Gain': gain, 'Loss': loss, 'Entrada': entrada,
                                'Gain/Loss': 'Gain', 'Resultado': retornos[idx_hit],
                                'Preço Saída': df_final.loc[idx_hit, 'Preço Sintetico Subtração'],
                                'Hora Saída': idx_hit
                            })
                            continue
                        elif stop_loss_hit.any():
                            idx_hit = stop_loss_hit.idxmax()
                            resultados_list.append({
                                'Hora Entrada': index, 'Compra/Venda': 'Venda', 'Preço Entrada': preço_entrada,
                                'Media': tempo, 'Gain': gain, 'Loss': loss, 'Entrada': entrada,
                                'Gain/Loss': 'Loss', 'Resultado': retornos[idx_hit],
                                'Preço Saída': df_final.loc[idx_hit, 'Preço Sintetico Subtração'],
                                'Hora Saída': idx_hit
                            })
                            continue
                        else:
                            # Caso não haja hit de stop gain ou loss, fecha no final do dia
                            last_index_same_day = next_index_same_day[-1]
                            resultado = preço_entrada - df_final.loc[last_index_same_day, 'Preço Sintetico Subtração']
                            resultados_list.append({
                                'Hora Entrada': index, 'Compra/Venda': 'Venda', 'Preço Entrada': preço_entrada,
                                'Media': tempo, 'Gain': gain, 'Loss': loss, 'Entrada': entrada,
                                'Gain/Loss': 'Hold', 'Resultado': resultado, 'Preço Saída': df_final.loc[last_index_same_day, 'Preço Sintetico Subtração'],
                                'Hora Saída': last_index_same_day
                            })

                # # Se não houver próximo índice, encerra a posição no último tick do dia
                # else:
                #     last_index_of_day = df_final[df_final.index.date == index.date()].index[-1]
                #     if row['Entradas_' + tempo + '_' + str(entrada)] == 'compra':
                #         resultado = df_final.loc[last_index_of_day, 'Preço Sintetico Subtração'] - preço_entrada
                #     else:
                #         resultado = preço_entrada - df_final.loc[last_index_of_day, 'Preço Sintetico Subtração']
                    
                #     resultados_list.append({
                #         'Hora Entrada': index, 'Compra/Venda': row['Entradas_' + tempo + '_' + str(entrada)].capitalize(),
                #         'Preço Entrada': preço_entrada, 'Media': tempo, 'Gain': gain, 'Loss': loss, 'Entrada': entrada,
                #         'Gain/Loss': 'Hold', 'Resultado': resultado,
                #         'Preço Saída': df_final.loc[last_index_of_day, 'Preço Sintetico Subtração'],
                #         'Hora Saída': last_index_of_day
                #     })




# Finaliza a contagem do tempo
end_time = time.time()

# Calcula o tempo total de execução
execution_time = end_time - start_time
print(execution_time/60)
# Converte a lista para DataFrame
resultados = pd.DataFrame(resultados_list)
#%%
# with pd.ExcelWriter(r'C:\Users\JoãoWeckerle\Downloads\resultados.xlsx') as writer:
#    resultados.to_excel(writer, engine='xlsxwriter', sheet_name= 'Resultados')
#    df_final.to_excel(writer, engine='xlsxwriter', sheet_name = 'Dados')
   #%%
resultados1 = resultados.to_csv(r'C:\Users\JoãoWeckerle\Downloads\resultados.csv', encoding='latin-1 ', decimal=',' )   
#%%

print(resultados.head(10))  

#%#

df_final1 = df_final.to_csv(r'C:\Users\JoãoWeckerle\Downloads\final.csv', encoding='latin-1 ', sep=';', decimal=',')