# -*- coding: utf-8 -*-
"""
Created on Mon Sep 30 12:36:08 2024

@author: JoãoWeckerle
"""
import pandas as pd
import subprocess
import mplfinance as mpf
import datetime
#%% FUNÇÕES
# =============================================================================
#                       BAIXAR ARQUIVO DA NUVEM
# =============================================================================
def baixar(path):
    # Open the file, which downloads it automatically
    subprocess.run('attrib -U +P "' + path + '"')
# =============================================================================
#                       ENVIAR ARQUIVO PRA NUVEM
# =============================================================================
def enviar_nuvem(path):
    # Free up space (OneDrive) after usage
    subprocess.run('attrib +U -P "' + path + '"')
        
# =============================================================================
#                      FORMATAR HORÁRIO DO DF
# =============================================================================
# Função para formatar o horário
def format_horario(horario):
    # Formatar o horário para HHMMSS
    return f"{horario[:2]}:{horario[2:4]}:{horario[4:6]}.{horario[6:9]}"
    
# =============================================================================
#                       CALCULAR RSI
# =============================================================================
def calculate_rsi(prices, period=20):
    delta = prices.diff(1)
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi
# =============================================================================
#                       PLOT DE CANDLES
# =============================================================================
def plot_ohlc(ohlc_df):
    # Converting 'start_time' to datetime if it's not already in that format
    if not pd.api.types.is_datetime64_any_dtype(ohlc_df['start_time']):
        ohlc_df['start_time'] = pd.to_datetime(ohlc_df['start_time'])
    
    # Setting the 'start_time' as the index for plotting
    ohlc_df.set_index('start_time', inplace=True)

    # Renaming columns to fit mplfinance's expected format (open, high, low, close)
    ohlc_df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'}, inplace=True)

    # Plotting the OHLC data with mplfinance
    mpf.plot(ohlc_df, type='candle', volume=True, style='charles')

# =============================================================================
#                       FUNÇÃO APPEND NA LISTA
# =============================================================================
def funcao_append(lista, gain, loss, ativo, entrada, hora_fim, preco_loss, diff_high_low, sma_20, vol_20, rsi):
    lista.append({
        'Gain': gain, 'Loss': loss, 'Ativo': ativo,
        'Hora Entrada': entrada[1], 'Direcao Entrada': entrada[2], 'Preco Entrada': entrada[0],
        'Hora Saída': hora_fim, 'Direcao Saída': "venda", 'Preco Saída': preco_loss, "Diff_High_low": diff_high_low,
        'SMA_20':sma_20 , "Vol_20": vol_20, "RSI": rsi
    })
    
# =============================================================================
#                       TRANSFORMAR TICKS EM OHLC
# =============================================================================
def ticks_to_ohlc(df, tick_interval, ticker):
    # Inicialize listas para armazenar os dados OHLC
    if df.empty:
        return pd.DataFrame()  # Retorna um DataFrame vazio
    
    ohlc_data = {
        'ticker': [],  # Adiciona a coluna do ticker
        'open': [],
        'high': [],
        'low': [],
        'close': [],
        'volume': [],
        'start_time': [],
        'end_time': []
    }
    
    # Inicializa a primeira vela
    open_price = df['Preço'].iloc[0]
    high_price = open_price
    low_price = open_price
    volume = 0
    start_time = df.index[0]
    current_day = start_time.date()

    for price, timestamp, vol in zip(df['Preço'], df.index, df['Quantidade']):
        day = timestamp.date()  # Extrai apenas o dia do timestamp
        
        # Verifica se o dia mudou
        if day != current_day:
            # Fecha o candle ao final do dia anterior
            ohlc_data['ticker'].append(ticker)  # Adiciona o ticker ao candle
            ohlc_data['open'].append(open_price)
            ohlc_data['high'].append(high_price)
            ohlc_data['low'].append(low_price)
            ohlc_data['close'].append(price)
            ohlc_data['volume'].append(volume)
            ohlc_data['start_time'].append(start_time)
            ohlc_data['end_time'].append(timestamp)
            
            # Reinicializa os valores para o novo dia
            open_price = price
            high_price = open_price
            low_price = open_price
            volume = 0
            start_time = timestamp
            current_day = day

        # Atualiza os valores de high, low e volume
        high_price = max(high_price, price)
        low_price = min(low_price, price)
        volume += vol
        
        # Verifica se o intervalo de ticks foi atingido
        if abs(price - open_price) >= tick_interval:
            # Fecha o candle baseado no intervalo de ticks
            ohlc_data['ticker'].append(ticker)  # Adiciona o ticker ao candle
            ohlc_data['open'].append(open_price)
            ohlc_data['high'].append(high_price)
            ohlc_data['low'].append(low_price)
            ohlc_data['close'].append(price)
            ohlc_data['volume'].append(volume)
            ohlc_data['start_time'].append(start_time)
            ohlc_data['end_time'].append(timestamp)
            
            # Inicia um novo candle
            open_price = price
            high_price = open_price
            low_price = open_price
            volume = 0
            start_time = timestamp

    # Fecha o último candle se ainda houver volume
    if volume > 0:
        ohlc_data['ticker'].append(ticker)  # Adiciona o ticker ao último candle
        ohlc_data['open'].append(open_price)
        ohlc_data['high'].append(high_price)
        ohlc_data['low'].append(low_price)
        ohlc_data['close'].append(price)
        ohlc_data['volume'].append(volume)
        ohlc_data['start_time'].append(start_time)
        ohlc_data['end_time'].append(timestamp)
        

    return pd.DataFrame(ohlc_data)

# =============================================================================
#                  SELECIONANDO TICKERS POR VENCIMENTO WDO
# =============================================================================
def define_ativo_wdol(data_str):
    # Converte a string de data 'dd-mm-aaaa' para um objeto datetime
    data = datetime.datetime.strptime(data_str, '%d-%m-%Y').date()

    # Define as datas de vencimento do WDOL para o ano de 2024 (primeiro dia útil de cada mês)
    datas = [
        ('WDOG24', datetime.date(2024, 1, 1), datetime.date(2024, 1, 31)),
        ('WDOH24', datetime.date(2024, 2, 1), datetime.date(2024, 2,29)),
        ('WDOJ24', datetime.date(2024, 3, 1), datetime.date(2024, 3, 28)),
        ('WDOK24', datetime.date(2024, 4, 1), datetime.date(2024, 4, 30)),
        ('WDOM24', datetime.date(2024, 5, 2), datetime.date(2024, 5, 31)),
        ('WDON24', datetime.date(2024, 6, 3), datetime.date(2024, 6, 28)),
        ('WDOQ24', datetime.date(2024, 7, 1), datetime.date(2024, 7, 31)),
        ('WDOU24', datetime.date(2024, 8, 1), datetime.date(2024, 8, 31)),
        ('WDOV24', datetime.date(2024, 9, 2), datetime.date(2024, 9, 30)),
        ('WDOV24', datetime.date(2024, 10, 1), datetime.date(2024, 10, 31)),
        ('WDOX24', datetime.date(2024, 11, 1), datetime.date(2024, 11, 29)),
        ('WDOF25', datetime.date(2024, 12, 2), datetime.date(2025, 12, 31))  ]

    # Verifica em qual intervalo de datas a string se encaixa
    for ticker, inicio, fim in datas:
        if inicio <= data <= fim:
            return ticker

    return None  # Se a data não corresponder a nenhum ticker

# =============================================================================
#                  SELECIONANDO TICKERS POR VENCIMENTO WIN
# =============================================================================
def define_ativo_win(data_str):
    # Converte a string de data 'dd-mm-aaaa' para um objeto datetime
    data = datetime.datetime.strptime(data_str, '%d-%m-%Y').date()

    # Define as datas de início e vencimento de cada contrato de 2024
    datas = [
        ('WING24', datetime.date(2023, 12, 13), datetime.date(2024, 2, 13)),
        ('WINJ24', datetime.date(2024, 2, 14), datetime.date(2024, 4, 16)),
        ('WINM24', datetime.date(2024, 4, 17), datetime.date(2024, 6, 11)),
        ('WINQ24', datetime.date(2024, 6, 12), datetime.date(2024, 8, 13)),
        ('WINV24', datetime.date(2024, 8, 14), datetime.date(2024, 10, 15)),
        ('WINZ24', datetime.date(2024, 10, 16), datetime.date(2024, 12, 10)),
        ('WING25', datetime.date(2024, 12, 11), datetime.date(2025, 2, 11))  # Contrato para 2025
    ]
    
    # Verifica em qual intervalo de datas a string se encaixa
    for ticker, inicio, fim in datas:
        if inicio <= data <= fim:
            return ticker
    
    return None  # Se a data não corresponder a nenhum ticker
