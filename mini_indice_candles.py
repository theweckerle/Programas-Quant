# =============================================================================
#                               PSEUDO CÓDIGO
#               1. Importar Biblioteca e Definir Variáveis de Input
#               2. Criar arquivo de Ticks Filtrado
#               3. Transformar Ticks em Candles
#               4. Adicionar Colunas de interesse na base Candles
#               5. Criar posições de Entrada/Saída baseada em Critérios das colunas
#               6. Verificar Correlações e testar Modelos de ML
# =============================================================================

#%%               1. Importar Funções e Definir Variáveis de Input
import pandas as pd
import os
import time
from itertools import product
import shutil
import funcoes

pd.options.mode.chained_assignment = None  # default='warn'


caminho = r'C:\Users\JoãoWeckerle\B6 Capital\Gestão - General (1)\Fundos em estruturacao\Fundo Quant\MARKET DATA\Dados B3'
caminho_novo = r'C:\Users\JoãoWeckerle\B6 Capital\Gestão - General (1)\Fundos em estruturacao\Fundo Quant\MARKET DATA\Teste\Teste'

#Definindo nomes
array_df = []
medias_desvios = pd.DataFrame()

pasta = os.listdir(caminho)
pasta_nova = os.listdir(caminho_novo)

tick_interval = 1.5  # Define o intervalo de ticks

#%%               2. Criar arquivo de Ticks Filtrado

for i in pasta:
    if '2024_NEGOCIOSAVISTA' in i:
        if i + '.csv' not in pasta_nova:
            shutil.unpack_archive(rf"C:\Users\JoãoWeckerle\B6 Capital\Gestão - General (1)\Fundos em estruturacao\Fundo Quant\MARKET DATA\Dados B3\{i}",
                                  caminho_novo)

            txt = i.split('.')[0] + '.txt'
            
            d = os.path.join(caminho_novo, txt)
                            
            print("Analisando", i.split('_')[0])
            print(len(array_df))
            
            ativo = funcoes.define_ativo_wdol(i.split('.')[0].split('_')[0])
            
            print(ativo, i.split('.')[0].split('_')[0])
            
            ohlc_list = []

            for chunk in pd.read_csv(fr'{d}',
                             encoding='Latin1', sep=';', chunksize = 100000, iterator=True, usecols= [1,3,4,5,8], decimal = ',',
                             names = ['Ticker', 'Preço', 'Quantidade', 'Hora','Data'], header = 0,
                             dtype={"Ticker": "category", "Quantidade": int,'Hora': str}, low_memory=True):
        
                temp_df = chunk[chunk['Ticker'] ==  ativo]
    
                # MESCLANDO COLUNA DE DATA E HORA NUMA SO
                temp_df['Hora'] = temp_df['Hora'].astype(str)
        
                # Aplicar a formatação à coluna 'horario'
                temp_df['horario_formatado'] = temp_df['Hora'].apply(funcoes.format_horario)
    
                # Combinar data e horário
                temp_df['Data Hora'] = pd.to_datetime(temp_df['Data'] + ' ' + temp_df['horario_formatado'], format='%Y-%m-%d %H:%M:%S')
    
                temp_df.set_index('Data Hora', inplace=True)
    
                temp_df = temp_df.between_time('10:00', '17:00')
                
                #array_df.append(temp_df)
                
#               3. Transformar Ticks em Candles

                ohlc_df = funcoes.ticks_to_ohlc(temp_df, tick_interval, ativo)
                print(ohlc_df.head(10))
                ohlc_list.append(ohlc_df)
                
                del temp_df
                del chunk
            
            ohlc_df_def = pd.concat(ohlc_list, ignore_index=False)
            
            csv = ohlc_df_def.to_csv(fr'C:\Users\JoãoWeckerle\B6 Capital\Gestão - General (1)\Fundos em estruturacao\Fundo Quant\MARKET DATA\Teste\Teste\{i}.csv',
                                     decimal = ',', encoding = 'latin-1', sep = ';')
            
            del ohlc_df_def

            ## Enviando pra nuvem e removendo arquivo
            novo = os.path.join(caminho, i)
            funcoes.enviar_nuvem(novo)
            os.remove(d)
    
#%%               4. Adicionar Colunas de interesse na base Candles
arquivo_ohlc = pd.read_csv(r'C:\Users\JoãoWeckerle\B6 Capital\Gestão - General (1)\Fundos em estruturacao\Fundo Quant\MARKET DATA\Teste\Teste\WDO24\WDO24_OHLC.csv',
                       sep = ';',  decimal = ',', dtype={"open": float, "high": float,"low": float, "close": float})

arquivo_ohlc['start_time'] = pd.to_datetime(arquivo_ohlc['start_time'])

arquivo_ohlc = arquivo_ohlc.sort_values('start_time')

# Calcula a diferença entre a máxima e a mínima dos últimos 20 candles
arquivo_ohlc['diff_high_low'] = (arquivo_ohlc['high'] - arquivo_ohlc['low']).rolling(window=20).mean()
    
# Calcular a média movel de 20 candles
arquivo_ohlc['SMA_20'] = arquivo_ohlc['close'].rolling(window=20).mean()

# Calcular a Volatilidade (Desvio Padrão) de 20 candles
arquivo_ohlc['Volatility_20'] = arquivo_ohlc['close'].rolling(window=20).std()

# Calcula o RSI
arquivo_ohlc['RSI'] = funcoes.calculate_rsi(arquivo_ohlc['close'])

#%%               5. Criar posições de Compra/Venda baseada em Critérios das colunas
resultados = []

# Lista de gains e losses para testar
gains = [3, 4, 5]
losses = [3, 4, 5]
start_time = time.time()

# Supondo que 'arquivo_ohlc' já esteja definido como um DataFrame
for gain, loss in product(gains, losses):
    print(f"Processando: Gain={gain}, Loss={loss}")

    # Variáveis iniciais
    posicao = "Não Posicionado"
    entrada = None
    ativo = None
    dia_atual = None

    # Converter dados relevantes em arrays do NumPy para operações vetorizadas
    open_prices = arquivo_ohlc['open'].to_numpy()
    low_prices = arquivo_ohlc['low'].to_numpy()
    high_prices = arquivo_ohlc['high'].to_numpy()
    start_times = arquivo_ohlc['start_time'].to_numpy()
    end_times = arquivo_ohlc['end_time'].to_numpy()
    tickers = arquivo_ohlc['ticker'].to_numpy()
    diff_high_low = arquivo_ohlc['diff_high_low'].to_numpy()
    sma_20 = arquivo_ohlc['SMA_20'].to_numpy()
    vol_20 = arquivo_ohlc['Volatility_20'].to_numpy()
    rsi = arquivo_ohlc['RSI'].to_numpy()

    for index in range(len(arquivo_ohlc)):
        preco_abertura = open_prices[index]
        hora_inicio = start_times[index]
        dia = pd.to_datetime(hora_inicio).date()
        
        # Verifica se estamos na última linha do DataFrame ou se o próximo candle é de um novo dia
        if index == len(arquivo_ohlc) - 1 or pd.to_datetime(start_times[index + 1]).date() != dia:
            ultima_linha_do_dia = True
        else:
            ultima_linha_do_dia = False
            
# # =============================================================================
# #                       INÍCIO DO DIA
# # =============================================================================            
        # Verifica se é um novo dia
        if dia_atual != dia:
            dia_atual = dia
            if posicao == "Não Posicionado":
                preco_compra = preco_abertura - loss
                preco_venda = preco_abertura + gain
# # =============================================================================
# #                        EXECUÇÃO DAS ORDENS
# # =============================================================================
        # Verifica execução de compra/venda
        if posicao == "Não Posicionado":
            if low_prices[index] < preco_compra:
                posicao = "comprado"
                entrada = (preco_compra, hora_inicio, "compra")
                preco_gain = entrada[0] + gain
                preco_loss = entrada[0] - loss
                
            elif high_prices[index] > preco_venda:
                posicao = "vendido"
                entrada = (preco_venda, hora_inicio, "venda")
                preco_gain = entrada[0] - gain
                preco_loss = entrada[0] + loss

# # =============================================================================
# #                            COMPRADO
# # =============================================================================
        # Saídas para comprado
        elif posicao == "comprado":
            if high_prices[index] > preco_gain:
                funcoes.funcao_append(resultados, gain, loss, tickers[index], entrada, end_times[index], preco_gain,
                                      diff_high_low, sma_20, vol_20, rsi)
                posicao = "Não Posicionado"
                preco_compra = preco_gain - loss  
                preco_venda = preco_gain + gain  
                
                
            elif low_prices[index] <= preco_loss:
                funcoes.funcao_append(resultados, gain, loss, tickers[index], entrada, end_times[index], preco_loss, 
                                      diff_high_low, sma_20, vol_20, rsi)
                posicao = "Não Posicionado"
                preco_compra = preco_loss - loss  
                preco_venda = preco_loss + gain
                                    
# # =============================================================================
# #                       VENDIDO
# # =============================================================================
        # Saídas para vendido
        elif posicao == "vendido":
            if low_prices[index] < preco_gain:
                funcoes.funcao_append(resultados, gain, loss, tickers[index], entrada, end_times[index], preco_gain,
                                      diff_high_low, sma_20, vol_20, rsi)
                posicao = "Não Posicionado"
                preco_compra = preco_gain - loss  
                preco_venda = preco_gain + gain  
                
            elif high_prices[index] >= preco_loss:
                
                funcoes.funcao_append(resultados, gain, loss, tickers[index], entrada, end_times[index], preco_loss,
                                      diff_high_low, sma_20, vol_20, rsi)
                posicao = "Não Posicionado"
                preco_compra = preco_loss - loss  
                preco_venda = preco_loss + gain 

# # =============================================================================
# #                      FIM DO DIA
# # =============================================================================
        # Se estamos na última linha do dia e ainda há uma posição aberta, forçar o fechamento
        if ultima_linha_do_dia and posicao != "Não Posicionado":
            if posicao == "comprado":
                funcoes.funcao_append(resultados, gain, loss, tickers[index], entrada, end_times[index], preco_loss,
                                      diff_high_low, sma_20, vol_20, rsi)
                
            elif posicao == "vendido":
                funcoes.funcao_append(resultados, gain, loss, tickers[index], entrada, end_times[index], preco_loss,
                                      diff_high_low, sma_20, vol_20, rsi)
                
            posicao = "Não Posicionado"  # Ordem concluída, reset posição

end_time = time.time()
execution_time = end_time - start_time
print(f'Tempo total: {execution_time / 60:,.2f} minutos')

#%%
# Converter resultados para DataFrame e salvar em CSV
resultados_df = pd.DataFrame(resultados)
resultados_df.to_csv(r'C:\Users\JoãoWeckerle\B6 Capital\Gestão - General (1)\Fundos em estruturacao\Fundo Quant\MARKET DATA\Teste\Teste\WDO24\resultados_operacoes.csv', index=False, sep = ';',  decimal = ',', encoding = 'latin-1')

#%%                6. Verificar Correlações e testar Modelos de ML

