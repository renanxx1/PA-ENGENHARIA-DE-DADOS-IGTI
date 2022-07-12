from iqoptionapi.stable_api import IQ_Option
from kafka.admin import KafkaAdminClient, NewTopic
from dotenv import load_dotenv, find_dotenv
from datetime import datetime, timedelta
from kafka import KafkaProducer
from ast import literal_eval
from pathlib import Path
from json import dumps
import json
import os


# Pega credenciais de conexão com a API
load_dotenv(find_dotenv())
login = os.getenv('login')
password = os.getenv('password')

bootstrap_server = '10.99.5.38:9092'

# Cria um cliente para criar topicos no kafka
admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_server, client_id='ingestion_src_api_iq')


# Cria um producer para enviar para o kafka
producer = KafkaProducer(bootstrap_servers=[bootstrap_server], acks='all', value_serializer=lambda x: dumps(x).encode('utf-8'))


# Cria a conexão com a API
Iq=IQ_Option(login, password)
Iq.connect()


# Abre o arquivo com os parametros para usar na chamada da API
with open('params.json', 'r') as f:
  data_json = json.load(f)


# Função que cria logs da chamada da API localmente
def check_logs_data(symbol, size, period_list): 
    date = datetime.now().date()
    exist_log = os.path.exists(f'./logs/{symbol}_{size}_{date}.txt')
    
    if exist_log:
        log_period_list = literal_eval(open(f"./logs/{symbol}_{size}_{date}.txt", "r").read())
        list_difference = [item for item in period_list if item not in log_period_list]

        if len(list_difference) > 0:
            log_period_list.extend(list_difference)
            open(f"./logs/{symbol}_{size}_{date}.txt", "w").write(str(log_period_list))
            
            return list_difference
    
        return False
        
    else:
        yesterday = date-timedelta(days=1)
        exist_log = os.path.exists(f'./logs/{symbol}_{size}_{yesterday}.txt')
       
        if exist_log:
            log_period_list= literal_eval(open(f"./logs/{symbol}_{size}_{yesterday}.txt", "r").read())
            list_difference = [item for item in period_list if item not in log_period_list]
       
            if len(list_difference) > 0:
                log_period_list = []
                log_period_list.extend(list_difference)
                open(f"./logs/{symbol}_{size}_{date}.txt", "w").write(str(log_period_list))
            
                return list_difference
        
        else:
            Path("./logs/").mkdir(parents=True, exist_ok=True)
            open(f"./logs/{symbol}_{size}_{date}.txt", "w").write(str(period_list))
            
            return False


# Função para gerar uma lista de ids dos candles retornados pela API
def get_period_id(candles):
    try:
        list_ids = []
        for key, value in candles.items():
            list_ids.append(value['id'])
        return list_ids
    except Exception:
        return None


# Função para enviar os dados para o topico do kafka
def send_data_kafka(topic, candles, id_list):
    try:
        for key, value in candles.items():
            if value['id'] in id_list:
                producer.send(topic, value)
    except Exception as e:
        print(e)
        

# Função principal que roda todo o processo e funçoes
def run():
    
    #Inicia os candles que serão coletados e cria o topico no kafka
    for coin in data_json:
        for size in coin['size']:
            try:
                Iq.start_candles_stream(coin['symbol'], size, 20)
                topic_list = [NewTopic(name=f"ingestion_src_api_iq_{coin['symbol']}_{size}_json", num_partitions=5, replication_factor=1)]
                admin_client.create_topics(new_topics=topic_list, validate_only=False)
            except Exception as e:
                print(e)
   
    #Pega os novos dados da API e envia para o topico kafka
    while True:
        for coin in data_json:
            for size in coin['size']:
                try:
                    candles=Iq.get_realtime_candles(coin['symbol'], size)
                    list_ids = get_period_id(candles)
                    new_data = check_logs_data(coin['symbol'], size, list_ids)
                    if new_data:
                        send_data_kafka(f"ingestion_src_api_iq_{coin['symbol']}_{size}_json", candles, new_data)
                except Exception as e:
                    print(e)
                
run()
