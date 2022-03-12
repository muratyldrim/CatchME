import datetime
import pandas as pd
import joblib
import logging
import queue
import threading
from elasticsearch import Elasticsearch
from pandasticsearch import Select
from sklearn.preprocessing import StandardScaler 
from sklearn.ensemble import IsolationForest


# variables
traindays = "now-1d/d"
hostname_list = ["unxmysqldb01", "ynmdcachep8", "vnnxtdp02"]
index = "metricbeat-6.8.9-"
todayDate = datetime.datetime.today().strftime("%Y.%m.%d")

# logging config
log_dir = r"C:\Users\murat.yildirim2\PycharmProjects\CatchME\logs"
log_format = '%(asctime)s - %(levelname)s : %(message)s'
log_level = logging.WARNING
log_date = '%d-%b-%y %H:%M:%S'
formatter = logging.Formatter(fmt=log_format, datefmt=log_date)

# all hosts logger config
allHostsFile = r"\ALL_createModel_log.txt"
allHostsLogger = logging.getLogger("allhosts")
allHostsHandler = logging.FileHandler(log_dir + allHostsFile, mode='w')
allHostsHandler.setLevel(log_level)
allHostsHandler.setFormatter(formatter)
allHostsLogger.addHandler(allHostsHandler)

# feature list
cpu = ["@timestamp",
       "system.cpu.user.pct",
       "system.cpu.system.pct",
       "system.cpu.iowait.pct",
       "system.cpu.idle.pct",
       "system.cpu.total.pct"]

memory = ["@timestamp",
          "system.memory.actual.free",
          "system.memory.swap.used.pct"]

load = ["@timestamp",
        "system.load.1",
        "system.load.5",
        "system.load.15"]

socket_summary = ["@timestamp",
                  "system.socket.summary.tcp.all.established",
                  "system.socket.summary.tcp.all.close_wait",
                  "system.socket.summary.tcp.all.time_wait",
                  "system.socket.summary.tcp.all.count",
                  "system.socket.summary.all.count"]

process_summary = ["@timestamp",
                   "system.process.summary.running",
                   "system.process.summary.sleeping",
                   "system.process.summary.stopped",
                   "system.process.summary.zombie",
                   "system.process.summary.total"]

# create dictionary for loop features by metrics
features_dict = {'cpu': cpu,
                 'memory': memory,
                 'load': load,
                 'socket_summary': socket_summary,
                 'process_summary': process_summary
                 }


# Classes & Functions
# getting hostname list
def create_hostlist(index_name, today_date):
    indexname = index_name + today_date
    es_query = {
        "size": 0,
        "aggregations": {
            "uniq_hostname": {
                "terms": {
                    "size": 10000,
                    "field": "beat.hostname"
                }
            }
        },
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "metricset.name": "uptime"
                        }
                    },
                    {
                        "match": {
                            "service.aim_of_use": "PROD"
                        }
                    }
                ]
            }
        }
    }
    res = es.search(index=indexname, body=es_query)

    hostname_listdict = res["aggregations"]["uniq_hostname"]["buckets"]
    host_list = []
    for i in range(len(hostname_listdict)):
        host_list.append(hostname_listdict[i]["key"])

    singleHostLogger.warning(f'hostnameList created with {len(hostname_listdict)} hosts')
    return host_list


# create model for all features
def create_model(df_name, host, feature):
    model_path = r"C:\Users\murat.yildirim2\PycharmProjects\CatchME\models\/"
    scaler_path = r"C:\Users\murat.yildirim2\PycharmProjects\CatchME\scalers\/"

    if df_name.shape[0] > 0:
        scaler = StandardScaler().fit(df_name)
        scaled_train_data = scaler.transform(df_name)

        iforest_model = IsolationForest(contamination=0.005)
        iforest_model.fit(scaled_train_data)

        model_file = model_path + host + "_" + feature + "_model.pkl"
        scaler_file = scaler_path + host + "_" + feature + "_scaler.pkl"

        joblib.dump(scaler, scaler_file)
        joblib.dump(iforest_model, model_file)
        singleHostLogger.warning(f'{feature} model&scaler created for {host}')
    else:
        singleHostLogger.warning(f'{feature} model&scaler creation for {host} FAILED!')


# create dataFrame for features
def get_features(host, metricset, features, days):
    es_query = {
        "_source": {
            "includes": features
        },
        "sort": {
            "@timestamp": {
                "order": "asc"
            }
        },
        "query": {
            "bool": {
                "filter": [
                    {
                        "match": {
                            "beat.hostname": host
                        }
                    },
                    {
                        "match": {
                            "metricset.name": metricset
                        }
                    },
                    {
                        "range": {
                            "@timestamp": {
                                "gte": days
                            }
                        }
                    }
                ]
            }
        }
    }
    res = es.search(index="metricbeat-6.8.9*", body=es_query, size=50000, request_timeout=100)

    df_feature = Select.from_dict(res).to_pandas()
    singleHostLogger.warning(f'{metricset} dataFrame created for {host}')

    df_feature["datetime"] = pd.to_datetime(df_feature["@timestamp"]).dt.strftime('%Y-%m-%d-%H:%M')
    df_feature.drop(columns=["@timestamp", "_index", "_type", "_id", "_score"], inplace=True)
    df_feature = df_feature.sort_values("datetime").set_index("datetime")
    df_feature.dropna(inplace=True)

    create_model(df_feature, hostname, metricset)  # call function for create model for all features

    global df_hostname
    df_hostname = pd.merge(df_hostname, df_feature, left_index=True, right_index=True, how='outer')
    df_hostname.dropna(inplace=True)


# Main Code()
allHostsLogger.warning(f'The createModel script is started for {traindays}.')

# connect to elasticsearch
conn = "False"
while conn == "False":
    es = Elasticsearch([{'host': '10.86.36.130', 'port': '9200'}])
    if es.ping():
        conn = "True"
        allHostsLogger.warning("connected to ElasticSearch.\n")
    else:
        conn = "False"
        allHostsLogger.warning("cannot connect to ElasticSearch trying again...")

# hostname_list = create_hostlist(index, todayDate)
queue = queue.Queue(maxsize=0)  # 0 means infinite
for hostname in hostname_list:
    queue.put(hostname)

while not queue.empty():
    hostname = queue.get()
    df_hostname = pd.DataFrame()
    orderhost = hostname_list.index(hostname) + 1
    lenlist = len(hostname_list)

    # single host logger config
    singleHostFile = f'\\{hostname}_createModel_log.txt'
    singleHostLogger = logging.getLogger(hostname)
    singleHostHandler = logging.FileHandler(log_dir + singleHostFile, mode='w')
    singleHostHandler.setLevel(log_level)
    singleHostHandler.setFormatter(formatter)
    singleHostLogger.addHandler(singleHostHandler)

    allHostsLogger.warning(f'{orderhost} of {lenlist}: {hostname}')
    singleHostLogger.warning(f'{orderhost} of {lenlist}: {hostname}')
    try:
        for key in features_dict:
            get_features(hostname, key, features_dict[key], traindays)
        singleHostLogger.warning(f'ALL dataFrame created for {hostname}')
        create_model(df_hostname, hostname, "ALL")
        singleHostLogger.warning(f'the creatModel script end for {hostname}')
        allHostsLogger.warning(f'the creatModel script end for {hostname}\n')
    except Exception as error:
        singleHostLogger.warning(f'the createModel script end for {hostname} with ERROR:{error}!')
        allHostsLogger.warning(f'the creatModel script end for {hostname} with ERROR:{error}!\n')
        pass
allHostsLogger.warning(f'The createModel script finished for ALL hosts.')
