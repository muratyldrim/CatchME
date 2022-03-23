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
traindays = "now-30d/d"
hostname_list = ["unxmysqldb01", "ynmdcachep8", "vnnxtdp02", "meddbp2", "esdp02", "cms1tasap05"]
index = "metricbeat-6.8.9-"
todayDate = datetime.datetime.today().strftime("%Y.%m.%d")

# logging config
log_dir = r"C:\Users\murat.yildirim2\PycharmProjects\CatchME\logs"
log_format = '%(asctime)s - %(levelname)s : %(message)s'
log_level = logging.WARNING
log_date = '%d-%b-%y %H:%M:%S'
formatter = logging.Formatter(fmt=log_format, datefmt=log_date)

# all hosts logger config
allhosts_file = r"\ALLHosts_createModel_log.txt"
allhosts_logger = logging.getLogger("allhosts")
allhosts_handler = logging.FileHandler(log_dir + allhosts_file, mode='w')
allhosts_handler.setLevel(log_level)
allhosts_handler.setFormatter(formatter)
allhosts_logger.addHandler(allhosts_handler)

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

    allhosts_logger.warning(f'hostnameList created with {len(hostname_listdict)} hosts')
    return host_list


# create model for all features
def create_model(df_name, host, feature, logger):
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
        logger.warning(f'{feature} model&scaler created for {host}')
    else:
        logger.warning(f'{feature} model&scaler creation for {host} FAILED!')


# create dataFrame for features
def get_features(host, metricset, features, days, df_featureall, logger):
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
    logger.warning(f'{metricset} dataFrame created for {host}')

    df_feature["datetime"] = pd.to_datetime(df_feature["@timestamp"]).dt.strftime('%Y-%m-%d-%H:%M')
    df_feature.drop(columns=["@timestamp", "_index", "_type", "_id", "_score"], inplace=True)
    df_feature = df_feature.sort_values("datetime").set_index("datetime")
    df_feature.dropna(inplace=True)

    create_model(df_feature, host, metricset, logger)

    df_featureall = pd.merge(df_featureall, df_feature, left_index=True, right_index=True, how='outer')
    df_featureall.dropna(inplace=True)
    return df_featureall


def main(_queue, _thread):
    while not _queue.empty():
        hostname = _queue.get()
        df_hostname = pd.DataFrame()
        orderhost = hostname_list.index(hostname) + 1
        lenlist = len(hostname_list)

        # single host logger config
        singlehost_file = f'\\{hostname}_createModel_log.txt'
        singlehost_logger = logging.getLogger(hostname)
        singlehost_handler = logging.FileHandler(log_dir + singlehost_file, mode='w')
        singlehost_handler.setLevel(log_level)
        singlehost_handler.setFormatter(formatter)
        singlehost_logger.addHandler(singlehost_handler)

        singlehost_logger.warning(f'{orderhost} of {lenlist}: {hostname}')
        singlehost_logger.warning(f'Runnig Thread-{_thread}')

        try:
            for key in features_dict:
                df_hostname = get_features(hostname, key, features_dict[key], traindays, df_hostname, singlehost_logger)

            singlehost_logger.warning(f'ALL dataFrame created for {hostname}')

            create_model(df_hostname, hostname, "ALL", singlehost_logger)

            singlehost_logger.warning(f'the creatModel script end for {hostname}')
            allhosts_logger.warning(f'{orderhost} of {lenlist}: Thread-{_thread} running for {hostname} done.')
        except Exception as error:
            singlehost_logger.warning(f'the createModel script end for {hostname} with ERROR:{error}!')
            allhosts_logger.warning(f'{orderhost} of {lenlist}: Thread-{_thread} running for {hostname} '
                                    f'end with ERROR:{error}!')
            return True


# Main Code()
allhosts_logger.warning(f'The createModel script is started for {traindays}.')

# connect to elasticsearch
conn = "False"
while conn == "False":
    es = Elasticsearch([{'host': '10.86.36.130', 'port': '9200'}])
    if es.ping():
        conn = "True"
        allhosts_logger.warning("connected to ElasticSearch.")
    else:
        conn = "False"
        allhosts_logger.warning("cannot connect to ElasticSearch trying again...")

# hostname_list = create_hostlist(index, todayDate)
queue = queue.Queue(maxsize=0)  # 0 means infinite
for j in hostname_list:
    queue.put(j)

num_threads = 3
threads = []
for thread in range(num_threads):
    worker = threading.Thread(target=main, args=(queue, thread,), daemon=True)
    worker.start()
    threads.append(worker)

for worker in threads:
    worker.join()

if threading.activeCount() == 1:
    allhosts_logger.warning("The createModel script finished for ALL hosts.")
