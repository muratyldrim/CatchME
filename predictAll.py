import os
import re
import datetime
import pandas as pd
import joblib
import pymysql
import logging
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from pandasticsearch import Select


# variables
#traindays = "now-30d/d"
traindays = "now-5m/m"
hostname_list = ["unxmysqldb01", "ynmdcachep8", "vnnxtdp02"]  # temp
index = "metricbeat-6.8.9-"
todayDate = datetime.datetime.today().strftime("%Y.%m.%d")


# logging config
log_dir = r"C:\Users\murat.yildirim2\PycharmProjects\CatchME"
log_file = r"\predictAll_log.txt"
log_filemode = "w"  # Default değeri "a" dır.)
log_format = '%(asctime)s - %(levelname)s : %(message)s'
log_level = logging.WARNING
log_date = '%d-%b-%y %H:%M:%S'
logging.basicConfig(filename=log_dir + log_file,
                    level=log_level,
                    format=log_format,
                    datefmt=log_date,
                    filemode=log_filemode)


# feature list
cpu = ["@timestamp",
       "system.cpu.user.pct",
       "system.cpu.system.pct",
       "system.cpu.iowait.pct",
       "system.cpu.idle.pct",
       "s.cloeystem.cpu.total.pct"]

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
    logging.warning(f'created hostnameList with {len(hostname_listdict)} hosts')
    return host_list


class Finder:
    @staticmethod
    def find_model(host, feature):
        models_path = r"C:\Users\murat.yildirim2\PycharmProjects\CatchME\models"
        model_dir = os.listdir(models_path)
        for modelname in model_dir:
            if re.search(f'{host}_{feature}', modelname):
                logging.warning(f'found {modelname}')
                return os.path.join(models_path, modelname)

    @staticmethod
    def find_scaler(host, feature):
        scalers_path = r"C:\Users\murat.yildirim2\PycharmProjects\CatchME\scalers"
        scaler_dir = os.listdir(scalers_path)
        for scalername in scaler_dir:
            if re.search(f'{host}_{feature}', scalername):
                logging.warning(f'found {scalername}')
                return os.path.join(scalers_path, scalername)


class InsertDB:
    @staticmethod
    def insert_mysql(host, feature, status, score):
        # sql query
        sql = 'INSERT INTO unixdb.catchme_dev(hostname, feature, status, score) VALUES (%s, %s, %s, %s)'
        values = (host, feature, status[0], score[0])

        # esenyurt db operations
        mysql_esy = pymysql.connect(host='10.86.36.170',
                                    user='root',
                                    password='5ucub4Day',
                                    db='unixdb',
                                    charset='utf8mb4')
        cursor_esy = mysql_esy.cursor()
        cursor_esy.execute(sql, values)
        mysql_esy.commit()
        logging.warning(f'inserting to mysql_esy for {host}')

        # gaziemir db operations
        mysql_gzm = pymysql.connect(host='172.31.44.50',
                                    user='superadmin',
                                    password='superadmin',
                                    db='unixdb',
                                    charset='utf8mb4')
        cursor_gzm = mysql_gzm.cursor()
        cursor_gzm.execute(sql, values)
        mysql_gzm.commit()
        logging.warning(f'inserting to mysql_gzm for {host}')

    @staticmethod
    def insert_es(df_name, host, date, connection):
        indexname = "dev_catchme_" + date
        df_name["hostname"] = host
        df_name["@timestamp"] = pd.to_datetime(df_name.index, format="%Y-%m-%d-%H:%M")
        esdocs = df_name.to_dict(orient='records')
        bulk(connection, esdocs, index=indexname, doc_type='doc', request_timeout=60)
        logging.warning(f'inserting to es index: {indexname} for {host}')


def predict_data(df_name, host, feature):
    if df_name.shape[0] > 0:
        model_file = Finder.find_model(host, feature)
        scaler_file = Finder.find_scaler(host, feature)

        scaler = joblib.load(scaler_file)
        model = joblib.load(model_file)

        logging.warning(f'predict {feature} data for {host}')
        scaled_test_data = scaler.transform(df_name)
        predict_result = model.predict(scaled_test_data)
        score = model.decision_function(scaled_test_data)
        df_name[f'{feature}_label'] = predict_result
        df_name[f'{feature}_score'] = score
        if score[0] < -0.1:
            logging.warning(f'{feature} anomaly detected for {host}')
            InsertDB.insert_mysql(hostname, feature, predict_result, score)
        else:
            logging.warning(f'no {feature} anomaly detect for {host}')
    else:
        logging.warning(f'predict {feature} data for {host} FAILED!')


# create result dataframe for visulation
def create_resultdf(df_name, dictionary):
    list_columns = []
    for i in dictionary:
        list_columns.append(f'{i}_score')
        list_columns.append(f'{i}_label')
    df_result = df_name[list_columns].copy()
    return df_result


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
    logging.warning(f'{metricset} dataFrame created for {host}')
    df_feature["datetime"] = pd.to_datetime(df_feature["@timestamp"]).dt.strftime('%Y-%m-%d-%H:%M')
    df_feature.drop(columns=["@timestamp", "_index", "_type", "_id", "_score"], inplace=True)
    df_feature = df_feature.sort_values("datetime").set_index("datetime")
    df_feature.dropna(inplace=True)
    predict_data(df_feature, hostname, metricset)  # call function for data predict
    global df_hostname
    df_hostname = pd.merge(df_hostname, df_feature, left_index=True, right_index=True, how='outer')
    df_hostname.dropna(inplace=True)


# Main Code()
logging.warning(f'The predictAll script started.')
# connect to elasticsearch
conn = "False"
while conn == "False":
    es = Elasticsearch([{'host': '10.86.36.130', 'port': '9200'}])
    if es.ping():
        conn = "True"
        logging.warning("connected to ElasticSearch.\n")
    else:
        logging.warning("cannot connect to ElasticSearch trying again...")
        conn = "False"


# hostname_list = create_hostlist(index, todayDate)
for hostname in hostname_list:
    df_hostname = pd.DataFrame()
    orderhost = hostname_list.index(hostname) + 1
    lenlist = len(hostname_list)
    logging.warning(f'{orderhost} of {lenlist}: {hostname}')
    try:
        for key in features_dict:
            get_features(hostname, key, features_dict[key], traindays)
        df_hostname_result = create_resultdf(df_hostname, features_dict)
        df_hostname.drop(columns=df_hostname_result, inplace=True)
        predict_data(df_hostname, hostname, "ALL")
        if df_hostname.shape[0] > 0:
            df_hostname_result["ALL_score"] = df_hostname["ALL_score"]
            df_hostname_result["ALL_label"] = df_hostname["ALL_label"]
            df_hostname.drop(columns=["ALL_score", "ALL_label"], inplace=True)
            logging.warning(f'created {hostname}_result dataFrame')
            InsertDB.insert_es(df_hostname_result, hostname, todayDate, es)
            logging.warning(f'the predictAll script end for {hostname}\n')
        else:
            logging.warning(f'the predictAll script end for {hostname} with ERROR!\n')
    except Exception as error:
        logging.warning(f'the predictAll script end for {hostname} with ERROR:{error}!\n')
        pass
logging.warning(f'The predictAll script finished for ALL hosts.')

