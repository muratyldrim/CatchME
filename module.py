import os
import sys
import re
import datetime
import pandas as pd
import joblib
import logging
import pymysql
from elasticsearch import Elasticsearch
from pandasticsearch import Select
from elasticsearch.helpers import bulk
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest
import plotly.graph_objs as go


# Common variables
index = "metricbeat-6.8.9-"
todayDate = datetime.datetime.today().strftime("%Y.%m.%d")


''' Feature list '''
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

socketSummary = ["@timestamp",
                 "system.socket.summary.tcp.all.established",
                 "system.socket.summary.tcp.all.close_wait",
                 "system.socket.summary.tcp.all.time_wait",
                 "system.socket.summary.tcp.all.count",
                 "system.socket.summary.all.count"]

processSummary = ["@timestamp",
                  "system.process.summary.running",
                  "system.process.summary.sleeping",
                  "system.process.summary.stopped",
                  "system.process.summary.zombie",
                  "system.process.summary.total"]

''' Create dictionary from features. Uses for loop by metrics. '''
featuresDict = {'cpu': cpu,
                'memory': memory,
                'load': load,
                'socket_summary': socketSummary,
                'process_summary': processSummary
                }


# Functions
def create_logger(loggername):
    log_dir = r"C:\Users\murat.yildirim2\PycharmProjects\CatchME\logs"
    log_format = '%(asctime)s - %(levelname)s : %(message)s'
    log_level = logging.WARNING
    log_date = '%d-%b-%y %H:%M:%S'
    log_formatter = logging.Formatter(fmt=log_format, datefmt=log_date)

    loggername_logfile = f'\\{loggername}_createModel_log.txt'
    loggername_logger = logging.getLogger(loggername)
    loggername_handler = logging.FileHandler(log_dir + loggername_logfile, mode='w')
    loggername_handler.setLevel(log_level)
    loggername_handler.setFormatter(log_formatter)
    loggername_logger.addHandler(loggername_handler)
    return loggername_logger


def connect_elasticsearch(logger):
    conn = "False"
    while conn == "False":
        es = Elasticsearch([{'host': '10.86.36.130', 'port': '9200'}])
        if es.ping():
            logger.warning("connected to ElasticSearch.")
            return es
        else:
            logger.warning("cannot connect to ElasticSearch trying again...")


def create_hostlist(conn, index_name, today_date, logger):
    index_name = index_name + today_date
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
    res = conn.search(index=index_name, body=es_query)

    hostname_listdict = res["aggregations"]["uniq_hostname"]["buckets"]
    host_list = []
    for i in range(len(hostname_listdict)):
        host_list.append(hostname_listdict[i]["key"])

    logger.warning(f'hostnameList created with {len(hostname_listdict)} hosts')
    return host_list


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


def get_features(conn, host, metricset, features, days, df_featureall, logger):
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
    res = conn.search(index="metricbeat-6.8.9*", body=es_query, size=50000, request_timeout=100)

    df_feature = Select.from_dict(res).to_pandas()
    logger.warning(f'{metricset} dataFrame created for {host}')

    df_feature["datetime"] = pd.to_datetime(df_feature["@timestamp"]).dt.strftime('%Y-%m-%d-%H:%M')
    df_feature.drop(columns=["@timestamp", "_index", "_type", "_id", "_score"], inplace=True)
    df_feature = df_feature.sort_values("datetime").set_index("datetime")
    df_feature.dropna(inplace=True)

    '''Call function for create model by metricset'''
    create_model(df_feature, host, metricset, logger)

    df_featureall = pd.merge(df_featureall, df_feature, left_index=True, right_index=True, how='outer')
    df_featureall.dropna(inplace=True)
    return df_featureall


def generate_models(conn, _queue, _thread, days, hostlist, logger):
    while not _queue.empty():
        hostname = _queue.get()
        df_hostname = pd.DataFrame()

        '''Use these variables for just log files'''
        orderhost = hostlist.index(hostname) + 1
        lenlist = len(hostlist)

        '''Call function for singlehost logger config'''
        singlehost_logger = create_logger(hostname)

        singlehost_logger.warning(f'{orderhost} of {lenlist}: {hostname}')
        singlehost_logger.warning(f'Running Thread-{_thread}')

        try:
            for key in featuresDict:
                '''Call function for create ALL dataFrame by hostname'''
                df_hostname = get_features(conn, hostname, key, featuresDict[key], days, df_hostname, singlehost_logger)

            singlehost_logger.warning(f'ALL dataFrame created for {hostname}')

            '''Call function for create ALL model by hostname'''
            create_model(df_hostname, hostname, "ALL", singlehost_logger)

            singlehost_logger.warning(f'the createModel script end for {hostname}')
            logger.warning(f'{orderhost} of {lenlist}: Thread-{_thread} running for {hostname} done.')
        except Exception as error:
            singlehost_logger.warning(f'the createModel script end for {hostname} with ERROR:{error}!')
            logger.warning(f'{orderhost} of {lenlist}: Thread-{_thread} running for {hostname} end with ERROR:{error}!')
            pass
