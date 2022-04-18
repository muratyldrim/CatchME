import os
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


# Feature list
cpu = ["@timestamp",
       "system.cpu.user.pct",
       "system.cpu.system.pct",
       "system.cpu.iowait.pct",
       "system.cpu.idle.pct",
       "system.cpu.nice.pct",
       "system.cpu.irq.pct",
       "system.cpu.softirq.pct",
       "system.cpu.steal.pct",
       "system.cpu.total.pct"]

memory = ["@timestamp",
          "system.memory.actual.free",
          "system.memory.swap.used.pct",
          "system.memory.hugepages.used.pct"]

load = ["@timestamp",
        "system.load.1",
        "system.load.5",
        "system.load.15"]

socket_summary = ["@timestamp",
                  "system.socket.summary.tcp.all.listening",
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
                   "system.process.summary.idle",
                   "system.process.summary.dead",
                   "system.process.summary.total"]

''' Create dictionary from features. Uses for loop by metrics. '''
featuresDict = {'cpu': cpu,
                'memory': memory,
                'load': load,
                'socket_summary': socket_summary,
                'process_summary': process_summary
                }


# Functions
def create_logger(logger_name, logfile_name):
    log_dir = r"C:\Users\murat.yildirim2\PycharmProjects\CatchME\logs"
    log_format = '%(asctime)s - %(levelname)s : %(message)s'
    log_level = logging.WARNING
    log_date = '%d-%b-%y %H:%M:%S'
    log_formatter = logging.Formatter(fmt=log_format, datefmt=log_date)

    loggername_logfile = f'\\{logger_name}_{logfile_name}_log.txt'
    loggername_logger = logging.getLogger(logger_name)

    loggername_handler = logging.FileHandler(log_dir + loggername_logfile, mode='w')
    loggername_handler.setLevel(log_level)
    loggername_handler.setFormatter(log_formatter)

    loggername_logger.addHandler(loggername_handler)
    return loggername_logger


def connect_elasticsearch(logger):
    conn_check = "False"
    while conn_check == "False":
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

    if feature == "socket_summary" or feature == "process_summary":
        cont_num = 0.0007
    else:
        cont_num = 0.005

    if df_name.shape[0] > 0:
        scaler = StandardScaler().fit(df_name)
        scaled_train_data = scaler.transform(df_name)

        iforest_model = IsolationForest(contamination=cont_num)
        iforest_model.fit(scaled_train_data)

        model_file = model_path + host + "_" + feature + "_model.pkl"
        scaler_file = scaler_path + host + "_" + feature + "_scaler.pkl"

        joblib.dump(scaler, scaler_file)
        joblib.dump(iforest_model, model_file)

        logger.warning(f'{feature} model&scaler created for {host}')
    else:
        logger.warning(f'Getting ERROR: {feature} model&scaler creation for {host} FAILED!')


def predict_feature(df_name, host, feature, logger):
    if df_name.shape[0] > 0:
        model_file = Finder.find_model(host, feature, logger)
        scaler_file = Finder.find_scaler(host, feature, logger)

        scaler = joblib.load(scaler_file)
        model = joblib.load(model_file)

        logger.warning(f'starting to predict {feature} data for {host}')

        scaled_test_data = scaler.transform(df_name)
        predict_result = model.predict(scaled_test_data)
        score = model.decision_function(scaled_test_data)

        df_name[f'{feature}_label'] = predict_result
        df_name[f'{feature}_score'] = score
        for i in score:
            if i < -0.1:
                '''Call function for insert mysql'''
                DatabaseOps.insert_mysql(host, feature, i, logger)

                logger.warning(f'anomaly detected for {feature} for {host} - score:{i}')
            else:
                logger.warning(f'no anomaly detect for {feature} for {host} - score:{i}')
    else:
        logger.warning(f'predict {feature} data for {host} FAILED!')


def create_predict_feature(df_name, host, feature, logger):
    logger.warning(f'starting to create model and predict {feature} data for {host}')

    if feature == "socket_summary" or feature == "process_summary":
        cont_num = 0.0007
    else:
        cont_num = 0.005

    std_scaler = StandardScaler()
    std_scaled_data = std_scaler.fit_transform(df_name)
    iforest_model = IsolationForest(contamination=cont_num)
    predict_result = iforest_model.fit_predict(std_scaled_data)
    score = iforest_model.decision_function(std_scaled_data)

    df_name[f'{feature}_score'] = score
    df_name[f'{feature}_label'] = predict_result


def create_resultdf(df_name, host, dictionary, logger):
    list_columns = []

    for i in dictionary:
        list_columns.append(f'{i}_score')
        list_columns.append(f'{i}_label')

    logger.warning(f'Result dataFrame created for {host}')

    df_result = df_name[list_columns].copy()
    return df_result


def get_features(conn, host, metricset, features, days, df_featureall, func, logger):
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
    df_feature = df_feature.sort_index(axis=1)
    df_feature.dropna(inplace=True)

    '''Call create_model OR predict_feature function by metricset'''
    func(df_feature, host, metricset, logger)

    df_featureall = pd.merge(df_featureall, df_feature, left_index=True, right_index=True, how='outer')
    df_featureall.dropna(inplace=True)
    return df_featureall


def generate_models(conn, _queue, _thread, days, hostlist):
    while not _queue.empty():
        hostname = _queue.get()
        df_hostname = pd.DataFrame()

        '''Use these variables for just log files'''
        orderhost = hostlist.index(hostname) + 1
        lenlist = len(hostlist)

        '''Call function for singlehost logger config'''
        singlehost_logger = create_logger(hostname, "createModels")

        singlehost_logger.warning(f'{orderhost} of {lenlist}: {hostname}')
        singlehost_logger.warning(f'Running Thread-{_thread}')

        try:
            for key in featuresDict:
                '''Call function for create ALL dataFrame and create models from them by hostname'''
                df_hostname = get_features(conn, hostname, key, featuresDict[key], days, df_hostname,
                                           create_model, singlehost_logger)

            singlehost_logger.warning(f'ALL metricsets dataFrame created for {hostname}')

            '''Call function to create model for ALL dataFrame by hostname'''
            create_model(df_hostname, hostname, "ALL", singlehost_logger)

            singlehost_logger.warning(f'the createModels script end for {hostname}')

        except Exception as error:
            singlehost_logger.warning(f'the createModels script end for {hostname} with ERROR:{error}!')
            pass


def predict_models(conn, _queue, _thread, days, hostlist):
    while not _queue.empty():
        hostname = _queue.get()
        df_hostname = pd.DataFrame()

        '''Use these variables for just log files'''
        orderhost = hostlist.index(hostname) + 1
        lenlist = len(hostlist)

        '''Call function for singlehost logger config'''
        singlehost_logger = create_logger(hostname, "predictModels")

        singlehost_logger.warning(f'{orderhost} of {lenlist}: {hostname}')
        singlehost_logger.warning(f'Running Thread-{_thread}')

        try:
            for key in featuresDict:
                '''Call function for creates ALL dataFrame and predict models from them by hostname'''
                df_hostname = get_features(conn, hostname, key, featuresDict[key], days, df_hostname,
                                           predict_feature, singlehost_logger)

            singlehost_logger.warning(f'ALL metricsets dataFrame created for {hostname}')

            '''Call function for creates a dataFrame only the score and label column by hostname'''
            df_hostname_result = create_resultdf(df_hostname, hostname, featuresDict, singlehost_logger)

            '''Convert dataFrame to origin version without score and label column by hostname '''
            df_hostname.drop(columns=df_hostname_result, inplace=True)

            '''Call function to predict model for ALL dataFrame by hostname'''
            predict_feature(df_hostname, hostname, "ALL", singlehost_logger)

            if df_hostname.shape[0] > 0:
                df_hostname_result["ALL_score"] = df_hostname["ALL_score"]
                df_hostname_result["ALL_label"] = df_hostname["ALL_label"]
                df_hostname.drop(columns=["ALL_score", "ALL_label"], inplace=True)

                '''Call function for insert elasticsearch'''
                DatabaseOps.insert_es(df_hostname_result, hostname, todayDate, conn, singlehost_logger)

                singlehost_logger.warning(f'the predictModels script end for {hostname}\n')
            else:
                singlehost_logger.warning(f'the predictModels script end for {hostname} with ERROR!\n')

        except Exception as error:
            singlehost_logger.warning(f'the predictModels script end for {hostname} with ERROR:{error}!')
            pass


def plotly_visulation(cls_name, host, df_name, logger):
    logger.warning(f'starting visualization for {host}')

    '''create objects'''
    cpu = cls_name("cpu", df_name)
    memory = cls_name("memory", df_name)
    load = cls_name("load", df_name)
    socket_summary = cls_name("socket_summary", df_name)
    process_summary = cls_name("process_summary", df_name)
    ALL = CreateVisual("ALL", df_name)

    fig = go.Figure()

    '''adding score values'''
    fig.add_trace(go.Scatter(x=cpu.score_date, y=cpu.score_name, mode="lines",
                             name="cpu score", line=dict(color="lightslategray")))

    fig.add_trace(go.Scatter(x=memory.score_date, y=memory.score_name, mode="lines",
                             name="memory score", line=dict(color="coral")))

    fig.add_trace(go.Scatter(x=load.score_date, y=load.score_name, mode="lines",
                             name="load score", line=dict(color="darkseagreen")))

    fig.add_trace(go.Scatter(x=socket_summary.score_date, y=socket_summary.score_name, mode="lines",
                             name="socket score", line=dict(color="cornflowerblue")))

    fig.add_trace(go.Scatter(x=process_summary.score_date, y=process_summary.score_name, mode="lines",
                             name="process score", line=dict(color="darkgoldenrod")))

    fig.add_trace(go.Scatter(x=ALL.score_date, y=ALL.score_name, mode="lines",
                             name="ALL score", line=dict(color="firebrick")))

    '''adding anomaly valus'''
    fig.add_trace(go.Scatter(x=cpu.anomaly_date, y=cpu.anomaly_name, mode="markers",
                             name="cpu - " + cpu.anomaly_count, marker=dict(size=8, color="lightslategray")))

    fig.add_trace(go.Scatter(x=memory.anomaly_date, y=memory.anomaly_name, mode="markers",
                             name="memory - " + memory.anomaly_count, marker=dict(size=8, color="coral")))

    fig.add_trace(go.Scatter(x=load.anomaly_date, y=load.anomaly_name, mode="markers",
                             name="load - " + load.anomaly_count, marker=dict(size=8, color="darkseagreen")))

    fig.add_trace(go.Scatter(x=socket_summary.anomaly_date, y=socket_summary.anomaly_name, mode="markers",
                             name="socket - " + socket_summary.anomaly_count,
                             marker=dict(size=8, color="cornflowerblue")))

    fig.add_trace(go.Scatter(x=process_summary.anomaly_date, y=process_summary.anomaly_name, mode="markers",
                             name="process - " + process_summary.anomaly_count,
                             marker=dict(size=8, color="darkgoldenrod")))

    fig.add_trace(go.Scatter(x=ALL.anomaly_date, y=ALL.anomaly_name, mode="markers",
                             name="ALL - " + ALL.anomaly_count, marker=dict(size=8, color="firebrick")))

    fig.update_layout(title=f'{host} Anomaly',
                      title_x=0.5,
                      width=1500,
                      height=800,
                      paper_bgcolor="whitesmoke",
                      template="seaborn",
                      xaxis=dict(
                                rangeselector=dict
                                (
                                    buttons=list
                                    ([
                                        dict(count=7, label='7d', step='day', stepmode='backward'),
                                        dict(count=15, label='15d', step='day', stepmode='backward'),
                                        dict(count=30, label='30d', step='day', stepmode='backward')
                                    ])
                                ),
                                rangeslider=dict(visible=False), type='date')
                      )
    fig.show()

    logger.warning(f'the visualization completed for {host}')


# Classes
class Finder:
    @staticmethod
    def find_model(host, feature, logger):
        models_path = r"C:\Users\murat.yildirim2\PycharmProjects\CatchME\models"
        model_dir = os.listdir(models_path)

        for modelname in model_dir:
            if re.search(f'{host}_{feature}', modelname):
                logger.warning(f'found {modelname}')
                return os.path.join(models_path, modelname)

    @staticmethod
    def find_scaler(host, feature, logger):
        scalers_path = r"C:\Users\murat.yildirim2\PycharmProjects\CatchME\scalers"
        scaler_dir = os.listdir(scalers_path)

        for scalername in scaler_dir:
            if re.search(f'{host}_{feature}', scalername):
                logger.warning(f'found {scalername}')
                return os.path.join(scalers_path, scalername)


class DatabaseOps:
    @staticmethod
    def delete_mysql(logger):
        interval_days = 120
        sql = f'DELETE FROM unixdb.catchme_dev WHERE DATETIME < now() - INTERVAL {interval_days} DAY;'

        '''esenyurt db operations'''
        mysql_esy = pymysql.connect(host='10.86.36.170',
                                    user='root',
                                    password='5ucub4Day',
                                    db='unixdb',
                                    charset='utf8mb4')

        cursor_esy = mysql_esy.cursor()
        cursor_esy.execute(sql)
        mysql_esy.commit()

        logger.warning(f'older than {interval_days} days records deleted from mysql_esy.')

        ''''gaziemir db operations'''
        mysql_gzm = pymysql.connect(host='172.31.44.50',
                                    user='superadmin',
                                    password='superadmin',
                                    db='unixdb',
                                    charset='utf8mb4')

        cursor_gzm = mysql_gzm.cursor()
        cursor_gzm.execute(sql)
        mysql_gzm.commit()

        logger.warning(f'older than {interval_days} days records deleted from mysql_gzm.')

    @staticmethod
    def insert_mysql(host, feature, score, logger):
        sql = 'INSERT INTO unixdb.catchme_dev(hostname, feature, status, score) VALUES (%s, %s, -1, %s)'
        values = (host, feature, score)

        '''esenyurt db operations'''
        mysql_esy = pymysql.connect(host='10.86.36.170',
                                    user='root',
                                    password='5ucub4Day',
                                    db='unixdb',
                                    charset='utf8mb4')

        cursor_esy = mysql_esy.cursor()
        cursor_esy.execute(sql, values)
        mysql_esy.commit()

        logger.warning(f'inserting score and anomaly value to mysql_esy')

        ''''gaziemir db operations'''
        mysql_gzm = pymysql.connect(host='172.31.44.50',
                                    user='superadmin',
                                    password='superadmin',
                                    db='unixdb',
                                    charset='utf8mb4')

        cursor_gzm = mysql_gzm.cursor()
        cursor_gzm.execute(sql, values)
        mysql_gzm.commit()

        logger.warning('inserting score and anomaly value to mysql_gzm')

    @staticmethod
    def insert_es(df_name, host, date, conn, logger):
        indexname = "dev_catchme_" + date
        df_name["hostname"] = host
        df_name["@timestamp"] = pd.to_datetime(df_name.index, format="%Y-%m-%d-%H:%M")
        esdocs = df_name.to_dict(orient='records')
        bulk(conn, esdocs, index=indexname, doc_type='doc', request_timeout=60)

        logger.warning(f'inserting score and anomaly values to es index: {indexname} for {host}')


class CreateVisual:
    def __init__(self, name, df_name):
        self.name = name
        self.df_hostname_result = df_name
        self.score_name = self.score_name()
        self.score_date = self.score_date()
        self.anomaly_name = self.anomaly_name()
        self.anomaly_date = self.anomaly_date()
        self.anomaly_count = self.anomaly_count()

    def score_name(self):
        return self.df_hostname_result[f'{self.name}_score']

    def score_date(self):
        return pd.to_datetime(self.df_hostname_result.index)

    def anomaly_name(self):
        return self.df_hostname_result[f'{self.name}_label'][self.df_hostname_result[f'{self.name}_label'] == -1]

    def anomaly_date(self):
        return pd.to_datetime(self.df_hostname_result.index[self.df_hostname_result[f'{self.name}_label'] == -1])

    def anomaly_count(self):
        return str(self.df_hostname_result[f'{self.name}_label']
                   [self.df_hostname_result[f'{self.name}_label'] == -1].count())
