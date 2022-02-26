import sys
import pandas as pd
import logging
import plotly.graph_objs as go
from pandasticsearch import Select
from elasticsearch import Elasticsearch
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest

# variables
traindays = "now-57d/d"
hostname = sys.argv[1]
df_hostname = pd.DataFrame()


# logging config
log_dir = r"C:\Users\murat.yildirim2\PycharmProjects\CatchME"
log_file = r"\predictSingle_log.txt"
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
# create model and predict data
def model_predict(df_name, feature):
    std_scaler = StandardScaler()
    std_scaled_data = std_scaler.fit_transform(df_name)
    iforest_model = IsolationForest(contamination=0.003)
    predict_result = iforest_model.fit_predict(std_scaled_data)
    score = iforest_model.decision_function(std_scaled_data)
    df_name[f'{feature}_score'] = score
    df_name[f'{feature}_label'] = predict_result


# create result dataframe for visulation
def create_resultdf(df_name, dictionary):
    list_columns = []
    for i in dictionary:
        list_columns.append(f'{i}_score')
        list_columns.append(f'{i}_label')
    df_result = df_name[list_columns].copy()
    return df_result


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
    logging.warning(f'{metricset} dataFrame created for {host}')
    df_feature["datetime"] = pd.to_datetime(df_feature["@timestamp"]).dt.tz_convert('Europe/Istanbul').dt.strftime(
        '%Y-%m-%d-%H:%M')
    df_feature.drop(columns=["@timestamp", "_index", "_type", "_id", "_score"], inplace=True)
    df_feature = df_feature.sort_values("datetime").set_index("datetime")
    df_feature.dropna(inplace=True)
    model_predict(df_feature, metricset)  # call function for create model and predict data for all features
    logging.warning(f'predict {metricset} data for {host}')
    global df_hostname
    df_hostname = pd.merge(df_hostname, df_feature, left_index=True, right_index=True, how='outer')
    df_hostname.dropna(inplace=True)


# class for plotly visulation
class CreateVisual:
    def __init__(self, name):
        self.name = name
        self.score_name = self.score_name()
        self.score_date = self.score_date()
        self.anomaly_name = self.anomaly_name()
        self.anomaly_date = self.anomaly_date()
        self.anomaly_count = self.anomaly_count()

    def score_name(self):
        return df_hostname_result[f'{self.name}_score']

    @staticmethod
    def score_date():
        return pd.to_datetime(df_hostname_result.index)

    def anomaly_name(self):
        return df_hostname_result[f'{self.name}_label'][df_hostname_result[f'{self.name}_label'] == -1]

    def anomaly_date(self):
        return pd.to_datetime(df_hostname_result.index[df_hostname_result[f'{self.name}_label'] == -1])

    def anomaly_count(self):
        return str(df_hostname_result[f'{self.name}_label'][df_hostname_result[f'{self.name}_label'] == -1].count())


# Main Code()
# connect to elasticsearch
conn = "False"
while conn == "False":
    es = Elasticsearch([{'host': '10.86.36.130', 'port': '9200'}])
    if es.ping():
        conn = "True"
        logging.warning("connected to ElasticSearch")
    else:
        logging.warning("cannot connect to ElasticSearch trying again...")
        conn = "False"

logging.warning(f'{hostname}')
for key in features_dict:
    get_features(hostname, key, features_dict[key], traindays)
df_hostname_result = create_resultdf(df_hostname, features_dict)
df_hostname.drop(columns=df_hostname_result, inplace=True)
model_predict(df_hostname, "ALL")
logging.warning(f'predict ALL data for {hostname}')
df_hostname_result["ALL_score"] = df_hostname["ALL_score"]
df_hostname_result["ALL_label"] = df_hostname["ALL_label"]
df_hostname.drop(columns=["ALL_score", "ALL_label"], inplace=True)
logging.warning(f'created {hostname}_result dataFrame')


# Plotly Visulation
# create objects for visulation
logging.warning(f'starting visulation for {hostname}')
cpu = CreateVisual("cpu")
memory = CreateVisual("memory")
load = CreateVisual("load")
socket_summary = CreateVisual("socket_summary")
process_summary = CreateVisual("process_summary")
ALL = CreateVisual("ALL")


# create graph
fig = go.Figure()
# score
fig.add_trace(go.Scatter(x=cpu.score_date,             y=cpu.score_name,             mode="lines", name="cpu score",     line=dict(color="lightslategray")))
fig.add_trace(go.Scatter(x=memory.score_date,          y=memory.score_name,          mode="lines", name="memory score",  line=dict(color="coral")))
fig.add_trace(go.Scatter(x=load.score_date,            y=load.score_name,            mode="lines", name="load score",    line=dict(color="darkseagreen")))
fig.add_trace(go.Scatter(x=socket_summary.score_date,  y=socket_summary.score_name,  mode="lines", name="socket score",  line=dict(color="cornflowerblue")))
fig.add_trace(go.Scatter(x=process_summary.score_date, y=process_summary.score_name, mode="lines", name="process score", line=dict(color="darkgoldenrod")))
fig.add_trace(go.Scatter(x=ALL.score_date,             y=ALL.score_name,             mode="lines", name="ALL score",     line=dict(color="firebrick")))
# anomaly
fig.add_trace(go.Scatter(x=cpu.anomaly_date,             y=cpu.anomaly_name,             mode="markers", name="cpu - " + cpu.anomaly_count,                 marker=dict(size=8, color="lightslategray")))
fig.add_trace(go.Scatter(x=memory.anomaly_date,          y=memory.anomaly_name,          mode="markers", name="memory - " + memory.anomaly_count,           marker=dict(size=8, color="coral")))
fig.add_trace(go.Scatter(x=load.anomaly_date,            y=load.anomaly_name,            mode="markers", name="load - " + load.anomaly_count,               marker=dict(size=8, color="darkseagreen")))
fig.add_trace(go.Scatter(x=socket_summary.anomaly_date,  y=socket_summary.anomaly_name,  mode="markers", name="socket - " + socket_summary.anomaly_count,   marker=dict(size=8, color="cornflowerblue")))
fig.add_trace(go.Scatter(x=process_summary.anomaly_date, y=process_summary.anomaly_name, mode="markers", name="process - " + process_summary.anomaly_count, marker=dict(size=8, color="darkgoldenrod")))
fig.add_trace(go.Scatter(x=ALL.anomaly_date,             y=ALL.anomaly_name,             mode="markers", name="ALL - " + ALL.anomaly_count,                 marker=dict(size=8, color="firebrick")))

fig.update_layout(title=f'{hostname} Anomaly',
                  title_x=0.5,
                  width=1500,
                  height=800,
                  paper_bgcolor="whitesmoke",
                  template="seaborn",
                  xaxis=dict
                  (
                      rangeselector=dict
                      (
                          buttons=list
                          ([
                              dict(count=7,
                                   label='7d',
                                   step='day',
                                   stepmode='backward'),
                              dict(count=15,
                                   label='15d',
                                   step='day',
                                   stepmode='backward'),
                              dict(count=30,
                                   label='30d',
                                   step='day',
                                   stepmode='backward')
                          ])
                      ),
                      rangeslider=dict
                      (
                          visible = False
                      ),
                      type='date'
                  )
                 )
fig.show()
logging.warning(f'the predictSingle script end for {hostname}\n')
