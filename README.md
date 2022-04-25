

# CatchME
CatchME is an open source anomaly detection tool based on system performance metrics. 

## Built With
Project is created with:

* Python v3.9.7
* Elasticsearch v6.8.23
* Metricbeat v6.8.9
* MySQL v5.6.35
* Grafana v8.2.1

![image](https://user-images.githubusercontent.com/51790526/164987351-5d9f0feb-7682-412e-bcc7-035958cb9d7a.png)


## Introduction
It uses Metricbeat data which is a lightweight agent that can be installed on target servers to periodically collect performance metrics data from them and send output to Elasticsearch directly.

It provides a proactive solution by predicting the anomaly in **real-time**. There is also an option of making a **historical** anomaly detection approach.

## Used ML&Scaler Algorithms

* StandartScaler()

https://en.wikipedia.org/wiki/Feature_scaling

https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.StandardScaler.html

<br/>

* IsolatinForest()

https://ieeexplore.ieee.org/document/4781136

https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.IsolationForest.html


## ML Features

Following 28 features were used for the model creation and prediction.

![image](https://user-images.githubusercontent.com/51790526/164985113-faef2a0a-ac55-4372-8d75-0ff2992bdfca.png)

## How It Works
### Real-time
- Models and scalers are created for 2000+ servers twice a week with using createModels.py.
- It predicts data every 5 minutes for 2000+ servers with using predictModels.py and sends output to ES and MySQL.

### Historical
- Creates an anomaly graph for the last 30 days for a specified server using createANDpredict.py.

<br/>

#### Architecture of Catchme
![how it works](https://user-images.githubusercontent.com/51790526/165002550-b80e043f-369c-49cc-9527-6e0f2d42ff6d.PNG)



## Usage

## Screenshots


## Links
Installation and Configuration Metricbeat
https://www.elastic.co/guide/en/beats/metricbeat/current/metricbeat-installation-configuration.html

Installation Elasticsearch
https://www.elastic.co/guide/en/elasticsearch/reference/current/install-elasticsearch.html

Installation Grafana
https://grafana.com/docs/grafana/latest/installation/

Installation MySQL
https://dev.mysql.com/doc/mysql-installation-excerpt/8.0/en/installing.html
