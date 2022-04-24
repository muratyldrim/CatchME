# CatchME
CatchME is an anomaly detection tool based on system performance metrics. 

## Technologies
Project is created with:

* Python v3.9.7
* Elasticsearch v6.8.23
* Metricbeat v6.8.9
* MySQL v5.6.35
* Grafana v8.2.1

## Introduction
It uses Metricbeat data which is a lightweight agent that can be installed on target servers to periodically collect performance metrics data from them and send output to Elasticsearch directly.

It provides a proactive solution by predicting the anomaly in **real-time**. There is also an option of making a **historical** anomaly detection approach.

## Using ML Algortihms

* StandartScaler()

https://en.wikipedia.org/wiki/Feature_scaling

https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.StandardScaler.html



* IsolatinForest()

https://ieeexplore.ieee.org/document/4781136

https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.IsolationForest.html


## How it works

Predict 


## ML Features

Following 28 features were used for the model creation and prediction.

![image](https://user-images.githubusercontent.com/51790526/164985113-faef2a0a-ac55-4372-8d75-0ff2992bdfca.png)


First Header | Second Header———— | ————-Content from cell 1 | Content from cell 2Content in the first column | Content in the second column



## Links
Installation and Configuration Metricbeat
https://www.elastic.co/guide/en/beats/metricbeat/current/metricbeat-installation-configuration.html

Installation Elasticsearch
https://www.elastic.co/guide/en/elasticsearch/reference/current/install-elasticsearch.html

Installation Grafana
https://grafana.com/docs/grafana/latest/installation/

Installation MySQL
https://dev.mysql.com/doc/mysql-installation-excerpt/8.0/en/installing.html
