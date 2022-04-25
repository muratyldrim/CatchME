

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

## Used ML & Scaler Algorithms

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
- Models and scalers are created for 2000+ servers twice a week with using **createModels.py**.
- It predicts data every 5 minutes for 2000+ servers with using **predictModels.py** and sends output to ES and MySQL.

### Historical
- Creates an anomaly graph for the last 30 days for a specified server using **createANDpredict.py**.

<br/>

#### Architecture of Catchme
![how it works](https://user-images.githubusercontent.com/51790526/165002550-b80e043f-369c-49cc-9527-6e0f2d42ff6d.PNG)


## Usage
* createModels.py runnig in crontab for twice a week.
'00 00 * * sun,wed /catchme/createModels.py'
* predictModels.py running in a loop in the backgroud.
nohup /catchme/catchmeRun.sh &
* createANDpredict.py running for a specific server.
/catchme/createANDpredict.py <server_name>

## Screenshots
### Real-time
![dxlmngdp3_2](https://user-images.githubusercontent.com/51790526/165002747-9881f301-5c27-4a24-b12d-e9c15e872da5.PNG)
![sblapp01](https://user-images.githubusercontent.com/51790526/165002752-83971e56-b0e1-4752-bf99-5328a56f6d7d.PNG)
![dxlmngdp3](https://user-images.githubusercontent.com/51790526/165002757-8f0a56a8-1cba-454f-8069-576b65825390.PNG)
![vnnxtdp02](https://user-images.githubusercontent.com/51790526/165002759-124dec2d-5185-4c5c-b014-6f03384e9140.PNG)

### Historical
![create predict](https://user-images.githubusercontent.com/51790526/165002616-7ca20671-ccf8-4e7c-abd4-7d2cfeaaddbd.PNG)

## Links
Installation and Configuration Metricbeat
https://www.elastic.co/guide/en/beats/metricbeat/current/metricbeat-installation-configuration.html

Installation Elasticsearch
https://www.elastic.co/guide/en/elasticsearch/reference/current/install-elasticsearch.html

Installation Grafana
https://grafana.com/docs/grafana/latest/installation/

Installation MySQL
https://dev.mysql.com/doc/mysql-installation-excerpt/8.0/en/installing.html
