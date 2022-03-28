from module import *


'''Call function for allhosts logger config'''
allhosts_logger = create_logger("AllHosts")

'''Call function for connect to elasticsearch'''
es = connect_elasticsearch(allhosts_logger)

# Script variables
processDays = "now-30d/d"
# hostnameList = create_hostlist(es, index, todayDate, allhosts_logger)
hostnameList = ["unxmysqldb01", "ynmdcachep8", "vnnxtdp02", "meddbp2", "esdp02", "cms1tasap05"]


# Main function()
def main(_queue, _thread, logger):
    while not _queue.empty():
        hostname = _queue.get()
        df_hostname = pd.DataFrame()

        '''Use these variables for just log files'''
        orderhost = hostnameList.index(hostname) + 1
        lenlist = len(hostnameList)

        '''Call function for singlehost logger config'''
        singlehost_logger = create_logger(hostname)

        singlehost_logger.warning(f'{orderhost} of {lenlist}: {hostname}')
        singlehost_logger.warning(f'Running Thread-{_thread}')

        try:
            for key in featuresDict:
                '''Call function for create ALL dataFrame by hostname'''
                df_hostname = get_features(es, hostname, key, featuresDict[key], processDays, df_hostname, singlehost_logger)

            singlehost_logger.warning(f'ALL dataFrame created for {hostname}')

            '''Call function for create ALL model by hostname'''
            create_model(df_hostname, hostname, "ALL", singlehost_logger)

            singlehost_logger.warning(f'the createModel script end for {hostname}')
            logger.warning(f'{orderhost} of {lenlist}: Thread-{_thread} running for {hostname} done.')
        except Exception as error:
            singlehost_logger.warning(f'the createModel script end for {hostname} with ERROR:{error}!')
            logger.warning(f'{orderhost} of {lenlist}: Thread-{_thread} running for {hostname} end with ERROR:{error}!')
            return True


allhosts_logger.warning(f'The createModel script is started for {processDays}.')

queue = queue.Queue(maxsize=0)  # 0 means infinite
for j in hostnameList:
    queue.put(j)

num_threads = 3
threads = []
for thread in range(num_threads):
    worker = threading.Thread(target=main, args=(queue, thread, allhosts_logger,), daemon=True)
    worker.start()
    threads.append(worker)

for worker in threads:
    worker.join()

if threading.activeCount() == 1:
    allhosts_logger.warning("The createModel script finished for ALL hosts.")
