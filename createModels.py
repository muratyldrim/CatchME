import threading
from module import *
from queue import Queue


# Main Function
def main():
    processdays = "now-30d/d"

    '''Call function for allhosts logger config'''
    allhosts_logger = create_logger("ALLHosts", "createModels")

    '''Call function for connect to elasticsearch'''
    es = connect_elasticsearch(allhosts_logger)

    '''Call function for delete old records from mysql dbs'''
    DatabaseOps.delete_mysql(allhosts_logger)

    # hostname_list = create_hostlist(es, index, todayDate, allhosts_logger)
    hostname_list = ["unxmysqldb01", "ynmdcachep8", "vnnxtdp02", "meddbp2", "esdp02", "cms1tasap05", "wraap3", "medzd1"]

    allhosts_logger.warning(f'The createModels script started for {len(hostname_list)} hosts for {processdays}.')

    q = Queue(maxsize=0)  # 0 means infinite
    num_threads = 10
    thread_list = []

    for j in hostname_list:
        q.put(j)

    for i in range(num_threads):
        thread = threading.Thread(target=generate_models, args=(es, q, i, processdays, hostname_list,), daemon=True)
        thread.start()
        thread_list.append(thread)

    for thread in thread_list:
        thread.join()

    if threading.activeCount() == 1:
        allhosts_logger.warning("The createModels script finished for ALL hosts.")


if __name__ == "__main__":
    main()
