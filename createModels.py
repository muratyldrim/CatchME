import threading
from module import *
from queue import Queue


# Main Function
def main():
    processdays = "now-120d/d"

    '''Call function for allhosts logger config'''
    allhosts_logger = create_logger("ALLHosts", "createModels")

    '''Call function for connect to elasticsearch'''
    es = connect_elasticsearch(allhosts_logger)

    '''Call function for delete old records from mysql dbs'''
    DatabaseOps.delete_mysql(allhosts_logger)

    hostname_list = create_hostlist(es, index, todayDate, allhosts_logger)

    allhosts_logger.warning(f'The createModels script started for {len(hostname_list)} hosts for {processdays}.')

    q = Queue(maxsize=0)  # 0 means infinite
    num_threads = 5
    thread_list = []

    for j in hostname_list:
        q.put(j)

    for i in range(1, num_threads+1):
        thread = threading.Thread(target=generate_models, args=(es, q, i, processdays, hostname_list,), daemon=True)
        thread_list.append(thread)
        thread.start()

    for thread in thread_list:
        thread.join()

    if threading.active_count() == 1:
        allhosts_logger.warning("The createModels script finished for ALL hosts.")


if __name__ == "__main__":
    main()
