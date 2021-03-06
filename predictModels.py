import threading
from module import *
from queue import Queue


# Main Function
def main():
    processdays = "now-5m/m"

    '''Call function for allhosts logger config'''
    allhosts_logger = create_logger("ALLHosts", "predictModels")

    '''Call function for connect to elasticsearch'''
    es = connect_elasticsearch(allhosts_logger)

    hostname_list = create_hostlist(es, index, todayDate, allhosts_logger)

    allhosts_logger.warning(f'The predictModels script is started for {len(hostname_list)} hosts for {processdays}.')

    q = Queue(maxsize=0)  # 0 means infinite
    num_threads = 10
    thread_list = []

    for j in hostname_list:
        q.put(j)

    for i in range(num_threads):
        thread = threading.Thread(target=predict_models, args=(es, q, i, processdays, hostname_list,), daemon=True)
        thread_list.append(thread)
        thread.start()

    for thread in thread_list:
        thread.join()

    if threading.active_count() == 1:
        allhosts_logger.warning("The predictModels script finished for ALL hosts.")


if __name__ == "__main__":
    main()
