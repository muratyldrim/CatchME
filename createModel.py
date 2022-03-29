import threading
from module import *
from queue import Queue


# Main Function
def main():
    # Script variables
    processdays = "now-30d/d"
    # hostname_list = create_hostlist(es, index, todayDate, allhosts_logger)
    hostname_list = ["unxmysqldb01", "ynmdcachep8", "vnnxtdp02", "meddbp2", "esdp02", "cms1tasap05"]

    '''Call function for allhosts logger config'''
    allhosts_logger = create_logger("ALLHosts")

    '''Call function for connect to elasticsearch'''
    es = connect_elasticsearch(allhosts_logger)

    allhosts_logger.warning(f'The createModel script is started for {processdays}.')

    q = Queue(maxsize=0)  # 0 means infinite
    num_threads = 3
    thread_list = []

    for j in hostname_list:
        q.put(j)

    for i in range(num_threads):
        thread = threading.Thread(target=generate_models, args=(es, q, i, processdays, hostname_list,
                                                                allhosts_logger,), daemon=True)
        thread.start()
        thread_list.append(thread)

    for thread in thread_list:
        thread.join()

    if threading.activeCount() == 1:
        allhosts_logger.warning("The createModel script finished for ALL hosts.")


if __name__ == "__main__":
    main()
