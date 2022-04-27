import sys
from module import *


# Main Function
def main():
    processdays = "now-30d/d"
    hostname = sys.argv[1]
    df_hostname = pd.DataFrame()

    '''Call function for logger config'''
    singlehost_logger = create_logger("ALLHosts", "createANDpredict")

    '''Call function for connect to elasticsearch'''
    es = connect_elasticsearch(singlehost_logger)

    singlehost_logger.warning(f'The createANDpredict script is running for {hostname} for {processdays}.')

    try:
        for key in featuresDict:
            '''Call function for create ALL dataFrame and create and predict models from them'''
            df_hostname = get_features(es, hostname, key, featuresDict[key], processdays, df_hostname,
                                       create_predict_feature, singlehost_logger)

        '''Call function for creates a dataFrame only the score and label column'''
        df_hostname_result = create_resultdf(df_hostname, hostname, featuresDict, singlehost_logger)

        '''Convert dataFrame to origin version without score and label column'''
        df_hostname.drop(columns=df_hostname_result, inplace=True)

        '''Call function to create and predict model for ALL dataFrame'''
        create_predict_feature(df_hostname, hostname, "ALL", singlehost_logger)

        if df_hostname.shape[0] > 0:
            df_hostname_result["ALL_score"] = df_hostname["ALL_score"]
            df_hostname_result["ALL_label"] = df_hostname["ALL_label"]
            df_hostname.drop(columns=["ALL_score", "ALL_label"], inplace=True)

            singlehost_logger.warning(f'the createANDpredict script end for {hostname}')
    except Exception as error:
        singlehost_logger.warning(f'the createANDpredict script end for {hostname} with ERROR: {error}!')
        return True

    '''Call function to plotly visualiton'''
    plotly_visulation(CreateVisual, hostname, df_hostname_result, singlehost_logger)


if __name__ == "__main__":
    main()
