import sys
import json

from Configs.Spark_Core import session,insertion


def update_API_status():
    spark = session.get_spark_session()
    
    all_request_ids = []
    for arg in sys.argv[1:]:
        if arg and arg!=None:
            all_request_ids.append(set(json.loads(arg)))
    passed_ids  = set.intersection(*all_request_ids)
    failed_ids = set.union(*all_request_ids) - passed_ids
    # print("ARGV=",sys.argv)
    # passed_ids_1 = json.loads(sys.argv[1])
    # passed_ids_2 = json.loads(sys.argv[2])

    # passed_ids = list(set(passed_ids_1).intersection(set(passed_ids_2)))

    # failed_ids = list(set(passed_ids_1).symmetric_difference(set(passed_ids_2)))

    try:
        if passed_ids:
            insertion.Update_api_response_status(passed_ids, spark)
        if failed_ids:
            insertion.Mark_api_response_as_failed(failed_ids,spark)
    except Exception as e:
        print(f"Error updating API response status: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    update_API_status()