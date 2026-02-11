import traceback
import pandas as pd
import json
from xpms_file_storage.file_handler import XpmsResource, LocalResource
from xpms_helper.executions.execution_variables import ExecutionVariables
from xpms_storage.db_handler import DBProvider


def read_data_from_minio(minio_file_path, file_type=None):
    minio_res = XpmsResource.get(key=minio_file_path)
    lcl_res_path = "/tmp/" + minio_file_path
    lcl_res = LocalResource(fullpath=lcl_res_path)
    minio_res.copy(lcl_res)
    if file_type:
        with open(lcl_res_path) as file_obj:
            data = json.load(file_obj)
    else:
        data = pd.read_csv(lcl_res_path)
    return data,minio_res.urn


def config_function(config, **inp_obj):
    try:

        solution_id = config['context']['solution_id']
        execution_id = config['context']['dag_execution_id']
        list_of_agent_ids = []
        if config["input_string1"] == "db":
            db = DBProvider.get_instance(solution_id) #this DB provider package is defined at the platforms end, so we abstract away which and how to connect to DB
            data = db.find(table="Dataset")
            print(data)
            for line in data:
                list_of_agent_ids.append(line['agent_id'])
                print("hello123:", line)
            return {"DATA_line_1": data[0]["agent_id"], "Data_line_2": data[1]["agent_id"],"agents": list_of_agent_ids}
        elif config["train_test"] == "Train":
            features_dataset, urn = read_data_from_minio(solution_id + "/data/Android_Malware.csv")
        elif config["train_test"] == "Test":
            features_dataset, urn = read_data_from_minio(solution_id + "/data/Android_Malware_test.csv")
        return {
            "dataset": {"data_format": "csv", "value": urn}

        }
    except Exception as e:
        return {"success": False, "error": str(e), "error_msg": traceback.format_exc()}
