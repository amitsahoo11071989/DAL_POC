from src.utilities.exceptions import CustomException
import json

def get_parent_directory(path, levels):
    path_splits = path.split('\\')[:-levels]
    return_str = ''
    for element in path_splits:
        return_str += element + '\\'
    return return_str.rstrip('\\')

def read_json(json_file):
    try:
        with open(json_file, 'r') as json_file:
            config = json.load(json_file)
            return config
    except Exception as e:
        raise CustomException(e)
