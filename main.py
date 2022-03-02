import os
import sys
import json
import requests
import time

from requests.auth import HTTPDigestAuth


def load_json(file_path):
    with open(file_path, encoding="utf8") as f:
        return json.load(f)


def push_data(url, auth, api_params, file_path):
    data = load_json(file_path)
    if data is not None:
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        size = sys.getsizeof(json.dumps(data))

        new_data = {}
        for key in api_params.keys():
            new_data[key] = api_params[key]
        new_data["data"] = data

        if auth is not None:
            rs = requests.post(url, data=json.dumps(new_data), headers=headers, auth=HTTPDigestAuth(auth['user'], auth['pass']))
        else:
            rs = requests.post(url, data=json.dumps(new_data), headers=headers)
        rs_time = rs.elapsed.total_seconds()
        print("post {size}byte in {time}s ".format(size=size, time=rs_time))


def run_app():
    config = load_json(os.path.abspath(".") + "/config.json")
    if config is not None:
        url = config["api_endpoint"]
        api_params = config["api_params"]
        data_files = config["first_data_files"]
        auth = None
        if "auth" in config["auth"]:
            auth = config["auth"]

        for file_path in data_files:
            push_data(url, auth, api_params, file_path)

        if 'schedule' in config:
            print("run schedule")
            limitation = config["schedule"]["limitation"]
            duration = config["schedule"]["duration"]
            data_files = config["schedule"]["data_files"]
            is_push_by_limitation = config["schedule"]["push_by_limitation"]

            file_index = 0
            while True:
                if is_push_by_limitation:
                    if file_index >= len(data_files):
                        file_index = 0

                for i, data_file in enumerate(data_files):
                    if is_push_by_limitation and i != file_index:
                        continue

                    if isinstance(data_file, list):
                        for file_path in data_file:
                            push_data(url, auth, api_params, file_path)
                    else:
                        push_data(url, auth, api_params, data_file)

                    if is_push_by_limitation:
                        file_index += 1
                        break

                if limitation != -1 and limitation > 1:
                    limitation -= 1
                    time.sleep(duration)
                else:
                    break

    else:
        print("Config error")


if __name__ == '__main__':
    run_app()
