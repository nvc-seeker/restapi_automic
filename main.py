import os
import sys
import logging
import json
import csv
import requests
import time

from requests.auth import HTTPDigestAuth
from datetime import datetime

if not os.path.exists('./logs'):
    os.makedirs('./logs')

now = datetime.now()

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.DEBUG)
stdout_handler.setFormatter(formatter)
file_handler = logging.FileHandler('./logs/{name}.log'.format(name=now.strftime('%Y%m%d%H%M%S')))
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(stdout_handler)


def read_csv(file_path, delimiter):
    with open(file_path, encoding='utf8') as f:
        data_arr = []
        if delimiter is None:
            csv_reader = csv.DictReader(f)
        else:
            csv_reader = csv.DictReader(f, delimiter=delimiter)

        for rows in csv_reader:
            data = {}
            for key in rows.keys():
                data[key] = rows[key]
            data_arr.append(data)

        return data_arr


def read_json(file_path):
    with open(file_path, encoding='utf8') as f:
        return json.load(f)


def remake_payload(payload, data):
    if isinstance(payload, dict):
        new_payload = {}
        for key in payload.keys():
            if '${data_files}' == payload[key]:
                new_payload[key] = data
            else:
                if isinstance(payload[key], dict):
                    new_payload[key] = remake_payload(payload[key], data)
                else:
                    new_payload[key] = payload[key]
        return new_payload
    elif '${data_files}' == payload:
        return data


def push_data(api, data):
    url = api['endpoint']
    request_method = api['request_method']
    auth = None
    if 'auth' in api:
        auth = HTTPDigestAuth(api['auth']['user'], api['auth']['pass'])

    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

    size = sys.getsizeof(json.dumps(data))

    rs = None
    if 'post' == request_method:
        rs = requests.post(url, data=json.dumps(data), headers=headers, auth=auth)
    elif 'put' == request_method:
        rs = requests.put(url, data=json.dumps(data), headers=headers, auth=auth)
    elif 'patch' == request_method:
        rs = requests.patch(url, data=json.dumps(data), headers=headers, auth=auth)
    elif 'delete' == request_method:
        rs = requests.delete(url, data=json.dumps(data), headers=headers, auth=auth)

    if rs is not None:
        rs_time = rs.elapsed.total_seconds()
        logger.info('Push {size}byte in {time}s '.format(size=size, time=rs_time))


def create_data(payload, csv_delimiter, file_paths):
    if isinstance(file_paths, list):
        data_arr = []
        for file_path in file_paths:
            extension = os.path.splitext(file_path)[1][1:]
            if 'csv' == extension:
                data_arr.append(read_csv(file_path, csv_delimiter))
            elif 'json':
                data_arr.append(read_json(file_path))
        return remake_payload(payload, data_arr)
    else:
        return remake_payload(payload, read_json(file_paths))


def validate_config(config):
    request_method = config['api']['request_method']
    assert request_method == 'post' or request_method == 'put' or request_method == 'patch' or request_method == 'delete'


def run_app():
    config = read_json(os.path.abspath('.') + '/config.json')
    validate_config(config)
    api = config['api']
    logger.info('Api: \n' + json.dumps(api, indent=4, separators=('. ', ' = ')))

    data_files = config['data_files']
    schedule = config['schedule']
    url = api['endpoint']
    payload = api['payload']

    logger.info('Run schedule')

    limitation = schedule['limitation']
    duration = schedule['duration']
    data_files = data_files['files']
    csv_delimiter = None
    if 'csv_delimiter' in config:
        csv_delimiter = data_files['csv_delimiter']

    file_index = -1
    while len(data_files) > 0:
        file_index += 1
        if file_index >= len(data_files):
            file_index = 0

        data = create_data(payload, csv_delimiter, data_files[file_index])
        if data is not None:
            push_data(api, data)

        if limitation != -1 and limitation > 1:
            limitation -= 1
            time.sleep(duration)
        else:
            break


if __name__ == '__main__':
    try:
        run_app()
    except Exception as e:
        logger.exception(e)
