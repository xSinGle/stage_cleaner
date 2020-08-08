# -*- coding: utf-8 -*-
__author__ = 'SinGle'
__date__ = '2020/6/22 10:23'


import time
from datetime import datetime

import requests
from requests import RequestException

from Settings import SUPER_USER, SCHEMA, PORT, LIST_URL, DELETE_URL, RENAME_URL, DEFAULT_STAGING_PATH, USER_PATH, INTERVAL
import logging


class ApiHandler:
    def __init__(self, schema):
        self.__schema = schema
        self.__host = self.schema_switch()

    def schema_switch(self):
        logging.info("Checking for ACTIVE NAMENODE HOST...")
        for host in SCHEMA[self.__schema]:
            try:
                code = requests.get(LIST_URL.format(host=host, port=PORT, path='', super_user=SUPER_USER)).status_code
                if code == 200:
                    logging.info("ACTIVE NAMENODE HOST SELECTED: {host}".format(host=host))
                    return host
                continue
            except RequestException as e:
                raise RequestException("SCHEMA CHECK FAILED! {schema}, Exception: {e}".format(schema=self.__schema, e=e))

    def list(self, path):
        try:
            logging.info("Checking directories under path: {path}".format(path=path))
            return requests.get(LIST_URL.format(host=self.__host, port=PORT, path=path, super_user=SUPER_USER)).json()
        except RequestException as e:
            raise RequestException("LIST PATH FAILED! {path}, Exception: {e}".format(path=path, e=e))

    def delete(self, user, path, recursive='true'):
        try:
            logging.info("Deleting path: {path}".format(path="hdfs://{schema}{default}/{path}".format(
                schema=self.__schema, default=DEFAULT_STAGING_PATH.format(user=user), path=path)))
            url = DELETE_URL.format(host=self.__host, port=PORT, path=DEFAULT_STAGING_PATH.format(user=user) + '/' + path,
                                    super_user=SUPER_USER, recursive=recursive)
            return requests.delete(url=url).json()["boolean"]
        except RequestException as e:
            raise RequestException("DELETE PATH FAILED! {path}, Exception: {e}".format(path=path, e=e))

    def rename(self, user, old_path, trash_path):
        try:
            logging.info("Moving path to Trash: {old_path}".format(old_path=DEFAULT_STAGING_PATH.format(user=user) + '/' + old_path))
            url = RENAME_URL.format(host=self.__host, port=PORT, old_path=DEFAULT_STAGING_PATH.format(user=user) + '/' + old_path,
                                    super_user=SUPER_USER, new_path=trash_path)
            logging.info(url)
            result = requests.put(url=url)
            logging.debug("Requests result: {result}, Rename result: {json}".format(result=result, json=result.json))
            return result.json()["boolean"]
        except RequestException as e:
            raise RequestException("MOVING PATH to TRASH FAILED! {old_path}, Exception: {e}".format(old_path=old_path, e=e))

    def get_outdated_paths(self, interval=INTERVAL):
        logging.info("INTERVAL ACCEPTED: {interval}".format(interval=interval))
        latest_timestamp = (int(time.mktime(datetime.today().timetuple())) - int(interval) * 24 * 60 * 60) * 1000
        logging.info("TIMESTAMP GENERATED: {timestamp}".format(timestamp=latest_timestamp))

        for user_info in self.list(path=USER_PATH)["FileStatuses"]["FileStatus"]:
            try:
                for path in self.list(path=DEFAULT_STAGING_PATH.format(user=user_info["pathSuffix"]))["FileStatuses"]["FileStatus"]:
                    if int(path["modificationTime"]) < latest_timestamp:
                        yield path, user_info["pathSuffix"]
            except KeyError as e:
                logging.error(e)
