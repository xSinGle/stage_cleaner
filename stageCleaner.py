# -*- coding: utf-8 -*-
__author__ = 'SinGle'
__date__ = '2020/6/22 10:22'

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from HadoopApi import ApiHandler
from Settings import DEFAULT_STAGING_PATH, LOG_FORMAT, TRASH_PATH, SCHEMA, MKDIR_RECUR

import logging
import subprocess


def clean(api_handler, schema, path):
    success = api_handler.delete(path=path)
    final_path = "hdfs://{schema}{default}/{path}".format(schema=schema, default=DEFAULT_STAGING_PATH, path=path)
    return "[DELETE SUCCESS] {final_path}".format(final_path=final_path) if success \
        else "[DELETE FAILED] {final_path}".format(final_path=final_path)


def move_to_trash(user, api_handler, schema, old_path, trash_path):
    success = api_handler.rename(user=user, old_path=old_path, trash_path=trash_path)
    old_final = "hdfs://{schema}{default}/{path}".format(schema=schema, default=DEFAULT_STAGING_PATH.format(user=user), path=old_path)
    trash_final = "hdfs://{schema}{trash_path}".format(schema=schema, trash_path=trash_path)
    return "[RENAME SUCCESS] FROM: {old_final} TO: {trash_final}".format(old_final=old_final, trash_final=trash_final) \
        if success else "[RENAME FAILED] FROM: {old_final} TO: {trash_final}".format(old_final=old_final, trash_final=trash_final)


def directory_check():
    for schema, _ in SCHEMA.items():
        logging.info("Checking Trash Paths of schema: {schema}".format(schema=schema))
        api_handler = ApiHandler(schema=schema)
        user_paths = ["/user/" + item["pathSuffix"] for item in api_handler.list(path="/user")["FileStatuses"]["FileStatus"]]
        logging.debug("User Count: [{length}]\nUser Paths: [{paths}]".format(length=len(user_paths), paths=user_paths))
        for user_path in user_paths:
            logging.info("Checking user path: [{path}]".format(path=user_path))
            detail_paths = [detail["pathSuffix"] for detail in api_handler.list(path=user_path)["FileStatuses"]["FileStatus"]]
            if "Trash" not in detail_paths:
                mkdir_cmd = MKDIR_RECUR.format(schema=schema, user=user_path.split('/')[-1])
                logging.debug("MKDIR COMMAND: {}".format(mkdir_cmd))
                code, result = subprocess.getstatusoutput(cmd=mkdir_cmd)
                logging.info("Mkdir Result: [{}], Code: [{}]".format(result, code))


if __name__ == '__main__':
    logging.basicConfig(format=LOG_FORMAT, filemode='a', filename='stage_clean.log', level=logging.DEBUG)

    if len(sys.argv) != 3:
        print("""[USAGE]: python3 stageCleaner.py [schema][interval]
        [EXAMPLE]: python3 stageCleaner.py data-batch-hdfs 10""")
        exit(-1)

    directory_check()

    handler = ApiHandler(schema=sys.argv[1])

    executor = ThreadPoolExecutor(max_workers=32)
    futures = {}

    for path_info, user in handler.get_outdated_paths(sys.argv[2]):
        future = executor.submit(move_to_trash, user, handler, sys.argv[1],
                                 path_info['pathSuffix'], TRASH_PATH.format(user=user))
        futures[future] = path_info['pathSuffix']

    for future in as_completed(futures):
        result = future.result()
        logging.info(result)