#!/usr/bin/python3.5
# Alunos:
# João Luccas Damiani
# Henrique Longuinho
# Renan Willian Raphael de Souza
import os
import requests
import base64
import json
import time

KEY_COUNTER = {}


def define_schema(fields: list):
    print("Here's an example of a record from your file: {}".format(fields))
    schema = ["key", "s:sl", "s:fd", "s:td"]

    # for field in fields:
    #     field_identifier = input("FIELD: {} = ".format(field))
    #     schema.append(field_identifier)

    print("Your declared field identification for HBase: {}".format(schema))
    ok = input("Is it ok [y/n]?")
    if ok == "y":
        if "key" not in schema:
            print("ERROR: missing 'key' declaration!")
            return None
        return schema
    return None


def parse_insert_data(data: dict):
    # HBase accepts only base64 encoded values
    # must get dicts in the format: {"Row": [{"key": <key>, "Cell": [{"column": "cf:cq" , "$": <value>}]}]}
    for r in data["Row"]:
        r["key"] = base64.b64encode(str(r["key"]).encode("utf-8")).decode("utf-8")
        for v in r["Cell"]:
            v["column"] = base64.b64encode(v["column"].encode("utf-8")).decode("utf-8")
            v["$"] = base64.b64encode(str(v["$"]).encode("utf-8")).decode("utf-8")
    return json.dumps(data)


def check_key_count(key: str):
    global KEY_COUNTER
    if key not in KEY_COUNTER:
        KEY_COUNTER[key] = 1
    else:
        KEY_COUNTER[key] = KEY_COUNTER[key] + 1
    return KEY_COUNTER[key]


def assemble_row(fields: list, schema: list):
    # this assembler is specific to our use case and needs an input file of oredered events in time and key as first field!!!
    # also, only works for one column family!
    record = {"Row": [{"key": None, "Cell": []}]}
    counter = 1
    for f in range(0, len(schema)):
        if schema[f] == "key":
            key = fields[f]
            counter = check_key_count(key)
            record["Row"][0]["key"] = key
        else:
            # key for checking is
            record["Row"][0]["Cell"].append({"column": schema[f] + "_{}".format(counter), "$": fields[f]})
    # with this we append the ct column and update all fields
    cf_name = record["Row"][0]["Cell"][0]["column"][: record["Row"][0]["Cell"][0]["column"].find(":")]
    record["Row"][0]["Cell"].append({"column": cf_name + ":ct", "$": counter})
    return record


def insert_data_from_tsv_file(route_insert: str):
    print("Inserting data")
    filepath = input("Type the full file path: ")
    if os.path.isfile(filepath) is False:
        print("ERROR: file {} not found".format(filepath))
        return False
    delimiter = ";"  # input("Insert field delimiter for file (check file before!): ")
    first_line = True
    record_list = list()
    print("Starting reading file")
    with open(filepath, "r") as f:
        print("Reading lines")
        for line in f:
            fields = line.strip().split(delimiter)
            if first_line:
                print("\nTotal of {} fields on file. Please identify them in the order they appear.\n"
                      "Key field should be named 'key', the others should receive the 'cf:qualifier' notation name.\n"
                      "Please don't use quotes.".format(len(fields)))
                schema = None
                while schema is None:
                    schema = define_schema(fields)
                first_line = False
                start_time = time.time()
            # parsing the lines:
            record = assemble_row(fields, schema)
            # print("record to parse: {}".format(record))
            record_list.append(record["Row"][0])

            if len(record_list) > 50000:
                send_batch(record_list)
                record_list.clear()
        # print("records: {}".format(record_list))
    # record = ['row' = record_list]

    if len(record_list) != 0:
        send_batch(record_list)

    end_time = time.time()
    total_time = end_time - start_time
    print("Total time taken: %.2f" % total_time)


def send_batch(batch: list):
    print("Sending batch")
    record = {'Row': batch}
    # print("data: {}".format(record))
    record = parse_insert_data(record)
    # print("record decoded base64: {}".format(record))

    # Inserting in htable
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    # using the 'fakerow' placeholder you can inser multiple rows in the cell set
    insertion = requests.put(route_insert, headers=headers, data=record)
    if insertion.status_code != 200:
        print("Error while inserting record: {}".format(insertion.reason))


def get_data_by_rowkey(route_get: str):
    rowkey = input("Insert the rowkey of the data you need: ")
    headers = {"Accept": "application/json"}
    hdata = requests.get(route_get + rowkey, headers=headers)
    if hdata.status_code == 200:
        print(hdata.content)
    # implemente a funcao de parsing dos dados de leitura
    else:
        print("ERROR: status code = {} ; reason = {}".format(hdata.status_code, hdata.reason))


if __name__ == "__main__":
    print("================= HBASE REST TOOL =================")
    hmaster_ip = input("Type HMaster IP Address (default: 127.0.0.1): ")
    if len(hmaster_ip) == 0:
        hmaster_ip = "127.0.0.1"
    hmaster_port = input("Type HMaster RESTful API port (default: 8080): ")
    if len(hmaster_port) == 0:
        hmaster_port = 8080
    hmaster_port = int(hmaster_port)
    url = "http://{}:{}".format(hmaster_ip, hmaster_port)
    print("Your configured HBase REST address: {}".format(url))
    actions = ["i", "q", "g"]
    while True:
        print("Choose your action:\n \
        i - Insert data from TSV file\n \
        g - Get data from a Hbase table \n \
        q - Exit")
        action = input("action: ")
        if action in actions:
            if action == "q":
                exit(0)
            elif action == "i":
                htable = "ex2:employee" # input("Insert Hbase table name in format <namespace>:<table>: ")
                print("NOTE: file with headers are currently not supported. Also, will only work if keys is the first "
                      "column!")
                route_insert = url + "/{}".format(htable) + "/fakerow"
                insert_data_from_tsv_file(route_insert)
            elif action == "g":
                htable = input("Insert Hbase table name in format <namespace>:<table>: ")
                print("NOTE: only rowkey queries are allowed so far")
                route_get = url + "/{}/".format(htable)
                get_data_by_rowkey(route_get)
            else:
                continue
