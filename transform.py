import os
import json
from typing import Dict
from typing_extensions import OrderedDict
import xml.etree.ElementTree as ET
import csv
import collections
import pprint


def load_xml(source):
    tree = ET.parse(source)
    root = tree.getroot()
    data = {}
    for child in root.findall("./objects/"):
        try:
            name = child.get("name")
            data[name] = []
            values = child.findall("value")
            for v in values:
                data[name].append(v.text)
        except Exception as ex:
            print(ex)
    print(f"loaded {len(data)} elements")
    return data


def load_json(source):
    data = {}
    with open(source, "r") as f:
        fields = json.load(f)["fields"]
        for field in fields:
            for key, value in field.items():
                if key not in data.keys():
                    data[key] = []
                data[key].append(value)
    print(f"loaded {len(data)} elements")
    # print(data)
    return data


def load_csv(source):
    data = {}
    with open(source, "r") as source_csv:
        csv_reader = csv.reader(source_csv, delimiter=",")
        A = list(zip(*csv_reader))
        for column in A:
            data[column[0]] = list(column[1:])
    print(f"loaded {len(data)} elements")
    return data


def load_by_extension(source):
    extension = source.rsplit(".")[-1].lower()
    print(f"filename: {source}")
    print(f"ext = {extension}")
    if extension == "json":
        return load_json(source)
    if extension == "xml":
        return load_xml(source)
    if extension == "csv":
        return load_csv(source)
    print(
        "This file type is not supported, please choose among these [.json, .csv, .xml] and try again"
    )
    return None


def load_all(source_list):
    complex_data = []
    for source in source_list:
        new_data = load_by_extension(source)
        if new_data is not None:
            complex_data.append(new_data)
        else:
            print(f"Could not load data from this file: {source}")
    print(complex_data)
    return complex_data


def sort_keys(data):
    return sorted(data, key=lambda s: tuple([str(s[0]), int(s[1:])]))


def format_data(data):
    return [int(element) for element in data]


if __name__ == "__main__":
    XML_PATH = "xml_data.xml"
    JSON_PATH = "json_data.json"
    CSV_PATH_1 = "csv_data_1.csv"
    CSV_PATH_2 = "csv_data_2.csv"
    BASIC_RESULT_PATH = 'basic_results.tsv'

    SOURCES = [XML_PATH, JSON_PATH, CSV_PATH_1, CSV_PATH_2]

    complex_data = load_all(SOURCES)
    if len(complex_data) != len(SOURCES):
        print(
            f"Not all data has been loaded, working with {len(complex_data)} datasets instead of {len(SOURCES)}"
        )

    pp = pprint.PrettyPrinter(indent=4)
    all_keys = []
    for data in complex_data:
        print("\n" * 4)
        pp.pprint(data)
        all_keys.extend(data.keys())
    all_keys = list(set(all_keys))
    print(f"unsorted: {all_keys}")
    sorted_keys = sort_keys(all_keys)
    print(f"  sorted: {sorted_keys}")
    result_dataset = {key: [] for key in sorted_keys}
    for dataset in complex_data:
        row_count = len(list(dataset.values())[0])
        print(f"this dataset contains {row_count} rows")
        for column in sorted_keys:
            if column in dataset.keys():
                result_dataset[column].extend(dataset[column])
            else:
                if column.startswith("D"):
                    result_dataset[column].extend([""] * row_count)
                else:
                    result_dataset[column].extend(["0"] * row_count)
    sorted_result = OrderedDict()
    for column in sorted_keys:
        sorted_result[column] = result_dataset[column]
    pp.pprint(sorted_result)

    writer = csv.writer(BASIC_RESULT_PATH, delimiter=',')
    for line in data:
        writer.writerow(line)

