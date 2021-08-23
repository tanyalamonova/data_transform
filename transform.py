import os
import json
from typing import Dict
from typing_extensions import OrderedDict
import xml.etree.ElementTree as ET
import csv
import warnings


def load_xml(source):
    """
    Load data from .xml file.
    """
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
    return data


def load_json(source):
    """
    Load data from .json file.
    """
    data = {}
    with open(source, "r") as f:
        fields = json.load(f)["fields"]
        for field in fields:
            for key, value in field.items():
                if key not in data.keys():
                    data[key] = []
                data[key].append(value)
    return data


def load_csv(source):
    """
    Load data from .csv file.
    """
    data = {}
    with open(source, "r") as source_csv:
        csv_reader = csv.reader(source_csv, delimiter=",")
        A = list(zip(*csv_reader))
        for column in A:
            data[column[0]] = list(column[1:])
    return data


def load_by_extension(source):
    """
    Load data from file depending on its format.
    """
    extension = source.rsplit(".")[-1].lower()
    print(f"Reading data from {source}")
    if extension == "json":
        return load_json(source)
    if extension == "xml":
        return load_xml(source)
    if extension == "csv":
        return load_csv(source)
    print(
        "This file type is not supported, please choose among these \
            [.json, .csv, .xml] and try again"
    )
    return None


def load_all(source_list):
    """
    Load data from all files at once.
    Each dataset is stored in a dictionary as follows:
    {
        "D1": [1, 1, 1, 1],
        ...,
        "Mn": [1, 1, 1, 1]
    }
    """
    complex_data = []
    for source in source_list:
        new_data = load_by_extension(source)
        new_data = format_values(new_data)
        if new_data is not None:
            complex_data.append(new_data)
        else:
            print(f"Could not load data from this file: {source}")

    return complex_data


def save_to_tsv(data, filename):
    """
    Save result as .tsv file.
    """
    with open(filename, "w") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerows(data)
    print(f"Saved to {filename}")


def sort_keys(data):
    """
    Sort column names as follows: D1, .., Dn, M1, .., Mn.
    """
    return sorted(data, key=lambda s: tuple([str(s[0]), int(s[1:])]))


def format_values(data):
    """
    Convert all values in "M" columns to int().
    """
    formatted_data = {}
    for column, values in data.items():
        formatted_data[column] = []
        if column.startswith("M"):
            formatted_data[column] = list(map(int, values))
        else:
            formatted_data[column] = values
    return formatted_data


def compose_rows(data, order_by="D1"):
    """
    Compose result dataset based on current dictionary,
    sort it by selected column.
    """

    # create a new dataset starting with column names
    rows = []
    rows.append(list(data.keys()))

    # find selected column's index in order to sort values
    order_index = rows[0].index(order_by)

    # convert dictionary values (column values) to list of tuples (row values)
    set_of_values = list(zip(*data.values()))

    # sort rows by selected column
    sorted_set = sorted(set_of_values, key=lambda v: v[order_index])

    # add sorted rows to the result dataset
    rows.extend([list(new_row) for new_row in sorted_set])

    return rows


def group_values(data):
    splitted_rows = []

    # get number of key and value columns
    index_count = len([column for column in data[0] if "D" in column])
    value_count = len(data[0]) - index_count

    # split row into keys and values
    for row in data[1:]:
        splitted_rows.append((tuple(row[:index_count]), tuple(row[index_count:])))

    # get list of unique index combinations and sort it
    unique_keys = list(set(list(zip(*splitted_rows))[0]))
    sorted_keys = sorted(unique_keys, key=lambda k: tuple([column for column in k]))

    # create ordered dictionary where keys are unique index combinations
    grouped_data = OrderedDict()
    for key in sorted_keys:
        grouped_data[key] = [0] * value_count

    # fill dictionary with values
    for this_key, values in splitted_rows:
        for index, value in enumerate(values):
            grouped_data[this_key][index] += value

    # create result list
    # fill it with column names and grouped data
    result = [data[0]]
    for i, v in grouped_data.items():
        result.append(list(i) + list(v))

    return result


if __name__ == "__main__":

    # set source data filenames
    XML_PATH = "xml_data.xml"
    JSON_PATH = "json_data.json"
    CSV_PATH_1 = "csv_data_1.csv"
    CSV_PATH_2 = "csv_data_2.csv"

    SOURCES = [CSV_PATH_1, CSV_PATH_2, XML_PATH, JSON_PATH]

    # set result data filenames
    BASIC_RESULT_PATH = "basic_results.tsv"
    ADVANCED_RESULT_PATH = "advanced_results.tsv"

    # load all datasets
    complex_data = load_all(SOURCES)

    # check if all data has been loaded
    if len(complex_data) != len(SOURCES):
        print(
            f"Not all data has been loaded, working with \
                {len(complex_data)} datasets instead of {len(SOURCES)}"
        )

    # get list of result column names based on csv_1 dataset
    all_keys = list(set(complex_data[0].keys()))

    # sort these values
    sorted_keys = sort_keys(all_keys)
    print(f"List of columns to be used: {sorted_keys}")

    # create a new dictionary and fill it with values from all datasets
    # list of keys equals column names sorted by name
    result_dataset = {key: [] for key in sorted_keys}

    print("Collecting all datasets")
    for index, dataset in enumerate(complex_data):

        row_count = len(list(dataset.values())[0])
        for column in sorted_keys:

            if column in dataset.keys():
                result_dataset[column].extend(dataset[column])

            # if the dataset doesn't have this column,
            # print a warning and fill it with zeros or an empty string
            # depending on the column name
            else:
                print(
                    f"This column not found: <{column}> in dataset with index <{index}>"
                )
                if column.startswith("D"):
                    print("Filling this column with empty strings")
                    result_dataset[column].extend([""] * row_count)
                else:
                    print("Filling this column with zeros")
                    result_dataset[column].extend([0] * row_count)

    # move the dataset to an ordered dictionary to fix the order of sorted columns
    sorted_result = OrderedDict()
    for column in sorted_keys:
        sorted_result[column] = result_dataset[column]

    # compose a basic result data as list of rows and save it as .tsv
    basic_result = compose_rows(sorted_result)
    save_to_tsv(basic_result, BASIC_RESULT_PATH)

    # group values by 'D' columns' keys and save the result as .tsv
    advanced_result = group_values(basic_result)
    save_to_tsv(advanced_result, ADVANCED_RESULT_PATH)
