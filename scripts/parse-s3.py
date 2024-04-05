#!/usr/bin/env python3

import argparse
import contextlib
import gzip
import json
import os
from collections import defaultdict

import boto3
import yaml
from botocore.exceptions import ClientError


def parse_args():
    """
    Parse command-line arguments for specifying an S3 bucket and prefix.

    Returns:
        argparse.Namespace: An object containing the parsed command-line arguments.
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default="molluscdb")
    parser.add_argument("--prefix", default="latest")
    parser.add_argument("--yaml", default="sources/assembly-data/files.types.yaml")
    parser.add_argument("--attribute", default="files")
    return parser.parse_args()


def get_directory_by_prefix(s3, bucket, prefix):
    """
    Get a directory with a specified prefix in an S3 bucket.

    Args:
        s3: The S3 client to use for retrieving the latest directory.
        bucket (str): The name of the S3 bucket.
        prefix (str): The prefix within the bucket to return.

    Returns:
        str: The path of the directory found with the specified prefix.
    """

    result = s3.list_objects_v2(Bucket=bucket, Prefix=f"{prefix}/", Delimiter="/")
    return sorted(result.get("CommonPrefixes", []), key=lambda x: x["Prefix"])[-1][
        "Prefix"
    ]


def list_subdirectories(s3, bucket, prefix):
    """
    List subdirectories within a specified prefix in an S3 bucket.

    Args:
        s3: The S3 client to use for listing subdirectories.
        bucket (str): The name of the S3 bucket.
        prefix (str): The prefix within the bucket to list subdirectories from.

    Returns:
        list: A list of subdirectory names found within the specified prefix.
    """

    names = []
    next_token = ""
    while next_token is not None:
        kwargs = {"Bucket": bucket, "Prefix": prefix, "Delimiter": "/"}
        if next_token:
            kwargs["ContinuationToken"] = next_token
        result = s3.list_objects_v2(**kwargs)
        for prefix in result.get("CommonPrefixes", []):
            subdir = prefix["Prefix"].split("/")[-2]
            names.append(subdir)
        next_token = result.get("NextContinuationToken")
    return names


def parse_yaml(yaml_file):
    """
    Parse a YAML file and load its contents.

    Args:
        yaml_file (str): The path to the YAML file to parse.

    Returns:
        dict: The parsed contents of the YAML file.

    Raises:
        Any errors that occur during file parsing.
    """
    if not os.path.exists(yaml_file):
        raise FileNotFoundError(f"YAML file not found: {yaml_file}")

    with open(yaml_file, "r") as f:
        return yaml.safe_load(f)


def extract_file_paths(yaml_data, attribute):
    """
    Extract file paths based on a specified attribute from a parsed YAML file.

    Args:
        yaml_data (dict): The parsed contents of a YAML file.
        attribute (str): The attribute key to extract file paths from.

    Returns:
        dict: A dictionary containing the extracted file paths, or an
              empty dictionary if the attribute or file paths are not found.
    """

    file_paths = {}
    with contextlib.suppress(KeyError):
        file_paths = yaml_data["attributes"][attribute]["file_paths"]
    return file_paths


def check_s3_file_exists(s3, bucket, key):
    """
    Check if a file exists in an S3 bucket.

    Args:
        s3 (boto3.client): S3 client object
        bucket (str): Name of the S3 bucket
        key (str): Key of the file to check

    Returns:
        bool: True if the file exists, False otherwise
    """
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def extract_field_meta(yaml_data, attribute):
    """
    Extract field metadata based on a specified attribute from a parsed YAML file.
    """
    separators = {}
    headers = {}
    with contextlib.suppress(KeyError):
        for key, value in yaml_data["attributes"].items():
            if key.startswith(attribute):
                separators[key] = value.get("separator", ",")
                headers[key] = value.get("header", key)
    return {"separators": separators, "headers": headers}


def get_metadata(yaml_file, attribute):
    yaml_data = parse_yaml(yaml_file)
    return {
        "file_paths": extract_file_paths(yaml_data, attribute),
        **extract_field_meta(yaml_data, attribute),
    }


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url="https://cog.sanger.ac.uk",
    )


def get_file_paths(meta):
    return meta.get("file_paths", {})


def extract_id(s3, bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    with gzip.GzipFile(fileobj=obj["Body"]) as gzipfile:
        data = json.load(gzipfile)
    return data["id"]


def get_run_value(analysis, s3, bucket, prefix):
    if analysis == "blobtoolkit":
        key = f"{prefix}blobdir/meta.json.gz"
        return extract_id(s3, bucket, key)
    else:
        return None


def get_entries(s3, bucket, latest_dir, subdirs, file_paths, attribute):
    entries = defaultdict(list)
    for subdir in subdirs:
        if subdir not in file_paths:
            continue
        if "all" in file_paths[subdir]:
            runs = ["all"]
        else:
            runs = list_subdirectories(s3, bucket, f"{latest_dir}{subdir}/")
        for key, value in file_paths[subdir].items():
            if not isinstance(value, dict):
                continue
            # print(key)
            name = value["name"]
            if runs:
                for run in runs:
                    if run == "all":
                        if not check_s3_file_exists(
                            s3, bucket, f"{latest_dir}{subdir}/{name}"
                        ):
                            continue
                    elif not check_s3_file_exists(
                        s3, bucket, f"{latest_dir}{subdir}/{run}/{name}"
                    ):
                        continue
                    entries[f"{attribute}.{subdir}.{run}"].append(key)
                    if run == "all":
                        if not entries[f"{attribute}.{subdir}.run"]:
                            entries[f"{attribute}.{subdir}.run"].append(
                                get_run_value(
                                    subdir, s3, bucket, f"{latest_dir}{subdir}/"
                                )
                            )
                    elif run not in entries[f"{attribute}.{subdir}.run"]:
                        entries[f"{attribute}.{subdir}.run"].append(run)
    return entries


def print_entries(entries, meta):
    print(
        {
            meta["headers"]
            .get(key, key): meta["separators"]
            .get(key, ",")
            .join([e for e in entry if e is not None])
            for key, entry in entries.items()
        }
    )


def main():

    args = parse_args()

    s3 = get_s3_client()

    bucket = args.bucket
    prefix = args.prefix
    yaml_file = args.yaml
    attribute = args.attribute

    latest_dir = get_directory_by_prefix(s3, bucket, prefix)
    subdirs = list_subdirectories(s3, bucket, latest_dir)

    meta = get_metadata(yaml_file, attribute)
    file_paths = get_file_paths(meta)

    entries = get_entries(s3, bucket, latest_dir, subdirs, file_paths, attribute)

    print_entries(entries, meta)


if __name__ == "__main__":
    main()
