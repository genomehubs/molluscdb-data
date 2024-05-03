#!/usr/bin/env python3

"""
This script uploads files to an S3 bucket using the AWS SDK for Python (Boto3).
"""

import argparse
import gzip
import os
import re
import sys
import tarfile

import boto3
import yaml
from tolkein import tolog

LOGGER = tolog.logger(__name__)


def substitute(string, variables):
    parts = re.split(r"{|}", string)
    for index, part in enumerate(parts):
        if index % 2 == 1:
            try:
                parts[index] = variables[part]
            except KeyError:
                LOGGER.error(f"Variable {part} not found.")
                LOGGER.error(f"{part} must be specified using --vars {part}=value")
                sys.exit(1)
    return "".join(parts)


def create_temp_file(filepath, s3path):
    if s3path.endswith(".tar.gz") and not filepath.endswith(".tar.gz"):
        temp_filepath = f"{filepath}.tar.gz"
        with tarfile.open(temp_filepath, "w:gz") as tar:
            tar.add(filepath, arcname=os.path.basename(filepath))
    elif s3path.endswith(".tar") and not filepath.endswith(".tar"):
        temp_filepath = f"{filepath}.tar"
        with tarfile.open(temp_filepath, "w") as tar:
            tar.add(filepath, arcname=os.path.basename(filepath))
    elif s3path.endswith(".gz") and not filepath.endswith(".gz"):
        temp_filepath = f"{filepath}.gz"
        with open(filepath, "rb") as f_in:
            with gzip.open(temp_filepath, "wb") as f_out:
                f_out.writelines(f_in)
    else:
        return filepath
    return temp_filepath


def upload_to_s3(s3, filepath, bucket, s3path, file_config):
    # Upload the file to S3 with the specified MIME type and content disposition
    with open(filepath, "rb") as data:
        s3.upload_fileobj(
            data,
            bucket,
            s3path,
            ExtraArgs={
                "ContentType": file_config["mime_type"],
                "ContentDisposition": file_config["content_disposition"],
                "ACL": "public-read",
            },
        )


def upload_files_to_s3(args, variables):
    # Load the config file
    with open(args.config, "r") as file:
        config = yaml.safe_load(file)

    # Create an S3 client
    s3 = boto3.client(
        "s3",
        endpoint_url=args.url,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    # For each file in the config
    for file_config in config["files"]:
        filename = substitute(file_config["filename"], variables)
        filepath = os.path.join(args.directory, filename)

        # Check if the file exists
        if not os.path.isfile(filepath) and not os.path.isdir(filepath):
            LOGGER.warning(f"File {filepath} does not exist.")
            continue
        s3path = substitute(file_config["s3path"], variables)
        tempfile = create_temp_file(filepath, s3path)
        upload_to_s3(s3, tempfile, args.bucket, s3path, file_config)
        if tempfile != filepath:
            os.remove(tempfile)


def parse_args():
    parser = argparse.ArgumentParser(description="Set the config filename")
    parser.add_argument("-c", "--config", help="Name of the config file", required=True)
    parser.add_argument(
        "-d", "--directory", help="Base directory for files to transfer", required=True
    )
    parser.add_argument(
        "--vars",
        nargs="*",
        help="Named variables used in the config file in the format key=value",
    )
    parser.add_argument("-b", "--bucket", help="s3 bucket name")
    parser.add_argument("-p", "--prefix", help="prefix for s3 object paths")
    parser.add_argument(
        "-u",
        "--url",
        help="s3 endpoint URL",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    try:
        variables = {var.split("=")[0]: var.split("=")[1] for var in args.vars}
    except IndexError:
        LOGGER.error("Variables must be in the format key=value")
        sys.exit(1)
    upload_files_to_s3(args, variables)
