#!/usr/bin/env python3

import argparse
import sys
from collections import defaultdict
from functools import reduce
from typing import Any, Optional

import boto3
from genomehubs import utils as gh_utils


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for specifying an S3 bucket and prefix.

    Returns:
        argparse.Namespace: An object containing the parsed command-line arguments.
    """

    parser: argparse.ArgumentParser = argparse.ArgumentParser()
    bucket: str = "molluscdb"
    prefix: str = "latest"
    url: str = "https://cog.sanger.ac.uk"
    config: str = "tests/integration_tests/sources/assembly-features"
    attribute: str = "files"

    parser.add_argument("--bucket", default=bucket)
    parser.add_argument("--prefix", default=prefix)
    parser.add_argument(
        "-u",
        "--url",
        default=url,
        help="s3 endpoint URL",
    )
    parser.add_argument(
        "-c",
        "--config",
        default=config,
        help="path to config directory",
    )
    parser.add_argument("--attribute", default=attribute)
    return parser.parse_args()


def extract_id(s3: boto3.resources.base.ServiceResource, bucket: str, key: str) -> str:
    """
    Extract the ID from the JSON data loaded from the specified S3 bucket and key.

    Args:
        s3 (boto3.resources.base.ServiceResource): The S3 resource object.
        bucket (str): The name of the S3 bucket.
        key (str): The key (file path) within the S3 bucket.

    Returns:
        str: The ID extracted from the JSON data.
    """
    data: dict = gh_utils.load_json_from_s3(s3, bucket, key)
    return data["id"]


def set_taxon_id(
    s3: boto3.resources.base.ServiceResource, bucket: str, prefix: str, entries: dict
):
    """
    Extract the taxon ID from the assembly information JSON data loaded from the
    specified S3 bucket and prefix.

    Args:
        s3 (boto3.resources.base.ServiceResource): The S3 resource object.
        bucket (str): The name of the S3 bucket.
        prefix (str): The prefix within the S3 bucket.
        entries (dict): A dictionary to store the extracted taxon ID.

    Returns:
        None
    """
    key = f"{prefix}assembly_info.json"
    data = gh_utils.load_json_from_s3(s3, bucket, key)
    entries["taxon_id"] = [data["taxon_id"]]


def get_run_value(
    analysis: str, s3: boto3.resources.base.ServiceResource, bucket: str, prefix: str
) -> Optional[str]:
    """
    Extract the ID from the JSON data loaded from the specified S3 bucket and key.

    Args:
        analysis (str): The analysis type.
        s3 (boto3.resources.base.ServiceResource): The S3 resource object.
        bucket (str): The name of the S3 bucket.
        prefix (str): The prefix within the S3 bucket.

    Returns:
        str: The ID extracted from the JSON data, or None if the analysis type is not
            "blobtoolkit".
    """
    if analysis == "blobtoolkit":
        key = f"{prefix}blobdir/meta.json.gz"
        return extract_id(s3, bucket, key)
    else:
        return None


def process_run_entry(
    s3: boto3.resources.base.ServiceResource,
    bucket: str,
    latest_dir: str,
    subdir: str,
    run: str,
    key: str,
    name: str,
    attribute: str,
    entries: dict[str, list],
) -> dict[str, list]:
    """
    Process a run entry for a given analysis subdirectory.

    Args:
        s3: The S3 resource object
        bucket: The name of the S3 bucket
        latest_dir: The base directory prefix
        subdir: The analysis subdirectory
        run: The run name or 'all'
        key: The S3 object key
        name: The expected filename
        attribute: The attribute name to update in entries
        entries: The dictionary to update

    Returns:
        The updated entries dictionary
    """

    if run == "all":
        if not gh_utils.check_s3_file_exists(
            s3, bucket, f"{latest_dir}{subdir}/{name}"
        ):
            return entries
    elif not gh_utils.check_s3_file_exists(
        s3, bucket, f"{latest_dir}{subdir}/{run}/{name}"
    ):
        return entries

    entries[f"{attribute}.{subdir}.{run}"].append(key)

    if run == "all":
        if not entries[f"{attribute}.{subdir}.run"]:
            entries[f"{attribute}.{subdir}.run"].append(
                get_run_value(subdir, s3, bucket, f"{latest_dir}{subdir}/")
            )
    elif run not in entries[f"{attribute}.{subdir}.run"]:
        entries[f"{attribute}.{subdir}.run"].append(run)

    return entries


def set_assembly_id(latest_dir: str, entries: dict[str, list]) -> dict[str, list]:
    """Set the assembly ID in the entries dict.

    Args:
        latest_dir: The base directory prefix
        entries: The dictionary to update

    Returns:
        The updated entries dictionary
    """

    entries["assembly_id"] = [latest_dir.split("/")[1]]
    return entries


def get_entries(
    s3: Any,
    bucket: str,
    assembly_dir: str,
    subdirs: list[str],
    file_paths: dict[str, Any],
    attribute: str,
) -> dict[str, list[Any]]:
    """Get entries for the given assembly directory.

    Args:
        s3: S3 client object
        bucket: S3 bucket name
        assembly_dir: Base assembly directory
        subdirs: List of analysis subdirectories
        file_paths: Dict mapping subdir to file paths
        attribute: Attribute name to update

    Returns:
        Dict mapping attributes to values
    """

    entries: dict[str, list[Any]] = defaultdict(list)
    set_assembly_id(assembly_dir, entries)
    set_taxon_id(s3, bucket, assembly_dir, entries)
    for subdir in subdirs:
        if subdir not in file_paths:
            continue
        if "all" in file_paths[subdir]:
            runs = ["all"]
        else:
            runs = gh_utils.list_subdirectories(s3, bucket, f"{assembly_dir}{subdir}/")
        for key, value in file_paths[subdir].items():
            if not isinstance(value, dict):
                continue
            name = value["name"]
            if runs:
                for run in runs:
                    process_run_entry(
                        s3,
                        bucket,
                        assembly_dir,
                        subdir,
                        run,
                        key,
                        name,
                        attribute,
                        entries,
                    )
        if f"{attribute}.{subdir}.run" in entries:
            entries[attribute].append(subdir)
    return entries


def extract_value_from_string(string: str, substring1: str, substring2: str) -> str:
    """
    Extract the value from a string.

    Args:
        string (str): The input string.

    Returns:
        str: The extracted value.
    """
    start_index = string.find(substring1) + len(substring1)
    end_index = string.find(substring2)
    return string[start_index:end_index]


def replace_substrings(template: dict, assembly_info: dict) -> dict:
    """
    Replace substrings in the template with corresponding values from assembly_info.

    Args:
        template (dict): The template dictionary.
        assembly_info (dict): The assembly information dictionary.

    Returns:
        dict: The modified template dictionary.
    """
    for key, value in template.items():
        if isinstance(value, dict):
            template[key] = replace_substrings(value, assembly_info)
        elif isinstance(value, str):
            for info_key, info_value in assembly_info.items():
                value = value.replace(f"{{{info_key}}}", str(info_value))
            template[key] = value
        elif isinstance(value, list):
            for index, item in enumerate(value):
                if isinstance(item, dict):
                    value[index] = replace_substrings(item, assembly_info)
                elif isinstance(item, str):
                    for info_key, info_value in assembly_info.items():
                        item = item.replace(f"{{{info_key}}}", str(info_value))
                    value[index] = item
            template[key] = value
    return template


def load_template(args, assembly_info, template_name):
    try:
        template_file = f"{args.config}/TEMPLATE_{template_name}.yaml"
        template = gh_utils.load_yaml(template_file)
        replaced_template = replace_substrings(template, assembly_info)
        meta = gh_utils.get_metadata(replaced_template, template_file)
        headers = gh_utils.set_headers(replaced_template)
        parse_fns = gh_utils.get_parse_functions(replaced_template)
    except FileNotFoundError:
        print(f"Template file {template_file} not found. Exiting.")
        sys.exit(1)
    return replaced_template, meta, headers, parse_fns


def create_file_pair(yaml_template, meta, headers, rows):
    yaml_file = meta["file_name"].replace(".tsv", ".types.yaml").replace(".gz", "")
    gh_utils.write_yaml(yaml_template, yaml_file)
    gh_utils.print_to_tsv(headers, rows, meta)


def process_window_stats(
    s3: boto3.resources.base.ServiceResource, args, prefix: str, assembly_info: dict
):
    files = gh_utils.list_files(s3, args.bucket, f"{prefix}stats/")
    sequence_template, meta, headers, parse_fns = load_template(
        args, assembly_info, "window_stats"
    )
    span = 0
    file_names = {}

    for file in files:
        if "window_stats" in file:
            if window_size := extract_value_from_string(file, "window_stats.", ".tsv"):
                file_names[window_size] = file
            else:
                feature_type = ["chromosome", "toplevel", "sequence"]
                rows = parse_tsv(s3, args, parse_fns, file, feature_type)
                span = reduce(lambda x, y: x + int(y["length"]), rows, 0)

                create_file_pair(
                    replace_substrings(sequence_template, {"span": span}),
                    meta,
                    headers,
                    rows,
                )

    for window_size, file in file_names.items():
        feature_type = [f"window-{window_size}", "window"]
        window_template, meta, headers, parse_fns = load_template(
            args, {**assembly_info, "window": window_size}, "window_stats.WINDOW"
        )
        rows = parse_tsv(s3, args, parse_fns, file, feature_type)
        create_file_pair(window_template, meta, headers, rows)


def parse_tsv(s3, args, parse_fns, file, feature_type=None, skip=0):
    rows = []
    lines = gh_utils.load_tsv_from_s3(s3, args.bucket, file, skip=skip)
    for line in lines:
        row = gh_utils.parse_report_values(parse_fns, line)
        if feature_type is not None:
            row["feature_type"] = feature_type
        rows.append(row)
    return rows


def process_busco(
    s3: boto3.resources.base.ServiceResource, args, prefix: str, assembly_info: dict
):
    lineages = gh_utils.list_subdirectories(s3, args.bucket, f"{prefix}busco/")
    for lineage in lineages:
        template, meta, headers, parse_fns = load_template(
            args, {**assembly_info, "lineage": lineage}, "busco"
        )
        feature_type = [f"{lineage}-busco-gene", "busco-gene", "gene"]

        file = f"{prefix}busco/{lineage}/full_table.tsv"
        rows = [
            row
            for row in parse_tsv(s3, args, parse_fns, file, feature_type, skip=2)
            if row["status"] != "Missing"
        ]
        create_file_pair(template, meta, headers, rows)


def main():
    """
    Main entry point for the script. Parses command line arguments, loads
    configuration, retrieves metadata, and processes the data for each assembly
    directory.

    Args:
        args (Namespace): Parsed command line arguments.

    Returns:
        None
    """

    args = parse_args()

    s3 = gh_utils.get_s3_client(args.url)

    # config = gh_utils.load_yaml(args.config)
    # meta = gh_utils.get_metadata(config, args.config, args.attribute)
    # file_paths = meta.get("file_paths", {})

    assembly_dirs = gh_utils.get_directories_by_prefix(s3, args.bucket, args.prefix)
    assembly_dirs = ["latest/GCA_964016885.1/", "latest/GCA_964016985.1/"]
    # rows = []

    for assembly_dir in assembly_dirs:
        subdirs = gh_utils.list_subdirectories(s3, args.bucket, assembly_dir)
        if "stats" not in subdirs:
            continue

        assembly_info = gh_utils.load_json_from_s3(
            s3, args.bucket, f"{assembly_dir}assembly_info.json"
        )
        assembly_info.update({"assembly_id": assembly_dir.split("/")[-2]})

        process_window_stats(s3, args, assembly_dir, assembly_info)

        process_busco(s3, args, assembly_dir, assembly_info)

        # entries = get_entries(
        #     s3, args.bucket, assembly_dir, subdirs, file_paths, args.attribute
        # )
        # rows.append(entries)

    # headers = gh_utils.set_headers(config)
    # gh_utils.print_to_tsv(headers, rows, meta)


if __name__ == "__main__":
    main()
