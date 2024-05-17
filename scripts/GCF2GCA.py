#!/usr/bin/env python3

import argparse

from genomehubs import utils as gh_utils


def parse_args():
    parser = argparse.ArgumentParser(description="GCF to GCA converter")
    bucket: str = "molluscdb"
    prefix: str = "2024-05"
    url: str = "https://cog.sanger.ac.uk"

    parser.add_argument("--gcf", help="GCF accession number", required=True)
    parser.add_argument("--gca", help="GCA accession number", required=True)
    parser.add_argument("--bucket", default=bucket)
    parser.add_argument("--prefix", default=prefix)
    parser.add_argument(
        "-u",
        "--url",
        default=url,
        help="s3 endpoint URL",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    gcf_accession = args.gcf
    gca_accession = args.gca
    s3 = gh_utils.get_s3_client(args.url)
    prefix = args.prefix
    bucket = args.bucket
    # Find all files in the bucket under prefix/gcf
    gcf_files = gh_utils.list_files(
        s3, bucket, f"{prefix}/{gcf_accession}/", recursive=True
    )

    # Move files to prefix/gca
    for file in gcf_files:
        new_key = file.replace(gcf_accession, gca_accession)
        s3.copy_object(
            Bucket=bucket, CopySource={"Bucket": bucket, "Key": file}, Key=new_key
        )
        s3.delete_object(Bucket=bucket, Key=file)
    # Rest of your code goes here


if __name__ == "__main__":
    main()
