#!/usr/bin/env python

import argparse
import contextlib
import csv

import matplotlib.cm as cm
import matplotlib.lines as mlines
import matplotlib.pyplot as plt


def get_start_end(row):
    start = int(row[3])
    end = int(row[4])
    return (start, end) if start < end else (end, start)


def read_ids(file):
    ids = {}
    with open(file, "r") as f:
        reader = csv.reader(f, delimiter="\t")
        for row in reader:
            if row[0].startswith("#"):
                continue
            with contextlib.suppress(IndexError):
                row_id = row[0]
                text = row[2]
                start, end = get_start_end(row)
                ids[row_id] = (text, start, end)
    return ids


def find_overlaps(ids1, ids2):
    overlaps = []
    for id1, (text1, start1, end1) in ids1.items():
        foundIn2 = False
        for id2, (text2, start2, end2) in ids2.items():
            if text1 == text2 and start1 <= end2 and start2 <= end1:
                overlap_length = max(0, min(end1, end2) - max(start1, start2))
                non_overlap_length1 = end1 - start1 - overlap_length
                non_overlap_length2 = end2 - start2 - overlap_length
                overlaps.append(
                    (id1, id2, overlap_length, non_overlap_length1, non_overlap_length2)
                )
                foundIn2 = True
        if not foundIn2:
            overlaps.append((id1, None, 0, end1 - start1, 0))

    return overlaps


def write_overlaps(overlaps, outfile):
    overlaps = sorted(overlaps, key=lambda x: x[2], reverse=True)
    with open(outfile, "w") as f:
        for id1, id2, overlap_length, non_overlap1, non_overlap2 in overlaps:
            f.write(f"{id1}\t{id2}\t{overlap_length}\t{non_overlap1}\t{non_overlap2}\n")


def generate_image(overlaps):
    # Set figure size to be 1000px square
    fig, ax = plt.subplots(figsize=(1000 / 80, 1000 / 80), dpi=80)

    # Get the viridis colormap
    viridis = cm.get_cmap("viridis")

    # Sort overlaps by overlap length
    overlaps.sort(key=lambda x: x[2] + x[3], reverse=True)

    max_non_overlap = max(
        max(abs(non_overlap1), abs(non_overlap2))
        for _, _, _, non_overlap1, non_overlap2 in overlaps
    )

    for i, (id1, id2, overlap_length, non_overlap1, non_overlap2) in enumerate(
        overlaps
    ):
        start_overlap = -overlap_length / 2
        end_overlap = overlap_length / 2

        line_overlap = mlines.Line2D(
            [start_overlap, end_overlap], [i, i], color=viridis(0)
        )  # Use a color from the middle of the colormap
        ax.add_line(line_overlap)

        line_non_overlap1 = mlines.Line2D(
            [start_overlap - non_overlap1, start_overlap], [i, i], color=viridis(0.5)
        )  # Use a color from the start of the colormap
        ax.add_line(line_non_overlap1)

        line_non_overlap2 = mlines.Line2D(
            [end_overlap, end_overlap + non_overlap2], [i, i], color=viridis(0.8)
        )  # Use a color from the end of the colormap
        ax.add_line(line_non_overlap2)

    ax.set_xlim(-max_non_overlap, max_non_overlap)
    # Set y-axis limits to make line height 1px
    ax.set_ylim(-1, 1000)

    # Remove y-axis labels
    ax.set_yticklabels([])

    # Minimize whitespace around the plot
    plt.tight_layout()

    # plt.show()
    plt.savefig("output.png", dpi=80)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("file1", help="first BUSCO results file")
    parser.add_argument("file2", help="second BUSCO results file")
    parser.add_argument("outfile", help="output file path")
    args = parser.parse_args()

    ids1 = read_ids(args.file1)
    ids2 = read_ids(args.file2)
    overlaps = find_overlaps(ids1, ids2)

    generate_image(overlaps)
    write_overlaps(overlaps, args.outfile)


if __name__ == "__main__":
    main()
