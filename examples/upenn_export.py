#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Imports #
# Standard Libraries #
from pathlib import Path

# Third-Party Packages #
from mxbids import Dataset
import ucsfndt.mxbids

# Local Packages #


# Main #
if __name__ == "__main__":
    mxbids_path = Path("/Users/changlab/Documents/JasperNAS/root_store/updated_subjects")
    export_path = Path("/Users/changlab/Documents/test")

    dataset = Dataset(mxbids_path, mode="r", load=True)

    name_map = {
        "EC0210": "UPenn0004",
    }

    upenn_dataset_exporter = dataset.create_exporter("UPENN")
    upenn_dataset_exporter.execute_export(path=export_path, name_map=name_map)
