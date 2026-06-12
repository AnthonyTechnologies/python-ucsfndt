"""humanneuroredcap.py
A BIDS Anatomy Pia Importer.
"""
# Package Header #
from ...header import *

# Header #
__author__ = __author__
__credits__ = __credits__
__maintainer__ = __maintainer__
__email__ = __email__


# Imports #
# Standard Libraries #
from collections.abc import Callable, Iterable
from pathlib import Path
from math import isnan
from typing import Any
from warnings import warn
import secrets
import string
import uuid
import tomllib

# Third-Party Packages #
from redcap import Project

# Local Packages #


# Definitions #
# Constants #
DEFAULT_KEY_FILE = Path(__file__).parent.joinpath("redcap_key.toml")


# Classes #
class HumanNeuroRedcap:
    """A class for importing human neuro REDCap data."""

    # Attributes #
    valid_id_chars = string.ascii_uppercase + string.digits

    key_file: Path = DEFAULT_KEY_FILE
    database: Project | None = None

    def __init__(self, config_file: str | Path | None = None, connect: bool = True):
        self.construct(config_file, connect)

    def construct(self, config_file: str | Path | None = None, connect: bool = True):

        if config_file is not None:
            match config_file:
                case str():
                    self.key_file = Path(config_file)
                case Path():
                    self.key_file = config_file
                case _:
                    raise ValueError(f"Invalid config_file type: {type(config_file)}")

        if connect:
            self.connect()

    def load_credentials(self):
        with open(self.key_file, "rb") as f:
            credentials = tomllib.load(f)
        return credentials["url"], credentials["token"]

    def connect(self, url: str | None = None, token: str | None = None):
        if url is None or token is None:
            credentials = self.load_credentials()
            if url is None:
                url = credentials[0]
            if token is None:
                token = credentials[1]

        self.database = Project(url, token)

    def create_ucsf_id(self) -> tuple[str, str]:
        return "".join(secrets.choice(self.valid_id_chars) for _ in range(4)), str(uuid.uuid4().hex)

    def add_patient(
        self,
        mrn: str,
        first_name: str,
        last_name: str,
        ucsf_id: str | None = None,
        ucsf_guid: str | None = None,
        nda_guid: str | None = None,
    ):
        if self.database is None:
            raise RuntimeError("Database not connected.")

        patient_info = dict()
        ucsf_ids = set()
        ucsf_guids = set()
        nda_guids = set()
        records = self.database.export_records(
            "df",
            fields=["record_id", "ucsf_id", "ucsf_guid", "nda_guid", "mrn", "first_name", "last_name"],
            events=["demographics_arm_1", "general_phi_arm_1"],
        )

        for row in records.iterrows():
            if isinstance(m := row[1]["mrn"], str) or not isnan(m):
                patient_info[f"{int(m):0{8}d}"] = (row[1]["first_name"], row[1]["last_name"])

            u = row[1]["ucsf_id"]
            if isinstance(u, str):
                ucsf_ids.add(u)
            elif not isnan(u):
                ucsf_ids.add(f"{int(u):0{4}d}")

            g = row[1]["ucsf_guid"]
            if isinstance(g, str):
                ucsf_guids.add(g)
            elif not isnan(g):
                ucsf_guids.add(f"{int(g):0{32}d}")

            n = row[1]["nda_guid"]
            if isinstance(n, str):
                nda_guids.add(n)
            elif not isnan(n):
                nda_guids.add(f"{int(n):0{32}d}")

        if (p_info := patient_info.get(mrn, None)) is not None:
            if first_name.lower() == p_info[0].lower() and last_name.lower() == p_info[1].lower():
                warn(f"MRN {mrn} already exists in database.")
                return
            else:
                raise ValueError(f"MRN {mrn} already exists in database with different name.")

        if ucsf_id is None:
            ucsf_id = "".join(secrets.choice(self.valid_id_chars) for _ in range(4))
            while ucsf_id in ucsf_ids:
                ucsf_id = "".join(secrets.choice(self.valid_id_chars) for _ in range(4))

        if ucsf_guid is None:
            ucsf_guid = str(uuid.uuid4().hex)
            while ucsf_guid in ucsf_guids:
                ucsf_guid = str(uuid.uuid4().hex)

        record_phi = {"record_id": ucsf_id, "redcap_event_name": "general_phi_arm_1", "mrn": mrn, "first_name": first_name, "last_name": last_name}
        record_info = {"record_id": ucsf_id, "redcap_event_name": "demographics_arm_1", "ucsf_id": ucsf_id, "ucsf_guid": ucsf_guid, "nda_guid": nda_guid}
        self.database.import_records([record_phi, record_info])

        try:
            _ = self.database.export_records(records=[ucsf_id])
        except Exception as e:
            warn(f"Failed to add patient {mrn} to database: {e}")

        return ucsf_id, ucsf_guid

    def ucsf_id_lookup(self, id_: str, id_type: str) -> str:
        pass
