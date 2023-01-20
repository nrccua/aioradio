"""Generic functions related to working with files or the file system."""

# pylint: disable=broad-except
# pylint: disable=consider-using-enumerate
# pylint: disable=import-outside-toplevel
# pylint: disable=invalid-name
# pylint: disable=logging-fstring-interpolation
# pylint: disable=too-many-arguments
# pylint: disable=too-many-boolean-expressions
# pylint: disable=too-many-branches
# pylint: disable=too-many-instance-attributes
# pylint: disable=too-many-lines
# pylint: disable=too-many-locals
# pylint: disable=too-many-nested-blocks
# pylint: disable=too-many-public-methods

import asyncio
import csv
import functools
import json
import logging
import os
import re
import time
import zipfile
from asyncio import sleep
from collections import defaultdict
from dataclasses import dataclass
from dataclasses import field as dc_field
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from types import coroutine
from typing import Any, Dict, List, Union

import cchardet as chardet
import mandrill
import numpy as np
from openpyxl import load_workbook
from smb.base import SharedFile
from smb.smb_structs import OperationFailure
from smb.SMBConnection import SMBConnection

from aioradio.aws.s3 import download_file, upload_file
from aioradio.aws.secrets import get_secret
from aioradio.psycopg2 import establish_psycopg2_connection

DIRECTORY = Path(__file__).parent.absolute()
LOG = logging.getLogger('file_ingestion')

@dataclass
class EFIParse:
    """EnrollmentFileIngestion parse class."""

    filename: str
    fice_enrolled_logic: set = dc_field(default_factory=set)
    entry_year_filter: dict = dc_field(default_factory=dict)

    def __post_init__(self):
        if not self.fice_enrolled_logic:
            self.fice_enrolled_logic = {
                "001100",
                "001397",
                "001507",
                "001526",
                "002120",
                "002122",
                "002180",
                "002760",
                "002778",
                "002795",
                "002907",
                "003301",
                "003450",
                "003505",
                "003535",
                "003688",
                "003709"
            }

        if not self.entry_year_filter:
            self.entry_year_filter = {
                "start": "2022",
                "end": "2026"
            }

        now = datetime.now()
        self.filed_date_min_max = {
            "BirthDate": (now - timedelta(days=80 * 365), now - timedelta(days=10 * 365)),
            "SrcDate": (now - timedelta(days=50 * 365), now + timedelta(days=365)),
            "Inquired": (now - timedelta(days=50 * 365), now + timedelta(days=365)),
            "Applied": (now - timedelta(days=50 * 365), now + timedelta(days=365)),
            "Completed": (now - timedelta(days=50 * 365), now + timedelta(days=365)),
            "Admitted": (now - timedelta(days=50 * 365), now + timedelta(days=365)),
            "Confirmed": (now - timedelta(days=50 * 365), now + timedelta(days=365)),
            "Enrolled": (now - timedelta(days=50 * 365), now + timedelta(days=365)),
            "Canceled": (now - timedelta(days=50 * 365), now + timedelta(days=365)),
            "Dropped": (now - timedelta(days=50 * 365), now + timedelta(days=365)),
            "Graduated": (now - timedelta(days=50 * 365), now + timedelta(days=365)),
            "ProspectDate": (now - timedelta(days=50 * 365), now + timedelta(days=365)),
            "FAFSASubmitted": (now - timedelta(days=50 * 365), now + timedelta(days=365)),
            "CustomDate1": (now - timedelta(days=50 * 365), now + timedelta(days=365)),
            "CustomDate2": (now - timedelta(days=50 * 365), now + timedelta(days=365)),
            "CustomDate3": (now - timedelta(days=50 * 365), now + timedelta(days=365)),
            "CustomDate4": (now - timedelta(days=50 * 365), now + timedelta(days=365)),
            "CustomDate5": (now - timedelta(days=50 * 365), now + timedelta(days=365))
        }

        self.filed_year_min_max = {
            "EntryYear": ((now - timedelta(days=50 * 365)).year, (now + timedelta(days=10 * 365)).year),
            "HSGradYear": ((now - timedelta(days=50 * 365)).year, (now + timedelta(days=10 * 365)).year)
        }

        self.field_to_max_widths = {
            "UniqueID": 50,
            "StudentID": 50,
            "LastName": 64,
            "FirstName": 64,
            "Gender": 1,
            "GPA": 20,
            "Address1": 150,
            "Address2": 150,
            "City": 50,
            "StateCode": 50,
            "ZipCode": 20,
            "BirthDate": 10,
            "EntryTerm": 14,
            "EntryYear": 4,
            "HSGradYear": 4,
            "SrcCode": 256,
            "SrcDate": 10,
            "Inquired": 10,
            "Applied": 10,
            "Completed": 10,
            "Admitted": 10,
            "Confirmed": 10,
            "Enrolled": 10,
            "Canceled": 10,
            "Dropped": 10,
            "Graduated": 10,
            "AcademicProgram": 256,
            "StudentAthlete": 50,
            "CampusLocation": 50,
            "Email": 75,
            "Ethnicity": 1,
            "FirstGenFlag": 1,
            "EFC": 20,
            "HSCode": 6,
            "ACTScore": 2,
            "SATScore": 4,
            "ProspectCode": 256,
            "ProspectDate": 10,
            "FAFSASubmitted": 10,
            "ApplicationPlan": 30,
            "AdmitCode": 30,
            "College": 30,
            "AdmittedProgram": 256,
            "HonorsProgram": 5,
            "StudentType": 20,
            "International": 5,
            "CountryOfOrigin": 30,
            "StudentStatus": 20,
            "Territory": 30,
            "EngagementScore": 10,
            "CellPhoneNumber": 10,
            "TextMessageOptIn": 5,
            "CustomFilter1": 20,
            "CustomFilter2": 20,
            "CustomFilter3": 20,
            "CustomFilter4": 20,
            "CustomFilter5": 20,
            "CustomDate1": 10,
            "CustomDate2": 10,
            "CustomDate3": 10,
            "CustomDate4": 10,
            "CustomDate5": 10
        }

        self.gender_map = {
            "MALE/MAN": "M",
            "MALE": "M",
            "MAN": "M",
            "M": "M",
            "FEMALE/WOMAN": "F",
            "FEMALE": "F",
            "WOMAN": "F",
            "F": "F"
        }

        self.grades_map = {
            "A+": "4.0",
            "A": "4.0",
            "A-": "3.667",
            "B+": "3.333",
            "B": "3.0",
            "B-": "2.667",
            "C+": "2.333",
            "C": "2.0",
            "C-": "1.667",
            "D+": "1.333",
            "D": "1.0",
            "D-": "0.667",
            "F+": "0.333",
            "F": "0.0",
            "F-": "0.0"
        }

        self.date_formats = [
            "%m/%d/%y",
            "%m/%d/%Y",
            "%-m/%-d/%Y",
            "%-m/%-d/%y",
            "%Y-%m-%d",
            "%Y%m%d",
            "%d-%b-%Y",
            "%m-%d-%y",
            "%m-%d-%Y",
            "%b %d, %Y",
            "%Y-%m-%dT%H:%M:%SZ",
            "%d-%b-%y",
            "%Y-%m-%dT%H:%M:%S",
            "%-d-%b-%y",
            "%-d-%b-%Y",
            "%Y/%m/%d"
        ]

        self.student_athlete_map = {
            "0": "N",
            "1": "Y",
            "CHRL": "Y",
            "NO": "N",
            "WLAX": "Y",
            "YES": "Y",
            "TRUE": "Y",
            "MWR": "Y",
            "MTR": "Y",
            "VB": "Y",
            "N": "N",
            "BASEB": "Y",
            "MSWIM": "Y",
            "WWR": "Y",
            "Y": "Y",
            "FALSE": "N",
            "FB": "Y"
        }

        self.season_year_map = {
            "FA19": "2019",
            "FA20": "2020",
            "FA21": "2021",
            "FA22": "2022",
            "FA23": "2023",
            "FA24": "2024",
            "FA25": "2025",
            "FA26": "2026",
            "FA27": "2027",
            "19FA": "2019",
            "20FA": "2020",
            "21FA": "2021",
            "22FA": "2022",
            "23FA": "2023",
            "24FA": "2024",
            "25FA": "2025",
            "26FA": "2026",
            "27FA": "2027"
        }

        self.seasons_map = {
            "SPRING": "SPRING",
            "SUMMER": "SUMMER",
            "FALL": "FALL",
            "WINTER": "WINTER",
            "FA": "FALL"
        }

        self.state_to_statecode = {
            "ALABAMA": "AL",
            "ALASKA": "AK",
            "AMERICAN SAMOA": "AS",
            "ARIZONA": "AZ",
            "ARKANSAS": "AR",
            "CALIFORNIA": "CA",
            "COLORADO": "CO",
            "CONNECTICUT": "CT",
            "DELAWARE": "DE",
            "DISTRICT OF COLUMBIA": "DC",
            "FEDERATED STATES OF MICRONESIA": "FM",
            "FLORIDA": "FL",
            "GEORGIA": "GA",
            "GUAM": "GU",
            "HAWAII": "HI",
            "IDAHO": "ID",
            "ILLINOIS": "IL",
            "INDIANA": "IN",
            "IOWA": "IA",
            "KANSAS": "KS",
            "KENTUCKY": "KY",
            "LOUISIANA": "LA",
            "MAINE": "ME",
            "MARSHALL ISLANDS": "MH",
            "MARYLAND": "MD",
            "MASSACHUSETTS": "MA",
            "MICHIGAN": "MI",
            "MINNESOTA": "MN",
            "MISSISSIPPI": "MS",
            "MISSOURI": "MO",
            "MONTANA": "MT",
            "NEBRASKA": "NE",
            "NEVADA": "NV",
            "NEW HAMPSHIRE": "NH",
            "NEW JERSEY": "NJ",
            "NEW MEXICO": "NM",
            "NEW YORK": "NY",
            "NORTH CAROLINA": "NC",
            "NORTH DAKOTA": "ND",
            "NORTHERN MARIANA ISLANDS": "MP",
            "OHIO": "OH",
            "OKLAHOMA": "OK",
            "OREGON": "OR",
            "PALAU": "PW",
            "PENNSYLVANIA": "PA",
            "PUERTO RICO": "PR",
            "RHODE ISLAND": "RI",
            "SOUTH CAROLINA": "SC",
            "SOUTH DAKOTA": "SD",
            "TENNESSEE": "TN",
            "TEXAS": "TX",
            "U.S. ARMED FORCES - AMERICAS": "AA",
            "U.S. ARMED FORCES - EUROPE": "AE",
            "U.S. ARMED FORCES - PACIFIC": "AP",
            "UTAH": "UT",
            "VERMONT": "VT",
            "VIRGIN ISLANDS": "VI",
            "VIRGINIA": "VA",
            "WASHINGTON": "WA",
            "WEST VIRGINIA": "WV",
            "WISCONSIN": "WI",
            "WYOMING": "WY"
        }

        self.cache = {
            'year': {},
            'sort_date': {},
            'date': {},
            'bad_date': set()
        }

        self.year_formats = [
            '%Y',
            '%y'
        ]

        self.apt_to_compiled = {
            "apt": re.compile(re.escape("apt"), re.IGNORECASE),
            "avenue": re.compile(re.escape("avenue"), re.IGNORECASE),
            "ave": re.compile(re.escape("ave"), re.IGNORECASE),
            "blvd": re.compile(re.escape("blvd"), re.IGNORECASE),
            "circle": re.compile(re.escape("circle"), re.IGNORECASE),
            "cir": re.compile(re.escape("cir"), re.IGNORECASE),
            "court": re.compile(re.escape("court"), re.IGNORECASE),
            "drive": re.compile(re.escape("drive"), re.IGNORECASE),
            "lane": re.compile(re.escape("lane"), re.IGNORECASE),
            "parkway": re.compile(re.escape("parkway"), re.IGNORECASE),
            "place": re.compile(re.escape("place"), re.IGNORECASE),
            "road": re.compile(re.escape("road"), re.IGNORECASE),
            "street": re.compile(re.escape("street"), re.IGNORECASE),
            "way": re.compile(re.escape("way"), re.IGNORECASE)
        }

        self.addr_suffix_list = [
            "ct",
            "dr",
            "pl",
            "rd",
            "st"
        ]

        self.addr_unit_to_compiled = {
            " unit ": re.compile(re.escape(" unit "), re.IGNORECASE),
            " bldg ": re.compile(re.escape(" bldg "), re.IGNORECASE),
            " ste ": re.compile(re.escape(" ste "), re.IGNORECASE),
            " # ": re.compile(re.escape(" # "), re.IGNORECASE),
            " #": re.compile(re.escape(" #"), re.IGNORECASE)
        }

        #### Used by EFI exclusively ####
        self.non_prospect_row_idxs = set()

        self.enrollment_funnel_fields = {
            'Inquired',
            'Applied',
            'Completed',
            'Admitted',
            'Confirmed',
            'Enrolled',
            'Canceled',
            'Dropped',
            'Graduated'
        }

        self.non_prospect_fields = self.enrollment_funnel_fields - {'Dropped', 'Graduated'}

        self.season_year_map = defaultdict(str, self.season_year_map)

        self.filtered = {
            'entryyear': 0,
            'prospects': 0
        }

        self.generic_bool_map = {
            'YES': 'Y',
            'NO': 'N',
            'Y': 'Y',
            'N': 'N',
            'TRUE': 'Y',
            'FALSE': 'N',
            '1': 'Y',
            '0': 'N'
        }

        self.ethnicity_federal_categories = {'1', '2', '3', '4', '5', '6', '7', '8'}

    def check_width(self, value: str, field: str, row_idx: int) -> str:
        """Check field value and truncate if it is longer than expected.

        Args:
            value (str): Value
            field (str): Column header field value
            row_idx (int): Row index

        Returns:
            str: [description]
        """

        if len(value) > self.field_to_max_widths[field]:
            new_value = value[:self.field_to_max_widths[field]].rstrip()
            LOG.warning(f"[{self.filename}] [row:{row_idx}] [{field}] - '{value}' "
                        f"exceeds max width of {self.field_to_max_widths[field]}. Trimming value to {new_value}")
            value = new_value

        return value

    def check_name(self, value: str, field: str, row_idx: int) -> str:
        """Check FirstName | LastName logic.

        Args:
            value (str): Name value
            field (str): Column header field value
            row_idx (int): Row number in file

        Returns:
            str: Name value
        """

        if value != '':
            value = value.replace('"', '')
            value = self.check_width(value, field, row_idx)

        return value

    def check_gender(self, value: str) -> str:
        """Check Gender logic.

        Args:
            value (str): Gender value

        Returns:
            str: Gender value
        """

        if value != '':
            value = self.gender_map.get(value.upper(), '')

        return value

    def check_gpa(self, value: str, field: str, row_idx: int) -> str:
        """Check GPA logic.

        Args:
            value (str): GPA value
            field (str): Column header field value
            row_idx (int): Row number in file

        Returns:
            str: GPA value
        """

        if value != '':
            try:
                value = '' if not (0 <= float(value) <= 200) else self.check_width(value, field, row_idx)
            except ValueError:
                value = self.grades_map.get(value.upper(), '')

        return value

    def check_statecode(self, value: str, field: str, row_idx: int) -> str:
        """Check StateCode logic.

        Args:
            value (str): StateCode value
            field (str): Column header field value
            row_idx (int): Row number in file

        Returns:
            str: StateCode value
        """

        if value != '':
            value = self.state_to_statecode.get(value.upper(), value)
            value = self.check_width(value, field, row_idx)

        return value

    def check_date(self, value: str, field: str, row_idx: int) -> str:
        """Check date conforms to expected date within time range.

        Args:
            value (str): Date value
            field (str): Column header field value
            row_idx (int): Row number in file

        Returns:
            str: Date value
        """

        if value != '':

            index = value.find(' ')
            if index != -1:
                value = value[:index]

            if value in self.cache['date']:
                value = self.cache['date'][value]
            elif value in self.cache['bad_date']:
                value = ''
            else:
                for idx, pattern in enumerate(self.date_formats):
                    try:
                        val = datetime.strptime(value, pattern)
                        if idx != 0:
                            self.date_formats[0], self.date_formats[idx] = self.date_formats[idx], self.date_formats[0]
                        if field in self.filed_date_min_max:
                            # we have date field with defined min/max range.
                            dmin = self.filed_date_min_max[field][0]
                            dmax = self.filed_date_min_max[field][1]
                            if dmin <= val <= dmax:
                                val = val.strftime('%Y/%m/%d')
                                self.cache['date'][value] = val
                                self.cache['sort_date'][val] = f"{val[5:7]}/{val[8:10]}/{val[:4]}"
                                value = val
                            else:
                                LOG.warning(f"[{self.filename}] [row:{row_idx}] [{field}] - {val.date()}"
                                            f" not between range of {dmin.date()} to {dmax.date()}")
                                value = ''
                        break
                    except ValueError:
                        pass
                else:
                    self.cache['bad_date'].add(value)
                    value = ''

        return value

    def check_year(self, value: str, field: str, row_idx: int) -> str:
        """Check year conforms to expected year within time range.

        Args:
            value (str): Year value
            field (str): Column header field value
            row_idx (int): Row number in file

        Returns:
            str: Year value
        """

        if value != '':
            if value in self.cache['year']:
                value = self.cache['year'][value]
            else:
                for idx, pattern in enumerate(self.year_formats):
                    try:
                        val = datetime.strptime(value, pattern).year
                        if idx != 0:
                            self.year_formats[0], self.year_formats[idx] = self.year_formats[idx], self.year_formats[0]
                        if field in self.filed_year_min_max:
                            # we have year field with defined min/max range.
                            ymin = self.filed_year_min_max[field][0]
                            ymax = self.filed_year_min_max[field][1]
                            if ymin <= val <= ymax:
                                val = str(val)
                                self.cache['year'][value] = val
                                value = val
                            else:
                                LOG.warning(f"[{self.filename}] [row:{row_idx}] [{field}] - {val} not between range of {ymin} to {ymax}")
                                self.cache['year'][value] = ''
                                value = ''
                        break
                    except ValueError:
                        pass
                else:
                    if field != 'EntryYear':
                        self.cache['year'][value] = ''
                        value = ''

        return value

    def check_srccode(self, value: str, field: str, row_idx: int) -> str:
        """Check SrcCode logic.

        Args:
            value (str): SrcCode value
            field (str): Column header field value
            row_idx (int): Row number in file

        Returns:
            str: SrcCode value
        """

        return self.check_width(value, field, row_idx) if value != '' else 'NRCCUA Unknown'

    def check_athlete(self, value: str) -> str:
        """Check StudentAthlete logic.

        Args:
            value (str): Athlete value
            rows (list[int]): List of row indicies

        Returns:
            str: Athlete value
        """

        if value != '':
            value = self.student_athlete_map.get(value.upper(), 'Y')

        return value

    def check_email(self, value: str, field: str, row_idx: int) -> str:
        """Check Email logic.

        Args:
            value (str): Email value
            field (str): Column header field value
            row_idx (int): Row number in file

        Returns:
            str: Email value
        """

        if value != '':
            value = self.check_width(value, field, row_idx) if '@' in value else ''

        return value

    def check_generic(self, value: str, field: str, row_idx: int) -> str:
        """Check generic column logic.

        Args:
            value (str): Generic value
            field (str): Column header field value
            row_idx (int): Row number in file

        Returns:
            str: Generic value
        """

        if value != '':
            value = self.check_width(value, field, row_idx)

        return value

    def check_address1(self, value: str, field: str, row_idx: int) -> str:
        """Check Address1 logic.

        Args:
            value (str): Address value
            field (str): Column header field value
            row_idx (int): Row number in file

        Returns:
            str: Address value
        """

        if value != '':
            value = self.check_no_spaces_address(value)
            value = self.check_address(value)
            value = self.check_width(value, field, row_idx)

        return value

    def check_address2(self, value: str, field: str, row_idx: int) -> str:
        """Check Address2 logic.

        Args:
            value (str): Address value
            field (str): Column header field value
            row_idx (int): Row number in file

        Returns:
            str: Address value
        """

        if value != '':
            value = self.check_address(f" {value}")
            value = self.check_width(value, field, row_idx)

        return value

    def check_no_spaces_address(self, value: str) -> str:
        """Check and adjust address values that have no spaces by detecting
        where spaces should exist and applying.

        Args:
            value (str): Address value

        Returns:
            str: Address value
        """

        if ' ' not in value:
            value = value.rstrip('.')
            for idx, character in enumerate(value):
                if not character.isdigit():
                    if idx != 0:
                        # add space after all starting digits detected
                        value = f"{value[:idx]} {value[idx:]}"
                    break

            value_lower = value.lower()
            for item, compiled in self.apt_to_compiled.items():
                if item in value_lower:
                    # add space before item
                    value = compiled.sub(f" {item}", value)
                    break

            for item in self.addr_suffix_list:
                if value_lower.endswith(item):
                    # add space before item
                    value = f"{value[:len(value) - 2]} {value[len(value) - 2:]}"
                    break

        return value

    def check_address(self, value: str) -> str:
        """Normalize address unit if necessary.

        Args:
            value (str): Address value

        Returns:
            str: Address value
        """

        value_lower = value.lower()
        for item, compiled in self.addr_unit_to_compiled.items():
            if item in value_lower:
                value = compiled.sub(" apt ", value)
                break

        return value.lstrip()

    def apply_fice_enrolled_logic(self, fice: str, confirmed: str, enrolled: str, canceled: str, dropped: str) -> str:
        """Apply FICE enrolled logic if defined in the config.

        Args:
            fice (str): Institution unique identifier
            confirmed (str): Confirmed enrollment funnel value
            enrolled (str): Enrolled enrollment funnel value
            canceled (str): Canceled enrollment funnel value
            dropped (str): Dropped enrollment funnel value

        Returns:
            str: Enrolled enrollment funnel value
        """

        if fice in self.fice_enrolled_logic:
            if confirmed != '' and enrolled == '' and canceled == '' and dropped == '':
                enrolled = confirmed

        return enrolled

    def check_entry_fields(self, entryyear, entryterm) -> tuple:
        """Check entryyear and entryterm fields and try to detect their values.
        Some colleges put the year and term in the same fields.

        Args:
            entryyear ([type]): Student's college entry year
            entryterm ([type]): Student's college entry term

        Returns:
            tuple: Skip record True/False, entryyear value, entryterm value
        """

        skip_record = False
        values = defaultdict(str)
        if entryyear != '':
            values['year'] = entryyear.replace('/', '')
            entryyear = ''

        if entryterm != '':
            values['term'] = entryterm.replace('/', '')
            entryterm = ''

        # search for season & year within entryyear and entryterm if available
        if len(values) > 0:
            concat_str = f"{values['year']} {values['term']}".upper()
            for key, value in self.seasons_map.items():
                if key in concat_str:
                    entryterm = value
                    break

            words = []
            for word in concat_str.split():
                word_length = len(word)
                if word_length >= 4:
                    words.append(word[:4])
                    if word[:4] != word[word_length - 4:]:
                        words.append(word[word_length - 4:])

            for word in words:
                if self.entry_year_filter['start'] <= self.season_year_map[word] <= self.entry_year_filter['end']:
                    entryyear = self.season_year_map[word]
                    break
                if self.entry_year_filter['start'] <= word <= self.entry_year_filter['end']:
                    entryyear = word
                    break

        if entryyear == '':
            self.filtered['entryyear'] += 1
            skip_record = True

        return skip_record, entryyear, entryterm

    def check_for_prospects(self, row: dict[str, Any]) -> bool:
        """Check if a record is a prospect, in which case we can skip and leave
        out of output.

        Args:
            row (dict[str, Any]): A single record that is a dict of column names to values

        Returns:
            bool: Skip record since it is a prospect
        """

        skip_record = True
        for field in self.non_prospect_fields:
            # Check if we have a date in one of the enrollment funnel fields
            if row[field]:
                skip_record = False
                break
        else:
            self.filtered['prospects'] += 1

        return skip_record


    ###############################################################################################
    ############################### New EL3 field parsing functions ###############################
    ###############################################################################################
    #
    # ETHNICITY
    # FIRSTGENFLAG
    # EFC
    # HSCODE
    # ACTSCORE
    # SATSCORE
    # PROSPECTCODE
    # PROSPECTDATE
    # FAFSASUBMITTED
    # APPLICATIONPLAN
    # ADMITCODE
    # COLLEGE
    # ADMITTEDPROGRAM
    # HONORSPROGRAM
    # STUDENTTYPE
    # INTERNATIONAL
    # COUNTRYOFORIGIN
    # STUDENTSTATUS
    # TERRITORY
    # ENGAGEMENTSCORE
    # CUSTOMFILTER1, ..., CUSTOMFILTER5
    # CUSTOMDATE1, ..., CUSTOMDATE5
    #
    # Many of these fields are parsed using the functions check_generic or check_date
    # else they use a function below.

    def check_generic_boolean(self, value: str) -> str:
        """Check generic boolean value.

        Args:
            value (str): Generic Boolean value

        Returns:
            str: Generic Boolean value
        """

        if value != '':
            value = self.generic_bool_map.get(value.upper(), '')

        return value

    def check_ethnicity(self, value: str) -> str:
        """Check Ethnicity is a federal category value.

        Args:
            value (str): Ethnicity category

        Returns:
            str: Ethnicity category
        """

        if value != '' and value not in self.ethnicity_federal_categories:
            value = ''

        return value

    @staticmethod
    def check_act_score(value: str) -> str:
        """Check ACT Score logic.

        Args:
            value (str): ACT score
            field (str): Column header field value

        Returns:
            str: ACT score
        """

        if value != '':
            try:
                integer = int(value)
                value = str(integer) if (1 <= integer <= 36) else ''
            except ValueError:
                value = ''

        return value

    @staticmethod
    def check_sat_score(value: str) -> str:
        """Check SAT Score logic.

        Args:
            value (str): SAT score
            field (str): Column header field value

        Returns:
            str: SAT score
        """

        if value != '':
            try:
                integer = int(value)
                value = str(integer) if (400 <= integer <= 1600) else ''
            except ValueError:
                value = ''

        return value

    @staticmethod
    def check_hscode(value: str) -> str:
        """Check HSCODE logic.

        Args:
            value (str): HSCODE value
            field (str): Column header field value

        Returns:
            str: HSCODE value
        """

        if value != '' and len(value) == 6:
            try:
                _ = int(value)
            except ValueError:
                value = ''
        else:
            value = ''

        return value

    ###############################################################################################
    ################################### Used by EFI exclusively ###################################
    ###############################################################################################

    def check_generic_boolean_efi(self, records: list[str]):
        """Check generic boolean logic.

        Args:
            records (list[str]): List of a specific columns values
            field (str): Column header field value
            row_idx (int): Row number in file
        """

        for idx in range(len(records)):
            records[idx] = self.check_generic_boolean(records[idx])

    def check_ethnicity_efi(self, records: list[str]):
        """Check ethnicity logic.

        Args:
            records (list[str]): List of a specific columns values
            field (str): Column header field value
            row_idx (int): Row number in file
        """

        for idx in range(len(records)):
            records[idx] = self.check_ethnicity(records[idx])

    def check_act_score_efi(self, records: list[str]):
        """Check ACT score logic.

        Args:
            records (list[str]): List of a specific columns values
            field (str): Column header field value
            row_idx (int): Row number in file
        """

        for idx in range(len(records)):
            records[idx] = self.check_act_score(records[idx])

    def check_sat_score_efi(self, records: list[str]):
        """Check SAT score logic.

        Args:
            records (list[str]): List of a specific columns values
            field (str): Column header field value
            row_idx (int): Row number in file
        """

        for idx in range(len(records)):
            records[idx] = self.check_sat_score(records[idx])

    def check_hscode_efi(self, records: list[str]):
        """Check HSCode logic.

        Args:
            records (list[str]): List of a specific columns values
            field (str): Column header field value
            row_idx (int): Row number in file
        """

        for idx in range(len(records)):
            records[idx] = self.check_hscode(records[idx])

    def check_year_efi(self, records: list[str], field: str, row_idx: int):
        """Check year conforms to expected year within time range.

        Args:
            records (list[str]): List of a specific columns values
            field (str): Column header field value
            past (datetime): Past datetime threshold --> Deprecated, remove
            future (datetime): Future datetime threshold --> Deprecated, remove
            row_idx (int): Row number in file
        """

        for idx in range(len(records)):
            records[idx] = self.check_year(records[idx], field, row_idx + idx)

    def check_date_efi(self, records: list[str], field: str, row_idx: int):
        """Check date conforms to expected date within time range.

        Args:
            records (list[str]): List of a specific columns values
            field (str): Column header field value
            past (datetime): Past datetime threshold --> Deprecated, remove
            future (datetime): Future datetime threshold --> Deprecated, remove
            row_idx (int): Row number in file
        """

        for idx in range(len(records)):
            records[idx] = self.check_date(records[idx], field, row_idx + idx)
            if field in self.non_prospect_fields and records[idx]:
                self.non_prospect_row_idxs.add(idx)

    def check_name_efi(self, records: list[str], field: str, row_idx: int):
        """Check FirstName | LastName logic.

        Args:
            records (list[str]): List of a specific columns values
            field (str): Column header field value
            row_idx (int): Row number in file
        """

        for idx in range(len(records)):
            records[idx] = self.check_name(records[idx], field, row_idx + idx)

    def check_gender_efi(self, records: list[str]):
        """Check Gender logic.

        Args:
            records (list[str]): List of a specific columns values
        """

        for idx in range(len(records)):
            records[idx] = self.check_gender(records[idx])

    def check_gpa_efi(self, records: list[str], field: str, row_idx: int):
        """Check GPA logic.

        Args:
            records (list[str]): List of a specific columns values
            field (str): Column header field value
            row_idx (int): Row number in file
        """

        for idx in range(len(records)):
            records[idx] = self.check_gpa(records[idx], field, row_idx + idx)

    def check_statecode_efi(self, records: list[str], field: str, row_idx: int):
        """Check StateCode logic.

        Args:
            records (list[str]): List of a specific columns values
            field (str): Column header field value
            row_idx (int): Row number in file
        """

        for idx in range(len(records)):
            records[idx] = self.check_statecode(records[idx], field, row_idx + idx)

    def check_srccode_efi(self, records: list[str], field: str, row_idx: int):
        """Check SrcCode logic.

        Args:
            records (list[str]): List of a specific columns values
            field (str): Column header field value
            row_idx (int): Row number in file
        """

        for idx in range(len(records)):
            records[idx] = self.check_srccode(records[idx], field, row_idx + idx)

    def check_athlete_efi(self, records: list[str]):
        """Check StudentAthlete logic.

        Args:
            records (list[str]): List of a specific columns values
            rows (list[int]): List of row indicies
        """

        for idx in range(len(records)):
            records[idx] = self.check_athlete(records[idx])

    def check_email_efi(self, records: list[str], field: str, row_idx: int):
        """Check Email logic.

        Args:
            records (list[str]): List of a specific columns values
            field (str): Column header field value
            row_idx (int): Row number in file
        """

        for idx in range(len(records)):
            records[idx] = self.check_email(records[idx], field, row_idx + idx)

    def check_generic_efi(self, records: list[str], field: str, row_idx: int):
        """Check generic column logic.

        Args:
            records (list[str]): List of a specific columns values
            field (str): Column header field value
            row_idx (int): Row number in file
        """

        for idx in range(len(records)):
            records[idx] = self.check_generic(records[idx], field, row_idx + idx)

    def check_address1_efi(self, records: list[str], field: str, row_idx: int):
        """Check Address1 logic.

        Args:
            records (list[str]): List of a specific columns values
            field (str): Column header field value
            row_idx (int): Row number in file
        """

        for idx in range(len(records)):
            records[idx] = self.check_address1(records[idx], field, row_idx + idx)

    def check_address2_efi(self, records: list[str], field: str, row_idx: int):
        """Check Address2 logic.

        Args:
            records (list[str]): List of a specific columns values
            field (str): Column header field value
            row_idx (int): Row number in file
        """

        for idx in range(len(records)):
            records[idx] = self.check_address2(records[idx], field, row_idx + idx)

    def check_for_prospects_efi(self, records: list[list[str]]):
        """Check and remove any records identified as prospects.

        Args:
            records (list[list[str]]): All records defined as a list of lists
        """

        remove_indices = [i for i in range(len(records[0])) if i not in self.non_prospect_row_idxs]
        if remove_indices:
            self.filtered['prospects'] = len(remove_indices)
            for col_idx, record in enumerate(records):
                records[col_idx] = np.delete(record, remove_indices).tolist()

    def check_entry_fields_efi(self, records: list[list[str]], header_to_index: dict[str, int]):
        """Check entryyear and entryterm fields and try to detect their values.
        Some colleges put the year and term in the same fields.

        Args:
            records (list[list[str]]): All records defined as a list of lists
            header_to_index (dict[str, int]): Column name to index in records
        """

        entryyear = records[header_to_index['EntryYear']]
        entryterm = records[header_to_index['EntryTerm']]
        remove_indices = []
        for idx in range(len(records[0])):

            skip_record, entryyear[idx], entryterm[idx] = self.check_entry_fields(entryyear[idx], entryterm[idx])
            if skip_record:
                remove_indices.append(idx)

        if remove_indices:
            for col_idx, record in enumerate(records):
                records[col_idx] = np.delete(record, remove_indices).tolist()

    def apply_fice_enrolled_logic_efi(self, records: list[list[str]], fice: str, header_to_index: dict[str, int]):
        """Apply FICE enrolled logic if defined in the config.

        Args:
            records (list[list[str]]): All records defined as a list of lists
            fice (str): Institution unique identifier
            header_to_index (dict[str, int]): Column name to index in records
        """

        if fice in self.fice_enrolled_logic:
            confirmed = records[header_to_index['Confirmed']]
            enrolled = records[header_to_index['Enrolled']]
            canceled = records[header_to_index['Canceled']]
            dropped = records[header_to_index['Dropped']]
            for idx in range(len(records[0])):
                enrolled[idx] = self.apply_fice_enrolled_logic(fice, confirmed[idx], enrolled[idx], canceled[idx], dropped[idx])


def async_wrapper(func: coroutine) -> Any:
    """Decorator to run functions using async. Found this handy to use with DAG
    tasks.

    Args:
        func (coroutine): async coroutine
    Returns:
        Any: any
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        """Decorator wrapper.

        Returns:
            Any: any
        """

        return asyncio.get_event_loop().run_until_complete(func(*args, **kwargs))

    return wrapper


def async_db_wrapper(db_info: List[Dict[str, Any]]) -> Any:
    """Decorator to run functions using async that handles database connection
    creation and closure.  Pulls database creds from AWS secret manager.

    Args:
        db_info (List[Dict[str, str]], optional): Database info {'name', 'db', 'secret', 'region'}. Defaults to [].

    Returns:
        Any: any
    """

    def parent_wrapper(func: coroutine) -> Any:
        """Decorator parent wrapper.

        Args:
            func (coroutine): async coroutine

        Returns:
            Any: any
        """

        @functools.wraps(func)
        async def child_wrapper(*args, **kwargs) -> Any:
            """Decorator child wrapper. All DB established/closed connections
            and commits or rollbacks take place in the decorator and should
            never happen within the inner function.

            Returns:
                Any: any
            """

            conns = {}
            rollback = {}

            # create connections
            for item in db_info:

                if item['db'] in ['pyodbc', 'psycopg2']:
                    if 'aws_creds' in item:
                        secret = await get_secret(item['secret'], item['region'], item['aws_creds'])
                    else:
                        secret = await get_secret(item['secret'], item['region'])
                    creds = {**json.loads(secret), **{'database': item.get('database', '')}}
                    if item['db'] == 'pyodbc':
                        # Add import here because it requires extra dependencies many systems
                        # don't have out of the box so only import when explicitly being used
                        from aioradio.pyodbc import establish_pyodbc_connection
                        conns[item['name']] = establish_pyodbc_connection(**creds, autocommit=False)
                    elif item['db'] == 'psycopg2':
                        conns[item['name']] = establish_psycopg2_connection(**creds)
                    rollback[item['name']] = item['rollback']
                    print(f"ESTABLISHED CONNECTION for {item['name']}")

            result = None
            error = None
            try:
                # run main function
                result = await func(*args, **kwargs, conns=conns) if conns else await func(*args, **kwargs)
            except Exception as err:
                error = err

            # close connections
            for name, conn in conns.items():

                if rollback[name]:
                    conn.rollback()
                elif error is None:
                    conn.commit()

                conn.close()
                print(f"CLOSED CONNECTION for {name}")

            # if we caught an exception raise it again
            if error is not None:
                raise error

            return result

        return child_wrapper

    return parent_wrapper


def async_wrapper_using_new_loop(func: coroutine) -> Any:
    """Decorator to run functions using async. Found this handy to use with DAG
    tasks.

    Args:
        func (coroutine): async coroutine

    Returns:
        Any: any
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        """Decorator wrapper.

        Returns:
            Any: any
        """

        return asyncio.run(func(*args, **kwargs))

    return wrapper


async def unzip_file(filepath: str, directory: str) -> List[str]:
    """Unzip supplied filepath in the supplied directory.

    Args:
        filepath (str): filepath to unzip
        directory (str): directory to write unzipped files

    Returns:
        List[str]: List of filenames
    """

    filenames = []
    with zipfile.ZipFile(filepath) as zipped:
        # exclude __MACOSX directory that could be added when creating zip on macs
        filenames = [i for i in zipped.namelist() if '__MACOSX' not in i]
        zipped.extractall(directory)

    return filenames


async def unzip_file_get_filepaths(
        filepath: str,
        directory: str,
        include_extensions: List[str] = None,
        exclude_extensions: List[str] = None) -> List[str]:
    """Get all the filepaths after unzipping supplied filepath in the supplied
    directory. If the zipfile contains zipfiles, those files will also be
    unzipped.

    Args:
        filepath (str): [description]
        directory (str): [description]
        include_extensions (List[str], optional): list of file types to add to result, if None add all. Defaults to None.
        exclude_extensions (List[str], optional): list of file types to exclude from result. Defaults to None.

    Returns:
        List[str]: [description]
    """

    paths = []
    zipfile_filepaths = [filepath]
    while zipfile_filepaths:

        new_zipfile_filepaths = []
        for path in zipfile_filepaths:

            for filename in await unzip_file(filepath=path, directory=directory):
                filepath = os.path.join(directory, filename)
                suffix = Path(filepath).suffix.lower()[1:]
                if suffix in 'zip':
                    new_zipfile_filepaths.append(filepath)
                elif include_extensions:
                    if suffix in include_extensions:
                        paths.append(filepath)
                elif exclude_extensions:
                    if suffix not in exclude_extensions:
                        paths.append(filepath)
                else:
                    paths.append(filepath)

        zipfile_filepaths = new_zipfile_filepaths

    return paths


async def get_current_datetime_from_timestamp(dt_format: str = '%Y-%m-%d %H_%M_%S.%f', time_zone: tzinfo = timezone.utc) -> str:
    """Get the datetime from the timestamp in the format and timezone desired.

    Args:
        dt_format (str, optional): date format desired. Defaults to '%Y-%m-%d %H_%M_%S.%f'.
        time_zone (tzinfo, optional): timezone desired. Defaults to timezone.utc.

    Returns:
        str: current datetime
    """

    return datetime.fromtimestamp(time.time(), time_zone).strftime(dt_format)


async def send_emails_via_mandrill(
        mandrill_api_key: str,
        emails: List[str],
        subject: str,
        global_merge_vars: List[Dict[str, Any]],
        template_name: str,
        template_content: List[Dict[str, Any]] = None) -> Any:
    """Send emails via Mailchimp mandrill API.

    Args:
        mandrill_api_key (str): mandrill API key
        emails (List[str]): receipt emails
        subject (str): email subject
        global_merge_vars (List[Dict[str, Any]]): List of dicts used to dynamically populated email template with data
        template_name (str): mandrill template name
        template_content (List[Dict[str, Any]], optional): mandrill template content. Defaults to None.

    Returns:
        Any: any
    """

    message = {
        'to': [{'email': email} for email in emails],
        'subject': subject,
        'merge_language': 'handlebars',
        'global_merge_vars': global_merge_vars
    }

    return mandrill.Mandrill(mandrill_api_key).messages.send_template(
        template_name=template_name,
        template_content=template_content,
        message=message
    )


async def establish_ftp_connection(
        user: str,
        pwd: str,
        name: str,
        server: str,
        dns: str,
        port: int = 139,
        use_ntlm_v2: bool = True,
        is_direct_tcp: bool = False) -> SMBConnection:
    """Establish FTP connection.

    Args:
        user (str): ftp username
        pwd (str): ftp password
        name (str): connection name
        server (str): ftp server
        dns (str): DNS
        port (int, optional): port. Defaults to 139.
        use_ntlm_v2 (bool, optional): use NTLMv1 (False) or NTLMv2(True) authentication algorithm. Defaults to True.
        is_direct_tcp (bool, optional): if NetBIOS over TCP (False) or SMB over TCP (True) is used for communication. Defaults to False.

    Returns:
        SMBConnection: SMB connection object
    """

    conn = SMBConnection(
        username=user,
        password=pwd,
        my_name=name,
        remote_name=server,
        use_ntlm_v2=use_ntlm_v2,
        is_direct_tcp=is_direct_tcp
    )
    conn.connect(ip=dns, port=port)
    return conn


async def list_ftp_objects(
        conn: SMBConnection,
        service_name: str,
        ftp_path: str,
        exclude_directories: bool = False,
        exclude_files: bool = False,
        regex_pattern: str = None) -> List[SharedFile]:
    """List all files and directories in an FTP directory.

    Args:
        conn (SMBConnection): SMB connection object
        service_name (str): FTP service name
        ftp_path (str): FTP directory path
        exclude_directories (bool, optional): directories to exclude. Defaults to False.
        exclude_files (bool, optional): files to exclude. Defaults to False.
        regex_pattern (str, optional): regex pattern to use to filter search. Defaults to None.

    Returns:
        List[SharedFile]: List of files with their attribute info
    """

    results = []
    for item in conn.listPath(service_name, ftp_path):
        is_directory = item.isDirectory
        if item.filename == '.' or item.filename == '..' or \
                (exclude_directories and is_directory) or (exclude_files and not is_directory):
            continue
        if regex_pattern is None or re.search(regex_pattern, item.filename) is not None:
            results.append(item)

    return results


async def delete_ftp_file(conn: SMBConnection, service_name: str, ftp_path: str) -> bool:
    """Remove a file from FTP and verify deletion.

    Args:
        conn (SMBConnection): SMB connection object
        service_name (str): FTP service name
        ftp_path (str): FTP directory path

    Returns:
        bool: deletion status
    """

    status = False
    conn.deleteFiles(service_name, ftp_path)
    try:
        conn.getAttributes(service_name, ftp_path)
    except OperationFailure:
        status = True

    return status


async def write_file_to_ftp(
        conn: SMBConnection,
        service_name: str,
        ftp_path: str,
        local_filepath) -> SharedFile:
    """Write file to FTP creating missing FTP directories if necessary.

    Args:
        conn (SMBConnection): SMB connection object
        service_name (str): FTP service name
        ftp_path (str): FTP directory path
        local_filepath ([type]): local filepath

    Returns:
        SharedFile: ftp file attribute info
    """

    # steps to create missing directories
    path = ''
    for directory in os.path.dirname(ftp_path).split(os.sep):
        folders = {i.filename for i in conn.listPath(service_name=service_name, path=path)}
        if directory not in folders:
            conn.createDirectory(service_name=service_name, path=f'{path}/{directory}')
            await sleep(1)
        path = directory if not path else f'{path}/{directory}'

    # write local file to FTP server
    with open(local_filepath, 'rb') as file_obj:
        conn.storeFile(service_name=service_name, path=ftp_path, file_obj=file_obj, timeout=300)
        await sleep(1)

    # return file attributes
    return await get_ftp_file_attributes(conn, service_name, ftp_path)


async def get_ftp_file_attributes(conn: SMBConnection, service_name: str, ftp_path: str) -> SharedFile:
    """GET FTP file attributes.

    Args:
        conn (SMBConnection): SMB connection object
        service_name (str): FTP service name
        ftp_path (str): FTP directory path

    Returns:
        SharedFile: ftp file attribute info
    """

    return conn.getAttributes(service_name=service_name, path=ftp_path)


async def xlsx_to_tsv(
        s3_source_bucket: str,
        s3_source_key: str,
        s3_destination_bucket: str,
        s3_destination_key: str,
        delimiter: str='\t'
) -> Union[str, None]:
    """Convert xlsx file to csv/tsv file.

    Args:
        s3_source_bucket (str): source xlsx file s3 bucket
        s3_source_key (str): source xlsx file s3 key
        s3_destination_bucket (str): destination xlsx file s3 bucket
        s3_destination_key (str): destination xlsx file s3 key
        delimiter (str, optional): Delimiter. Defaults to '\t'.

    Returns:
        Union[str, None]: Error message during process else None
    """

    try:
        with NamedTemporaryFile(suffix='.xlsx') as tmp:
            await download_file(bucket=s3_source_bucket, filepath=tmp.name, s3_key=s3_source_key)
            records, _ = xlsx_to_records(tmp)

        await tsv_to_s3(records, delimiter, s3_destination_bucket, s3_destination_key)
    except Exception as err:
        print(err)
        return str(err)

    return None


async def zipfile_to_tsv(
        s3_source_bucket: str,
        s3_source_key: str,
        s3_destination_bucket: str,
        s3_destination_key: str,
        delimiter: str='\t'
) -> Union[str, None]:
    """Convert zipfile to csv/tsv file.

    Args:
        s3_source_bucket (str): source zipfile s3 bucket
        s3_source_key (str): source zipfile s3 key
        s3_destination_bucket (str): destination zipfile s3 bucket
        s3_destination_key (str): destination zipfile s3 key
        delimiter (str, optional): Delimiter. Defaults to '\t'.

    Returns:
        Union[str, None]: Error message during process else None
    """


    extensions = ['xlsx', 'txt', 'csv', 'tsv']
    records = []
    header = None

    with NamedTemporaryFile(suffix='.zip') as tmp:
        await download_file(bucket=s3_source_bucket, filepath=tmp.name, s3_key=s3_source_key)
        with TemporaryDirectory() as tmp_directory:
            for path in await unzip_file_get_filepaths(tmp.name, tmp_directory, include_extensions=extensions):
                ext = os.path.splitext(path)[1].lower()
                if ext == '.xlsx':
                    records_from_path, header = xlsx_to_records(path, header)
                    records.extend(records_from_path)
                else:
                    encoding = detect_encoding(path)
                    if encoding is None:
                        raise IOError(f"Failed to detect proper encoding for {path}")
                    encodings = [encoding] + [i for i in ['UTF-8', 'LATIN-1', 'UTF-16'] if i != encoding]
                    for encoding in encodings:
                        try:
                            detected_delimiter = detect_delimiter(path, encoding)
                            if detected_delimiter:
                                try:
                                    records_from_path, header = tsv_to_records(path, encoding, detected_delimiter, header)
                                    records.extend(records_from_path)
                                    break
                                except Exception as err:
                                    if str(err) == 'Every file must contain the exact same header':
                                        raise ValueError('Every file must contain the exact same header') from err
                                    continue
                        except Exception as err:
                            if str(err) == 'Every file must contain the exact same header':
                                raise ValueError('Every file must contain the exact same header') from err
                            continue
                    else:
                        raise IOError(f"Failed to detect proper encoding for {path}")

    await tsv_to_s3(records, delimiter, s3_destination_bucket, s3_destination_key)

    return None


def tsv_to_records(path: str, encoding: str, delimiter: str, header: str) -> tuple:
    """Translate the file data into 2-dimensional list for efficient
    processing.

    Args:
        path (str): Enrollment file path
        encoding (str): File encoding
        delimiter (str): Delimiter
        header (Union[str, None], optional): Header. Defaults to None.

    Returns:
        tuple: Records as list of lists, header
    """

    records = []
    with open(path, newline='', encoding=encoding) as csvfile:

        dialect = csv.Sniffer().sniff(csvfile.read(4096))
        csvfile.seek(0)

        # remove any null characters in the file
        reader = csv.reader((line.replace('\0', '') for line in csvfile), dialect=dialect, delimiter=delimiter, doublequote=True)
        for row in reader:

            if reader.line_num == 1:
                if header is None:
                    header = row
                elif header != row:
                    raise ValueError("Every file must contain the exact same header")
                else:
                    continue

            records.append(row)

    return records, header


def xlsx_to_records(filepath: str, header: Union[str, None]=None) -> tuple:
    """Load excel file to records object as list of lists.

    Args:
        filepath (str): Temporary Filepath
        header (Union[str, None], optional): Header. Defaults to None.

    Raises:
        ValueError: Excel sheets must contain the exact same header

    Returns:
        tuple: Records as list of lists, header
    """

    records = []
    workbook = load_workbook(filepath, read_only=True)
    for sheet in workbook:
        if sheet.title != 'hiddenSheet':
            sheet.calculate_dimension(force=True)

            for idx, row in enumerate(sheet.values):
                items = [str(value) if value is not None else "" for value in row]

                if idx == 0:
                    if header is None:
                        header = items
                    elif header != items:
                        raise ValueError("Excel sheets must contain the exact same header")
                    else:
                        continue

                records.append(items)
    workbook.close()

    return records, header


async def tsv_to_s3(records: str, delimiter: str, s3_bucket: str, s3_key: str):
    """Write records to tsv/csv file then upload to s3.

    Args:
        records (str): list of lists with values as strings
        delimiter (str): File delimiter
        s3_bucket (str): destination s3 bucket
        s3_key (str): destination s3 key
    """

    with NamedTemporaryFile(mode='w') as tmp:
        writer = csv.writer(tmp, delimiter=delimiter)
        writer.writerows(records)
        tmp.seek(0)
        await upload_file(bucket=s3_bucket, filepath=tmp.name, s3_key=s3_key)


def detect_encoding(path: str) -> str:
    """Detect enrollment file encoding.

    Args:
        path (str): Enrollment file path

    Returns:
        str: Enrollment file encoding
    """

    encoding = None
    with open(path, "rb") as handle:
        encoding_dict = chardet.detect(handle.read())
        if 'encoding' in encoding_dict:
            encoding = encoding_dict['encoding'].upper()

    return encoding


def detect_delimiter(path: str, encoding: str) -> str:
    """Detect enrollment file delimiter.

    Args:
        path (str): Enrollment file path
        encoding (str): File encoding

    Returns:
        str: Delimiter
    """

    delimiter = ''
    with open(path, newline='', encoding=encoding) as csvfile:
        data = csvfile.read(4096)
        count = -1
        for item in [',', '\t', '|']:
            char_count = data.count(item)
            if char_count > count:
                delimiter = item
                count = char_count

    return delimiter
