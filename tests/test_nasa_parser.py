"""Tests for NASA GCMD parser."""

import pytest
from src.parsers.nasa_gcmd import _parse_csv_text, _detect_columns, _build_full_path


SAMPLE_SCIENCE_CSV = '''"Keyword Version: 23.7","Revision: 2024-11-04 10:00:00","Terms Of Use"
"Category","Topic","Term","Variable_Level_1","Variable_Level_2","Variable_Level_3","Detailed_Variable","UUID"
"EARTH SCIENCE","AGRICULTURE","","","","","","uuid-topic-ag"
"EARTH SCIENCE","AGRICULTURE","AGRICULTURAL AQUATIC SCIENCES","","","","","uuid-term-aas"
"EARTH SCIENCE","AGRICULTURE","AGRICULTURAL AQUATIC SCIENCES","AQUACULTURE","","","","uuid-var1-aq"
"EARTH SCIENCE","ATMOSPHERE","","","","","","uuid-topic-atm"
"EARTH SCIENCE","ATMOSPHERE","AEROSOLS","","","","","uuid-term-aer"
"EARTH SCIENCE","ATMOSPHERE","AEROSOLS","AEROSOL OPTICAL DEPTH/THICKNESS","","","","uuid-var1-aod"
'''

SAMPLE_INSTRUMENTS_CSV = '''"Keyword Version: 23.7","Revision: 2024-11-04","Terms Of Use"
"Category","Class","Type","Subtype","Short_Name","Long_Name","UUID"
"Earth Remote Sensing Instruments","Active Remote Sensing","Altimeters","Lidar/Laser Altimeters","ATM","Airborne Topographic Mapper","uuid-inst-atm"
"Earth Remote Sensing Instruments","Active Remote Sensing","Altimeters","","","","uuid-inst-alt"
'''


class TestDetectColumns:
    def test_science_keywords(self):
        headers = ["Category", "Topic", "Term", "Variable_Level_1", "UUID"]
        hier, uid, sn, ln = _detect_columns(headers)
        assert uid == "UUID"
        assert sn is None
        assert ln is None
        assert "UUID" not in hier
        assert hier == ["Category", "Topic", "Term", "Variable_Level_1"]

    def test_instruments(self):
        headers = ["Category", "Class", "Type", "Subtype", "Short_Name", "Long_Name", "UUID"]
        hier, uid, sn, ln = _detect_columns(headers)
        assert uid == "UUID"
        assert sn == "Short_Name"
        assert ln == "Long_Name"
        assert hier == ["Category", "Class", "Type", "Subtype"]


class TestParseCsv:
    def test_science_keywords_count(self):
        records = list(_parse_csv_text(SAMPLE_SCIENCE_CSV, "sciencekeywords"))
        assert len(records) == 7  # 6 from CSV + 1 synthetic root (EARTH SCIENCE)

    def test_hierarchy_levels(self):
        records = list(_parse_csv_text(SAMPLE_SCIENCE_CSV, "sciencekeywords"))
        by_label = {r["label"]: r for r in records}
        assert by_label["AGRICULTURE"]["level"] == 1
        assert by_label["AGRICULTURAL AQUATIC SCIENCES"]["level"] == 2
        assert by_label["AQUACULTURE"]["level"] == 3

    def test_full_path(self):
        records = list(_parse_csv_text(SAMPLE_SCIENCE_CSV, "sciencekeywords"))
        by_label = {r["label"]: r for r in records}
        assert by_label["AEROSOL OPTICAL DEPTH/THICKNESS"]["full_path"] == \
            "EARTH SCIENCE > ATMOSPHERE > AEROSOLS > AEROSOL OPTICAL DEPTH/THICKNESS"

    def test_uuid_preserved(self):
        records = list(_parse_csv_text(SAMPLE_SCIENCE_CSV, "sciencekeywords"))
        by_label = {r["label"]: r for r in records}
        assert by_label["AEROSOLS"]["id"] == "uuid-term-aer"

    def test_parent_id_set(self):
        records = list(_parse_csv_text(SAMPLE_SCIENCE_CSV, "sciencekeywords"))
        by_label = {r["label"]: r for r in records}
        assert by_label["EARTH SCIENCE"]["parent_id"] is None
        assert by_label["AGRICULTURE"]["parent_id"] is not None

    def test_instruments_aliases(self):
        records = list(_parse_csv_text(SAMPLE_INSTRUMENTS_CSV, "instruments"))
        by_label = {r["label"]: r for r in records}
        atm = by_label.get("Lidar/Laser Altimeters") or by_label.get("ATM")
        found = [r for r in records if "ATM" in (r.get("aliases") or [])
                 or "Airborne Topographic Mapper" in (r.get("aliases") or [])]
        assert len(found) > 0

    def test_source_type_set(self):
        records = list(_parse_csv_text(SAMPLE_SCIENCE_CSV, "sciencekeywords"))
        assert all(r["type"] == "sciencekeywords" for r in records)

    def test_no_empty_labels(self):
        records = list(_parse_csv_text(SAMPLE_SCIENCE_CSV, "sciencekeywords"))
        assert all(r["label"] for r in records)

    def test_deduplication(self):
        duped_csv = SAMPLE_SCIENCE_CSV + \
            '"EARTH SCIENCE","AGRICULTURE","","","","","","uuid-topic-ag"\n'
        records = list(_parse_csv_text(duped_csv, "sciencekeywords"))
        paths = [r["full_path"] for r in records]
        assert len(paths) == len(set(paths))
