"""Tests for grant number extractor."""

import pytest
from src.grants.extractor import extract_grant_numbers


class TestNSF:
    def test_plain_number(self):
        matches = extract_grant_numbers("Funded by NSF 1234567")
        assert len(matches) == 1
        assert matches[0].number == "1234567"
        assert matches[0].agency == "NSF"

    def test_award_prefix(self):
        matches = extract_grant_numbers("NSF Award No. 2345678")
        assert matches[0].number == "2345678"


class TestNIH:
    def test_standard_format(self):
        matches = extract_grant_numbers("Supported by R01GM123456")
        assert len(matches) == 1
        assert matches[0].agency == "NIH"
        assert "GM" in matches[0].number

    def test_with_dashes(self):
        matches = extract_grant_numbers("NIH grant R01-CA-234567")
        assert matches[0].agency == "NIH"


class TestDOE:
    def test_contract_format(self):
        matches = extract_grant_numbers("Contract DE-AC02-05CH11231")
        assert len(matches) == 1
        assert matches[0].agency == "DOE"
        assert matches[0].number == "DE-AC02-05CH11231"


class TestNASA:
    def test_nnx_format(self):
        matches = extract_grant_numbers("NASA grant NNX14AH35G")
        assert len(matches) == 1
        assert matches[0].agency == "NASA"

    def test_80nssc_format(self):
        matches = extract_grant_numbers("Award 80NSSC18K1234")
        assert matches[0].agency == "NASA"


class TestEU:
    def test_grant_agreement(self):
        matches = extract_grant_numbers("EU grant agreement no. 654321")
        assert len(matches) == 1
        assert matches[0].agency == "EU"
        assert matches[0].number == "654321"


class TestEPSRC:
    def test_standard_format(self):
        matches = extract_grant_numbers("EPSRC grant EP/A123456/1")
        assert len(matches) == 1
        assert matches[0].agency == "EPSRC"


class TestGeneric:
    def test_grant_number_pattern(self):
        matches = extract_grant_numbers("Grant number: ABC-12345-XY")
        assert len(matches) >= 1
        assert any("ABC" in m.number for m in matches)

    def test_funded_by_pattern(self):
        matches = extract_grant_numbers("This work was funded by DARPA (HR0011-20-C-0023)")
        assert len(matches) >= 1


class TestEdgeCases:
    def test_empty_text(self):
        assert extract_grant_numbers("") == []

    def test_no_grants(self):
        assert extract_grant_numbers("The weather is nice today.") == []

    def test_multiple_grants(self):
        text = "Funded by NSF 1234567 and NIH R01GM654321 and DOE DE-AC02-05CH11231"
        matches = extract_grant_numbers(text)
        agencies = {m.agency for m in matches}
        assert "NSF" in agencies
        assert "NIH" in agencies
        assert "DOE" in agencies

    def test_deduplication(self):
        text = "Grant 1234567 and grant 1234567 again"
        matches = extract_grant_numbers(text)
        numbers = [m.number for m in matches]
        assert len(set(numbers)) == len(numbers)

    def test_sorted_by_position(self):
        text = "NIH R01GM123456 then NSF 7654321"
        matches = extract_grant_numbers(text)
        assert matches[0].start < matches[1].start
