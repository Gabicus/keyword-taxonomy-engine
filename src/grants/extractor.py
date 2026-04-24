"""Grant and funding number extractor.

Extracts grant/funding/agreement/project numbers from free-text fields
using agency-specific regex patterns. Each agency has distinct numbering
conventions that make regex extraction reliable.
"""

import re
from dataclasses import dataclass


@dataclass
class GrantMatch:
    number: str
    agency: str
    pattern_name: str
    start: int
    end: int


# Agency-specific patterns
# Each tuple: (pattern_name, agency, compiled_regex)
PATTERNS = [
    # NSF: 7-digit numbers, sometimes with prefix
    ("NSF Award", "NSF", re.compile(
        r'\b(?:NSF[- ]?)?(?:Award\s*(?:No\.?\s*)?)?(\d{7})\b'
    )),

    # NIH: mechanism-institute-serial (e.g., R01-GM-123456, R01GM123456)
    ("NIH Grant", "NIH", re.compile(
        r'\b([RKUPFTSDM]\d{2})\s*[-]?\s*([A-Z]{2})\s*[-]?\s*(\d{6,7})\b'
    )),

    # DOE: DE-XX00-00XXXXX format
    ("DOE Contract", "DOE", re.compile(
        r'\b(DE-[A-Z]{2}\d{2}-\d{2}[A-Z]{2}\d{5})\b'
    )),

    # DOE OSTI: shorter DOE format
    ("DOE Award", "DOE", re.compile(
        r'\b(DE-[A-Z]{2,4}\d{7,10})\b'
    )),

    # NASA: NNX/NNG/NNH followed by digits and letters
    ("NASA Grant", "NASA", re.compile(
        r'\b(NN[XGHM]\d{2}[A-Z]{2}\d{2,5}[A-Z]?)\b'
    )),

    # NASA newer format: 80NSSC followed by digits
    ("NASA Award", "NASA", re.compile(
        r'\b(80NSSC\d{2}[A-Z]\d{4})\b'
    )),

    # EU Horizon/FP7: 6-digit grant numbers
    ("EU Grant", "EU", re.compile(
        r'\b(?:grant\s*(?:agreement\s*)?(?:no\.?\s*)?|project\s*(?:no\.?\s*)?)(\d{6})\b',
        re.IGNORECASE,
    )),

    # EPSRC (UK): EP/ followed by reference
    ("EPSRC Grant", "EPSRC", re.compile(
        r'\b(EP/[A-Z]\d{6}/\d)\b'
    )),

    # DFG (Germany): project numbers
    ("DFG Project", "DFG", re.compile(
        r'\b(?:DFG|Deutsche\s+Forschungsgemeinschaft)\s*[-:]\s*(\d{6,9})\b',
        re.IGNORECASE,
    )),

    # NSERC (Canada)
    ("NSERC Grant", "NSERC", re.compile(
        r'\b(?:NSERC)\s*[-]?\s*(?:grant\s*)?(\d{4,6}[-]?\d{0,4})\b',
        re.IGNORECASE,
    )),

    # Generic: "grant number", "award number", "project number" followed by alphanumeric
    ("Generic Grant", "Unknown", re.compile(
        r'(?:grant|award|funding|project|agreement|contract)\s*(?:number|no\.?|#|id)\s*[:.]?\s*([A-Z0-9][-A-Z0-9/]{4,20})',
        re.IGNORECASE,
    )),

    # Generic: "funded by ... (NUMBER)" pattern
    ("Funded Reference", "Unknown", re.compile(
        r'funded\s+by\s+.*?\(([A-Z0-9][-A-Z0-9/]{4,20})\)',
        re.IGNORECASE,
    )),
]


def extract_grant_numbers(text: str) -> list[GrantMatch]:
    """Extract all grant/funding numbers from text.

    Returns list of GrantMatch objects sorted by position in text.
    Deduplicates by normalized number.
    """
    if not text:
        return []

    matches = []
    seen = set()

    for pattern_name, agency, regex in PATTERNS:
        for m in regex.finditer(text):
            if agency == "NIH" and len(m.groups()) == 3:
                number = f"{m.group(1)}{m.group(2)}{m.group(3)}"
            else:
                number = m.group(1) if m.lastindex else m.group(0)

            normalized = number.strip().upper().replace(" ", "").replace("-", "")
            if normalized in seen:
                continue
            seen.add(normalized)

            matches.append(GrantMatch(
                number=number,
                agency=agency,
                pattern_name=pattern_name,
                start=m.start(),
                end=m.end(),
            ))

    matches.sort(key=lambda m: m.start)
    return matches
