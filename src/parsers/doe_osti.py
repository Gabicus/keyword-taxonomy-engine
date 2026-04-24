"""DOE OSTI Subject Categories parser.

Fetches energy research subject categories from the DOE OSTI system.
Falls back to a hardcoded list of known categories if the API is unavailable.
"""

import logging
import uuid

from ..config import SOURCES
from ..http_client import get_session

logger = logging.getLogger(__name__)

OSTI_BASE = SOURCES["doe_osti"]["url"]

# Official DOE OSTI subject categories with codes.
# Source: https://www.osti.gov/subject-categories
# These are grouped by broad area. Codes 01-99 with sub-groups.
FALLBACK_CATEGORIES = [
    # Coal, Lignite, and Peat
    ("01", "Coal, Lignite, and Peat", "Coal preparation, coal mining, properties of coal, lignite, and peat"),
    # Petroleum
    ("02", "Petroleum", "Crude oil, petroleum products, oil shale, tar sands, natural gas"),
    # Natural Gas
    ("03", "Natural Gas", "Natural gas exploration, production, processing, and utilization"),
    # Oil Shales and Tar Sands
    ("04", "Oil Shales and Tar Sands", "Oil shale and tar sand resources, extraction, and processing"),
    # Synthetic Fuels
    ("05", "Synthetic Fuels", "Synthetic fuel production from coal, biomass, and other sources"),
    # Hydrogen
    ("08", "Hydrogen", "Hydrogen production, storage, transport, and utilization as fuel"),
    # Biomass Fuels
    ("09", "Biomass Fuels", "Biomass energy sources, biofuels, biogas, and biomass conversion"),
    # Solar Energy
    ("14", "Solar Energy", "Solar thermal, photovoltaic, and passive solar energy systems"),
    # Geothermal Energy
    ("15", "Geothermal Energy", "Geothermal resource assessment, exploration, and energy conversion"),
    # Tidal and Wave Power
    ("16", "Tidal and Wave Power", "Ocean thermal, tidal, wave, and current energy conversion"),
    # Wind Energy
    ("17", "Wind Energy", "Wind resource assessment, wind turbine design, and wind energy systems"),
    # Nuclear Fission — Power Reactors
    ("21", "Specific Nuclear Reactors and Associated Plants", "Design, operation, and safety of specific nuclear reactor types"),
    ("22", "General Studies of Nuclear Reactors", "General nuclear reactor theory, design, and engineering"),
    # Nuclear Fuel Cycle
    ("24", "Nuclear Fuel Cycle and Fuel Materials", "Nuclear fuel fabrication, reprocessing, and fuel cycle analysis"),
    # Energy Storage
    ("25", "Energy Storage", "Batteries, flywheels, compressed air, thermal, and other storage systems"),
    # Energy Conservation
    ("29", "Energy Planning, Policy, and Economy", "Energy policy, planning, economics, and conservation programs"),
    ("30", "Direct Energy Conversion", "Thermoelectric, thermionic, magnetohydrodynamic, and fuel cells"),
    ("32", "Energy Conservation, Consumption, and Utilization", "Energy efficiency, end-use analysis, and demand management"),
    # Nuclear Physics
    ("33", "Advanced Propulsion Systems", "Nuclear and advanced propulsion for aerospace applications"),
    # Physics
    ("36", "Materials Science", "Properties and behavior of materials, metallurgy, and ceramics"),
    ("37", "Inorganic, Organic, Physical, and Analytical Chemistry", "Chemical research supporting energy applications"),
    ("38", "Radiation Sciences", "Radiation physics, dosimetry, and radiation protection"),
    ("40", "Nuclear Fuels", "Nuclear fuel materials science and engineering"),
    ("42", "Engineering", "Engineering research supporting energy technologies"),
    ("43", "Particle Accelerators", "Design and operation of particle accelerators"),
    ("45", "Military Technology, Weaponry, and National Defense", "Nuclear weapons and defense-related energy research"),
    ("46", "Instrumentation Related to Nuclear Science and Technology", "Nuclear instrumentation and measurement"),
    ("47", "Other Instrumentation", "General scientific instrumentation"),
    # Nuclear Physics Research
    ("54", "Environmental Sciences", "Environmental monitoring, ecology, and atmospheric sciences"),
    ("55", "Biology and Medicine", "Radiation biology, health effects, and biomedical research"),
    ("56", "Biology and Medicine — Human Genome Studies", "Genomics and genetics research"),
    ("58", "Geosciences", "Geology, hydrology, and earth sciences"),
    ("59", "Basic Biological Sciences", "Fundamental biology research"),
    # Mathematics and Computing
    ("60", "Applied Life Sciences", "Applied biology and biotechnology"),
    ("61", "Radiation Protection and Dosimetry", "Radiation protection standards and dosimetry methods"),
    ("62", "Radiology and Nuclear Medicine", "Medical applications of radiation and nuclear techniques"),
    ("63", "Radiation, Thermal, and Other Environmental Pollutant Effects on Living Organisms", "Biological effects of pollutants"),
    ("65", "Radioactive Waste Management", "Nuclear waste treatment, storage, and disposal"),
    ("66", "Physics", "General and applied physics research"),
    ("70", "Plasma Physics and Fusion Technology", "Controlled fusion research and plasma physics"),
    ("71", "Classical and Quantum Mechanics, General Physics", "Theoretical physics research"),
    ("72", "Physics of Elementary Particles and Fields", "High energy physics and particle physics"),
    ("73", "Nuclear Physics and Radiation Physics", "Nuclear structure, reactions, and radiation physics"),
    ("74", "Atomic and Molecular Physics", "Atomic structure, spectroscopy, and molecular physics"),
    ("75", "Condensed Matter Physics, Superconductivity, and Superfluidity", "Solid state physics and superconductivity"),
    ("77", "Nanoscience and Nanotechnology", "Nanomaterials, nanodevices, and nanotechnology"),
    ("79", "Astronomy and Astrophysics", "Astrophysics, cosmology, and space science"),
    ("97", "Mathematics and Computing", "Applied mathematics, computer science, and modeling"),
    ("98", "Nuclear Disarmament, Safeguards, and Physical Protection", "Arms control, safeguards, and security"),
    ("99", "General and Miscellaneous", "Topics not covered by other categories"),
]

# Group parents for hierarchical organization
CATEGORY_GROUPS = {
    "fossil_fuels": {
        "label": "Fossil Fuels",
        "codes": ["01", "02", "03", "04", "05"],
    },
    "alternative_fuels": {
        "label": "Alternative Fuels",
        "codes": ["08", "09"],
    },
    "renewable_energy": {
        "label": "Renewable Energy",
        "codes": ["14", "15", "16", "17"],
    },
    "nuclear_energy": {
        "label": "Nuclear Energy",
        "codes": ["21", "22", "24", "40"],
    },
    "energy_systems": {
        "label": "Energy Systems and Policy",
        "codes": ["25", "29", "30", "32", "33"],
    },
    "physical_sciences": {
        "label": "Physical Sciences",
        "codes": ["36", "37", "38", "42", "43", "46", "47"],
    },
    "life_sciences": {
        "label": "Life and Environmental Sciences",
        "codes": ["54", "55", "56", "58", "59", "60", "61", "62", "63"],
    },
    "physics": {
        "label": "Physics",
        "codes": ["66", "70", "71", "72", "73", "74", "75", "77", "79"],
    },
    "other": {
        "label": "Other",
        "codes": ["45", "65", "97", "98", "99"],
    },
}


def _build_code_to_group() -> dict[str, str]:
    """Map category codes to their group IDs."""
    mapping = {}
    for group_id, group in CATEGORY_GROUPS.items():
        for code in group["codes"]:
            mapping[code] = group_id
    return mapping


def _try_fetch_api(session) -> list[dict] | None:
    """Attempt to fetch categories from OSTI API. Returns None if unavailable."""
    urls_to_try = [
        "https://www.osti.gov/api/v1/subject-categories",
        "https://www.osti.gov/api/v1/records?subject_categories=true",
    ]
    for url in urls_to_try:
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list) and len(data) > 0:
                    return data
            logger.info("OSTI API %s returned status %d", url, resp.status_code)
        except Exception as e:
            logger.info("OSTI API %s failed: %s", url, e)
    return None


def _try_scrape_page(session) -> list[tuple[str, str, str]] | None:
    """Attempt to scrape categories from the OSTI website. Returns None if unavailable."""
    url = "https://www.osti.gov/subject-categories"
    try:
        resp = session.get(url, timeout=30)
        if resp.status_code != 200:
            return None

        # Simple extraction — look for patterns like "01 - Category Name"
        import re
        text = resp.text
        # Match patterns like "01 - Coal, Lignite, and Peat" or "01. Coal..."
        matches = re.findall(
            r'(\d{2})\s*[-–.]\s*([^<\n]+)',
            text,
        )
        if matches and len(matches) > 10:
            return [(code.strip(), name.strip(), "") for code, name in matches]
    except Exception as e:
        logger.info("OSTI scrape failed: %s", e)
    return None


def _build_records_from_tuples(categories: list[tuple[str, str, str]]) -> list[dict]:
    """Convert (code, label, definition) tuples into unified schema records."""
    code_to_group = _build_code_to_group()
    records = []

    # Create group parent records
    group_records = {}
    for group_id, group in CATEGORY_GROUPS.items():
        gid = f"osti-group-{group_id}"
        group_records[group_id] = gid
        records.append({
            "id": gid,
            "label": group["label"],
            "definition": None,
            "parent_id": None,
            "type": "subject_category",
            "uri": "https://www.osti.gov/subject-categories",
            "full_path": group["label"],
            "level": 0,
            "aliases": [],
            "cross_refs": [],
            "version": None,
        })

    # Create category records
    for code, label, definition in categories:
        cat_id = f"osti-{code}"
        group_id = code_to_group.get(code)
        parent_id = group_records.get(group_id) if group_id else None
        parent_label = CATEGORY_GROUPS[group_id]["label"] if group_id else None
        full_path = f"{parent_label} > {label}" if parent_label else label
        level = 1 if parent_id else 0

        records.append({
            "id": cat_id,
            "label": label,
            "definition": definition or None,
            "parent_id": parent_id,
            "type": "subject_category",
            "uri": f"https://www.osti.gov/subject-categories#{code}",
            "full_path": full_path,
            "level": level,
            "aliases": [f"OSTI {code}", f"DOE Subject {code}"],
            "cross_refs": [],
            "version": None,
        })

    return records


def parse_doe_osti(session=None) -> list[dict]:
    """Fetch and parse DOE OSTI subject categories.

    Tries the OSTI API first, then scraping, then falls back to
    a hardcoded list of known categories.

    Args:
        session: Optional requests session (created if not provided).

    Returns:
        List of unified schema records.
    """
    session = session or get_session(cache_name="osti_cache")

    # Try API first
    api_data = _try_fetch_api(session)
    if api_data:
        print("  OSTI: fetched categories from API")
        categories = []
        for item in api_data:
            code = str(item.get("code", item.get("id", ""))).zfill(2)
            label = item.get("name", item.get("label", ""))
            defn = item.get("description", "")
            if code and label:
                categories.append((code, label, defn))
        if categories:
            records = _build_records_from_tuples(categories)
            print(f"  OSTI total: {len(records)} subject categories")
            return records

    # Try scraping
    scraped = _try_scrape_page(session)
    if scraped:
        print("  OSTI: scraped categories from website")
        records = _build_records_from_tuples(scraped)
        print(f"  OSTI total: {len(records)} subject categories")
        return records

    # Fallback to hardcoded list
    print("  OSTI: using fallback category list")
    records = _build_records_from_tuples(FALLBACK_CATEGORIES)
    print(f"  OSTI total: {len(records)} subject categories (fallback)")
    return records
