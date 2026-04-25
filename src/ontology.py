"""Ontology layer — disciplines, hierarchy envelopes, lenses, senses.

Populates the multi-perspective ontology tables from existing data.
"""

import json
import logging
import uuid
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# =============================================================================
# 14 Primary Disciplines
# =============================================================================

DISCIPLINES = [
    ("fossil_energy", "Fossil Energy & Carbon", "Coal, oil, natural gas, CCUS, gasification, combustion, carbon management", None, 1, 1),
    ("coal_science", "Coal Science & Technology", "Coal chemistry, coal-to-liquids, coal upgrading, ash utilization, coal combustion", None, 1, 2),
    ("natural_gas", "Natural Gas & Unconventional", "Shale gas, methane, LNG, hydraulic fracturing, unconventional resources, methane hydrates", None, 1, 3),
    ("materials", "Materials & Manufacturing", "Alloys, ceramics, nanomaterials, corrosion, coatings, extreme environment materials", None, 2, 4),
    ("chemical_sciences", "Chemical Sciences", "Catalysis, thermodynamics, electrochemistry, reaction kinetics, molecular chemistry", None, 2, 5),
    ("earth_environmental", "Earth & Environmental", "Geology, hydrology, remediation, climate science, geochemistry, subsurface", None, 2, 6),
    ("computation_data", "Computation & Data", "CFD, machine learning, simulation, HPC, data analytics, numerical methods", None, 2, 7),
    ("nuclear_particle", "Nuclear & Particle", "Fission, fusion, accelerators, isotopes, radiation, nuclear engineering", None, 3, 8),
    ("biological_medical", "Biological & Medical", "Genomics, pharmacology, toxicology, biomedical engineering, ecology", None, 3, 9),
    ("ee_me_engineering", "Electrical & Mechanical Engineering", "Turbines, sensors, power systems, controls, instrumentation, heat transfer", None, 2, 10),
    ("policy_economics", "Policy & Economics", "Regulation, funding, workforce development, cost analysis, energy policy", None, 3, 11),
    ("renewable_alternative", "Renewable & Alternative Energy", "Solar, wind, hydrogen, geothermal, energy storage, grid integration", None, 3, 12),
    ("space_atmospheric", "Space & Atmospheric", "Remote sensing, satellite, climate modeling, atmospheric chemistry, weather", None, 3, 13),
    ("math_physics", "Mathematics & Physics Fundamentals", "Quantum mechanics, fluid dynamics, statistical mechanics, thermophysics", None, 4, 14),
]


def populate_disciplines(conn) -> int:
    """Insert the 14 primary disciplines."""
    conn.execute("DELETE FROM disciplines")
    conn.executemany(
        """INSERT INTO disciplines
           (discipline_id, name, description, parent_id, tier, sort_order)
           VALUES (?, ?, ?, ?, ?, ?)""",
        DISCIPLINES,
    )
    count = conn.execute("SELECT COUNT(*) FROM disciplines").fetchone()[0]
    logger.info(f"Populated {count} disciplines")
    return count


# =============================================================================
# Hierarchy Envelope Builder
# =============================================================================

def _make_envelope_id(envelope_name: str, label: str, level: int) -> str:
    """Deterministic envelope node ID from name + label + level."""
    slug = label.lower().replace(" ", "_").replace("&", "and")[:50]
    return f"{envelope_name}:L{level}:{slug}"


def populate_netl_envelope(conn, version: str = "NETL_pre2026") -> int:
    """Build hierarchy envelope from raw_wos_netl_tech (Tab 3).

    Extracts the Program Area → Sub-Program → Technology Area → Turbines Sub-Tech
    hierarchy and stores it as a versioned envelope.
    """
    envelope_name = "NETL_org"

    conn.execute(
        "DELETE FROM hierarchy_envelopes WHERE envelope_name = ? AND version = ?",
        [envelope_name, version],
    )

    programs = conn.execute("""
        SELECT DISTINCT program_area FROM raw_wos_netl_tech
        WHERE program_area IS NOT NULL
        ORDER BY program_area
    """).fetchall()

    nodes = []
    now = datetime.now(timezone.utc).isoformat()

    for (prog,) in programs:
        prog_id = _make_envelope_id(envelope_name, prog, 0)
        pub_count = conn.execute(
            "SELECT COUNT(*) FROM raw_wos_netl_tech WHERE program_area = ?", [prog]
        ).fetchone()[0]
        nodes.append((
            prog_id, envelope_name, version, prog, None, 0,
            prog, "program", json.dumps({"pub_count": pub_count}),
            True, None,
        ))

        subs = conn.execute("""
            SELECT DISTINCT sub_program_area FROM raw_wos_netl_tech
            WHERE program_area = ? AND sub_program_area IS NOT NULL
            ORDER BY sub_program_area
        """, [prog]).fetchall()

        for (sub,) in subs:
            sub_id = _make_envelope_id(envelope_name, sub, 1)
            sub_count = conn.execute(
                "SELECT COUNT(*) FROM raw_wos_netl_tech WHERE program_area = ? AND sub_program_area = ?",
                [prog, sub],
            ).fetchone()[0]
            nodes.append((
                sub_id, envelope_name, version, sub, prog_id, 1,
                f"{prog} > {sub}", "sub_program",
                json.dumps({"pub_count": sub_count}),
                True, None,
            ))

            techs = conn.execute("""
                SELECT DISTINCT technology_area FROM raw_wos_netl_tech
                WHERE program_area = ? AND sub_program_area = ? AND technology_area IS NOT NULL
                ORDER BY technology_area
            """, [prog, sub]).fetchall()

            for (tech,) in techs:
                tech_id = _make_envelope_id(envelope_name, tech, 2)
                tech_count = conn.execute(
                    "SELECT COUNT(*) FROM raw_wos_netl_tech WHERE program_area = ? AND sub_program_area = ? AND technology_area = ?",
                    [prog, sub, tech],
                ).fetchone()[0]
                full_path = f"{prog} > {sub} > {tech}"
                nodes.append((
                    tech_id, envelope_name, version, tech, sub_id, 2,
                    full_path, "technology_area",
                    json.dumps({"pub_count": tech_count}),
                    True, None,
                ))

                if tech in ("Advanced Turbines", "Hydrogen Turbines"):
                    turb_subs = conn.execute("""
                        SELECT DISTINCT turbines_sub_tech FROM raw_wos_netl_tech
                        WHERE technology_area = ? AND turbines_sub_tech IS NOT NULL
                        ORDER BY turbines_sub_tech
                    """, [tech]).fetchall()

                    for (tsub,) in turb_subs:
                        tsub_id = _make_envelope_id(envelope_name, f"{tech}_{tsub}", 3)
                        tsub_count = conn.execute(
                            "SELECT COUNT(*) FROM raw_wos_netl_tech WHERE technology_area = ? AND turbines_sub_tech = ?",
                            [tech, tsub],
                        ).fetchone()[0]
                        nodes.append((
                            tsub_id, envelope_name, version, tsub, tech_id, 3,
                            f"{full_path} > {tsub}", "sub_technology",
                            json.dumps({"pub_count": tsub_count}),
                            True, None,
                        ))

    # Deduplicate by (envelope_id, envelope_name, version)
    seen = set()
    unique_nodes = []
    for n in nodes:
        key = (n[0], n[1], n[2])
        if key not in seen:
            seen.add(key)
            unique_nodes.append(n)

    conn.executemany(
        """INSERT INTO hierarchy_envelopes
           (envelope_id, envelope_name, version, node_label, parent_id, level,
            full_path, node_type, metadata, active, superseded_by)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        unique_nodes,
    )

    count = conn.execute(
        "SELECT COUNT(*) FROM hierarchy_envelopes WHERE envelope_name = ? AND version = ?",
        [envelope_name, version],
    ).fetchone()[0]
    logger.info(f"Populated NETL envelope: {count} nodes")
    return count


# =============================================================================
# Template Hat Builder
# =============================================================================

ROLE_WEIGHT_PROFILES = {
    "director": {"strategic": 0.95, "budget": 0.9, "congressional": 0.85, "cross_domain": 0.8, "technical_detail": 0.3},
    "program_mgr": {"strategic": 0.6, "tech_readiness": 0.9, "milestones": 0.85, "team": 0.8, "technical_detail": 0.6},
    "researcher": {"technical_detail": 0.95, "methods": 0.9, "publications": 0.85, "measurements": 0.8, "strategic": 0.2},
}

ALTITUDE_MAP = {"director": 100000, "program_mgr": 10000, "researcher": 1000}


def populate_template_hats(conn) -> int:
    """Generate the ~87 template hats (14 disciplines × 3 altitudes + intersections)."""
    conn.execute("DELETE FROM ontology_lenses WHERE is_template = true")

    disciplines = conn.execute(
        "SELECT discipline_id, name, tier FROM disciplines ORDER BY sort_order"
    ).fetchall()

    hats = []

    for disc_id, disc_name, tier in disciplines:
        for role, altitude in ALTITUDE_MAP.items():
            role_label = {"director": "Director", "program_mgr": "Program Manager", "researcher": "Researcher"}[role]
            hat_id = f"hat:{disc_id}:{role}"
            name = f"{disc_name} {role_label}"

            base_weight = {d[0]: 0.1 for d in disciplines}
            base_weight[disc_id] = 0.95
            for d_id, _, d_tier in disciplines:
                if d_tier == tier and d_id != disc_id:
                    base_weight[d_id] = max(base_weight[d_id], 0.4)
                elif abs(d_tier - tier) == 1:
                    base_weight[d_id] = max(base_weight[d_id], 0.25)

            hats.append((
                hat_id, name,
                f"{role_label}-level view of {disc_name}",
                True, altitude, role, None, None, None,
                disc_id, [],
                json.dumps(base_weight),
                json.dumps({}),
                json.dumps(ROLE_WEIGHT_PROFILES[role]),
                "system",
            ))

    INTERSECTIONS = [
        ("fossil_energy", "materials", "Fossil-Materials"),
        ("fossil_energy", "computation_data", "Fossil-Computation"),
        ("fossil_energy", "earth_environmental", "Fossil-Earth"),
        ("fossil_energy", "chemical_sciences", "Fossil-Chemistry"),
        ("fossil_energy", "ee_me_engineering", "Fossil-Engineering"),
        ("fossil_energy", "renewable_alternative", "Fossil-Renewable Transition"),
        ("fossil_energy", "policy_economics", "Fossil-Policy"),
        ("coal_science", "chemical_sciences", "Coal-Chemistry"),
        ("coal_science", "earth_environmental", "Coal-Earth"),
        ("natural_gas", "earth_environmental", "NatGas-Earth"),
        ("natural_gas", "nuclear_particle", "NatGas-Nuclear Hybrid"),
        ("materials", "computation_data", "Materials-Computation"),
        ("chemical_sciences", "biological_medical", "Chemistry-Biology"),
        ("earth_environmental", "biological_medical", "Earth-Biology"),
        ("nuclear_particle", "materials", "Nuclear-Materials"),
        ("computation_data", "earth_environmental", "Computation-Earth"),
        ("ee_me_engineering", "computation_data", "Engineering-Computation"),
        ("renewable_alternative", "materials", "Renewable-Materials"),
    ]

    for disc_a, disc_b, label in INTERSECTIONS:
        for role, altitude in ALTITUDE_MAP.items():
            role_label = {"director": "Director", "program_mgr": "Program Manager", "researcher": "Researcher"}[role]
            hat_id = f"hat:{disc_a}+{disc_b}:{role}"
            name = f"{label} {role_label}"

            base_weight = {d[0]: 0.1 for d in disciplines}
            base_weight[disc_a] = 0.9
            base_weight[disc_b] = 0.85

            hats.append((
                hat_id, name,
                f"{role_label}-level view at intersection of {disc_a} and {disc_b}",
                True, altitude, role, None, None, None,
                disc_a, [disc_b],
                json.dumps(base_weight),
                json.dumps({"cross_domain": 0.9}),
                json.dumps(ROLE_WEIGHT_PROFILES[role]),
                "system",
            ))

    neutral = (
        "hat:neutral", "Neutral Lens",
        "Unweighted baseline — all disciplines and roles equal",
        True, 10000, "neutral", None, None, None,
        None, [],
        json.dumps({d[0]: 1.0 for d in disciplines}),
        json.dumps({}),
        json.dumps({"all": 1.0}),
        "system",
    )
    hats.append(neutral)

    conn.executemany(
        """INSERT INTO ontology_lenses
           (lens_id, name, description, is_template, altitude, role_type,
            org_envelope, org_version, org_node_id,
            discipline_primary, disciplines_secondary,
            discipline_weights, interest_weights, role_weights, created_by)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        hats,
    )

    count = conn.execute(
        "SELECT COUNT(*) FROM ontology_lenses WHERE is_template = true"
    ).fetchone()[0]
    logger.info(f"Populated {count} template hats")
    return count


# =============================================================================
# Discipline Mapping Rules
# =============================================================================

OPENALEX_FIELD_TO_DISCIPLINE = {
    "Energy": "fossil_energy",
    "Chemical Engineering": "chemical_sciences",
    "Chemistry": "chemical_sciences",
    "Computer Science": "computation_data",
    "Earth and Planetary Sciences": "earth_environmental",
    "Engineering": "ee_me_engineering",
    "Environmental Science": "earth_environmental",
    "Materials Science": "materials",
    "Mathematics": "math_physics",
    "Physics and Astronomy": "math_physics",
    "Agricultural and Biological Sciences": "biological_medical",
    "Biochemistry, Genetics and Molecular Biology": "biological_medical",
    "Immunology and Microbiology": "biological_medical",
    "Neuroscience": "biological_medical",
    "Pharmacology, Toxicology and Pharmaceutics": "biological_medical",
    "Medicine": "biological_medical",
    "Dentistry": "biological_medical",
    "Health Professions": "biological_medical",
    "Nursing": "biological_medical",
    "Veterinary": "biological_medical",
    "Arts and Humanities": "policy_economics",
    "Business, Management and Accounting": "policy_economics",
    "Decision Sciences": "policy_economics",
    "Economics, Econometrics and Finance": "policy_economics",
    "Psychology": "policy_economics",
    "Social Sciences": "policy_economics",
}

GCMD_TOPIC_TO_DISCIPLINE = {
    "ATMOSPHERE": "space_atmospheric",
    "BIOSPHERE": "biological_medical",
    "BIOLOGICAL CLASSIFICATION": "biological_medical",
    "CLIMATE INDICATORS": "earth_environmental",
    "CRYOSPHERE": "earth_environmental",
    "LAND SURFACE": "earth_environmental",
    "OCEANS": "earth_environmental",
    "PALEOCLIMATE": "earth_environmental",
    "SOLID EARTH": "earth_environmental",
    "TERRESTRIAL HYDROSPHERE": "earth_environmental",
    "SUN-EARTH INTERACTIONS": "space_atmospheric",
    "SPECTRAL/ENGINEERING": "ee_me_engineering",
    "AGRICULTURE": "biological_medical",
    "HUMAN DIMENSIONS": "policy_economics",
    "DATA ANALYSIS AND VISUALIZATION": "computation_data",
    "DATA MANAGEMENT/DATA HANDLING": "computation_data",
    "MACHINE LEARNING TRAINING DATA": "computation_data",
    "MODELS": "computation_data",
    "WEB SERVICES": "computation_data",
}

DOE_GROUP_TO_DISCIPLINE = {
    "Fossil Fuels": "fossil_energy",
    "Alternative Fuels": "fossil_energy",
    "Renewable Energy": "renewable_alternative",
    "Nuclear Energy": "nuclear_particle",
    "Energy Systems and Policy": "policy_economics",
    "Physical Sciences": "math_physics",
    "Physics": "math_physics",
    "Life and Environmental Sciences": "earth_environmental",
    "Other": "materials",
}


def _map_keyword_to_discipline(source: str, full_path: str | None, label: str, ktype: str | None) -> str:
    """Map a keyword to its primary discipline based on source + hierarchy."""
    path = full_path or ""
    parts = [p.strip() for p in path.split(" > ") if p.strip()]

    if source == "OpenAlex":
        if len(parts) >= 2:
            field = parts[1]
            return OPENALEX_FIELD_TO_DISCIPLINE.get(field, "math_physics")
        if len(parts) >= 1:
            domain = parts[0]
            return {"Physical Sciences": "math_physics", "Life Sciences": "biological_medical",
                    "Health Sciences": "biological_medical", "Social Sciences": "policy_economics"}.get(domain, "math_physics")
        return "math_physics"

    if source == "NASA GCMD":
        if ktype == "sciencekeywords" and len(parts) >= 2:
            topic = parts[1]
            return GCMD_TOPIC_TO_DISCIPLINE.get(topic, "earth_environmental")
        if ktype == "instruments":
            return "ee_me_engineering"
        if ktype == "platforms":
            return "space_atmospheric"
        if ktype == "locations":
            return "earth_environmental"
        if ktype == "providers":
            return "policy_economics"
        if ktype == "projects":
            return "space_atmospheric"
        return "earth_environmental"

    if source == "DOE OSTI":
        if len(parts) >= 1:
            group = parts[0]
            return DOE_GROUP_TO_DISCIPLINE.get(group, "fossil_energy")
        return "fossil_energy"

    if source == "Library of Congress":
        path_lower = path.lower()
        for kw, disc in [
            ("technology", "ee_me_engineering"), ("engineering", "ee_me_engineering"),
            ("machinery", "ee_me_engineering"), ("manufactur", "materials"),
            ("life sciences", "biological_medical"), ("biology", "biological_medical"),
            ("medicine", "biological_medical"), ("ecology", "earth_environmental"),
            ("agriculture", "biological_medical"),
            ("physical sciences", "math_physics"), ("physics", "math_physics"),
            ("chemistry", "chemical_sciences"),
            ("astronomy", "space_atmospheric"), ("astronautics", "space_atmospheric"),
            ("space sciences", "space_atmospheric"),
            ("mathematics", "math_physics"), ("statistics", "math_physics"),
            ("computer science", "computation_data"),
            ("environmental sciences", "earth_environmental"),
            ("earth sciences", "earth_environmental"), ("geology", "earth_environmental"),
            ("aquatic sciences", "earth_environmental"),
            ("forensic sciences", "policy_economics"),
            ("nuclear", "nuclear_particle"), ("atomic", "nuclear_particle"),
            ("energy", "fossil_energy"), ("fuel", "fossil_energy"),
            ("coal", "coal_science"), ("petroleum", "natural_gas"),
            ("natural gas", "natural_gas"),
            ("material", "materials"), ("metal", "materials"), ("ceramic", "materials"),
            ("nanosci", "materials"), ("polymer", "materials"),
        ]:
            if kw in path_lower:
                return disc
        return "math_physics"

    if source == "UNESCO Thesaurus":
        label_lower = label.lower()
        for kw, disc in [
            ("energy", "fossil_energy"), ("fuel", "fossil_energy"), ("coal", "coal_science"),
            ("gas", "natural_gas"), ("nuclear", "nuclear_particle"), ("physics", "math_physics"),
            ("math", "math_physics"), ("comput", "computation_data"), ("engineer", "ee_me_engineering"),
            ("material", "materials"), ("chem", "chemical_sciences"), ("bio", "biological_medical"),
            ("medic", "biological_medical"), ("health", "biological_medical"),
            ("ecol", "earth_environmental"), ("environ", "earth_environmental"),
            ("geol", "earth_environmental"), ("ocean", "earth_environmental"),
            ("climat", "earth_environmental"), ("atmos", "space_atmospheric"),
            ("space", "space_atmospheric"), ("satellite", "space_atmospheric"),
            ("polic", "policy_economics"), ("econom", "policy_economics"),
            ("educ", "policy_economics"), ("social", "policy_economics"),
            ("renew", "renewable_alternative"), ("solar", "renewable_alternative"),
            ("wind", "renewable_alternative"),
        ]:
            if kw in label_lower:
                return disc
        return "policy_economics"

    if source == "NCBI Taxonomy":
        return "biological_medical"

    if source == "MeSH":
        ktype_str = ktype or ""
        cat_letter = ktype_str[0] if ktype_str else ""
        mesh_cat_map = {
            "A": "biological_medical",    # Anatomy
            "B": "biological_medical",    # Organisms
            "C": "biological_medical",    # Diseases
            "D": "chemical_sciences",     # Chemicals and Drugs
            "E": "biological_medical",    # Techniques/Equipment
            "F": "biological_medical",    # Psychiatry/Psychology
            "G": "math_physics",          # Phenomena and Processes
            "H": "policy_economics",      # Disciplines/Occupations
            "I": "policy_economics",      # Anthropology/Education
            "J": "ee_me_engineering",     # Technology/Industry/Agriculture
            "K": "policy_economics",      # Humanities
            "L": "computation_data",      # Information Science
            "M": "biological_medical",    # Named Groups
            "N": "policy_economics",      # Health Care
            "V": "policy_economics",      # Publication Characteristics
            "Z": "earth_environmental",   # Geographicals
        }
        disc = mesh_cat_map.get(cat_letter, "biological_medical")
        label_lower = label.lower()
        for kw, override in [
            ("coal", "coal_science"), ("fossil", "fossil_energy"), ("petroleum", "fossil_energy"),
            ("natural gas", "natural_gas"), ("methane", "natural_gas"),
            ("nuclear", "nuclear_particle"), ("radiation", "nuclear_particle"),
            ("solar", "renewable_alternative"), ("wind energy", "renewable_alternative"),
            ("climate", "earth_environmental"), ("geolog", "earth_environmental"),
            ("ocean", "earth_environmental"), ("atmospher", "space_atmospheric"),
        ]:
            if kw in label_lower:
                return override
        return disc

    return "math_physics"


def _get_resolution_tier(discipline_id: str, discipline_tiers: dict[str, int]) -> int:
    """Get resolution tier from discipline."""
    return discipline_tiers.get(discipline_id, 4)


def populate_keyword_senses(conn, batch_size: int = 10000) -> int:
    """Generate one sense per keyword from the unified keywords table.

    Uses temp table + SQL INSERT...SELECT for speed — Python executemany
    with DuckDB array columns is pathologically slow at scale.
    """
    conn.execute("DELETE FROM keyword_senses")

    discipline_tiers = {d[0]: d[4] for d in DISCIPLINES}

    rows = conn.execute(
        "SELECT id, source, label, full_path, type FROM keywords ORDER BY source, id"
    ).fetchall()
    total = len(rows)
    logger.info(f"Generating senses for {total} keywords...")

    conn.execute("DROP TABLE IF EXISTS _tmp_kw_disc")
    conn.execute("CREATE TEMP TABLE _tmp_kw_disc (kid VARCHAR, ksource VARCHAR, disc VARCHAR, tier INTEGER)")

    mappings = []
    for kid, ksource, klabel, kpath, ktype in rows:
        disc = _map_keyword_to_discipline(ksource, kpath, klabel, ktype)
        tier = discipline_tiers.get(disc, 4)
        mappings.append((kid, ksource, disc, tier))

    for i in range(0, len(mappings), batch_size):
        conn.executemany("INSERT INTO _tmp_kw_disc VALUES (?, ?, ?, ?)", mappings[i:i + batch_size])

    conn.execute("""
        INSERT INTO keyword_senses
            (sense_id, keyword_id, keyword_source, keyword_label,
             origin_source, origin_path, origin_level,
             discipline_primary, disciplines_secondary, resolution_tier,
             definition_in_context, scope_note, disambiguation,
             relevance_tags, confidence, provenance)
        SELECT
            k.id || '@' || REPLACE(k.source, ' ', '_') || '#0',
            k.id, k.source, k.label,
            k.source, k.full_path, k.level,
            d.disc, ARRAY[]::VARCHAR[], d.tier,
            k.definition, NULL, NULL,
            ARRAY[]::VARCHAR[], 0.8, 'initial_generation'
        FROM keywords k
        JOIN _tmp_kw_disc d ON k.id = d.kid AND k.source = d.ksource
    """)

    conn.execute("DROP TABLE IF EXISTS _tmp_kw_disc")
    inserted = conn.execute(
        "SELECT COUNT(*) FROM keyword_senses WHERE provenance = 'initial_generation'"
    ).fetchone()[0]
    logger.info(f"Generated {inserted} keyword senses")
    return inserted


def populate_polysemy_senses(conn) -> int:
    """Find labels appearing in multiple sources and tag disambiguation."""
    polysemous = conn.execute("""
        SELECT keyword_label, COUNT(DISTINCT keyword_source) as src_count,
               LIST(DISTINCT keyword_source) as sources
        FROM keyword_senses
        GROUP BY keyword_label
        HAVING src_count >= 2
        ORDER BY src_count DESC
    """).fetchall()

    updated = 0
    for label, src_count, sources in polysemous:
        senses = conn.execute("""
            SELECT sense_id, keyword_source, discipline_primary, origin_path
            FROM keyword_senses
            WHERE keyword_label = ?
        """, [label]).fetchall()

        disciplines_seen = set()
        for sense_id, ksource, disc, path in senses:
            short_source = ksource.split()[0]
            disambig = f"{label} ({short_source})"

            secondary = []
            for other_id, other_source, other_disc, _ in senses:
                if other_disc != disc and other_disc not in disciplines_seen:
                    secondary.append(other_disc)
            disciplines_seen.add(disc)

            conn.execute("""
                UPDATE keyword_senses
                SET disambiguation = ?,
                    disciplines_secondary = ?,
                    confidence = CASE WHEN ? >= 3 THEN 0.7 ELSE 0.75 END
                WHERE sense_id = ?
            """, [disambig, secondary, src_count, sense_id])
            updated += 1

    logger.info(f"Tagged {updated} polysemous senses ({len(polysemous)} labels)")
    return len(polysemous)


def populate_sense_relationships(conn) -> int:
    """Convert cross_taxonomy_alignment records into sense-level relationships."""
    conn.execute("DELETE FROM sense_relationships")

    alignments = conn.execute("""
        SELECT source_id, source_name, target_id, target_name,
               match_type, confidence, method, source_label, target_label
        FROM cross_taxonomy_alignment
    """).fetchall()

    rels = []
    skipped = 0

    for src_id, src_name, tgt_id, tgt_name, mtype, conf, method, slabel, tlabel in alignments:
        src_sense = f"{src_id}@{src_name.replace(' ', '_')}#0"
        tgt_sense = f"{tgt_id}@{tgt_name.replace(' ', '_')}#0"

        exists_src = conn.execute(
            "SELECT 1 FROM keyword_senses WHERE sense_id = ?", [src_sense]
        ).fetchone()
        exists_tgt = conn.execute(
            "SELECT 1 FROM keyword_senses WHERE sense_id = ?", [tgt_sense]
        ).fetchone()

        if not exists_src or not exists_tgt:
            skipped += 1
            continue

        src_disc = conn.execute(
            "SELECT discipline_primary FROM keyword_senses WHERE sense_id = ?", [src_sense]
        ).fetchone()[0]
        tgt_disc = conn.execute(
            "SELECT discipline_primary FROM keyword_senses WHERE sense_id = ?", [tgt_sense]
        ).fetchone()[0]

        if src_disc == tgt_disc:
            rel_type = "equivalent_sense"
            direction = "toward"
        else:
            rel_type = "cross_domain_bridge"
            direction = "across"

        rels.append((
            src_sense, tgt_sense, rel_type, direction,
            conf, f"alignment_{method}", [],
        ))

    if rels:
        seen = set()
        unique_rels = []
        for r in rels:
            key = (r[0], r[1], r[2])
            if key not in seen:
                seen.add(key)
                unique_rels.append(r)

        conn.executemany(
            """INSERT INTO sense_relationships
               (source_sense_id, target_sense_id, relationship_type, direction,
                confidence, provenance, lens_contexts)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            unique_rels,
        )

    total = conn.execute("SELECT COUNT(*) FROM sense_relationships").fetchone()[0]
    logger.info(f"Created {total} sense relationships ({skipped} skipped, no matching sense)")
    return total


# =============================================================================
# WoS Publication Keyword Sense Generator
# =============================================================================

WOS_CAT_TO_DISCIPLINE = {
    "Energy & Fuels": "fossil_energy",
    "Engineering, Chemical": "chemical_sciences",
    "Chemistry, Multidisciplinary": "chemical_sciences",
    "Chemistry, Physical": "chemical_sciences",
    "Chemistry, Applied": "chemical_sciences",
    "Chemistry, Analytical": "chemical_sciences",
    "Chemistry, Inorganic & Nuclear": "chemical_sciences",
    "Chemistry, Organic": "chemical_sciences",
    "Materials Science, Multidisciplinary": "materials",
    "Materials Science, Ceramics": "materials",
    "Materials Science, Coatings & Films": "materials",
    "Materials Science, Composites": "materials",
    "Metallurgy & Metallurgical Engineering": "materials",
    "Nanoscience & Nanotechnology": "materials",
    "Engineering, Mechanical": "ee_me_engineering",
    "Engineering, Electrical & Electronic": "ee_me_engineering",
    "Engineering, Aerospace": "ee_me_engineering",
    "Thermodynamics": "ee_me_engineering",
    "Mechanics": "ee_me_engineering",
    "Engineering, Environmental": "earth_environmental",
    "Environmental Sciences": "earth_environmental",
    "Geochemistry & Geophysics": "earth_environmental",
    "Geosciences, Multidisciplinary": "earth_environmental",
    "Geology": "earth_environmental",
    "Mining & Mineral Processing": "earth_environmental",
    "Water Resources": "earth_environmental",
    "Mineralogy": "earth_environmental",
    "Electrochemistry": "chemical_sciences",
    "Green & Sustainable Science & Technology": "renewable_alternative",
    "Computer Science, Interdisciplinary Applications": "computation_data",
    "Computer Science, Artificial Intelligence": "computation_data",
    "Computer Science, Theory & Methods": "computation_data",
    "Mathematics, Applied": "math_physics",
    "Mathematics, Interdisciplinary Applications": "math_physics",
    "Physics, Applied": "math_physics",
    "Physics, Atomic, Molecular & Chemical": "math_physics",
    "Physics, Condensed Matter": "math_physics",
    "Physics, Multidisciplinary": "math_physics",
    "Nuclear Science & Technology": "nuclear_particle",
    "Optics": "ee_me_engineering",
    "Instruments & Instrumentation": "ee_me_engineering",
    "Polymer Science": "materials",
    "Biotechnology & Applied Microbiology": "biological_medical",
    "Toxicology": "biological_medical",
    "Pharmacology & Pharmacy": "biological_medical",
}

FOSSIL_ENERGY_KEYWORDS = {
    "carbon capture", "co2 capture", "carbon dioxide", "co2", "ccus", "ccs",
    "coal", "coal combustion", "coal gasification", "coal-to-liquids",
    "natural gas", "shale gas", "methane", "lng", "hydraulic fracturing", "fracking",
    "fossil fuel", "fossil energy", "petroleum", "crude oil", "oil shale",
    "gasification", "syngas", "synthesis gas", "fischer-tropsch",
    "fluidized bed", "circulating fluidized bed", "fluidization",
    "combustion", "oxy-combustion", "oxyfuel", "chemical looping",
    "solid oxide fuel cell", "sofc", "fuel cell",
    "carbon sequestration", "carbon storage", "geologic storage",
    "enhanced oil recovery", "eor",
    "rare earth", "rare earth elements", "critical minerals",
    "power generation", "power plant", "turbine", "gas turbine",
    "supercritical co2", "sco2",
    "mercury", "mercury emission", "fly ash", "bottom ash",
    "wellbore", "drilling", "reservoir", "permeability", "porosity",
    "subsurface", "formation", "sandstone", "shale",
    "membrane", "sorbent", "solvent", "amine", "monoethanolamine",
    "hydrogen", "hydrogen production", "hydrogen storage",
    "methane hydrate", "gas hydrate", "clathrate",
}

COAL_SUBSTRINGS = [
    "coal ", " coal", "coal-", "lignite", "anthracite", "bituminous",
    "fly ash", "bottom ash", "coal ash", "coking", " coke ",
    "pulverized coal", "coal seam", "coal bed", "coal mine",
    "coal fired", "coal-fired", "coal dust", "coal slurry",
    "coal gasif", "coal combust", "coal pyrol", "coal conver",
    "coal char", "coal blend", "coal rank", "coal utiliz",
    "coal prep", "coal clean", "coal-to-", "coal tar",
    "sub-bituminous", "devolatilization",
]

NATGAS_SUBSTRINGS = [
    "natural gas", "shale gas", "shale well", "shale formation",
    "marcellus", "utica shale", "barnett", "haynesville",
    "hydraulic fractur", "fracking", "frac fluid", "proppant",
    "wellbore", "well completion", "horizontal drill",
    "methane hydrate", "gas hydrate", "clathrate",
    "methane leak", "methane migrat", "methane emit", "methane mitigat",
    "fugitive emission", "flaring",
    "coalbed methane", "tight gas", "unconventional gas",
    "natural gas liquid", " ngl ", " lng ",
]

FOSSIL_SUBSTRINGS = [
    "carbon capture", "co2 capture", "co2 sequest", "carbon sequest",
    "carbon storage", "geologic storage", "carbon dioxide capture",
    "ccus", " ccs ", "carbon management",
    "gasification", "syngas", "synthesis gas", "fischer-tropsch",
    "fluidized bed", "fluidization", "circulating fluidized",
    "oxy-combustion", "oxyfuel", "chemical looping",
    "solid oxide fuel cell", " sofc", "fuel cell",
    "enhanced oil recovery", " eor ",
    "rare earth element", "critical mineral",
    "power generation", "power plant",
    "supercritical co2", " sco2",
    "mercury emission", "scrubber",
    "fossil fuel", "fossil energy",
    "petroleum", "crude oil", "oil shale", "tar sand",
    "carbon dioxide", "co2 storage", "co2 transport",
    "amine scrub", "monoethanolamine", " mea ",
    "sorbent", "post-combustion", "pre-combustion",
    "carbon mineral", "mineral carbon",
]

COAL_KEYWORDS = {
    "coal", "coal combustion", "coal gasification", "coal-to-liquids", "coal ash",
    "fly ash", "bottom ash", "coal tar", "coal preparation", "coal cleaning",
    "lignite", "anthracite", "bituminous", "coal slurry", "coal pyrolysis",
    "coal char", "coal utilization", "coal conversion", "coal-biomass",
    "pulverized coal", "coal fired", "coal-fired", "coal mine", "coal dust",
    "coal seam", "coal bed", "sub-bituminous", "coal blend", "coal rank",
    "coal geology", "coking", "coke", "coal devolatilization",
}

NATGAS_KEYWORDS = {
    "natural gas", "shale gas", "methane", "lng", "hydraulic fracturing",
    "unconventional gas", "tight gas", "coalbed methane", "marcellus",
    "methane hydrate", "gas hydrate", "methane emission", "methane mitigation",
    "natural gas processing", "gas pipeline", "gas turbine",
    "marcellus shale", "utica shale", "barnett shale", "haynesville",
    "shale formation", "shale well", "proppant", "fracking", "frac fluid",
    "wellbore integrity", "well completion", "horizontal drilling",
    "natural gas liquids", "ngl", "ethane", "propane",
    "methane leakage", "fugitive emissions", "flaring",
}


def _classify_wos_keyword(keyword_lower: str, wos_category: str | None) -> str:
    """Classify a WoS publication keyword into a discipline.

    Uses keyword content first (most specific), then WoS category as fallback.
    All keywords from fossil energy publications carry provenance back to T1
    via sense relationships, even if the keyword itself maps to T2-T4.
    """
    if keyword_lower in COAL_KEYWORDS or any(k in keyword_lower for k in COAL_SUBSTRINGS):
        return "coal_science"
    if keyword_lower in NATGAS_KEYWORDS or any(k in keyword_lower for k in NATGAS_SUBSTRINGS):
        return "natural_gas"
    if keyword_lower in FOSSIL_ENERGY_KEYWORDS or any(k in keyword_lower for k in FOSSIL_SUBSTRINGS):
        return "fossil_energy"

    for kw, disc in [
        ("machine learning", "computation_data"), ("deep learning", "computation_data"),
        ("neural network", "computation_data"), ("artificial intelligence", "computation_data"),
        ("cfd", "computation_data"), ("computational fluid dynamics", "computation_data"),
        ("simulation", "computation_data"), ("finite element", "computation_data"),
        ("density functional theory", "computation_data"), ("molecular dynamics", "computation_data"),
        ("optimization", "computation_data"), ("numerical", "computation_data"),
        ("catalyst", "chemical_sciences"), ("catalysis", "chemical_sciences"),
        ("catalytic", "chemical_sciences"), ("electrochemi", "chemical_sciences"),
        ("thermodynami", "chemical_sciences"), ("kinetics", "chemical_sciences"),
        ("adsorption", "chemical_sciences"), ("absorption", "chemical_sciences"),
        ("corrosion", "materials"), ("alloy", "materials"), ("ceramic", "materials"),
        ("coating", "materials"), ("microstructure", "materials"), ("nanoparticle", "materials"),
        ("composite", "materials"), ("polymer", "materials"), ("steel", "materials"),
        ("nickel", "materials"), ("oxide", "materials"), ("thin film", "materials"),
        ("turbine", "ee_me_engineering"), ("heat transfer", "ee_me_engineering"),
        ("combustion", "ee_me_engineering"), ("sensor", "ee_me_engineering"),
        ("power", "ee_me_engineering"), ("pressure", "ee_me_engineering"),
        ("temperature", "ee_me_engineering"), ("flow", "ee_me_engineering"),
        ("geolog", "earth_environmental"), ("seismic", "earth_environmental"),
        ("reservoir", "earth_environmental"), ("groundwater", "earth_environmental"),
        ("soil", "earth_environmental"), ("sediment", "earth_environmental"),
        ("climate", "earth_environmental"), ("environment", "earth_environmental"),
        ("remediation", "earth_environmental"), ("contamin", "earth_environmental"),
        ("renewable", "renewable_alternative"), ("solar", "renewable_alternative"),
        ("wind energy", "renewable_alternative"), ("biomass", "renewable_alternative"),
        ("nuclear", "nuclear_particle"), ("radiation", "nuclear_particle"),
        ("biolog", "biological_medical"), ("cell", "biological_medical"),
        ("protein", "biological_medical"), ("gene", "biological_medical"),
        ("policy", "policy_economics"), ("economic", "policy_economics"),
        ("cost", "policy_economics"), ("regulation", "policy_economics"),
    ]:
        if kw in keyword_lower:
            return disc

    if wos_category and wos_category in WOS_CAT_TO_DISCIPLINE:
        return WOS_CAT_TO_DISCIPLINE[wos_category]

    if wos_category:
        cat_lower = wos_category.lower()
        for kw, disc in [
            ("energy", "fossil_energy"), ("fuel", "fossil_energy"),
            ("engineer", "ee_me_engineering"), ("mechanic", "ee_me_engineering"),
            ("chem", "chemical_sciences"), ("material", "materials"),
            ("physic", "math_physics"), ("math", "math_physics"),
            ("comput", "computation_data"), ("environ", "earth_environmental"),
            ("geo", "earth_environmental"), ("bio", "biological_medical"),
            ("nuclear", "nuclear_particle"),
        ]:
            if kw in cat_lower:
                return disc

    return "ee_me_engineering"


def populate_wos_keyword_senses(conn) -> dict:
    """Generate senses from WoS publication keywords (Tab 1).

    Each author keyword and Keywords_Plus term gets a sense with:
    - Origin = "WoS_publication" (not a pillar source — publication-derived)
    - Full publication context (WoS category, journal, grant agency)
    - Discipline assignment based on keyword content + WoS category
    - Cross-links to existing pillar senses where labels match
    """
    conn.execute("DELETE FROM keyword_senses WHERE origin_source = 'WoS_publication'")

    pubs = conn.execute("""
        SELECT accession_number, keywords_author, keywords_plus,
               subject_cat_traditional_1, source_title, title
        FROM raw_wos_publications
    """).fetchall()

    senses = []
    seen_sense_ids = set()
    existing_labels = set()

    existing = conn.execute("SELECT sense_id FROM keyword_senses").fetchall()
    for (sid,) in existing:
        seen_sense_ids.add(sid)

    existing_kw = conn.execute(
        "SELECT DISTINCT LOWER(keyword_label) FROM keyword_senses"
    ).fetchall()
    existing_labels = {row[0] for row in existing_kw}

    new_rels = []
    keyword_pub_counts = {}

    for acc, kw_author, kw_plus, wos_cat, journal, title in pubs:
        all_keywords = []
        if kw_author:
            for kw in kw_author:
                all_keywords.append(("author", kw.strip()))
        if kw_plus:
            for kw in kw_plus:
                all_keywords.append(("plus", kw.strip()))

        for kw_type, kw_raw in all_keywords:
            if not kw_raw:
                continue
            kw_lower = kw_raw.lower()
            kw_normalized = kw_lower.strip()

            if kw_normalized not in keyword_pub_counts:
                keyword_pub_counts[kw_normalized] = {"author": 0, "plus": 0, "pubs": set()}
            keyword_pub_counts[kw_normalized][kw_type] += 1
            keyword_pub_counts[kw_normalized]["pubs"].add(acc)

    pub_categories = {}
    for acc, kw_author, kw_plus, wos_cat, journal, title in pubs:
        all_kws = []
        if kw_author:
            all_kws.extend(k.strip().lower() for k in kw_author)
        if kw_plus:
            all_kws.extend(k.strip().lower() for k in kw_plus)
        for kw in all_kws:
            if kw and kw not in pub_categories and wos_cat:
                pub_categories[kw] = wos_cat

    for kw_normalized, counts in keyword_pub_counts.items():
        wos_cat = pub_categories.get(kw_normalized)
        disc = _classify_wos_keyword(kw_normalized, wos_cat)

        sense_id = f"wos:{kw_normalized.replace(' ', '_')[:60]}@WoS_publication#0"
        if sense_id in seen_sense_ids:
            continue
        seen_sense_ids.add(sense_id)

        pub_count = len(counts["pubs"])
        author_count = counts["author"]
        plus_count = counts["plus"]

        confidence = min(0.95, 0.5 + (pub_count / 100))

        tags = []
        if author_count > 0:
            tags.append("author_keyword")
        if plus_count > 0:
            tags.append("keywords_plus")
        if pub_count >= 10:
            tags.append("high_frequency")
        if kw_normalized in existing_labels:
            tags.append("pillar_overlap")

        senses.append((
            sense_id, f"wos:{kw_normalized}", "WoS_publication", kw_normalized,
            "WoS_publication", None, None,
            disc, [], _get_resolution_tier(disc, dict(conn.execute(
                "SELECT discipline_id, tier FROM disciplines"
            ).fetchall())),
            None, None, None,
            tags, confidence, "wos_publication_extraction",
        ))

        if kw_normalized in existing_labels:
            pillar_senses = conn.execute("""
                SELECT sense_id, discipline_primary
                FROM keyword_senses
                WHERE LOWER(keyword_label) = ?
            """, [kw_normalized]).fetchall()
            for p_sense_id, p_disc in pillar_senses:
                direction = "toward" if p_disc == disc else "across"
                rel_type = "equivalent_sense" if p_disc == disc else "cross_domain_bridge"
                new_rels.append((
                    sense_id, p_sense_id, rel_type, direction,
                    0.8, "wos_label_match", [],
                ))

    if senses:
        conn.executemany(
            """INSERT INTO keyword_senses
               (sense_id, keyword_id, keyword_source, keyword_label,
                origin_source, origin_path, origin_level,
                discipline_primary, disciplines_secondary, resolution_tier,
                definition_in_context, scope_note, disambiguation,
                relevance_tags, confidence, provenance)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            senses,
        )

    if new_rels:
        seen_rel_keys = set()
        unique_rels = []
        for r in new_rels:
            key = (r[0], r[1], r[2])
            if key not in seen_rel_keys:
                seen_rel_keys.add(key)
                unique_rels.append(r)
        conn.executemany(
            """INSERT OR IGNORE INTO sense_relationships
               (source_sense_id, target_sense_id, relationship_type, direction,
                confidence, provenance, lens_contexts)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            unique_rels,
        )

    wos_sense_count = conn.execute(
        "SELECT COUNT(*) FROM keyword_senses WHERE origin_source = 'WoS_publication'"
    ).fetchone()[0]
    wos_rel_count = len(new_rels) if new_rels else 0
    pillar_overlaps = sum(1 for s in senses if "pillar_overlap" in s[13])

    logger.info(
        f"WoS senses: {wos_sense_count} new, {pillar_overlaps} overlap with pillars, "
        f"{wos_rel_count} new relationships"
    )

    return {
        "wos_senses": wos_sense_count,
        "pillar_overlaps": pillar_overlaps,
        "new_relationships": wos_rel_count,
        "unique_keywords": len(keyword_pub_counts),
    }


def populate_wos_category_senses(conn) -> dict:
    """Generate senses from WoS subject categories, journals, and grant agencies.

    These are all keyword-like entities that carry their own vector bundles.
    """
    conn.execute("DELETE FROM keyword_senses WHERE origin_source IN ('WoS_category', 'WoS_journal', 'WoS_grant_agency')")

    senses = []
    seen = set()

    # WoS traditional categories (91 unique)
    cats = conn.execute("""
        SELECT DISTINCT subject_cat_traditional_1 FROM raw_wos_publications
        WHERE subject_cat_traditional_1 IS NOT NULL
        UNION
        SELECT DISTINCT subject_cat_traditional_2 FROM raw_wos_publications
        WHERE subject_cat_traditional_2 IS NOT NULL
    """).fetchall()

    for (cat,) in cats:
        cat_clean = cat.strip()
        if not cat_clean:
            continue
        disc = WOS_CAT_TO_DISCIPLINE.get(cat_clean, "math_physics")
        sense_id = f"woscat:{cat_clean.lower().replace(' ', '_').replace(',', '')[:60]}@WoS_category#0"
        if sense_id in seen:
            continue
        seen.add(sense_id)

        pub_count = conn.execute("""
            SELECT COUNT(*) FROM raw_wos_publications
            WHERE subject_cat_traditional_1 = ? OR subject_cat_traditional_2 = ?
        """, [cat_clean, cat_clean]).fetchone()[0]

        senses.append((
            sense_id, f"woscat:{cat_clean}", "WoS_category", cat_clean,
            "WoS_category", None, None,
            disc, [], _get_resolution_tier(disc, dict(conn.execute(
                "SELECT discipline_id, tier FROM disciplines"
            ).fetchall())),
            None, None, None,
            ["wos_category", f"pub_count:{pub_count}"], 0.9, "wos_category",
        ))

    # Journals (top 200 by pub count — the long tail adds noise)
    journals = conn.execute("""
        SELECT source_title, COUNT(*) as cnt
        FROM raw_wos_publications
        WHERE source_title IS NOT NULL
        GROUP BY source_title
        ORDER BY cnt DESC
        LIMIT 200
    """).fetchall()

    for journal, cnt in journals:
        j_clean = journal.strip()
        top_cat = conn.execute("""
            SELECT subject_cat_traditional_1, COUNT(*) as c
            FROM raw_wos_publications
            WHERE source_title = ? AND subject_cat_traditional_1 IS NOT NULL
            GROUP BY subject_cat_traditional_1 ORDER BY c DESC LIMIT 1
        """, [j_clean]).fetchone()
        disc = WOS_CAT_TO_DISCIPLINE.get(top_cat[0], "fossil_energy") if top_cat else "fossil_energy"

        sense_id = f"wosj:{j_clean.lower().replace(' ', '_')[:60]}@WoS_journal#0"
        if sense_id in seen:
            continue
        seen.add(sense_id)

        senses.append((
            sense_id, f"wosj:{j_clean}", "WoS_journal", j_clean,
            "WoS_journal", None, None,
            disc, [], _get_resolution_tier(disc, dict(conn.execute(
                "SELECT discipline_id, tier FROM disciplines"
            ).fetchall())),
            None, None, None,
            ["journal", f"pub_count:{cnt}"], 0.85, "wos_journal",
        ))

    # Grant agencies (top 100, need entity resolution later)
    agencies = conn.execute("""
        WITH exploded AS (
            SELECT UNNEST(grant_agencies) as agency
            FROM raw_wos_publications
            WHERE len(grant_agencies) > 0
        )
        SELECT agency, COUNT(*) as cnt FROM exploded
        GROUP BY agency ORDER BY cnt DESC LIMIT 100
    """).fetchall()

    for agency, cnt in agencies:
        a_clean = agency.strip()
        if not a_clean or len(a_clean) < 3:
            continue

        disc = "fossil_energy"
        a_lower = a_clean.lower()
        for kw, d in [
            ("nsf", "math_physics"), ("national science foundation", "math_physics"),
            ("nih", "biological_medical"), ("national institutes of health", "biological_medical"),
            ("nasa", "space_atmospheric"),
            ("china", "policy_economics"), ("european", "policy_economics"),
            ("uk research", "policy_economics"),
        ]:
            if kw in a_lower:
                disc = d
                break

        sense_id = f"wosg:{a_clean.lower().replace(' ', '_')[:60]}@WoS_grant_agency#0"
        if sense_id in seen:
            continue
        seen.add(sense_id)

        senses.append((
            sense_id, f"wosg:{a_clean}", "WoS_grant_agency", a_clean,
            "WoS_grant_agency", None, None,
            disc, [], _get_resolution_tier(disc, dict(conn.execute(
                "SELECT discipline_id, tier FROM disciplines"
            ).fetchall())),
            None, None, None,
            ["grant_agency", f"mention_count:{cnt}"], 0.8, "wos_grant_agency",
        ))

    if senses:
        conn.executemany(
            """INSERT INTO keyword_senses
               (sense_id, keyword_id, keyword_source, keyword_label,
                origin_source, origin_path, origin_level,
                discipline_primary, disciplines_secondary, resolution_tier,
                definition_in_context, scope_note, disambiguation,
                relevance_tags, confidence, provenance)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            senses,
        )

    cat_count = sum(1 for s in senses if s[4] == "WoS_category")
    journal_count = sum(1 for s in senses if s[4] == "WoS_journal")
    agency_count = sum(1 for s in senses if s[4] == "WoS_grant_agency")

    logger.info(f"WoS metadata senses: {cat_count} categories, {journal_count} journals, {agency_count} agencies")

    return {
        "categories": cat_count,
        "journals": journal_count,
        "grant_agencies": agency_count,
    }


def populate_tab2_vocab_senses(conn) -> int:
    """Generate senses from Tab 2 Keywords Plus vocabulary (7,488 unique terms)."""
    conn.execute("DELETE FROM keyword_senses WHERE origin_source = 'WoS_keywords_plus_vocab'")

    existing_labels = set(r[0] for r in conn.execute(
        "SELECT DISTINCT LOWER(keyword_label) FROM keyword_senses"
    ).fetchall())

    vocab = conn.execute("SELECT keyword, normalized FROM raw_wos_keywords_plus_vocab").fetchall()
    discipline_tiers = dict(conn.execute("SELECT discipline_id, tier FROM disciplines").fetchall())

    senses = []
    new_count = 0
    overlap_count = 0

    for keyword, normalized in vocab:
        if normalized in existing_labels:
            overlap_count += 1
            continue

        disc = _classify_wos_keyword(normalized, "Energy & Fuels")
        sense_id = f"wos2:{normalized.replace(' ', '_')[:60]}@WoS_KP_vocab#0"

        senses.append((
            sense_id, f"wos2:{normalized}", "WoS_keywords_plus_vocab", normalized,
            "WoS_keywords_plus_vocab", None, None,
            disc, [], _get_resolution_tier(disc, discipline_tiers),
            None, None, None,
            ["keywords_plus", "vocab_only"], 0.6, "wos_vocab_supplement",
        ))
        new_count += 1

    if senses:
        conn.executemany(
            """INSERT INTO keyword_senses
               (sense_id, keyword_id, keyword_source, keyword_label,
                origin_source, origin_path, origin_level,
                discipline_primary, disciplines_secondary, resolution_tier,
                definition_in_context, scope_note, disambiguation,
                relevance_tags, confidence, provenance)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            senses,
        )

    logger.info(f"Tab 2 vocab senses: {new_count} new, {overlap_count} already existed")
    return new_count


# =============================================================================
# Lens Query Engine
# =============================================================================

def query_lens(conn, lens_id: str, limit: int = 50, search: str | None = None) -> list[dict]:
    """Query keyword senses through a composed lens.

    Returns senses ranked by: lens_weight × confidence × tier_boost.
    Cross-domain bridges are flagged.
    """
    lens = conn.execute(
        "SELECT * FROM ontology_lenses WHERE lens_id = ?", [lens_id]
    ).fetchone()
    if not lens:
        raise ValueError(f"Lens {lens_id} not found")

    cols = [desc[0] for desc in conn.description]
    lens_dict = dict(zip(cols, lens))

    disc_primary = lens_dict["discipline_primary"]
    disc_secondary = lens_dict.get("disciplines_secondary") or []
    altitude = lens_dict.get("altitude") or 10000
    disc_weights_raw = lens_dict.get("discipline_weights")

    if disc_weights_raw and isinstance(disc_weights_raw, str):
        import json as _json
        disc_weights = _json.loads(disc_weights_raw)
    elif isinstance(disc_weights_raw, dict):
        disc_weights = disc_weights_raw
    else:
        disc_weights = {}

    if not disc_weights and disc_primary:
        disc_weights = {disc_primary: 1.0}
        for d in disc_secondary:
            disc_weights[d] = 0.5

    tier_boost = {1: 2.0, 2: 1.5, 3: 1.0, 4: 0.5}
    level_filter = None
    if altitude >= 100000:
        level_filter = 2
    elif altitude >= 10000:
        level_filter = 4
    # altitude 1000 = no filter (researcher sees everything)

    where_clauses = ["1=1"]
    params = []

    if search:
        where_clauses.append("LOWER(keyword_label) LIKE ?")
        params.append(f"%{search.lower()}%")

    if level_filter is not None:
        where_clauses.append("(origin_level IS NULL OR origin_level <= ?)")
        params.append(level_filter)

    where_sql = " AND ".join(where_clauses)

    senses = conn.execute(f"""
        SELECT sense_id, keyword_label, discipline_primary,
               disciplines_secondary, resolution_tier, confidence,
               origin_source, origin_path, disambiguation,
               definition_in_context, relevance_tags
        FROM keyword_senses
        WHERE {where_sql}
    """, params).fetchall()

    results = []
    for row in senses:
        (sid, label, disc, disc_sec, tier, conf,
         osrc, opath, disambig, defn, tags) = row

        weight = disc_weights.get(disc, 0.1)
        for d in (disc_sec or []):
            w = disc_weights.get(d, 0)
            if w > 0:
                weight = max(weight, w * 0.7)

        boost = tier_boost.get(tier, 0.5)
        score = weight * (conf or 0.5) * boost

        is_bridge = False
        if disc_sec:
            bridge_discs = set(disc_sec) & set(disc_weights.keys())
            if bridge_discs and disc not in disc_weights:
                is_bridge = True
            elif bridge_discs and disc in disc_weights:
                is_bridge = len(bridge_discs) > 0

        results.append({
            "sense_id": sid,
            "label": label,
            "discipline": disc,
            "score": round(score, 4),
            "confidence": conf,
            "tier": tier,
            "source": osrc,
            "path": opath,
            "disambiguation": disambig,
            "definition": defn,
            "is_bridge": is_bridge,
            "relevance_tags": tags,
        })

    results.sort(key=lambda x: -x["score"])
    return results[:limit]


def query_discipline(conn, discipline_id: str, limit: int = 50, search: str | None = None) -> list[dict]:
    """Quick query: all senses in a discipline, ranked by confidence."""
    where = "discipline_primary = ?"
    params = [discipline_id]
    if search:
        where += " AND LOWER(keyword_label) LIKE ?"
        params.append(f"%{search.lower()}%")

    rows = conn.execute(f"""
        SELECT sense_id, keyword_label, discipline_primary, resolution_tier,
               confidence, origin_source, disambiguation, definition_in_context
        FROM keyword_senses
        WHERE {where}
        ORDER BY confidence DESC, keyword_label
        LIMIT ?
    """, params + [limit]).fetchall()

    return [
        {"sense_id": r[0], "label": r[1], "discipline": r[2], "tier": r[3],
         "confidence": r[4], "source": r[5], "disambiguation": r[6], "definition": r[7]}
        for r in rows
    ]


def get_bridges(conn, discipline_a: str, discipline_b: str, limit: int = 30) -> list[dict]:
    """Find cross-domain bridges between two disciplines."""
    rows = conn.execute("""
        SELECT s.sense_id, s.keyword_label, s.discipline_primary,
               s.disciplines_secondary, s.confidence, s.origin_source,
               s.disambiguation
        FROM keyword_senses s
        WHERE (s.discipline_primary = ? AND list_contains(s.disciplines_secondary, ?))
           OR (s.discipline_primary = ? AND list_contains(s.disciplines_secondary, ?))
        ORDER BY s.confidence DESC
        LIMIT ?
    """, [discipline_a, discipline_b, discipline_b, discipline_a, limit]).fetchall()

    return [
        {"sense_id": r[0], "label": r[1], "primary_discipline": r[2],
         "secondary_disciplines": r[3], "confidence": r[4], "source": r[5],
         "disambiguation": r[6]}
        for r in rows
    ]


def lens_summary(conn, lens_id: str) -> dict:
    """Get summary stats for a lens view."""
    lens = conn.execute(
        "SELECT discipline_primary, disciplines_secondary, altitude FROM ontology_lenses WHERE lens_id = ?",
        [lens_id]
    ).fetchone()
    if not lens:
        return {}

    disc_primary, disc_sec, altitude = lens
    all_discs = [disc_primary] + (disc_sec or [])

    total = conn.execute("""
        SELECT COUNT(*) FROM keyword_senses
        WHERE discipline_primary = ANY(?)
           OR EXISTS (
               SELECT 1 FROM UNNEST(disciplines_secondary) AS d(v)
               WHERE d.v = ANY(?)
           )
    """, [all_discs, all_discs]).fetchone()[0]

    by_source = conn.execute("""
        SELECT origin_source, COUNT(*) FROM keyword_senses
        WHERE discipline_primary = ANY(?)
        GROUP BY origin_source ORDER BY COUNT(*) DESC
    """, [all_discs]).fetchall()

    bridges = conn.execute("""
        SELECT COUNT(*) FROM keyword_senses
        WHERE discipline_primary = ANY(?)
          AND array_length(disciplines_secondary) > 0
    """, [all_discs]).fetchone()[0]

    return {
        "lens_id": lens_id,
        "total_senses": total,
        "bridge_senses": bridges,
        "by_source": {r[0]: r[1] for r in by_source},
        "altitude": altitude,
    }


# =============================================================================
# Lens Query Engine — the product feature
# =============================================================================

def compose_lens(conn, discipline: str, role: str = "researcher",
                 interests: list[str] | None = None,
                 org_node: str | None = None) -> dict:
    """Compose a lens on the fly from Role × Discipline × Interest × Org.

    Returns a lens dict (not persisted) that can be passed to query functions.
    """
    template_id = f"hat:{discipline}:{role}"
    template = conn.execute(
        "SELECT * FROM ontology_lenses WHERE lens_id = ?", [template_id]
    ).fetchone()

    if template:
        cols = [desc[0] for desc in conn.description]
        lens = dict(zip(cols, template))
    else:
        disc_row = conn.execute(
            "SELECT discipline_id, long_name FROM disciplines WHERE discipline_id = ?",
            [discipline]
        ).fetchone()
        if not disc_row:
            raise ValueError(f"Unknown discipline: {discipline}")

        altitudes = {"director": 100000, "program_mgr": 10000, "researcher": 1000}
        lens = {
            "lens_id": f"composed:{discipline}:{role}",
            "name": f"{disc_row[1]} {role.replace('_', ' ').title()}",
            "discipline_primary": discipline,
            "disciplines_secondary": [],
            "altitude": altitudes.get(role, 1000),
            "role_type": role,
            "discipline_weights": {discipline: 0.95},
        }

    dw = lens.get("discipline_weights") or {}
    if isinstance(dw, str):
        dw = json.loads(dw)
    lens["discipline_weights"] = dw

    if interests:
        iw = {}
        for interest in interests:
            iw[interest] = 1.0
        lens["interest_weights"] = iw

    if org_node:
        lens["org_node_id"] = org_node

    return lens


def query_through_lens(conn, lens: dict, search: str | None = None,
                       limit: int = 50, min_score: float = 0.0,
                       source_filter: str | None = None) -> list[dict]:
    """Query senses through a composed lens with scoring and relationship context.

    Enhanced version: includes relationship counts, enrichment signals, and
    cross-domain bridge detection.
    """
    disc_weights = lens.get("discipline_weights", {})
    altitude = lens.get("altitude", 1000)

    tier_boost = {1: 2.0, 2: 1.5, 3: 1.0, 4: 0.5}

    level_filter = None
    if altitude >= 100000:
        level_filter = 2
    elif altitude >= 10000:
        level_filter = 4

    where_clauses = ["1=1"]
    params = []

    if search:
        where_clauses.append("LOWER(ks.keyword_label) LIKE ?")
        params.append(f"%{search.lower()}%")

    if level_filter is not None:
        where_clauses.append("(ks.origin_level IS NULL OR ks.origin_level <= ?)")
        params.append(level_filter)

    if source_filter:
        where_clauses.append("ks.origin_source = ?")
        params.append(source_filter)

    relevant_discs = [d for d, w in disc_weights.items() if w >= 0.2]
    if relevant_discs:
        placeholders = ",".join(["?" for _ in relevant_discs])
        where_clauses.append(f"ks.discipline_primary IN ({placeholders})")
        params.extend(relevant_discs)

    where_sql = " AND ".join(where_clauses)

    rows = conn.execute(f"""
        SELECT ks.sense_id, ks.keyword_label, ks.discipline_primary,
               ks.disciplines_secondary, ks.resolution_tier, ks.confidence,
               ks.origin_source, ks.origin_path, ks.disambiguation,
               ks.definition_in_context, ks.relevance_tags,
               (SELECT COUNT(*) FROM sense_relationships sr
                WHERE sr.source_sense_id = ks.sense_id
                   OR sr.target_sense_id = ks.sense_id) as rel_count
        FROM keyword_senses ks
        WHERE {where_sql}
        ORDER BY ks.confidence DESC NULLS LAST
        LIMIT ?
    """, params + [limit * 5]).fetchall()

    results = []
    for row in rows:
        (sid, label, disc, disc_sec, tier, conf,
         osrc, opath, disambig, defn, tags, rel_count) = row

        weight = disc_weights.get(disc, 0.05)
        for d in (disc_sec or []):
            w = disc_weights.get(d, 0)
            if w > 0:
                weight = max(weight, w * 0.7)

        boost = tier_boost.get(tier, 0.5)

        enrichment_bonus = 0.0
        if tags:
            tag_str = " ".join(tags)
            if "abstract_freq:" in tag_str:
                enrichment_bonus += 0.1
            if "title_freq:" in tag_str:
                enrichment_bonus += 0.05
            if "pub_freq:" in tag_str:
                enrichment_bonus += 0.05

        score = weight * (conf or 0.5) * boost + enrichment_bonus

        is_bridge = False
        bridge_to = []
        if disc_sec:
            for d in disc_sec:
                if d in disc_weights and d != disc:
                    is_bridge = True
                    bridge_to.append(d)

        if score < min_score:
            continue

        results.append({
            "sense_id": sid,
            "label": label,
            "discipline": disc,
            "score": round(score, 4),
            "confidence": conf,
            "tier": tier,
            "source": osrc,
            "path": opath,
            "disambiguation": disambig,
            "definition": defn,
            "is_bridge": is_bridge,
            "bridge_to": bridge_to,
            "rel_count": rel_count,
            "tags": tags,
        })

    results.sort(key=lambda x: -x["score"])
    return results[:limit]


def explore_from_keyword(conn, keyword: str, lens: dict,
                         depth: int = 1, limit: int = 30) -> dict:
    """Explore relationships from a keyword through a lens.

    Returns the keyword's senses and their connected neighbors, scored through
    the lens perspective. Like looking at one node in the graph through a lens.
    """
    disc_weights = lens.get("discipline_weights", {})

    senses = conn.execute("""
        SELECT sense_id, keyword_label, discipline_primary,
               confidence, origin_source, disambiguation
        FROM keyword_senses
        WHERE LOWER(keyword_label) = LOWER(?)
        ORDER BY confidence DESC
    """, [keyword]).fetchall()

    if not senses:
        senses = conn.execute("""
            SELECT sense_id, keyword_label, discipline_primary,
                   confidence, origin_source, disambiguation
            FROM keyword_senses
            WHERE LOWER(keyword_label) LIKE LOWER(?)
            ORDER BY confidence DESC
            LIMIT 10
        """, [f"%{keyword}%"]).fetchall()

    center_senses = []
    for s in senses:
        weight = disc_weights.get(s[2], 0.1)
        center_senses.append({
            "sense_id": s[0], "label": s[1], "discipline": s[2],
            "confidence": s[3], "source": s[4], "disambiguation": s[5],
            "lens_weight": round(weight, 2),
        })

    all_sense_ids = [s["sense_id"] for s in center_senses]
    if not all_sense_ids:
        return {"keyword": keyword, "senses": [], "neighbors": [], "paths": []}

    placeholders = ",".join(["?" for _ in all_sense_ids])
    neighbors_raw = conn.execute(f"""
        SELECT sr.source_sense_id, sr.target_sense_id,
               sr.relationship_type, sr.confidence, sr.provenance,
               ks.keyword_label, ks.discipline_primary, ks.origin_source
        FROM sense_relationships sr
        JOIN keyword_senses ks ON ks.sense_id = CASE
            WHEN sr.source_sense_id IN ({placeholders}) THEN sr.target_sense_id
            ELSE sr.source_sense_id
        END
        WHERE sr.source_sense_id IN ({placeholders})
           OR sr.target_sense_id IN ({placeholders})
        ORDER BY sr.confidence DESC
        LIMIT ?
    """, all_sense_ids + all_sense_ids + all_sense_ids + [limit * 3]).fetchall()

    neighbors = []
    seen_labels = set()
    for row in neighbors_raw:
        src, tgt, rtype, conf, prov, nlabel, ndisc, nsrc = row
        if nlabel.lower() in seen_labels:
            continue
        seen_labels.add(nlabel.lower())

        weight = disc_weights.get(ndisc, 0.05)
        score = weight * (conf or 0.5)

        neighbors.append({
            "label": nlabel,
            "discipline": ndisc,
            "relationship": rtype,
            "confidence": conf,
            "provenance": prov,
            "source": nsrc,
            "lens_score": round(score, 4),
        })

    neighbors.sort(key=lambda x: -x["lens_score"])

    return {
        "keyword": keyword,
        "senses": center_senses,
        "neighbors": neighbors[:limit],
        "total_neighbors": len(neighbors_raw),
    }


def compare_lenses(conn, keyword: str, lens_ids: list[str]) -> dict:
    """Compare how a keyword looks through different lenses.

    Shows which senses surface, which get suppressed, and what relationships
    appear most relevant from each perspective.
    """
    results = {}
    for lid in lens_ids:
        lens_row = conn.execute(
            "SELECT * FROM ontology_lenses WHERE lens_id = ?", [lid]
        ).fetchone()
        if not lens_row:
            continue
        cols = [desc[0] for desc in conn.description]
        lens = dict(zip(cols, lens_row))
        dw = lens.get("discipline_weights") or {}
        if isinstance(dw, str):
            dw = json.loads(dw)
        lens["discipline_weights"] = dw

        exploration = explore_from_keyword(conn, keyword, lens, limit=15)

        top_disciplines = {}
        for n in exploration["neighbors"]:
            d = n["discipline"]
            if d not in top_disciplines:
                top_disciplines[d] = 0
            top_disciplines[d] += 1

        results[lid] = {
            "lens_name": lens.get("name", lid),
            "role": lens.get("role_type"),
            "primary_discipline": lens.get("discipline_primary"),
            "senses_found": len(exploration["senses"]),
            "top_neighbors": exploration["neighbors"][:5],
            "discipline_spread": dict(sorted(
                top_disciplines.items(), key=lambda x: -x[1]
            )[:5]),
            "total_connections": exploration["total_neighbors"],
        }

    return {"keyword": keyword, "perspectives": results}


def list_lenses(conn, role: str | None = None,
                discipline: str | None = None) -> list[dict]:
    """List available template lenses with optional filtering."""
    where = ["is_template = true"]
    params = []
    if role:
        where.append("role_type = ?")
        params.append(role)
    if discipline:
        where.append("discipline_primary = ?")
        params.append(discipline)

    rows = conn.execute(f"""
        SELECT lens_id, name, role_type, discipline_primary, altitude
        FROM ontology_lenses
        WHERE {' AND '.join(where)}
        ORDER BY discipline_primary, altitude DESC
    """, params).fetchall()

    return [
        {"lens_id": r[0], "name": r[1], "role": r[2],
         "discipline": r[3], "altitude": r[4]}
        for r in rows
    ]


def init_ontology(conn) -> dict:
    """Initialize the full ontology layer."""
    stats = {}
    stats["disciplines"] = populate_disciplines(conn)
    stats["netl_envelope"] = populate_netl_envelope(conn)
    stats["template_hats"] = populate_template_hats(conn)
    stats["keyword_senses"] = populate_keyword_senses(conn)
    stats["polysemous_labels"] = populate_polysemy_senses(conn)
    stats["sense_relationships"] = populate_sense_relationships(conn)
    stats["wos_keyword_senses"] = populate_wos_keyword_senses(conn)
    stats["wos_metadata_senses"] = populate_wos_category_senses(conn)
    stats["wos_vocab_senses"] = populate_tab2_vocab_senses(conn)
    return stats
