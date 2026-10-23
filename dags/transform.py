"""Normalise rows from all sources into one schema and drop duplicates across sources."""
import re

BRANDS = [
    ("WESTERN DIGITAL", "WD"), ("WD", "WD"), ("ADATA", "ADATA"), ("XPG", "ADATA"), ("KINGSTON", "KINGSTON"),
    ("SAMSUNG", "SAMSUNG"), ("SEAGATE", "SEAGATE"), ("CRUCIAL", "CRUCIAL"), ("LEXAR", "LEXAR"),
    ("GIGABYTE", "GIGABYTE"), ("AORUS", "GIGABYTE"), ("PNY", "PNY"), ("CORSAIR", "CORSAIR"),
    ("G.SKILL", "GSKILL"), ("GSKILL", "GSKILL"), ("TEAMGROUP", "TEAMGROUP"), ("TEAM", "TEAMGROUP"),
    ("KLEVV", "KLEVV"), ("APACER", "APACER"), ("PATRIOT", "PATRIOT"), ("ASUS", "ASUS"), ("MSI", "MSI"),
    ("SANDISK", "SANDISK"), ("SK HYNIX", "SKHYNIX"), ("HYNIX", "SKHYNIX"), ("HIKVISION", "HIKVISION"),
    ("BIWIN", "BIWIN"), ("ADDLINK", "ADDLINK"), ("ZADAK", "ZADAK"), ("KIOXIA", "KIOXIA"),
    ("TOSHIBA", "TOSHIBA"), ("INTEL", "INTEL"), ("MICRON", "MICRON"), ("PREDATOR", "ACER"),
    ("ACER", "ACER"), ("HP", "HP"), ("V-COLOR", "VCOLOR"), ("TFORCE", "TEAMGROUP"), ("T-FORCE", "TEAMGROUP"),
]
GENERIC = {
    "SSD", "M", "2", "M2", "NVME", "PCIE", "PCI", "E", "GEN", "GEN3", "GEN4", "GEN5", "3", "4", "5", "X4",
    "4X4", "3X4", "SATA", "III", "RAM", "DDR4", "DDR5", "UDIMM", "DIMM", "DESKTOP", "KIT", "PC", "NAND",
    "INTERNAL", "FOR", "THE", "AND", "WITH", "GB", "TB", "MHZ", "Y", "NB", "NOTEBOOK",
}
PRIORITY = {"ihavecpu": 0, "jib": 1, "notebookspec": 2}


def _brand(upper):
    best = None
    for key, norm in BRANDS:
        m = re.search(r"(?<![A-Z0-9])" + re.escape(key) + r"(?![A-Z0-9])", upper)
        if m and (best is None or m.start() < best[0]):
            best = (m.start(), norm, key)
    return (best[1], best[2]) if best else ("OTHER", None)


def _capacity_gb(upper):
    m = re.search(r"(\d+(?:\.\d+)?)\s*(TB|GB)", upper)
    if not m:
        return None
    v = float(m.group(1))
    return v * 1024 if m.group(2) == "TB" else v


def normalise(row):
    name = row["name"]
    upper = name.upper()
    price = row.get("price")
    cap = _capacity_gb(upper)
    if not price or price <= 0 or cap is None:
        return None
    cat = row["category"]
    ddr = re.search(r"DDR[45]", upper)
    if cat == "RAM":
        if not ddr or re.search(r"NOTEBOOK|SO-?DIMM|โน้ตบุ๊ก|โน๊ตบุ๊ก", upper + name):
            return None
        iface = ddr.group(0)
    else:
        iface = "NVME" if re.search(r"NVME|PCIE|PCI-E|M\.2", upper) else ("SATA" if re.search(r"SATA|2\.5", upper) else "UNKNOWN")
    brand, brand_key = _brand(upper)
    s = re.sub(r"\([^)]*\)", " ", upper)                         # drop (Thai text / codes)
    s = re.sub(r"[฀-๿]", " ", s)                      # drop Thai chars
    s = re.sub(r"\d+(?:\.\d+)?\s*(TB|GB)", " ", s)                 # capacity
    if brand_key:
        s = re.sub(r"(?<![A-Z0-9])" + re.escape(brand_key) + r"(?![A-Z0-9])", " ", s)
    toks = [t for t in re.findall(r"[A-Z0-9]+", s)
            if t not in GENERIC and not re.fullmatch(r"\d+Y|\d{4}|\d+MHZ|CL\d+|\d+X\d+|\d{1,2}", t)]
    key = f"{cat}|{brand}|{int(cap)}|{iface if cat == 'RAM' else ''}|{' '.join(toks[:3])}"
    return {
        "shop": row["shop"], "dedup_key": key, "source": row["source"], "category": cat, "brand": brand,
        "capacity_gb": cap, "interface": iface, "name": name[:300], "price": price, "url": row["url"],
        "image_url": row.get("image"), "scraped_at": row["scraped_at"],
    }


def merge(rows):
    """Normalise, then drop duplicates (same product at the same shop seen via several sources)."""
    out = [r for r in (normalise(x) for x in rows) if r]
    out.sort(key=lambda r: (PRIORITY.get(r["source"], 9), r["price"]))
    seen, uniq = set(), []
    for r in out:
        k = (r["shop"], r["dedup_key"])
        if k in seen:
            continue
        seen.add(k)
        uniq.append(r)
    return uniq, len(out) - len(uniq)
