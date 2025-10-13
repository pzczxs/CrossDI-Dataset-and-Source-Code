# -*- coding: utf-8 -*-
import csv
import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import sys
import math
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import openpyxl

def _drop_header(lines):
    for idx, line in enumerate(lines):
        if line.strip():
            return lines[idx+1:]
    return []

def read_citation_file(filepath):
    citations = []
    with open(filepath, "r", encoding="utf_8_sig") as f:
        raw_lines = f.read().splitlines()
        total_lines = len(raw_lines)
        lines = _drop_header(raw_lines)

        for i, line in enumerate(lines, start=2):
            if line.strip():
                parts = line.strip().split("\t")
                if len(parts) != 2:
                    print(f"Warning: file {filepath} line {i} has invalid format: {line}")
                else:
                    citations.append((parts[0].strip().strip('"'), parts[1].strip().strip('"')))
    print(f"Loaded file {filepath}: {len(citations)} data lines (total lines: {total_lines})")
    return citations, total_lines

def read_doi_year_file(filepath):
    doi_year = {}
    with open(filepath, "r", encoding="utf_8_sig") as f:
        raw_lines = f.read().splitlines()
        total_lines = len(raw_lines)
        lines = _drop_header(raw_lines)

        for i, line in enumerate(lines, start=2):
            if line.strip():
                parts = line.strip().split("\t")
                if len(parts) != 2:
                    print(f"[{filepath}] line {i}: expected 2 columns, got {len(parts)} -> {line}")
                    continue
                doi = parts[0].strip().strip('"')
                year = parts[1].strip().strip('"')
                if not year.isdigit():
                    print(f"[{filepath}] line {i}: non-numeric year '{year}' (DOI={doi})")
                doi_year[doi] = year

    print(f"Loaded file {filepath}: {len(doi_year)} rows (total lines: {total_lines})")
    return doi_year

def read_target_file(filepath):
    targets = []
    with open(filepath, "r", encoding="utf_8_sig") as f:
        raw_lines = f.read().splitlines()
        total_lines = len(raw_lines)
        lines = _drop_header(raw_lines)

        for i, line in enumerate(lines, start=2):
            doi = line.strip().strip('"').lstrip('\ufeff')
            if doi:
                targets.append(doi)
    print(f"Loaded target file {filepath}: {len(targets)} DOIs (total lines: {total_lines})")
    return targets

def load_all_inputs(doi_path, target_path, source_files):
    expected_citation_keys = set(source_files.keys())
    results = {"doi_year": None, "targets": None, "citations": {}}

    with ThreadPoolExecutor(max_workers=2 + len(source_files)) as executor:
        future_map = {}
        future_map[executor.submit(read_doi_year_file, doi_path)] = ("doi_year", None)
        future_map[executor.submit(read_target_file, target_path)] = ("targets", None)
        for source_name, src_path in source_files.items():
            future_map[executor.submit(read_citation_file, src_path)] = ("citations", source_name)

        for future in as_completed(future_map):
            result_type, key = future_map[future]
            data = future.result()
            if result_type == "doi_year":
                results["doi_year"] = data
            elif result_type == "targets":
                results["targets"] = data
            else:
                results["citations"][key] = data

    missing = expected_citation_keys - set(results["citations"].keys())
    if missing:
        raise RuntimeError(f"Missing citation data for sources: {sorted(missing)}")
    if results["doi_year"] is None or results["targets"] is None:
        raise RuntimeError("Base DOI-year mapping or target list failed to load")

    return results["doi_year"], results["targets"], results["citations"]

# ===== Build citation networks =====
def build_citation_dict(citations, doi_year_dict):
    citation_dict = defaultdict(set)
    citing_year = {}
    for cited, citing in citations:
        citation_dict[citing].add(cited)
        if citing in doi_year_dict:
            citing_year[citing] = doi_year_dict[citing]
    return citation_dict, citing_year

def build_reverse_dict(citation_dict):
    rev = defaultdict(set)
    for citing, cited_set in citation_dict.items():
        for cited in cited_set:
            rev[cited].add(citing)
    return rev

def prepare_citing_year_bins(citing_year_dict):
    year_bins = defaultdict(set)
    invalid_entries = []
    for doi, year in citing_year_dict.items():
        year_str = str(year)
        if year_str.isdigit():
            year_bins[int(year_str)].add(doi)
        else:
            invalid_entries.append((doi, year))
    sorted_years = sorted(year_bins.keys())
    return sorted_years, year_bins, invalid_entries

def _count_intersection_size(set_a, set_b):
    if not set_a or not set_b:
        return 0
    if len(set_a) < len(set_b):
        small, large = set_a, set_b
    else:
        small, large = set_b, set_a
    return sum(1 for item in small if item in large)

# ===== Global hot references =====
def get_global_hotrefs_windowed(target_dois, citation_dict, reverse_dict, allowed_citing, x_pct=0.03):
    freq_A = {}
    for target in target_dois:
        setA = citation_dict.get(target, set())
        if not setA:
            continue
        for a in setA:
            rev_set = reverse_dict.get(a, set())
            if not rev_set:
                continue
            cnt = _count_intersection_size(rev_set, allowed_citing)
            if cnt:
                freq_A[a] = freq_A.get(a, 0) + cnt

    if not freq_A:
        return set()

    k = max(1, math.ceil(x_pct * len(freq_A)))
    sorted_items = sorted(freq_A.items(), key=lambda kv: (-kv[1], kv[0]))
    kth_freq = sorted_items[min(k, len(sorted_items)) - 1][1]
    return {a for a, c in freq_A.items() if c >= kth_freq}

# ===== Metrics calculation =====
def calc_DI_metrics(target_doi, citation_dict, reverse_dict, allowed_citing, X_top_global):
    setA = citation_dict.get(target_doi, set())
    setD_target = (reverse_dict.get(target_doi, set()) & allowed_citing)

    # Original DI
    setB, setC, setM = set(), set(), set()
    for citing in allowed_citing:
        cited_set = citation_dict.get(citing)
        if not cited_set or citing == target_doi:
            continue
        intersects_A = bool(setA) and not setA.isdisjoint(cited_set)
        if target_doi in cited_set:
            if intersects_A:
                setB.add(citing)
            else:
                setM.add(citing)
        elif intersects_A:
            setC.add(citing)

    C_val = len(setM)   # N_F
    D_val = len(setB)   # N_B
    E_val = len(setC)   # N_R
    denominator = (C_val + D_val + E_val)
    DI = (C_val - D_val) / denominator if denominator != 0 else None

    m = len(setD_target)
    mDI = m * DI if DI is not None else None

    # DI5
    D5_count = 0
    if setA:
        for citing in allowed_citing:
            cited_set = citation_dict.get(citing)
            if not cited_set or target_doi not in cited_set:
                continue
            if _count_intersection_size(cited_set, setA) >= 5:
                D5_count += 1
    DI5 = (C_val - D5_count) / (C_val + D5_count + E_val) if (C_val + D5_count + E_val) != 0 else None

    # DI^noR
    DI_noR = (C_val - D_val) / (C_val + D_val) if (C_val + D_val) != 0 else None

    # Global 3% DI
    setB_new, setC_new, setM_new = set(), set(), set()
    for citing in allowed_citing:
        cited_set = citation_dict.get(citing)
        if not cited_set or citing == target_doi:
            continue
        if citing in setD_target:
            cited_after_trim = cited_set - X_top_global if X_top_global else cited_set
        else:
            cited_after_trim = cited_set
        intersects_trimmed = bool(setA) and not setA.isdisjoint(cited_after_trim)
        if target_doi in cited_after_trim:
            if intersects_trimmed:
                setB_new.add(citing)
            else:
                setM_new.add(citing)
        elif intersects_trimmed:
            setC_new.add(citing)

    C_new = len(setM_new)
    D_new = len(setB_new)
    E_new = len(setC_new)
    denominator_new = (C_new + D_new + E_new)
    DI_global = (C_new - D_new) / denominator_new if denominator_new != 0 else None

    # DEP
    TR = 0
    for d in setD_target:
        refs_d = citation_dict.get(d, set())
        TR += _count_intersection_size(refs_d, setA)
    C = len(setD_target)
    DEP = (TR / C) if C != 0 else None

    # Orig_base
    Count = 0
    if setA:
        for d in setD_target:
            refs_d = citation_dict.get(d, set())
            Count += _count_intersection_size(refs_d, setA)
    Origbase = 1 - (Count / (len(setD_target) * len(setA))) if (len(setD_target) * len(setA)) != 0 else None

    # Dual-view Destabilization and Consolidation
    ratio_list_destab, ratio_list_consol = [], []
    R_target = setD_target
    for a in setA:
        rev_set = reverse_dict.get(a, set())
        if not rev_set:
            R_a = set()
        else:
            R_a = {doi for doi in rev_set if doi in allowed_citing and doi != target_doi}
        p = len(R_target & R_a)
        o = len(R_target - R_a)
        q = len(R_a - R_target)
        denom = o + p + q
        if denom != 0:
            ratio_list_destab.append(o / denom)
            ratio_list_consol.append(p / denom)

    Destabilization = (sum(ratio_list_destab) / len(ratio_list_destab)) if ratio_list_destab else None
    Consolidation  = (sum(ratio_list_consol) / len(ratio_list_consol)) if ratio_list_consol else None

    metrics = {
        "N_F": C_val,
        "N_B": D_val,
        "N_R": E_val,
        "DI": DI,
        "mDI": mDI,
        "N_B^5": D5_count,
        "DI_5": DI5,
        "DI^noR": DI_noR,
        "N_F_new": C_new,
        "N_B_new": D_new,
        "DI_3%": DI_global,
        "DEP": DEP,
        "Orig_base": Origbase,
        "Destabilization(D)": Destabilization,
        "Consolidation(C)": Consolidation,
    }
    return metrics

# ===== Worker =====
_worker_state = {}

def _init_worker(citation_dict, reverse_dict, relevant_years, year_bins, cutoff_year, source_name, target_dois):
    global _worker_state
    _worker_state = {
        "citation_dict": citation_dict,
        "reverse_dict": reverse_dict,
        "relevant_years": tuple(relevant_years),
        "year_bins": year_bins,
        "cutoff_year": cutoff_year,
        "source_name": source_name,
        "target_dois": tuple(target_dois),  
    }


def _process_target(task):
    target, target_year = task
    data = _worker_state
    citation_dict = data["citation_dict"]
    reverse_dict = data["reverse_dict"]
    relevant_years = data["relevant_years"]
    year_bins = data["year_bins"]
    cutoff_year = data["cutoff_year"]

    Y_max = cutoff_year - target_year + 1
    allowed_citing = set()
    year_idx = 0
    if target in citation_dict:
        allowed_citing.add(target)

    results = []
    for Y in range(1, Y_max):
        threshold_year = target_year + Y
        while year_idx < len(relevant_years) and relevant_years[year_idx] <= threshold_year:
            allowed_citing.update(year_bins[relevant_years[year_idx]])
            year_idx += 1
        X_top_global_Y = get_global_hotrefs_windowed(
            data["target_dois"], citation_dict, reverse_dict, allowed_citing, x_pct=0.03)

        metrics = calc_DI_metrics(target, citation_dict, reverse_dict, allowed_citing, X_top_global_Y)
        metrics["DOI"] = target
        metrics["Publication year"] = target_year
        metrics["Y"] = Y
        metrics["Source"] = data["source_name"]
        # metrics["X_top_global_size"] = len(X_top_global_Y)
        results.append(metrics)

    return results

# ===== Main =====
if __name__ == "__main__":
    SOURCE_FILES = {
        "Dimensions":    "citations/citations-1-DIMENSIONS.csv",
        "OpenCitations": "citations/citations-1-OPEN_CITATIONS.csv",
        "WebOfScience":  "citations/citations-1-WEB_OF_SCIENCE.csv",
    }

    doi_year_dict, target_dois, citations_by_source = load_all_inputs(
        "doi/dois-1.csv",
        "target/target-1.csv",
        SOURCE_FILES,
    )

    cutoff_year = 2023
    all_sources_results = []
    logged_invalid_citing_years = set()

    for source_name, src_path in SOURCE_FILES.items():
        print(f"\n===== Processing source: {source_name} =====")
        citations, _ = citations_by_source[source_name]

        src_dict, src_citing_year = build_citation_dict(citations, doi_year_dict)
        src_reverse_dict = build_reverse_dict(src_dict)
        sorted_years, year_bins, invalid_year_entries = prepare_citing_year_bins(src_citing_year)
        relevant_years = [year for year in sorted_years if year <= cutoff_year]

        if invalid_year_entries:
            for doi, raw_year in invalid_year_entries:
                if doi not in logged_invalid_citing_years:
                    print(f"Warning: citing DOI {doi} invalid year -> {raw_year} (source: {source_name})")
                    logged_invalid_citing_years.add(doi)


        src_results = []
        tasks = []
        for target in target_dois:
            if target not in doi_year_dict:
                print(f"Target DOI {target} not found in doi_year_dict (source: {source_name}), skipped.")
                continue
            try:
                target_year = int(doi_year_dict[target])
            except Exception:
                print(f"Target DOI {target} invalid year format (source: {source_name}), skipped.")
                continue

            Y_max = cutoff_year - target_year + 1
            print(f"-- Target {target} ({target_year}), Y: 1..{Y_max-1}")
            tasks.append((target, target_year))

        if tasks:
            processes = min(cpu_count(), len(tasks)) or 1
            with Pool(
                processes=processes,
                initializer=_init_worker,
                initargs=(src_dict, src_reverse_dict, relevant_years, year_bins, cutoff_year, source_name, target_dois),
            ) as pool:
                for target_results in tqdm(
                    pool.imap_unordered(_process_target, tasks),
                    total=len(tasks),
                    desc=f"{source_name} | targets",
                    leave=False,
                ):
                    src_results.extend(target_results)

        df_src = pd.DataFrame(src_results)
        if not df_src.empty:
            df_src.sort_values(["DOI", "Y"], inplace=True)
            df_src.reset_index(drop=True, inplace=True)
            df_src["invDEP"] = 1 + (df_src.groupby(["Source", "Y"])["DEP"].transform("max") - df_src["DEP"])

        out_name = f"results-1-{source_name}.xlsx"
        df_src.to_excel(out_name, index=False)
        all_sources_results.append(df_src)

    if all_sources_results:
        df_all = pd.concat(all_sources_results, ignore_index=True)
        if not df_all.empty:
            df_all.sort_values(["Source", "DOI", "Y"], inplace=True)
            df_all.reset_index(drop=True, inplace=True)
            df_all["invDEP"] = 1 + (df_all.groupby(["Source", "Y"])["DEP"].transform("max") - df_all["DEP"])
        df_all.to_excel("results-1-ALL-SOURCES.xlsx", index=False)

