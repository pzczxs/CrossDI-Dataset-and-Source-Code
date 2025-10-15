# CrossDI-Dataset-and-Source-Code
**C**ross-source **D**isruption **I**ndexes (CrossDI) dataset and source code

Python implementation for computing disruption-style metrics using yearly windows. Only the citing side is time-truncated, and metrics are computed per target paper and per window. Multiple data sources and parallel processing are supported.

## 1. Domains
The numeral 1–4 in file names denotes a domain. For example, citations-1-DIMENSIONS.csv belongs to domain 1.

Domain mapping:  
ID = 1 — Synthetic Biology  
ID = 2 — Astronomy & Astrophysics  
ID = 3 — Blockchain-based Information System Management  
ID = 4 — Socio-Economic Impacts of Biological Invasions

For each domain, three files should be prepared: one article–year list (years are required for citing articles), and one target DOI list. Then, the script can be run to produce the resulting outputs for each domain.

## 2. Data File Layout
dataset  
├─ doi/  
│  ├─ dois-1.csv  
│  ├─ dois-2.csv  
│  ├─ dois-3.csv  
│  └─ dois-4.csv  
├─ target/  
│  ├─ target-1.csv  
│  ├─ target-2.csv  
│  ├─ target-3.csv  
│  └─ target-4.csv  
├─ citations/  
│  ├─ citations-1-DIMENSIONS.csv  
│  ├─ citations-1-OPEN_CITATIONS.csv  
│  ├─ citations-1-WEB_OF_SCIENCE.csv  
│  ├─ citations-2-DIMENSIONS.csv  
│  ├─ citations-2-OPEN_CITATIONS.csv  
│  ├─ citations-2-WEB_OF_SCIENCE.csv  
│  ├─ citations-3-DIMENSIONS.csv  
│  ├─ citations-3-OPEN_CITATIONS.csv  
│  ├─ citations-3-WEB_OF_SCIENCE.csv  
│  ├─ citations-4-DIMENSIONS.csv  
│  ├─ citations-4-OPEN_CITATIONS.csv  
│  └─ citations-4-WEB_OF_SCIENCE.csv  
└─ result/  
   ├─ results-1-DIMENSIONS.xlsx  
   ├─ results-1-OPEN_CITATIONS.xlsx  
   ├─ results-1-WEB_OF_SCIENCE.xlsx  
   ├─ results-1-ALL-SOURCES.xlsx  
   └─ same pattern for domains 2–4  

File definitions
- Article list: doi/dois-{ID}.csv  
  TSV with two columns: doi and year. Only citing articles must have valid years; cited references may have blank years.
- Citation edges: citations/citations-{ID}-{SOURCE}.csv  
  TSV with two columns: cited_doi and citing_doi. Direction is cited → citing. 
- Target articles: target/target-{ID}.csv  
  TSV with a single column named doi. These are the focal articles.
- Results: result/results-{ID}-{SOURCE}.xlsx  
  One spreadsheet per source. A merged file result/results-{ID}-ALL-SOURCES.xlsx is also produced.

## 3. Windowing
Let $y$ be the publication year of the target paper. Window $Y$ includes citing papers with year $\le y + Y$. The citing side is truncated by year.

## 4. DI Metrics
For each (Source, Target, $Y$), the script reports:  

$DI$, $mDI$, $DI_5$, $DI^{noR}$, $DI_{3\\%}$, $DEP$, $invDEP$, $Orig_{base}$, Destabilization ($D$), and Consolidation ($C$)

## 5. How to run
### 5.1 Parameter Setting
Set the paths at the bottom of the script. Example for domain 1:

```python
SOURCE_FILES = {
    "Dimensions":    "citations/citations-1-DIMENSIONS.csv",
    "OpenCitations": "citations/citations-1-OPEN_CITATIONS.csv",
    "WebOfScience":  "citations/citations-1-WEB_OF_SCIENCE.csv",
}
doi_year_path = "doi/dois-1.csv"
target_path   = "target/target-1.csv"
cutoff_year   = 2023
```

### 5.2 Run
```
python DI_windowed_parallel.py
```
Outputs:
- One Excel per source: result/results-{ID}-{SOURCE}.xlsx
- Merged results: result/results-{ID}-ALL-SOURCES.xlsx

### 5.3 Output columns  
N_F, N_B, N_R, DI, mDI, N_B^5, DI_5, DI^noR, N_F_new, N_B_new, DI_3%, DEP, Orig_base, Destabilization(D), Consolidation(C), DOI, Publication year, Y, Source, invDEP

## 6. References
[1] Shuo Xu, Congcong Wang, Xin An, and Jianhua Liu, 2025. CrossDI: A Comprehensive Dataset Crossing Three Databases for Calculating Disruption Indexes. *Scientific Data*. (Under review)

[2] Shuo Xu, Congcong Wang, Xin An, Yunkang Deng, and Jianhua Liu, 2025. [Do OpenCitations and Dimensions Serve as an Alternative to Web of Science for Calculating Disruption Indexes?](https://doi.org/10.1016/j.joi.2025.101685) *Journal of Informetrics*, Vol. 19, No. 3, pp. 101685. 

[3] Shuo Xu, Liyuan Hao, Xin An, Dongsheng Zhai, and Hongshen Pang, 2019. [Types of DOI Errors of Cited References in Web of Science with a Cleaning Method](https://doi.org/10.1007/s11192-019-03162-4). *Scientometrics*, Vol. 120, No. 3, pp. 1427-1437.
