# animation

07 — Data Search from Analysis-Ready Datasets (ARDs) [Exploration]

In this final phase, the objective is to develop a structured data search layer on top of the Analysis-Ready Datasets (ARDs), enabling efficient identification and retrieval of specific variables, parameters (e.g., PARAMCD), and clinical signals across studies.

This step focuses on building a searchable and user-friendly mechanism (e.g., R scripts or Shiny application) that allows the team to quickly locate relevant data elements without manually navigating multiple datasets.

The goal is to improve data accessibility, traceability, and usability, facilitating faster exploration and supporting both clinical understanding and downstream analysis.

Objectives of this step:
Enable fast lookup of parameters (PARAMCD/PARAM) and variables across ARDs
Provide cross-study search capabilities (e.g., UNIFI, UNIFI-JR, FIGARO-UC1, GALAXI)
Improve traceability of variables back to source datasets (ADaM/SDTM)
Reduce manual effort in identifying where specific clinical data points are located
Support exploratory analysis and feature discovery for ML modeling
Outputs of this step:
Searchable variable/parameter dashboard per study
Centralized parameter (PMT) dictionary with search functionality
Search tool (R script or Shiny app) for dynamic querying of ARDs
Indexed mapping of variables and parameters across studies
Tools / Scripts used:
R (data.table / dplyr for fast search and indexing)
Optional: R Shiny app for interactive exploration
Script: data_search.R
