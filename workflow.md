
# Proposed Workflow

## Description

Process for automatically populating the foundational industry energy dataset.

## Steps

1. Download all relevant "site" and "facility" fields from EPA FRS service for all covered facilities, including corresponding GHGRP Facility IDs and National Emissions Inventory (NEI) IDs.
2. Match GHGRP Facility IDs to pre-populated data set developed by NREL and ANL for ammonia, iron and steel, and cement facilities.
    Fields: annual total GHG emissions (MTCO2eq), plant capacity (tonnes), ...
3. For NEI facilities,
    1. Use SCC codes to parse unit and fuel types
    2. Use WebFire emissions factors and SCC codes to back out combustion energy and througput
4. Assign average production and energy intensity values for all remaining facilities
5. ...