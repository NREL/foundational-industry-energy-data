
# Proposed Workflow

## Description

Process for automatically populating the foundational industry energy dataset.

## Steps

1. Download all relevant "site" and "facility" fields from EPA FRS service for all covered facilities, including corresponding GHGRP Facility IDs and National Emissions Inventory (NEI) IDs.
2. Match GHGRP Facility IDs to pre-populated data set developed by NREL and ANL for ammonia, iron and steel, and cement facilities.
    Fields: annual total GHG emissions (MTCO2eq), plant capacity (tonnes), ...
    **Need to figure out if unit information is obtained from GHGRP or NEI.** Will need to develop some sort of algorithm to decide. 
3. For NEI facilities,
    1. Pull out unique unit IDs
    2. Use SCC codes to parse unit and fuel types for each unique unit ID. 
    3. **Which emissions to use?**
    4. Use WebFire emissions factors and SCC codes to back out combustion energy and througput
4. Assign average production and energy intensity values for all remaining facilities
5. ...

## APIs

* SCC
* Emissions factors (WebFires)
* GHGRP
* FRS
