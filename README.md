# Foundational Industry Energy Data

## Summary

This is an effort by the National Renewable Energy Laboratory (NREL) and Argonne National Laboratory (ANL) to create an experimental foundational industry dataset for energy and emissions analysis and modeling. The code draws from various publicly-available data, primarily from the U.S. EPA, to compile a data set on unit-level energy use and characterization for U.S. industrial facilities in 2017.

## Getting Started

### Manual Data Downloads

Due to the nature of how they are provided, several data sets must be manually downloaded before the code can be run sucessfully. These data sets and their director locations are:

1. Source Classification Codes (SCCs)

    * Download from <https://sor-scc-api.epa.gov/sccwebservices/sccsearch/>

    * Save to `data/SCC/SCCDownload.csv`

2. 2017 National Emissions Inventory (NEI)

    * Download from <https://gaftp.epa.gov/air/nei/2017/data_summaries/2017v1/2017neiJan_facility_process_byregions.zip>

    * Save **and unzip** data to `data/NEI/`.

    * `nei_EF_calculations.py` will format and combine the unzipped csv files into `nei_ind_data.csv`
  
3. GHGRP Emissions by Unit and Fuel Type

    * Download from <https://www.epa.gov/system/files/other-files/2022-10/emissions_by_unit_and_fuel_type_c_d_aa_10_2022.zip>

    * Save to `data/GHGRP/`

    * `ghgrp_fac_unit.py` will unzip and format these data.

### Environment

`fied_environment.yml` is the conda environment used when creating the foundational dataset. Its key dependencies include:

* `python=3.9.18=h6244533_0`
* `pandas=1.2.0=py39h2e25243_1`
* `numpy=1.23.4=py39hbccbffa_1`
* `geopandas=0.12.1=pyhd8ed1ab_1`
* `openpyxl=3.0.10=py39h2bbff1b_0`

## Compiling the Dataset

In addition to manually downloading the above datasets, executing the calulations and data compilation requires two steps after activating the fied environment.

1. `./frs/frs_extraction.py`. This will download, extract, and format EPA FRS data. The resulting csv should be saved in `data/FRS/`.
2. `fied_compilation.py`. This will execute all of the remaining steps for compiling the foundational data set.

So, from the terminal or Anaconda prompt:

```text
conda activate fied

python ./frs/frs_extraction.py

python fied_compilation.py
```

## Directory Navigation

The underlying submodules and data are organized as follows:

* [analysis](/analysis/): Methods for analyzing and generating figures of the final dataset.
* [data](/data/): Most folders are created locally for organizing raw data. Contains a [directory list](/data/dir_structure.md).
* [energy](/energy/): Not currently used. For future estimation of facility energy use based on alternative approaches. 
* [frs](/frs): Methods for downloading and formatting EPA Facility Registry Service data.
* [geocoder](/geocoder/): Methods for collecting missing geographical information for facilities.
* [ghgrp](/ghgrp/): Methods for estimating energy use from GHG emissions reported under EPA's Greenhouse Gas Reporting Program. Based on previous projects, such as the [Industry Energy Data Book](https://github.com/NREL/Industry-energy-data-book).
* [nei](/nei/): Methods for downloading and formatting data from EPA's National Emissions Inventory and for using these data to characterize combustion units.
* [qpc](/qpc/): Methods for downloading and formatting operating hours reported under the Census Bureau's Quaterly Survey of Plant Capacity Utlization.
* [scc](/scc/):  Methods to download and apply EPA's Source Classification Codes for characterizing units.
* [tests](/tests/): Testing. Currently very limited.
* [tools](/tools/): Methods that act as various tools used across submodules.

## Overivew of Foundational Industry Energy Data Fields

All facilities in the data set are represented by their unique `registryID`, which is their EPA [Facility Registry Service ID](https://www.epa.gov/frs/frs-physical-data-model).

Many of these data fields were included in original EPA data sources. See the [FRS data dictionary](https://www.epa.gov/frs/frs-data-dictionary) for more information.

### Identity

In addition to `registryID`, other identifying fields include

* `eisFacilityID`: EPA ID assigned to facilities reporting to the Emissions Inventory System (EIS)
* `ghgrpID`: EPA ID assigned to facilities reporting under the Greenhouse Gas Reporting Program (GHGRP)
* `name`: Name of facility.
* `locationDescription`: Description of the facility location.
* `naicsCode`: The facility's North American Industrial Classification System (NAICS) code.
* `naicsCodeAdditional`: A facility may have additional NAICS codes assigned (e.g., different reporting systems may have different NAICS assigned).

### Geography

Various levels of geographic identifiers are included, such as

* `geoID`: see [Census description of geographic identifiers (GEOIDs)](https://www.census.gov/programs-surveys/geography/guidance/geo-identifiers.html)
* `latitude`
* `longitude`
* `postalCode`
* `countyNAME`
* `countyFIPS`
* `stateName`
* `legislativeDisctrictNumber`

### Units and Processes

Individual units are characterized (e.g., unit type, capacity, energy, throughput) where possible. Individual units may be associated with multiple processes.

* `designCapacity`
* `eisUnitID`
* `unitName`
* `unitType`
* `unitTypeStd`
* `processDescription`
* `eisProcessID`

### Energy

Depending on the estimation approach, a unit may have a single estimate of energy use, or a range of energy estimates (i.e., minimum, median, upper quartile). Energy estimates based on the NEI are presented as a range.

* `energyMJ`: energy estimate in MJ
* `energyMJ0`: minimum of energy estimate, in MJ
* `energyMJq2`: median of energy estimate, in MJ
* `energyMJq3`: upper quartile of energy estimate, in MJ.
* `fuelType`: combusted fuel type as reported by original data source
* `fuelTypeStd`: combusted fuel type, standardized

### Other

We've attempted to include additional descriptive fields where possible. These tend to be sparsely populated at this time.

* `hucCode8`: Hydrolic Unit Code. Not currently implemented.
* `weeklyOpHours`: Average weekly operating hours by quarter, including 95% confidence interval ranges.
* `sensitiveInd`: Indicates whether or not the associated data is enforcement sensitive.
* `envJusticeCode`: The code that identifies the type of environmental justice concern affecting the facility or enforcement action.
* `smallBusInd`: Code indicating whether or not a business is requesting relief under EPA’s Small Business Policy, which applies to businesses having less than 100 employees.
* `througputTonne`: Estimated mass throughput
