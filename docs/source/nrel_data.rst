###############################
NREL Industrial Energy Datasets
###############################

NREL has developed several related and oftentimes overlapping industrial energy datasets. All datasets have been developed as attempts to overcome the limited resolution of traditional sources of energy data and are linked by their use of `EPA Greenhouse Gas Reporting Program (GHGRP) <https://enviro.epa.gov/envirofacts/ghg/search>`_ greenhouse gas emissions data to derive facility combustion energy use.

This page serves as a guide to those datasets and their use. The datasets are presented in chronological order and each is referred to by its respository name. In addition to a link to the dataset itself, links are also provided to associated Github repositories and known publications that have used each dataset.

Quick Guide
***********
**Looking for estimates of process heat demand by temperature?** See the :ref:`Industrial Process Heat Demand Characterization (2018) dataset <ind_comb_data_2018>`. 

**Looking for county-level energy estimates for all of U.S. industry (manufacturing, agriculture, construction and mining)?** See the :ref:`2018 Industrial Energy Data Book <iedb_2018>`.

**Looking for estimates of manufacturing facility-level combustion energy?** See the :ref:`2018 Industrial Energy Data Book <iedb_2018>`

**Looking for unit-level characteristics of industrial facilities, including energy estimates and design capacity?** See the :ref:`Foundational Industry Energy Dataset (FIED) <fied_2024>`.

**Looking for estimates of industrial facility-level electricity use?** 


.. _ind_comb_data_2016:

Industrial Facility Combustion Energy Use (2016)
************************************************

The first systematic use of EPA GHGRP data to derive facility-level combustion energy use. This initial version of the derivation methodology applied EPA default emissions factors to reported fuel use by fuel type. Additional facility information is included with calculated combustion energy values, such as industry type (six-digit NAICS code), location (lat, long, zip code, county, and state), combustion unit type, and combustion unit name. Energy estimates were disaggregated by end-use by applying data from the `2010 Manufacturing Energy Consumption Survey (MECS) <http://www.eia.gov/consumption/manufacturing/data/2010/>`_. 
A rough characterization of process heat temperatures was also included.
*Note that this dataset only includes 14 six-digit NAICS codes that were identified as focus industries.*  


* `NREL Data Catalog dataset link <https://doi.org/10.7799/1278644>`_
* `NREL/INL joint technical report:  <https://doi.org/doi:10.2172/1334495>`_
* GitHub repository: n/a

.. _ind_comb_data_2018:

Industrial Process Heat Demand Characterization (2018)
******************************************************

Represents an evolution of the methodology used in the :ref:`2016 dataset <ind_comb_data_2016>`, based on additional GHGRP data to derive combustion energy estimates based on the reporting Tier used by each industry reporter. 
*Note that this dataset only includes 14 six-digit NAICS codes that were identified as focus industries.*  

* `NREL Data Catalog dataset link <https://doi.org/10.7799/1461488>`_
* `Paper published in *Applied Energy* <https://doi.org/10.1016/j.apenergy.2019.01.077>`_
* `GitHub repository <https://github.com/NREL/Industrial-Heat-Demand-Analysis>`_
* Example applications:
   * Vanatta, Max, Deep Patel, Todd Allen, Daniel Cooper, and Michael T. Craig. "Technoeconomic analysis of small modular reactors decarbonizing industrial process heat." Joule 7, no. 4 (2023): 713-737 `https://doi.org/10.1016/j.joule.2023.03.009 <https://doi.org/10.1016/j.joule.2023.03.009>`_.

.. _county_data_2018:

United States County-Level Industrial Energy Use (2018)
*******************************************************

First implementation of a method to derive national estimates of industrial energy use at a county level using the foundataion of deriving facility-level combustion energy estimates. Includes estimates for non-manufacturing industries, in addition to manufacturing industries, as well as additional disaggregation by end use (e.g. machine drive process heating facility lighting is provided for manufacturing agriculture and mining industries).   

* `NREL Data Catalog dataset link <https//github.com/NREL/Industry-Energy-Tool/>`_
* `NREL technical report: "The Industry Energy Tool (IET): Documentation" <https://doi.org/10.2172/1484348>`_
* `GitHub Repository <https//github.com/NREL/Industry-Energy-Tool/>`_

.. _mfg_thermal:

Manufacturing Thermal Energy Use in 2014 (2019)
***********************************************

Further expansion of the method to derive :ref:`county-level industrial energy estimates <county_data_2018>`, focusing on characterizing indusrial process heat demand. Estimated thermal energy use (i.e., fuels combusted for process heating, boilers, and combined heat and power/cogeneration) by end use, temperature, county, and facility employment size class for all U.S. manufacturing industries in 2014. The estimation methodology builds off of the 
The Data Catalog entry also includes hourly representative heat load shapes for boilers and generic process heating equipment. These load shapes were used to 

* `NREL Data Catalog dataset link <https://data.nrel.gov/submissions/118>`_
* `NREL technical report: Opportunities for Solar Industrial Process Heat <https://doi.org/10.2172/1762440>`_
* `GitHub repository <https://github.com/NREL/Solar-for-Industry-Process-Heat/>`_

.. _iedb_2018:

2018 Industrial Energy Data Book (2020)
***************************************

The Industrial Energy Data Book (IEDB) aggregates and synthesizes information on the trends in industrial energy use, energy prices, economic activity, and water use. The IEDB also estimates county-level industrial energy use and combustion energy use of large energy-using facilities (i.e., facilities required to report greenhouse gas emissions under the EPA's Greenhouse Gas Reporting Program). These estimates are derived from publicly available sources from EPA, EIA, Census Bureau, USDA, and USGS.
The IEDB included analysis of county-level industrial energy use, as well as facility-level combustion energy estimates from GHGRP reporters.

* `NREL Data Catalog dataset link <https://doi.org/10.7799/1575074>`_
* `EERE Industrial Energy Data Book <https://www.energy.gov/eere/analysis/articles/2018-industrial-energy-data-book>`_
* `GitHub repository <https://github.com/NREL/Industry-energy-data-book>`_


.. _fied_2024:

Foundational Industry Energy Dataset (FIED) (2024)
**************************************************

The Foundational Industry Energy Dataset (FIED) addresses several of the areas of growing disconnect between the demands of industrial energy analysis and the state of industrial energy data by providing unit-level characterization by facility. Each facility is identified by a unique registryID, based on the EPA's `Facility Registry Service <https://www.epa.gov/frs>`_, and includes its coordinates and other geographic identifiers. 
Energy-using units are characterized by design capacity, as well as their estimated energy use, greenhouse gas emissions, and physical throughput using 2017 data from the EPA's `National Emissions Inventory <https://www.epa.gov/air-emissions-inventories/2017-national-emissions-inventory-nei-data>`_ and `Greenhouse Gas Reporting Program <https://www.epa.gov/ghgreporting>`_.

* `Open Energy Data Initiative link <https://doi.org/10.25984/2437657>`_.
* `NREL report: The Foundational Industry Energy Dataset (FIED): Open-Source Data on Industrial Facilities <https://www.nrel.gov/docs/fy24osti/90442.pdf>`_
* `GitHub repository <https://github.com/NREL/foundational-industry-energy-data/>`_
