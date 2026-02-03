# TYNDP_processing

This script reads the ten-year network development plans (TYNDP) published by ENTSO-E and ENTSOG in 2022 and in 2024 and processes the data to generate a common format of generator capacities, dispatched generation, and demand with associated demand profiles for selected scenarios (GA: Global Ambition, DE: Distributed Energy), target years, and climate years (CY).

The folder TYNDP_in contains the input data. 
Input files associated to TYNDP 2022 can be downloaded from https://2022.entsos-tyndp-scenarios.eu/download/ and include:
- 220310_Updated_Electricity_Modelling_Results_TYNDP2022.xlsx
- Demand_TimeSeries_{YEAR}_GA_release.xlsb   where YEAR = {2030, 2040, 2050}
(Total number of files: 4)

Input files associated to TYNDP 2024 can be downloaded from https://2024.entsos-tyndp-scenarios.eu/download/ and include:
- MMStandardOutputFile_DE{YEAR}_Plexos_{CY}_offshore_v11_SoS.xlsb   where YEAR = {2035, 2050} and CY = {CY1995, CY2008}
- MMStandardOutputFile_DE{YEAR}_Plexos_{CY}_v11_SoS.xlsb   where YEAR = {2035, 2050} and CY = {CY1995, CY2008}
(Total number of files: 8)

The share of hydro run-of-river and hydro dam power plant capacity and generation from TYNDP 2024 serves to split hydro power into run-of-river and dam for the TYNDP 2022 dataset, associated to the input file:
- TYNDP2024_HydroShare.xlsx

The folder TYNDP_out contains the output data grouped by climate year.
