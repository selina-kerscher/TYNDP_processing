# Python script to process input data 
# Author : Selina Kerscher
# Date   : 260128
# ============================================================================
import time
import os
import pandas as pd
from openpyxl import load_workbook
# ============================================================================
def map_tech(tech):
    t = tech.strip()
    if t.lower().startswith("solar"):
        return "Solar"
    if "Reservoir" in t or "Pondage" in t:
        return "Hydro Dam"
    if "Run-of-River" in t:
        return "Hydro RoR"
    if "biofuel" in t.lower():
        return "Biofuels"
    if t.lower().startswith("gas") and "biofuel" not in t.lower() or "Hydrogen CCGT" in t:
        return "Gas"
    if ("lignite" in t.lower() or "coal" in t.lower()) and "biofuel" not in t.lower():
        return "Coal and lignite"
    if "oil" in t.lower():
        return "Oil"
    if "nuclear" in t.lower():
        return "Nuclear"
    if "Others renewable" in t:
        return "Others renewable"
    if "Others non-renewable" in t:
        return "Others non-renewable"
    return t
# ============================================================================
def safe_excel_writer(path, sheet_name, df, idx):
    if os.path.exists(path):
        with pd.ExcelWriter(
            path,
            engine="openpyxl",
            mode="a",
            if_sheet_exists="replace"
        ) as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=idx)
    else:
        with pd.ExcelWriter(
            path,
            engine="openpyxl",
            mode="w"
        ) as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=idx)
# ============================================================================
def interpolate_2035(df_2030, df_2040):
    df = df_2030.copy()
    for col in df.columns[1:]:
        df[col] = df_2030[col] + 0.5 * (df_2040[col] - df_2030[col])
    return df
# ============================================================================
def build_generator_table(df, year, parameter, nodes, node_map, fuel_map, tech_order):
    subset = df[
        (df["Year"] == year) &
        (df["Parameter"] == parameter)
    ].copy()
    subset["Tech_group"] = subset["Fuel"].map(fuel_map)
    if subset["Tech_group"].isna().any():
        missing = subset.loc[subset["Tech_group"].isna(), "Fuel"].unique()
        raise ValueError(f"Unmapped Fuel values found: {missing}")
    pivot = (
        subset
        .groupby(["Tech_group", "Node"], as_index=False)["Value"]
        .sum()
        .pivot(index="Tech_group", columns="Node", values="Value")
        .reindex(columns=nodes, fill_value=0)
        .reindex(tech_order, fill_value=0)
        .reset_index()
        .fillna(0)
    )
    pivot.rename(columns=node_map, inplace=True)
    return pivot
# ============================================================================
def build_demand_table(df, year, nodes, node_map):
    subset = df[df["Year"] == year]
    pivot = (
        subset.groupby(["Type_node", "Node"])["Value"]
        .sum()
        .unstack(fill_value=0)
        .reindex(columns=nodes, fill_value=0)
        .reset_index()
    )
    pivot.rename(columns={"Type_node": "Demand_node"}, inplace=True)
    pivot.rename(columns=node_map, inplace=True)
    return pivot
# ============================================================================
def split_hydro(df, df_share):
    df_idx = df.set_index("Tech_group")
    hydro = df_idx.loc["Hydro"]
    hydro_split = df_share.mul(hydro, axis=1)
    hydro_pos = df_idx.index.get_loc("Hydro")
    df_before = df_idx.iloc[:hydro_pos]
    df_after = df_idx.iloc[hydro_pos + 1 :]
    df_out = (
        pd.concat([df_before, hydro_split, df_after])
        .reset_index()
        .rename(columns={"index": "Tech_group"})
    )
    return df_out
# ============================================================================
def read_profiles(dir_in, fname, nodes, CY):
    year = CY[2:] 
    data = {n[:2]: None for n in nodes}   # initialize country containers
    xls = pd.ExcelFile(os.path.join(dir_in, fname), engine="pyxlsb")
    for sheet in xls.sheet_names:
        country = sheet[:2]
        if country not in data:
            continue
        df = pd.read_excel(
            xls,
            sheet_name=sheet,
            skiprows=6
        )
        df.columns = df.columns.astype(str)
        values = df[year].to_numpy(copy=True)
        if data[country] is None:
            data[country] = values
        else:
            data[country] += values
    return pd.DataFrame(data)

# ============================================================================
def make_tyndp24(dir_in, dir_out, CY):
    years = ["DE2035", "DE2050"]
    nodes = ["AT00", "CH00", "DE00", "FR00", "IT00"]
    countries = [n[:2] for n in nodes]
    sheet = "Yearly Outputs"
    sheet2 = "Hourly Market Data emarket"
    output_types = ["Annual generation [GWh]", "Installed Capacities [MW]"]
    zones = ['AT00', 'AT00RETE', 'AT00 SRES', 'CH00', 'CH00 SRES', 'DE00', 'DE00RETE', 'DE00 SRES', 'FR00', 'FR00RETE', 'FR00 SRES','ITCA',
            'ITCARETE', 'ITCN', 'ITCNRETE', 'ITCS', 'ITCSRETE', 'ITN1', 'ITN1RETE',
            'ITS1', 'ITS1RETE', 'ITSA', 'ITSARETE', 'ITSI', 'ITSIRETE', 'ITCA SRES', 'ITCN SRES', 'ITCS SRES', 'ITN1 SRES', 'ITS1 SRES', 'ITSA SRES', 'ITSI SRES']
    zones2 = ['DEOH001 DRES', 'DEOH001OHEL', 'DEOH002 DRES', 'DEOH002OHEL', 'FROH001 DRES', 
                'FROH001OHEL', 'FROH002 DRES', 'FROH002OHEL', 'FROH003 DRES', 'FROH003OHEL']
    zone2country = {
        "AT": ["AT00", "AT00RETE", "AT00 SRES"],
        "CH": ["CH00", "CH00 SRES"],
        "DE": ["DE00", "DE00RETE", "DE00 SRES"],
        "FR": ["FR00", "FR00RETE", "FR00 SRES"],
        "IT": [
        "ITCA", "ITCARETE", "ITCN", "ITCNRETE", "ITCS", "ITCSRETE",
        "ITN1", "ITN1RETE", "ITS1", "ITS1RETE", "ITSA", "ITSARETE",
        "ITSI", "ITSIRETE", "ITCA SRES", "ITCN SRES", "ITCS SRES", 
        "ITN1 SRES", "ITS1 SRES", "ITSA SRES", "ITSI SRES"
        ]
    }
    zone2country2 = {
        "DE": ['DEOH001 DRES', 'DEOH001OHEL', 'DEOH002 DRES', 'DEOH002OHEL'],
        "FR": ['FROH001 DRES', 'FROH001OHEL', 'FROH002 DRES', 
                'FROH002OHEL', 'FROH003 DRES', 'FROH003OHEL']
    }
    tech_order = [
        "Hydro Dam",
        "Hydro RoR",
        "Solar",
        "Wind Onshore",
        "Wind Offshore",
        "Others renewable",
        "Biofuels",
        "Nuclear",
        "Gas",
        "Coal and lignite",
        "Oil",
        "Others non-renewable",
        ]
        #------------------------------------------------------------------------
        ### GENERATION
    for year in years:
        filepath = os.path.abspath(os.path.join(dir_in, "MMStandardOutputFile_" + str(year) + f"_Plexos_{CY}_v11_SoS.xlsb"))
        filepath2 = os.path.abspath(os.path.join(dir_in, "MMStandardOutputFile_" + str(year) + f"_Plexos_{CY}_offshore_v11_SoS.xlsb"))
        try:
            df = pd.read_excel(filepath, sheet_name=sheet, engine="pyxlsb")
        except:
            print(f"Can not find file :{filepath}\n")
            return
        header_row = 4
        df.columns = df.iloc[header_row]
        df = df.iloc[header_row+1:].reset_index(drop=True)
        df.columns = ["Category", "Technology"] + list(df.columns[2:])
        df["Category"] = df["Category"].ffill()
        df = df[df["Technology"].notna()].copy()
        df["Tech_group"] = df["Technology"].astype(str).apply(map_tech)    
        df = df[df["Category"].isin(output_types)]
        #------------------------------------------------------------------------
        for country, zones in zone2country.items():
            real = [z for z in zones if z in df.columns]
            df[country] = df[real].sum(axis=1)
        country_cols = list(zone2country.keys())
        df_gen = df[df["Category"] == "Annual generation [GWh]"]
        df_cap = df[df["Category"] == "Installed Capacities [MW]"]
        # df_gen[country_cols] = df_gen[country_cols] / 1000.0
        # df_cap[country_cols] = df_cap[country_cols] / 1000.0
        gen_pivot = df_gen.groupby("Tech_group")[country_cols].sum()
        cap_pivot = df_cap.groupby("Tech_group")[country_cols].sum()
        idx = [t for t in tech_order if t in gen_pivot.index] + \
        [t for t in gen_pivot.index if t not in tech_order]
        gen_pivot = gen_pivot.reindex(idx)
        cap_pivot = cap_pivot.reindex(idx)
        #------------------------------------------------------------------------
        try:
            df_offsh = pd.read_excel(filepath2, sheet_name=sheet, engine="pyxlsb")
        except:
            print(f"Can not find file :{filepath2}\n")
            return
        df_offsh.columns = df_offsh.iloc[header_row]
        df_offsh = df_offsh.iloc[header_row+1:].reset_index(drop=True)
        df_offsh.columns = ["Category"] + list(df_offsh.columns[1:])
        #------------------------------------------------------------------------
        for country, zones2 in zone2country2.items():
            real = [z for z in zones2 if z in df_offsh.columns]
            df_offsh[country] = df_offsh[real].sum(axis=1)
        country_cols = list(zone2country2.keys())
        df_gen_offsh = df_offsh[df_offsh["Category"] == "Annual generation [GWh]"]
        # df_gen_offsh[country_cols] = df_gen_offsh[country_cols] / 1000.0
        gen_offsh_pivot = df_gen_offsh.groupby("Category")[country_cols].sum()
        gen_pivot.loc['Wind Offshore', ['DE','FR']] += gen_offsh_pivot.loc['Annual generation [GWh]', ['DE','FR']]
        #------------------------------------------------------------------------
        out_path = os.path.join(dir_out, "tyndp_generators.xlsx")
        sheet_gwh = f"GWh_{year}_{CY}"
        sheet_mw = f"MW_{year}_{CY}"
        safe_excel_writer(out_path, sheet_gwh, gen_pivot, idx=True)
        safe_excel_writer(out_path, sheet_mw, cap_pivot, idx=True)
        #------------------------------------------------------------------------
        ### DEMAND
        try:
            df = pd.read_excel(filepath, sheet_name=sheet2, engine="pyxlsb", header=None)
        except:
            print(f"Can not find file :{filepath}\n")
            return
        categories = df.iloc[10, 2:]
        nodes_raw = df.iloc[11, 2:]
        data = (
                df
                .iloc[13:, 2:]
                .apply(pd.to_numeric, errors="coerce")
                .fillna(0.0)
                .reset_index(drop=True)
            )
        profiles = pd.DataFrame(0.0, index=data.index, columns=countries)
        totals = pd.DataFrame(0.0, index=[
            "Electrolysis",
            "Prosumer Node",
            "Transmission Node",
            "Transport Node"
        ], columns=countries)

        for col in data.columns:
            node = nodes_raw[col]
            cat = categories[col]
            if not isinstance(node, str) or not isinstance(cat, str):
                continue
            country = node[:2]
            if country not in countries:
                continue
            series = data[col]            
            is_demand = "Demand [MW]" in cat
            is_electrolysis = "Electrolyser" in cat
            if is_demand:
                profiles[country] += series
            if is_electrolysis:
                totals.loc["Electrolysis", country] += series.sum()
                profiles[country] += series
            elif is_demand and node.endswith("RETE"):
                totals.loc["Prosumer Node", country] += series.sum()
            elif is_demand and (
                "EV Passenger Prosumer" in node
                or "EV Passenger Street" in node
            ):
                totals.loc["Transport Node", country] += series.sum()
            elif is_demand:
                totals.loc["Transmission Node", country] += series.sum()
        totals.loc["Total demand"] = totals.sum()
        totals /= 1000
        totals = totals.reset_index().rename(columns={"index": "Demand_node"})
        #------------------------------------------------------------------------
        out_path = os.path.abspath(os.path.join(dir_out, "tyndp_demand.xlsx"))
        safe_excel_writer(out_path, f"GWh_{year}_{CY}", totals, idx=False)
        out_path = os.path.abspath(os.path.join(dir_out, "tyndp_demand_profiles.xlsx"))
        safe_excel_writer(out_path, f"MWh_{year}_{CY}", profiles, idx=False)
    return()

# ============================================================================
def make_tyndp22(dir_in, dir_out, CY):
    inpath = os.path.abspath(os.path.join(dir_in, "220310_Updated_Electricity_Modelling_Results_TYNDP2022.xlsx"))
    out_gen = os.path.abspath(os.path.join(dir_out, "tyndp_generators.xlsx"))
    out_dem = os.path.abspath(os.path.join(dir_out, "tyndp_demand.xlsx"))
    out_dem_prof = os.path.abspath(os.path.join(dir_out, "tyndp_demand_profiles.xlsx"))
    nodes = ["AT00", "CH00", "DE00", "FR00", "IT00"]
    node_map = {"AT00": "AT", "CH00": "CH", "DE00": "DE", "FR00": "FR", "IT00": "IT"}
    scenario = "Global Ambition"
    climate_year = CY[:2] + " " + CY[2:]    # add space acc. to naming in data file
    fuel_map = {
        "Hydro": "Hydro",
        "Solar": "Solar",
        "Wind Onshore": "Wind Onshore",
        "Wind Offshore": "Wind Offshore",
        "Other RES": "Others renewable",
        "Biofuels": "Biofuels",
        "Nuclear": "Nuclear",
        "Gas": "Gas",
        "Coal & Lignite": "Coal and lignite",
        "Oil": "Oil",
        "Other Non RES": "Others non-renewable",
    } 
    fuel_map2 = {
        "Battery": "Battery Storage",
        "DSR": "Demand Side Response",
        "Gas": "Flexible Gas",
        "Hydro Pump Storage": "Pump Storage",
    }  
    tech_order = [
            "Hydro",
            "Solar",
            "Wind Onshore",
            "Wind Offshore",
            "Others renewable",
            "Biofuels",
            "Nuclear",
            "Gas",
            "Coal and lignite",
            "Oil",
            "Others non-renewable",
            ]
    tech_order2 = [
        "Battery Storage",
        "Demand Side Response",
        "Flexbile Gas",
        "Pump Storage",
    ]
    # shares come from TYNDP 2024 data
    share_MW_DE2035 = pd.DataFrame(
        {
            "AT": [0.4243, 0.5757],
            "CH": [0.6720, 0.3280],
            "DE": [0.1723, 0.8277],
            "FR": [0.4200, 0.5800],
            "IT": [0.5550, 0.4450],
        },
        index=["Hydro Dam", "Hydro RoR"],
    )
    share_MW_DE2050 = pd.DataFrame(
        {
            "AT": [0.4239, 0.5761],
            "CH": [0.6745, 0.3255],
            "DE": [0.1723, 0.8277],
            "FR": [0.4200, 0.5800],
            "IT": [0.5550, 0.4450],
        },
        index=["Hydro Dam", "Hydro RoR"],
    )
    share_GWh_DE2035 = pd.DataFrame(
        {
            "AT": [0.2223, 0.7777],
            "CH": [0.4162, 0.5838],
            "DE": [0.0275, 0.9725],
            "FR": [0.3068, 0.6932],
            "IT": [0.4132, 0.5868],
        },
        index=["Hydro Dam", "Hydro RoR"],
    )
    share_GWh_DE2050 = pd.DataFrame(
        {
            "AT": [0.2290, 0.7710],
            "CH": [0.4209, 0.5791],
            "DE": [0.0273, 0.9727],
            "FR": [0.3067, 0.6933],
            "IT": [0.4154, 0.5846],
        },
        index=["Hydro Dam", "Hydro RoR"],
    )
    #------------------------------------------------------------------------
    try:
        df = pd.read_excel(inpath, sheet_name="Capacity & Dispatch")
    except:
        print(f"Can not find file :{inpath}\n")
        return
    df["Year"] = df["Year"].astype(int)
    df = df[
        (df["Node"].isin(nodes))
        & (df["Scenario"] == scenario)
        & (df["Climate Year"] == climate_year)
        & (df["Year"].isin([2030, 2040, 2050]))
    ]
    #------------------------------------------------------------------------
    try:
        df2 = pd.read_excel(inpath, sheet_name="Flexibility")
    except:
        print(f"Can not find file :{inpath}\n")
        return
    df2["Year"] = df2["Year"].astype(int)
    df2 = df2[
        (df2["Node"].isin(nodes))
        & (df2["Scenario"] == scenario)
        & (df2["Climate Year"] == climate_year)
        & (df2["Year"].isin([2030, 2040, 2050]))
    ]
    #------------------------------------------------------------------------
    gwh_2050 = build_generator_table(df, 2050, "Dispatch (GWh)", nodes, node_map, fuel_map, tech_order)
    mw_2050 = build_generator_table(df, 2050, "Capacity (MW)", nodes, node_map, fuel_map, tech_order)
    gwh_2050_flex = build_generator_table(df2, 2050, "Dispatch (GWh)", nodes, node_map, fuel_map2, tech_order2)
    mw_2050_flex = build_generator_table(df2, 2050, "Capacity (MW)", nodes, node_map, fuel_map2, tech_order2)
    gwh_2050 = pd.concat([gwh_2050, gwh_2050_flex], ignore_index=True)
    mw_2050 = pd.concat([mw_2050, mw_2050_flex], ignore_index=True)
    gwh_2050 = split_hydro(gwh_2050, share_GWh_DE2050)
    mw_2050 = split_hydro(mw_2050, share_MW_DE2050)
    safe_excel_writer(out_gen, f"GWh_GA2050_{CY}", gwh_2050, idx=False)
    safe_excel_writer(out_gen, f"MW_GA2050_{CY}", mw_2050, idx=False)
    #------------------------------------------------------------------------
    gwh_2030 = build_generator_table(df, 2030, "Dispatch (GWh)", nodes, node_map, fuel_map, tech_order)
    gwh_2040 = build_generator_table(df, 2040, "Dispatch (GWh)", nodes, node_map, fuel_map, tech_order)
    gwh_2030_flex = build_generator_table(df2, 2030, "Dispatch (GWh)", nodes, node_map, fuel_map2, tech_order2)
    gwh_2040_flex = build_generator_table(df2, 2040, "Dispatch (GWh)", nodes, node_map, fuel_map2, tech_order2)
    gwh_2030 = pd.concat([gwh_2030, gwh_2030_flex], ignore_index=True)
    gwh_2040 = pd.concat([gwh_2040, gwh_2040_flex], ignore_index=True)
    mw_2030 = build_generator_table(df, 2030, "Capacity (MW)", nodes, node_map, fuel_map, tech_order)
    mw_2040 = build_generator_table(df, 2040, "Capacity (MW)", nodes, node_map, fuel_map, tech_order)
    mw_2030_flex = build_generator_table(df2, 2030, "Capacity (MW)", nodes, node_map, fuel_map2, tech_order2)
    mw_2040_flex = build_generator_table(df2, 2040, "Capacity (MW)", nodes, node_map, fuel_map2, tech_order2)
    mw_2030 = pd.concat([mw_2030, mw_2030_flex], ignore_index=True)
    mw_2040 = pd.concat([mw_2040, mw_2040_flex], ignore_index=True)
    #------------------------------------------------------------------------
    gwh_2035 = interpolate_2035(gwh_2030, gwh_2040)
    mw_2035 = interpolate_2035(mw_2030, mw_2040)
    gwh_2035 = split_hydro(gwh_2035, share_GWh_DE2035)
    mw_2035 = split_hydro(mw_2035, share_MW_DE2035)
    safe_excel_writer(out_gen, f"GWh_GA2035_{CY}", gwh_2035, idx=False)
    safe_excel_writer(out_gen, f"MW_GA2035_{CY}", mw_2035, idx=False)
    #------------------------------------------------------------------------
    try:
        df = pd.read_excel(inpath, sheet_name="Demand")
    except:
        print(f"Can not find file :{inpath}\n")
        return
    df["Year"] = df["Year"].astype(int)
    df = df[
        (df["Node"].isin(nodes))
        & (df["Scenario"] == scenario)
        & (df["Climate Year"] == climate_year)
        & (df["Parameter"] == "Native Demand (GWh)")
        & (df["Year"].isin([2030, 2040, 2050]))
    ]
    #------------------------------------------------------------------------
    dem_2050 = build_demand_table(df, 2050, nodes, node_map)
    sum_nodes = [
        "Electrolysis Config 1",
        "Prosumer Node",
        "Transmission Node",
        "Transport Node",
    ]
    dem_2050.loc[len(dem_2050)] = (
        ["Total demand"]
        + dem_2050
        .loc[dem_2050["Demand_node"].isin(sum_nodes), dem_2050.columns[1:]]
        .sum()
        .tolist()
    )
    safe_excel_writer(out_dem, f"GWh_GA2050_{CY}", dem_2050, idx=False)
    dem_2030 = build_demand_table(df, 2030, nodes, node_map)
    dem_2040 = build_demand_table(df, 2040, nodes, node_map)
    dem_2035 = interpolate_2035(dem_2030, dem_2040)
    dem_2035.loc[len(dem_2035)] = (
        ["Total demand"]
        + dem_2035
        .loc[dem_2035["Demand_node"].isin(sum_nodes), dem_2035.columns[1:]]
        .sum()
        .tolist()
    )
    safe_excel_writer(out_dem, f"GWh_GA2035_{CY}", dem_2035, idx=False)
    #------------------------------------------------------------------------
    df_2050 = read_profiles(dir_in, "Demand_TimeSeries_2050_GA_release.xlsb", nodes, CY)
    df_2030 = read_profiles(dir_in, "Demand_TimeSeries_2030_GA_release.xlsb", nodes, CY)
    df_2040 = read_profiles(dir_in, "Demand_TimeSeries_2040_GA_release.xlsb", nodes, CY)
    df_2035 = df_2030 + 0.5 * (df_2040 - df_2030)
    safe_excel_writer(out_dem_prof, f"MWh_GA2035_{CY}", df_2035, idx=False)
    safe_excel_writer(out_dem_prof, f"MWh_GA2050_{CY}", df_2050, idx=False)
    return()

# ============================================================================
def mod_tyndp24(dir_out, CY):
    inpath = os.path.abspath(os.path.join(dir_out, "tyndp_demand_profiles.xlsx"))
    sheet = [f"MWh_DE2035_{CY}", f"MWh_DE2050_{CY}"]
    hoy = 8760
    for s in sheet:
        try:
            df = pd.read_excel(inpath, sheet_name=s)
        except:
            print(f"Can not find file :{inpath}\n")
            return
        missing = hoy - len(df)
        if missing > 0:
            sum = df.sum()
            to_add = df.tail(missing)
            df = pd.concat([df, to_add], ignore_index=True)
            df *= sum / df.sum()
            safe_excel_writer(inpath, s, df, idx=False)
        elif missing == 0:
            pass
    return()

# ============================================================================
def main(run_mode = 101):
    start_time = time.perf_counter()  
    CY = 'CY1995'   # CY1995 or CY2008
    # Directory
    top_dir = os.path.abspath(os.path.dirname(__file__))
    # The input/output directory relative to the top directory
    dir_in	= os.path.abspath(os.path.join(top_dir, "TYNDP_in/"))
    dir_out	= os.path.abspath(os.path.join(top_dir, f"TYNDP_out/{CY}/"))
    if not os.path.exists(dir_out):
        os.makedirs(dir_out)
    else:
        pass

    if (run_mode == 101): # TYNDP 2024
        make_tyndp24(dir_in, dir_out, CY)
    elif (run_mode == 102): # TYNDP 2022
        make_tyndp22(dir_in, dir_out, CY)
    elif (run_mode == 103): # Correct TYNDP 2024 profiles
        mod_tyndp24(dir_out, CY)
    else:
        pass

    run_time = round(time.perf_counter() - start_time, 3)
    msg_str = f"Run time : {run_time:.3f} second"
    print(msg_str)
# ============================================================================
# Normal run modes:   101 : TYNDP24 , 102 : TYNDP22 , 103 : Correct Profile TYNDP24
if __name__ == "__main__" : main(103)
# ============================================================================
