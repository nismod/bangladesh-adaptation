"""Merge and filter road damages from the coastal zone
"""

import json
from pathlib import Path

import pandas

with open(Path(__file__).parent.parent / "config.json", "r") as fh:
    config = json.load(fh)
base_path = Path(config["base_path"])

road_damage_paths = [
    "Bangladesh GCA-UNOPS data/Data/Output2/Coastal flood/Feature wise damage/road_barisal damage.csv",
    "Bangladesh GCA-UNOPS data/Data/Output2/Coastal flood/Feature wise damage/road_chittagong damage.csv",
    "Bangladesh GCA-UNOPS data/Data/Output2/Coastal flood/Feature wise damage/road_khulna damage.csv",
]
damage_columns = [
    "Damage_euro_hist_rp002",
    "Damage_euro_hist_rp010",
    "Damage_euro_hist_rp025",
    "Damage_euro_hist_rp050",
    "Damage_euro_hist_rp100",
    "Damage_euro_2030_rcp4p5_rp002",
    "Damage_euro_2030_rcp4p5_rp010",
    "Damage_euro_2030_rcp4p5_rp025",
    "Damage_euro_2030_rcp4p5_rp050",
    "Damage_euro_2030_rcp4p5_rp100",
    "Damage_euro_2050_rcp4p5_rp002",
    "Damage_euro_2050_rcp4p5_rp010",
    "Damage_euro_2050_rcp4p5_rp025",
    "Damage_euro_2050_rcp4p5_rp050",
    "Damage_euro_2050_rcp4p5_rp100",
    "Damage_euro_2080_rcp4p5_rp002",
    "Damage_euro_2080_rcp4p5_rp010",
    "Damage_euro_2080_rcp4p5_rp025",
    "Damage_euro_2080_rcp4p5_rp050",
    "Damage_euro_2080_rcp4p5_rp100",
    "Damage_euro_2030_rcp8p5_rp002",
    "Damage_euro_2030_rcp8p5_rp010",
    "Damage_euro_2030_rcp8p5_rp025",
    "Damage_euro_2030_rcp8p5_rp050",
    "Damage_euro_2030_rcp8p5_rp100",
    "Damage_euro_2050_rcp8p5_rp002",
    "Damage_euro_2050_rcp8p5_rp010",
    "Damage_euro_2050_rcp8p5_rp025",
    "Damage_euro_2050_rcp8p5_rp050",
    "Damage_euro_2050_rcp8p5_rp100",
    "Damage_euro_2080_rcp8p5_rp002",
    "Damage_euro_2080_rcp8p5_rp010",
    "Damage_euro_2080_rcp8p5_rp025",
    "Damage_euro_2080_rcp8p5_rp050",
    "Damage_euro_2080_rcp8p5_rp100",
]
for path in road_damage_paths:
    print(path)
    df = pandas.read_csv(
        base_path / path,
        usecols=["fid"] + damage_columns,
        dtype={col: "float64" for col in damage_columns},
    )
    print("read", len(df))
    df_damage = df[damage_columns]
    mask = df_damage.max(axis=1) > 0
    damaged = df[mask]
    print("filtered", len(damaged))
    damaged.to_parquet(f"scratch/{(base_path / path).name}.pq")
    print("wrote")

    del damaged
    del mask
    del df_damage
    del df

tmp_damage_paths = [
    "scratch/road_barisal damage.csv.pq",
    "scratch/road_chittagong damage.csv.pq",
    "scratch/road_khulna damage.csv.pq",
]
dfs = []
for path in tmp_damage_paths:
    df = pandas.read_parquet(path)
    dfs.append(df)
    print("read", path)
coastal_road_damages = pandas.concat(dfs)
coastal_road_damages.to_parquet(
    base_path
    / "Bangladesh GCA-UNOPS data/Data/Output2/Coastal flood/Feature wise damage/road_coastal_merged damage.csv.pq"
)
print("wrote pq")
coastal_road_damages.to_csv(
    base_path
    / "Bangladesh GCA-UNOPS data/Data/Output2/Coastal flood/Feature wise damage/road_coastal_merged damage.csv",
    index=False,
)
print("wrote csv")
