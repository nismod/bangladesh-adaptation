#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
import geopandas as gpd
import dask.dataframe as dd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.neighbors import BallTree


def infra_id_add(rural_PCA, urban_PCA, id_infra):
    wealth_rural_columns = ['rural_wealthQ1','rural_wealthQ2','rural_wealthQ3','rural_wealthQ4','rural_wealthQ5']
    rural_infra = rural_PCA.groupby(['wealth_group',id_infra])['hid'].count().reset_index().compute().set_index([id_infra,'wealth_group']).unstack(level = 'wealth_group')
    rural_infra.columns = wealth_rural_columns
    rural_infra['rural_households'] = rural_infra.sum(axis = 1)
    rural_infra = rural_infra.reset_index()

    wealth_urban_columns = ['urban_wealthQ5','urban_wealthQ4','urban_wealthQ3','urban_wealthQ2','urban_wealthQ1']
    urban_infra = urban_PCA.groupby(['wealth_group',id_infra])['hid'].count().reset_index().compute().set_index([id_infra,'wealth_group']).unstack(level = 'wealth_group')
    urban_infra.columns = wealth_urban_columns
    urban_infra['urban_households'] = urban_infra.sum(axis = 1)
    urban_infra = urban_infra.reset_index()

    return rural_infra, urban_infra


#### merge various spatial data
####LOAD DATA
base_path = os.path.join('D:/Bangladesh')
path_data_files = os.path.join(base_path,'incoming')

## education
education_path = os.path.join(path_data_files,'critical_infra/bgd_poi_educationfacilities_lged/bgd_poi_educationfacilities_lged.shp')
education = gpd.read_file(education_path, crs = 'EPSG:4326')
#education = gpd.read_file(path_data_files+'bgd_poi_educationfacilities_lged/bgd_poi_educationfacilities_lged.shp', crs = 'EPSG:4326')
education = education.to_crs('EPSG:4326')
education['Lat']= education.geometry.y
education['Long']= education.geometry.x
education = education.reset_index().rename(columns = {'index':'id_edu'})

## hospital
hospital_path = os.path.join(path_data_files, 'critical_infra/cegis_buildings/Hospitals/Hospitals.shp')
hospital = gpd.read_file(hospital_path, crs = 'EPSG:4326')
#health_facilities = gpd.read_file(path_data_files+'bgd_poi_healthfacilities_lged/bgd_poi_healthfacilities_lged.shp',crs = 'EPSG:3106')
#hospital = health_facilities[health_facilities['FType']=='Hospital']
hospital = hospital.to_crs('EPSG:4326')
hospital['Lat']= hospital.geometry.y
hospital['Long']= hospital.geometry.x
hospital = hospital.reset_index(drop = True).reset_index().rename(columns  = {'index':'id_hospital'})

### health facilities
health_path = os.path.join(path_data_files, 'critical_infra/cegis_buildings/Health_facilities/Health_Facilities.shp')
health = gpd.read_file(health_path, crs = 'EPSG:4326')
#health = health_facilities[health_facilities['FType']!='Hospital']
health = health.to_crs('EPSG:4326')
health['Lat']= health.geometry.y
health['Long']= health.geometry.x
health = health.reset_index(drop = True).reset_index().rename(columns  = {'index':'id_health'})

## shelters
shelters_path = os.path.join(path_data_files,'critical_infra/Shelters/cyclone_shelters.shp')
shelters = gpd.read_file(shelters_path)
#shelters = gpd.read_file(path_data_files+'Shelters/cyclone_shelters.shp')
shelters =shelters.to_crs('EPSG:4326')
shelters['Lat']= shelters.geometry.y
shelters['Long']= shelters.geometry.x
shelters = shelters.reset_index(drop = True).reset_index().rename(columns  = {'index':'id_shelter'})

## urban road
#road_file = gpd.read_file(path_data_files+'bgd_trs_roads_lged/bgd_trs_roads_lged.shp')
#road = road_file[road_file['FType'].isin(['National Highway','Regional Highway','Zila Road'])].reset_index(drop = True)
#road =road.to_crs('EPSG:4326')
#road['Lat']= road.geometry.centroid.y
#road['Long']= road.geometry.centroid.x
#road = road.reset_index(drop = True).reset_index().rename(columns  = {'index':'id_road'})

## rural road
#road_rural = road_file[~road_file['FType'].isin(['National Highway','Regional Highway','Zila Road'])].reset_index(drop = True)
#road_rural =road_rural.to_crs('EPSG:4326')
#road_rural['Lat']= road_rural.geometry.centroid.y
#road_rural['Long']= road_rural.geometry.centroid.x
#road_rural = road_rural.reset_index(drop = True).reset_index().rename(columns  = {'index':'id_road_rural'})

### growth centre
#growth_centre = gpd.read_file(path_data_files+'Growth_centre/G_Centre_BTM.shp')
growth_centre_path = os.path.join(path_data_files, 'critical_infra/Growth_centre_locations/G_Centre_BTM.shp')
growth_centre = gpd.read_file(growth_centre_path)
growth_centre =growth_centre.to_crs('EPSG:4326')
growth_centre['Lat']= growth_centre.geometry.y
growth_centre['Long']= growth_centre.geometry.x
growth_centre= growth_centre.reset_index(drop = True).reset_index().rename(columns  = {'index':'id_growth'})

### electricity grid
#elec_grid = gpd.read_file(path_data_files+'Electricity/MajorPowerGridLine.shp')
#elec_grid =elec_grid.to_crs('EPSG:4326')
#elec_grid['Lat']= elec_grid.geometry.centroid.y
#elec_grid['Long']= elec_grid.geometry.centroid.x
#elec_grid= elec_grid.reset_index(drop = True).reset_index().rename(columns  = {'index':'id_grid'})

### electricity substation
elec_sub_path = os.path.join(path_data_files, 'energy/cegis_energy/Electricity/Existing_Sub_station.shp')
elec_sub = gpd.read_file(elec_sub_path)
#elec_sub = gpd.read_file(path_data_files+'Electricity/SubStations.shp')
elec_sub = elec_sub.to_crs('EPSG:4326')
elec_sub['Lat']= elec_sub.geometry.y
elec_sub['Long']= elec_sub.geometry.x
elec_sub= elec_sub.reset_index(drop = True).reset_index().rename(columns  = {'index':'id_substation'})

## railway stations
rail_station_path = os.path.join(path_data_files, 'transport/cegis_transport/Railway/Railway_Stations.shp')
rail_station = gpd.read_file(rail_station_path)
rail_station = rail_station.to_crs('EPSG:4326')
rail_station['Lat']= rail_station.geometry.y
rail_station['Long']= rail_station.geometry.x
rail_station = rail_station.reset_index(drop = True).reset_index().rename(columns  = {'index':'id_railstation'})

## road nodes
road_node_path = os.path.join(path_data_files, 'transport/osm_road_corrected/osm_road_nodes_corrected.gpkg')
road_node = gpd.read_file(road_node_path)
road_node = road_node.to_crs('EPSG:4326')
road_node['Lat']= road_node.geometry.y
road_node['Long']= road_node.geometry.x
road_node = road_node.reset_index(drop = True).reset_index().rename(columns  = {'index':'id_roadnode'})

print('All infrastructure layers loaded in')


#### add the PCA derived data to this and create new infrastructure layers with households added
path_rural_PCA_data = os.path.join(base_path,'incoming/Population/WB_household_data_jasper/Wealth_index/Wealth_index/rural_PCA_households.csv')
path_urban_PCA_data = os.path.join(base_path,'incoming/Population/WB_household_data_jasper/Wealth_index/Wealth_index/urban_PCA_households.csv')

rural_PCA = dd.read_csv(path_rural_PCA_data)
urban_PCA = dd.read_csv(path_urban_PCA_data)

list_district = ['Noakhali','Khulna','Barisal','Satkhira','Bhola','Bagerhat','Patuakhali','Barguna','Pirojpur','Jhalokati','Narail','Chittagong','Jessore', 'Chandpur','Coxs_Bazar','Lakshmipur','Feni','Shariatpur','Gopalganj']


urban_data = pd.DataFrame()
for district in list_district:
    print('urban',district)
    path_infra_access_urban = os.path.join(base_path, 'data/household_asset_analysis/Infra_access/urban_infra_access_'+str(district)+'.csv')
    district_data = pd.read_csv(path_infra_access_urban, usecols=['hid','dist_health','dist_edu','dist_shelter','dist_hospital','dist_growth','dist_substation', 'dist_railstation', 'dist_roadnode', 'id_edu','id_health','id_shelter','id_hospital','id_growth','id_substation', 'id_railstation', 'id_roadnode'])
    urban_data = pd.concat([urban_data, district_data], ignore_index = True, sort = False)
    del district_data

print(urban_data.columns)

rural_data = pd.DataFrame()
for district in list_district:
    print('rural',district)
    path_infra_access_rural = os.path.join(base_path, 'data/household_asset_analysis/Infra_access/rural_infra_access_'+str(district)+'.csv')
    district_data = pd.read_csv(path_infra_access_rural, usecols=['hid','dist_health','dist_edu','dist_shelter','dist_hospital','dist_growth','dist_substation', 'dist_railstation', 'dist_roadnode', 'id_edu','id_health','id_shelter','id_hospital','id_growth','id_substation', 'id_railstation', 'id_roadnode'])
    rural_data = pd.concat([rural_data, district_data], ignore_index = True, sort = False)
    del district_data
print(rural_data.columns)
print('all link data loaded')

# combining the PCA household data with the nearest neighbor household data
rural_PCA = rural_PCA.merge(rural_data, on = 'hid')
urban_PCA = urban_PCA.merge(urban_data, on = 'hid')

print('data merged')


#### process the infrastructure data based on the PCA derived wealth estimates
## improved roads
#rural_road_dev, urban_road_dev = infra_id_add(rural_PCA, urban_PCA, 'id_road')
#road_dev_hh = rural_road_dev.merge(urban_road_dev, on = ['id_road'], how = 'outer').replace(np.nan,0) #combines the households-per-asset data for urban and rural households in one df
#road_dev_hh['households'] = road_dev_hh['rural_households'] + road_dev_hh['urban_households']         #creates new column that has total number of households
#road.merge(road_dev_hh, on = ['id_road']).to_file('Output/Processed_infrastructure_layers/improved_road_households.gpkg', driver = 'GPKG')
#print('road improved finished')

## unimproved roads
#rural_road_un, urban_road_un = infra_id_add(rural_PCA, urban_PCA, 'id_road_rural')
#road_un_hh = rural_road_un.merge(urban_road_un, on = ['id_road_rural'], how = 'outer').replace(np.nan,0)
#road_un_hh['households'] = road_un_hh['rural_households'] + road_un_hh['urban_households']
#road_rural.merge(road_un_hh, on = ['id_road_rural']).to_file('Output/Processed_infrastructure_layers/unimproved_road_households.gpkg', driver = 'GPKG')
#print('road unimproved finished')

## improved educational facilities
rural_edu, urban_edu = infra_id_add(rural_PCA, urban_PCA, 'id_edu')
edu_hh = rural_edu.merge(urban_edu, on = ['id_edu'], how = 'outer').replace(np.nan,0)
edu_hh['households'] = edu_hh['rural_households'] + edu_hh['urban_households']
path_edu_results = os.path.join(base_path, 'data/household_asset_analysis/assets_with_hh/education_households.gpkg')
education.merge(edu_hh, on = ['id_edu']).to_file(path_edu_results, driver = 'GPKG', layer='nodes')
print('education finished')

## cyclone shelter
rural_shelter, urban_shelter = infra_id_add(rural_PCA, urban_PCA, 'id_shelter')
shelter_hh = rural_shelter.merge(urban_shelter, on = ['id_shelter'], how = 'outer').replace(np.nan,0)
shelter_hh['households'] = shelter_hh['rural_households'] + shelter_hh['urban_households']
path_shelters_results = os.path.join(base_path, 'data/household_asset_analysis/assets_with_hh/shelter_households.gpkg')
shelters.merge(shelter_hh, on = ['id_shelter']).to_file(path_shelters_results, driver = 'GPKG', layer='nodes')
print('shelters finished')

## health centres
rural_health, urban_health = infra_id_add(rural_PCA, urban_PCA, 'id_health')
health_hh = rural_health.merge(urban_health, on = ['id_health'], how = 'outer').replace(np.nan,0)
health_hh['households'] = health_hh['rural_households'] + health_hh['urban_households']
path_health_results = os.path.join(base_path, 'data/household_asset_analysis/assets_with_hh/health_households.gpkg')
health.merge(health_hh, on = ['id_health']).to_file(path_health_results, driver = 'GPKG', layer='nodes')
print('health finished')

## hospital
rural_hospital, urban_hospital = infra_id_add(rural_PCA, urban_PCA, 'id_hospital')
hospital_hh = rural_hospital.merge(urban_hospital, on = ['id_hospital'], how = 'outer').replace(np.nan,0)
hospital_hh['households'] = hospital_hh['rural_households'] + hospital_hh['urban_households']
path_hospital_results = os.path.join(base_path, 'data/household_asset_analysis/assets_with_hh/hospital_households.gpkg')
hospital.merge(hospital_hh, on = ['id_hospital']).to_file(path_hospital_results, driver = 'GPKG', layer='nodes')
print('hospital finished')

## growth centre
rural_growth, urban_growth = infra_id_add(rural_PCA, urban_PCA, 'id_growth')
growth_hh = rural_growth.merge(urban_growth, on = ['id_growth'], how = 'outer').replace(np.nan,0)
growth_hh['households'] = growth_hh['rural_households'] + growth_hh['urban_households']
path_growth_results = os.path.join(base_path, 'data/household_asset_analysis/assets_with_hh/growth_centre_households.gpkg')
growth_centre.merge(growth_hh, on = ['id_growth']).to_file(path_growth_results, driver = 'GPKG', layer='nodes')
print('growth centre finished')

### electricity grid and substation
#rural_grid, urban_grid = infra_id_add(rural_PCA[rural_PCA['electric_class']==1], urban_PCA[urban_PCA['electric_class']==1], 'id_grid')
#grid_hh = rural_grid.merge(urban_grid, on = ['id_grid'], how = 'outer').replace(np.nan,0)
#grid_hh['households'] = grid_hh['rural_households'] + grid_hh['urban_households']
#elec_grid.merge(grid_hh, on = ['id_grid']).to_file('Output/Processed_infrastructure_layers/electricity_grid_households.gpkg', driver = 'GPKG')

rural_substation, urban_substation = infra_id_add(rural_PCA[rural_PCA['electric_class']==1], urban_PCA[urban_PCA['electric_class']==1], 'id_substation')
substation_hh = rural_substation.merge(urban_substation, on = ['id_substation'], how = 'outer').replace(np.nan,0)
substation_hh['households'] = substation_hh['rural_households'] + substation_hh['urban_households']
path_substation_results = os.path.join(base_path, 'data/household_asset_analysis/assets_with_hh/electricity_substation_households.gpkg')
elec_sub.merge(substation_hh, on = ['id_substation']).to_file(path_substation_results, driver = 'GPKG', layer='nodes')
print('electricity finished')


rural_railstation, urban_railstation = infra_id_add(rural_PCA, urban_PCA, 'id_railstation')
railstation_hh = rural_railstation.merge(urban_railstation, on = ['id_railstation'], how = 'outer').replace(np.nan,0)
railstation_hh['households'] = railstation_hh['rural_households'] + railstation_hh['urban_households']
path_railstation_results = os.path.join(base_path, 'data/household_asset_analysis/assets_with_hh/railstation_households.gpkg')
rail_station.merge(railstation_hh, on = ['id_railstation']).to_file(path_railstation_results, driver = 'GPKG', layer='nodes')
print('railway stations finished')


rural_roadnode, urban_roadnode = infra_id_add(rural_PCA, urban_PCA, 'id_roadnode')
roadnode_hh = rural_roadnode.merge(urban_roadnode, on = ['id_roadnode'], how = 'outer').replace(np.nan,0)
roadnode_hh['households'] = roadnode_hh['rural_households'] + roadnode_hh['urban_households']
path_roadnode_results = os.path.join(base_path, 'data/household_asset_analysis/assets_with_hh/roadnode_households.gpkg')
road_node.merge(roadnode_hh, on = ['id_roadnode']).to_file(path_roadnode_results, driver = 'GPKG', layer='nodes')
print('roadnodes finished')