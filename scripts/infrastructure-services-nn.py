#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
import geopandas as gpd
import dask.dataframe as dd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.neighbors import BallTree
from math import radians, cos, sin, asin, sqrt

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r


def nearest_neighbour(df_input, df_merge, point_column_input, point_columns_merge):
    """
    df_input -> is the households
    df_merge -> is the asset layer
    point_column_input -> is the household point long & lat
    point column_merge -> is the asset point long & lat"""

    tree = BallTree(df_merge[point_columns_merge].values, leaf_size=2) #creates a tree with the asset points(df_merge)
    # Query the BallTree on each feature
    df_input['distance'], df_input['id_nearest'] = tree.query(
        df_input[point_column_input].values, # The input array for the tree query for the households; 
        k=1, # The number of nearest neighbors
    )
    df_merge = df_merge.reset_index().rename(columns  = {'index':'id_nearest'})
    #offshore_loc.drop(,inplace = True)
    df_input = df_input.merge(df_merge, on = 'id_nearest')
    return df_input

def run_accessibility(data):
    """
    data (input) is either urban data or rural data (households)
    first intersects with the polders
    then goes through the asset categories and adds the id of the nearest asset using the nearest neighbor function
    and then calculates the distance between the household and the asset using haversine
    """
    ## make geodataframe
    gdf = gpd.GeoDataFrame(data, geometry=gpd.points_from_xy(data.Long, data.Lat), crs = 'EPSG:3395')
    gdf = gdf.to_crs('EPSG:4326')
    gdf['latitude'] = gdf.geometry.y
    gdf['longitude'] = gdf.geometry.x

    ### polder information
    gdf_within_polder = gpd.overlay(gdf, embankment[['Polder no.','geometry']],how = 'intersection')
    gdf_within_polder['polder'] = 1
    gdf = gdf.merge(gdf_within_polder[['hid','polder','Polder no.']], on = 'hid', how = 'outer').replace(np.nan,0)

    ### hospital
    gdf = nearest_neighbour(gdf,hospital[['id_hospital','Lat','Long']].rename(columns = {'Lat':'Lat_hospital','Long':'Long_hospital'}),['longitude','latitude'],['Long_hospital','Lat_hospital']).drop(columns = ['id_nearest'])
    gdf['dist_hospital'] = gdf.apply(lambda row: haversine(row['longitude'], row['latitude'],row['Long_hospital'],row['Lat_hospital']), axis=1)

    ### health
    gdf = nearest_neighbour(gdf,health[['id_health','Lat','Long']].rename(columns = {'Lat':'Lat_health','Long':'Long_health'}),['longitude','latitude'],['Long_health','Lat_health']).drop(columns = ['id_nearest'])
    gdf['dist_health'] = gdf.apply(lambda row: haversine(row['longitude'], row['latitude'],row['Long_health'],row['Lat_health']), axis=1)

    ### education
    gdf = nearest_neighbour(gdf,education[['id_edu','Lat','Long']].rename(columns = {'Lat':'Lat_edu','Long':'Long_edu'}),['longitude','latitude'],['Long_edu','Lat_edu']).drop(columns = ['id_nearest'])
    gdf['dist_edu'] = gdf.apply(lambda row: haversine(row['longitude'], row['latitude'],row['Long_edu'],row['Lat_edu']), axis=1)

    ### shelters
    gdf = nearest_neighbour(gdf,shelters[['id_shelter','Lat','Long']].rename(columns = {'Lat':'Lat_shel','Long':'Long_shel'}),['longitude','latitude'],['Long_shel','Lat_shel']).drop(columns = ['id_nearest'])
    gdf['dist_shelter'] = gdf.apply(lambda row: haversine(row['longitude'], row['latitude'],row['Long_shel'],row['Lat_shel']), axis=1)
    
    ### road
    #gdf = nearest_neighbour(gdf,road[['id_road','Lat','Long']].rename(columns = {'Lat':'Lat_road','Long':'Long_road'}),['longitude','latitude'],['Long_road','Lat_road']).drop(columns = ['id_nearest'])
    #gdf['dist_road'] = gdf.apply(lambda row: haversine(row['longitude'], row['latitude'],row['Long_road'],row['Lat_road']), axis=1)

    ### road rural
    #gdf = nearest_neighbour(gdf,road_rural[['id_road_rural','Lat','Long']].rename(columns = {'Lat':'Lat_road_rural','Long':'Long_road_rural'}),['longitude','latitude'],['Long_road_rural','Lat_road_rural']).drop(columns = ['id_nearest'])
    #gdf['dist_road_rural'] = gdf.apply(lambda row: haversine(row['longitude'], row['latitude'],row['Long_road_rural'],row['Lat_road_rural']), axis=1)

    ### embankment
    gdf = nearest_neighbour(gdf,embankment_point[['id_embank','Lat','Long']].rename(columns = {'Lat':'Lat_embank','Long':'Long_embank'}),['longitude','latitude'],['Long_embank','Lat_embank']).drop(columns = ['id_nearest'])
    gdf['dist_embank'] = gdf.apply(lambda row: haversine(row['longitude'], row['latitude'],row['Long_embank'],row['Lat_embank']), axis=1)

    ### cities
    #gdf = nearest_neighbour(gdf,cities[['id_urban','Lat','Long']].rename(columns = {'Lat':'Lat_urban','Long':'Long_urban'}),['longitude','latitude'],['Long_urban','Lat_urban']).drop(columns = ['id_nearest'])
    #gdf['dist_urban'] = gdf.apply(lambda row: haversine(row['longitude'], row['latitude'],row['Long_urban'],row['Lat_urban']), axis=1)

    ### coast
    #gdf = nearest_neighbour(gdf,coast[['Lat','Long']].rename(columns = {'Lat':'Lat_coast','Long':'Long_coast'}),['longitude','latitude'],['Long_coast','Lat_coast']).drop(columns = ['id_nearest'])
    #gdf['dist_coast'] = gdf.apply(lambda row: haversine(row['longitude'], row['latitude'],row['Long_coast'],row['Lat_coast']), axis=1)
    #gdf = gdf.drop(columns = ['Lat_coast','Long_coast'])

    ### growth_centre
    gdf = nearest_neighbour(gdf,growth_centre[['id_growth','Lat','Long']].rename(columns = {'Lat':'Lat_growth','Long':'Long_growth'}),['longitude','latitude'],['Long_growth','Lat_growth']).drop(columns = ['id_nearest'])
    gdf['dist_growth'] = gdf.apply(lambda row: haversine(row['longitude'], row['latitude'],row['Long_growth'],row['Lat_growth']), axis=1)

    ### electricity grid
    #gdf = nearest_neighbour(gdf,elec_grid[['id_grid','Lat','Long']].rename(columns = {'Lat':'Lat_grid','Long':'Long_grid'}),['longitude','latitude'],['Long_grid','Lat_grid']).drop(columns = ['id_nearest'])
    #gdf['dist_grid'] = gdf.apply(lambda row: haversine(row['longitude'], row['latitude'],row['Long_grid'],row['Lat_grid']), axis=1)

    ### electricity grid
    gdf = nearest_neighbour(gdf,elec_sub[['id_substation','Lat','Long']].rename(columns = {'Lat':'Lat_substation','Long':'Long_substation'}),['longitude','latitude'],['Long_substation','Lat_substation']).drop(columns = ['id_nearest'])
    gdf['dist_substation'] = gdf.apply(lambda row: haversine(row['longitude'], row['latitude'],row['Long_substation'],row['Lat_substation']), axis=1)

    ### railway stations
    gdf = nearest_neighbour(gdf, rail_station[['id_railstation','Lat','Long']].rename(columns = {'Lat':'Lat_railstation','Long':'Long_railstation'}),['longitude','latitude'],['Long_railstation','Lat_railstation']).drop(columns = ['id_nearest'])
    gdf['dist_railstation'] = gdf.apply(lambda row: haversine(row['longitude'], row['latitude'],row['Long_railstation'],row['Lat_railstation']), axis=1)

    ### road nodes
    gdf = nearest_neighbour(gdf, road_node[['id_roadnode','Lat','Long']].rename(columns = {'Lat':'Lat_roadnode','Long':'Long_roadnode'}),['longitude','latitude'],['Long_roadnode','Lat_roadnode']).drop(columns = ['id_nearest'])
    gdf['dist_roadnode'] = gdf.apply(lambda row: haversine(row['longitude'], row['latitude'],row['Long_roadnode'],row['Lat_roadnode']), axis=1)

    return gdf


#### merge various spatial data
#path_data_files = 'Input_data/' #jaspers code

base_path = os.path.join('D:/Bangladesh')
path_data_files = os.path.join(base_path,'incoming')
    
## education
education_path = os.path.join(path_data_files,'critical_infra/bgd_poi_educationfacilities_lged/bgd_poi_educationfacilities_lged.shp')
education = gpd.read_file(education_path, crs = 'EPSG:4326')
#education = gpd.read_file(path_data_files,'critical_infra/bgd_poi_educationfacilities_lged/bgd_poi_educationfacilities_lged.shp', crs = 'EPSG:4326')
education = education.to_crs('EPSG:4326')
education['Lat']= education.geometry.y
education['Long']= education.geometry.x
education = education.reset_index().rename(columns = {'index':'id_edu'})
print('education data loaded')

## hospital
#health_facilities = gpd.read_file(path_data_files+'bgd_poi_healthfacilities_lged/bgd_poi_healthfacilities_lged.shp',crs = 'EPSG:3106')
#hospital = health_facilities[health_facilities['FType']=='Hospital']
hospital_path = os.path.join(path_data_files, 'critical_infra/cegis_buildings/Hospitals/Hospitals.shp')
hospital = gpd.read_file(hospital_path, crs = 'EPSG:4326')
hospital = hospital.to_crs('EPSG:4326')
hospital['Lat']= hospital.geometry.y
hospital['Long']= hospital.geometry.x
hospital = hospital.reset_index(drop = True).reset_index().rename(columns  = {'index':'id_hospital'})
print('hospital data loaded')

### health facilities
health_path = os.path.join(path_data_files, 'critical_infra/cegis_buildings/Health_facilities/Health_Facilities.shp')
health = gpd.read_file(health_path, crs = 'EPSG:4326')
#health = health_facilities[health_facilities['FType']!='Hospital']
health = health.to_crs('EPSG:4326')
health['Lat']= health.geometry.y
health['Long']= health.geometry.x
health = health.reset_index(drop = True).reset_index().rename(columns  = {'index':'id_health'})
print('health data loaded')

## shelters
shelters_path = os.path.join(path_data_files,'critical_infra/Shelters/cyclone_shelters.shp')
shelters = gpd.read_file(shelters_path)
shelters =shelters.to_crs('EPSG:4326')
shelters['Lat']= shelters.geometry.y
shelters['Long']= shelters.geometry.x
shelters = shelters.reset_index(drop = True).reset_index().rename(columns  = {'index':'id_shelter'})
print('shelter data loaded')

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

### embankments polygon
embankment_path = os.path.join(path_data_files, 'critical_infra/Embankments/Polder_boundary.shp')
embankment = gpd.read_file(embankment_path)
embankment =embankment.to_crs('EPSG:4326')

### embankments points
embankment_point_path = os.path.join(path_data_files,'critical_infra/Embankments/embankment_points.gpkg')
embankment_point = gpd.read_file(embankment_point_path)
embankment_point =embankment_point.to_crs('EPSG:4326')
embankment_point['Lat']= embankment_point.geometry.y
embankment_point['Long']= embankment_point.geometry.x
embankment_point= embankment_point.reset_index(drop = True).reset_index().rename(columns  = {'index':'id_embank'})

### cities
#urban_city_path = path_data_files+'Cities/'
#cities = gpd.read_file(urban_city_path+'GHS_STAT_UCDB2015MT_GLOBE_R2019A_V1_2_short_pnt.gpkg')
#cities['Lat']= cities.geometry.y
#cities['Long']= cities.geometry.x
#cities['id_urban'] = cities.ID_HDC_G0

### coastline
#coast = gpd.read_file(path_data_files+'Coastline/coastline_bangladesh.gpkg').explode()
#coast= coast.reset_index(drop = True).reset_index().rename(columns  = {'index':'id_coast'})
#coast['Lat']= coast.geometry.y
#coast['Long']= coast.geometry.x

### growth centre
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

print('data loaded')

##### list districts
list_district = ['Noakhali','Khulna','Barisal','Satkhira','Bhola','Bagerhat','Patuakhali','Barguna','Pirojpur','Jhalokati','Narail','Chittagong','Jessore', 'Chandpur','Coxs_Bazar','Lakshmipur','Feni','Shariatpur','Gopalganj']
#path_hh_data = 'Output/Processed_households/'
path_hh_data = os.path.join(base_path,'incoming/Population/WB_household_data_jasper/Household_data/Synthetic_data_coded/')


for district in list_district:
    print(district)
    district_urban = pd.read_csv(path_hh_data+'urban_'+str(district)+'.csv')
    district_rural = pd.read_csv(path_hh_data+'rural_'+str(district)+'.csv')

    urban_data = district_urban[['hid','htype_class','Long','Lat','electric_class','water_class','toilet_class']].copy()
    rural_data = district_rural[['hid','htype_class','Long','Lat','electric_class','water_class','toilet_class']].copy()
    del district_urban, district_rural
    urban_data_infra = run_accessibility(urban_data)
    rural_data_infra = run_accessibility(rural_data)

    urban_path = os.path.join(base_path, 'data/household_asset_analysis/Infra_access/urban_infra_access_'+str(district)+'.csv')
    urban_data_infra.to_csv(urban_path, index = False)
    rural_path = os.path.join(base_path, 'data/household_asset_analysis/Infra_access/rural_infra_access_'+str(district)+'.csv')
    rural_data_infra.to_csv(rural_path, index = False)
