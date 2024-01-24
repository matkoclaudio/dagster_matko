
from osgeo import ogr
import geopandas as gpd
import glob
import os
from dagster import Output, asset, get_dagster_logger

@asset
def overlay():
    return "C:\\Users\\clod\\Downloads\\siberian-fed-district\\omsk\\omsk.shp"

@asset
def file_list():
    file_list = glob.glob("C\\Users\\clod\\Downloads\\siberian-fed-district\\*.shp")
    return file_list

@asset
def clip(overlay, file_list):
    adm = gpd.read_file(overlay)
    os.mkdir("C:\\Users\\clod\\Downloads\\siberian-fed-district\\clipp\\")
    for shp_file in file_list:
        shp = gpd.read_file(shp_file)
        gpd.clip(shp, adm, keep_geom_type=True).to_file("C:\\Users\\clod\\Downloads\\siberian-fed-district\\clipp\\" + os.path.basename(shp_file), mode='w')
    return "Ready"

@asset
def reproject(clip):
    for shp_file in glob.glob("C:\\Users\\clod\\Downloads\\siberian-fed-district\\clipp" + "\\*.shp"):
        shp = gpd.read_file(shp_file)
        shp.to_crs({'init':'epsg:32643'}).to_file("C:\\Users\\clod\\Downloads\\siberian-fed-district\\reproy\\" + os.path.basename(shp_file), mode='w')
    return "Done"









