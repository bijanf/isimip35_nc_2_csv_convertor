import io
import os
import zipfile
import requests
import geopandas as gpd
import matplotlib.pyplot as plt
import xarray as xr
cmd="pip install regionmask"
#os.system(cmd)
import regionmask
import numpy as np
from multiprocessing import Pool
import pandas as pd
import gc
#variable="sfcwind"
#variable="hurs"
#variable="tasmax"
#variable="tasmin"
#variable="tas"
#variable="rsds"
#variable="pr"
variable="prsn"
###countries= ["KAZ","AFG","KGZ","TJK","UZB","TKM"]
###countries= ["UZB"]
output="./"
scenario="ssp585"
os.system("mkdir -p "+scenario)
output=output+"/"+scenario


def process_country(country):
    

#    country = countries[i]
    local_path = './'
    z = zipfile.ZipFile("../shapefiles/gadm36_"+country+"_shp.zip")
    z.extractall(path=local_path)
    # Load shapefile                                                                                                                                                                  
    if country == "TKM":
        gdf = gpd.read_file(local_path + '/gadm36_'+country+'_1.shp')
        
    else:
        gdf = gpd.read_file(local_path + '/gadm36_'+country+'_1.shp')



    # Load netCDF file
#    for date in ["1850_1850","1851_1860",
#                 "1861_1870","1871_1880","1891_1890","1891_1900","1901_1910",
#                 "1911_1920","1921_1930","1931_1940","1941_1950","1951_1960",
#                 "1961_1970","1971_1980","1981_1990","1991_2000","2001_2010","2011_2014"]:
#    for date in ["1961_1970","1971_1980","1981_1990","1991_2000","2001_2010","2011_2014"]:
    for date in ["1960-2100"]:
         print(date,'-----------------')
         for model in ["GFDL-ESM4",  "IPSL-CM6A-LR" , "MPI-ESM1-2-HR",  "MRI-ESM2-0"  ,"UKESM1-0-LL"]:

#            dir_to_netcdf="/p/projects/isimip/isimip/ISIMIP3a/InputData/climate/atmosphere/obsclim/global/daily/historical/CHELSA-W5E5v1.0/"
#             dir_to_netcdf="/p/projects/isimip/isimip/ISIMIP3b/InputData/climate/atmosphere/bias-adjusted/global/daily/historical/" 
             dir_to_netcdf="/p/projects/gvca/data/qaisar_KZ/ncdf_transient/"+scenario+"/"
#            if model == "GFDL-ESM4":
#                file=variable+'
#            if model == "IPSL-CM6A-LR" :
#                file='ipsl-cm6a-lr_r1i1p1f1_w5e5_'+scenario+'_'+variable+'_global_daily_'+date+'.nc'
#            if model =="MPI-ESM1-2-HR":
#                file='mpi-esm1-2-hr_r1i1p1f1_w5e5_'+scenario+'_'+variable+'_global_daily_'+date+'.nc'
#            if model =="MRI-ESM2-0" :
#                file='mri-esm2-0_r1i1p1f1_w5e5_'+scenario+'_'+variable+'_global_daily_'+date+'.nc'
#            if model =="UKESM1-0-LL" :
#                file='ukesm1-0-ll_r1i1p1f2_w5e5_'+scenario+'_'+variable+'_global_daily_'+date+'.nc'
             file=variable+"_"+model+"_isimip3b_"+date+".nc"
             file = dir_to_netcdf+"/"+file
             print(file)
             ########
            
             ds = xr.open_dataset(file)
             ds.load()
             # Increase resolution of netCDF data using spatial interpolation
#            print("started remapping------------")
#            new_lon = np.arange(0, 80, 0.2)
#            new_lat = np.arange(0, 130, 0.2)
#            ds = ds.interp(lon=new_lon, lat=new_lat)
#            print("finished remapping------------")
    

            # Create mask for each polygon in shapefile
#            mask = regionmask.mask_geopandas(gdf, ds['lon'], ds['lat'])
    
            ## Calculate mean for each polygon
            # loop over polygones
             var = ds[variable]

#            resi=[]
#            for ind in range(len(gdf)):
#                print(ind)
#                gdf1=gdf[gdf.index == ind]
             mask = regionmask.mask_geopandas(gdf, var['lon'], var['lat'])
             result = var.groupby(mask).mean(dim="stacked_lat_lon")
             ds.close() 
#                resi.append(list(result.values.squeeze()))
#            result.to_netcdf(output+variable+"_converted_"+country+"_"+date+"_"+model+"_"+scenario+".nc")

#            del result
#            del ds
#            del mask 
#            gc.collect()   

            #df = pd.DataFrame(resi)
            #df = df.T
            #df.index = result.time
            #df.columns = gdf.NAME_1
            #df.to_csv(output+variable+"_converted_"+country+"_"+date+"_"+model+"_"+scenario+".csv")
            #del resi                
               
             result.to_netcdf(output+"/"+variable+"_converted_"+country+"_"+date+"_"+model+"_"+scenario+".nc")
             del result
             del ds
             del mask
             gc.collect()




if __name__ == '__main__':

####    num_cpus = int(os.environ.get('SLURM_CPUS_PER_TASK', 1))
#    countries= ["KAZ","AFG","KGZ","TJK","UZB","TKM"]
#    countries= ["KAZ","AFG"]
#    countries= ["KGZ","TJK"]
#    countries= ["UZB","TKM"]
    countries= ["KAZ"]
    pool = Pool(1)
    pool.map(process_country, countries)
    pool.close()
    pool.join()

