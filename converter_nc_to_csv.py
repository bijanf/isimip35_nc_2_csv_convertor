import os
import pandas as pd
import xarray as xr
import geopandas as gpd
import zipfile
import requests
import io


variables=["rsds","tasmax","tasmin" , "pr", "prsn", "hurs", "sfcwind" , "tas"]
#countries= ["KAZ","AFG","KGZ","TJK","UZB","TKM"]
countries= ["KAZ"]
scenarios=["ssp126","ssp370","ssp585"]
models=["GFDL-ESM4","IPSL-CM6A-LR", "MPI-ESM1-2-HR","MRI-ESM2-0","UKESM1-0-LL"]
dir_input="/home/fallah/scripts/qaisar/ISIMIP/"
dir_output=dir_input

for mod in models:
    for scen in scenarios:
            for count in countries :
                for vari in variables:
                        gdf = gpd.read_file( '../gadm36_'+count+'_1.shp')
                        ds = xr.open_dataset(dir_input+"/"+scen+"/"+vari+"_converted_"+count+"_1960-2100_"+mod+"_"+scen+".nc")
                        var = ds[vari][:]
                #        print(var.shape)
                #        print(var.values)
                #        df = pd.DataFrame(var)
                #        print(gdf.NAME_1.values)
                #        print(ds.time.values)
                #        df.columns=gdf.NAME_1.values
                        # extract only the date part by slicing the strings
                        dates = []
                        ddd = pd.Series(ds.time.values)
                #        print(ddd)
                        for dd in ddd:
                                dates.append(dd.strftime('%Y-%m-%d'))
                #        dates = ds.time.values.astype(str).str[:10]
                #        print(dates)
                #        df = pd.DataFrame(var.values)
                #        df.index = dates
                        namess = []
                        for nnn in gdf.NAME_1.values:
                                namess.append(nnn)
                #        print(namess)
                        print("======================================================")
                        print("======================================================")
                        df = pd.DataFrame(var.values)
                        df = pd.DataFrame(var.values)
                        df.index = dates

                        df.columns=namess

                        print("writing-------------------")
                        df.to_csv(dir_input+"/"+scen+"/"+vari+"_converted_"+count+"_1960-2100_"+mod+"_"+scen+".csv")
                        del df
                        del var
                        del ds
                        del gdf
                        print("FINISHED "+"merged_"+vari+"_converted_"+count+"_"+scen+"_"+mod)





