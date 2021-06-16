import pandas as pd
import csv
import requests
import numpy
import json
import arcpy
import os
import xlsxwriter
import shutil
import fnmatch
import pickle
from datetime import datetime
import time
import sys
from ReportGenerator import reportgenerate
#############  DECLARING LOCAL VARIABLES



s_url = 'https://api.census.gov/data/2020/dec/responserate?get=RESP_DATE&for=us:*'
s_r = requests.get(s_url)
s_json = s_r.json()
apidate = s_json [1][0]
apidate = datetime.strptime(apidate, '%Y-%m-%d').date()
f = open('ReportData/pulldate.p', 'r')
mostrecentapi = pickle.load(f)
f.close()

if apidate == mostrecentapi:
    print('API Last Updated at:')
    print(mostrecentapi)
    print("API not updated yet, Reports and Featureclasses up to date.")
    sys.exit(1)



#change these paths to match where you would like the resulting csv to be located on your own system. Make sure you keep .csv at the end ("your\\local\\path\\Tract_APIPull.csv"). This is where the csv files will be stored for importing into the gdb
Tract_APIPull_csv = "ReportData\\Tract_APIPull.csv"
County_APIPull_csv = "ReportData\\Counties__APIPull.csv"
Place_APIPull_csv = "ReportData\\Place_APIPull.csv"
MCD_APIPull_csv = "ReportData\\MCD_APIPull.csv"
CD_APIPull_csv = "ReportData\\CD_APIPull.csv"
AIA_APIPull_csv = "ReportData\\AIA_APIPull.csv"


###change this to where you would like to export the excel format for the API pull. This is an easier to read format than the CSVs and will only be generated for viewing data in tabular format, not for otherwise use in this script.
exceloutput_location = "output"
archive_location = "output"

#change this to where your file GDB is located that contains the regional geography files to join to. In order for this script to work, there must already be empty BAS20 vintage geography files present in the geodatabase for the data to join to.
ResponseMapper_gdb = "M:\\08_Geography\\ResponseRateMapper\\ResponseMapper.gdb"
#Assign these variables the name of the featureclass ALREADY IN the geodatabaseabove. These are the original BAS20 vintage geography files. The data will be temporarily joined to these during the script but will return to their original state after the script has ran.
#Ensure these all have a 'GEOID' text field for the data to join to or else the join will fail.
Tracts_featureclass = "Tracts"               #GEOID field is state&county&tract FIPS codes concatenated
Counties_featureclass = "Counties_"           #GEOID field is state&county FIPS codes concatenated
CountySub_featureclass = "MCDs"              #GEOID field is state&county&county_subdivision FIPS codes concatenated
IncPlaces_featureclass = "IncPlaces"         #GEOID field is state&place FIPS codes concatenated
CD_featureclass = "CongressionalDistricts"  #GEOID field is state&congressional_district FIPS codes concatenated
AIA_featureclass = "AIAs"

##insert your local regional state codes into this list, separated by a comma and with quotes on either side.
Regional_States = ['09','23','25','33','34','36','44','50','72']

#my personal API key, affording me unlimited API calls. obtain from census bureau developers page
key = '3ddb65263043819d65aff52e0ff095ce75b8ed03'

#############  END VARIABLE DECLARATION
 






print('Grabbing data from API...')
##creating empty dataframes to append the respective geography's API results to
states_finaldf = pd.DataFrame()
tracts_finaldf = pd.DataFrame()
counties_finaldf = pd.DataFrame()
MCD_finaldf = pd.DataFrame()
place_finaldf = pd.DataFrame()
cd_finaldf = pd.DataFrame()


#iterating through all of the state FIPS codes for our region and adding that to the API URL
for state in Regional_States:
    print('Getting ' + state +  ' data...')
    s_url = 'https://api.census.gov/data/2020/dec/responserate?get=DRRALL,RESP_DATE,CRRALL,CRRINT,DRRINT,GEO_ID&for=state:'+state
    s_r = requests.get(s_url)
    s_json = s_r.json()
    s_df = pd.DataFrame(data= s_json[1:],columns= s_json[0])
    states_finaldf = states_finaldf.append(s_df)
    t_url = 'https://api.census.gov/data/2020/dec/responserate?get=DRRALL,CRRINT,DRRINT,RESP_DATE,CRRALL,GEO_ID&for=tract:*&in=state:'+state+'&key='+key
    r = requests.get(t_url)
    tract_json = r.json()
    #making a temporary dataframe from the json for each iteration and then appending it to the global dataframe    
    t_df = pd.DataFrame(data= tract_json[1:],columns= tract_json[0])
    tracts_finaldf= tracts_finaldf.append(t_df)
    #Puerto Rico (FIPS code 72) does not have congressional districts, MCD or IncPlaces and therefore the API call to that URL would 404
    if state != '72':
        cd_url = 'https://api.census.gov/data/2020/dec/responserate?get=DRRALL,CRRINT,DRRINT,RESP_DATE,CRRALL,GEO_ID&for=congressional%20district:*&in=state:'+state+'&key='+key
        cd_r = requests.get(cd_url)
        cd_json = cd_r.json()
        cd_df = pd.DataFrame(data= cd_json[1:],columns= cd_json[0])
        cd_finaldf = cd_finaldf.append(cd_df)
        mcd_url = 'https://api.census.gov/data/2020/dec/responserate?get=DRRALL,RESP_DATE,CRRALL,CRRINT,DRRINT,GEO_ID&for=county%20subdivision:*&in=state:'+state+'&key='+key
        mcd_r = requests.get(mcd_url)
        mcd_json = mcd_r.json()
        mcd_df = pd.DataFrame(data= mcd_json[1:],columns= mcd_json[0])
        MCD_finaldf = MCD_finaldf.append(mcd_df)
        pl_url = 'https://api.census.gov/data/2020/dec/responserate?get=DRRALL,CRRINT,DRRINT,RESP_DATE,CRRALL,GEO_ID&for=place:*&in=state:'+state+'&key='+key
        pl_r = requests.get(pl_url)
        pl_json = pl_r.json()
        pl_df = pd.DataFrame(data= pl_json[1:],columns= pl_json[0])
        place_finaldf = place_finaldf.append(pl_df)
    c_url = 'https://api.census.gov/data/2020/dec/responserate?get=DRRALL,RESP_DATE,CRRINT,DRRINT,CRRALL,GEO_ID&for=county:*&in=state:'+state+'&key='+key
    county_r = requests.get(c_url)
    county_json = county_r.json()
    c_df = pd.DataFrame(data= county_json[1:],columns= county_json[0])
    counties_finaldf = counties_finaldf.append(c_df)
print('Getting American Indian Area Response Rates...')
aia_url = 'https://api.census.gov/data/2020/dec/responserate?get=DRRALL,CRRINT,RESP_DATE,CRRALL,CRRINT,DRRINT,GEO_ID,DRRINT&for=american%20indian%20area/alaska%20native%20area/hawaiian%20home%20land:*'
aia_r = requests.get(aia_url)
aia_json = aia_r.json()
aia_df = pd.DataFrame(data= aia_json[1:],columns= aia_json[0])

#The GEOID from the API includes a random string of 9 numbers before the actual geoid, in order to have this
# join successfully to the shapefile we need to remove these characters
# we are then exporting to a csv to import into a GDB

tracts_finaldf['GEO_ID'] =  tracts_finaldf.GEO_ID.str.slice(start=9)
#tracts_finaldf.to_csv(Tract_APIPull_csv)
counties_finaldf['GEO_ID'] =  counties_finaldf.GEO_ID.str.slice(start=9)
#counties_finaldf.to_csv(County_APIPull_csv)
place_finaldf['GEO_ID'] =  place_finaldf.GEO_ID.str.slice(start=9)
#place_finaldf.to_csv(Place_APIPull_csv)
MCD_finaldf['GEO_ID'] =  MCD_finaldf.GEO_ID.str.slice(start=9)
#MCD_finaldf.to_csv(MCD_APIPull_csv)
cd_finaldf['GEO_ID'] =  cd_finaldf.GEO_ID.str.slice(start=9)
#cd_finaldf.to_csv(CD_APIPull_csv)
states_finaldf['GEO_ID'] =  states_finaldf.GEO_ID.str.slice(start=9)
cd_finaldf = cd_finaldf.reset_index()
date = cd_finaldf.at[1,'RESP_DATE']
date = date.replace('-','_')

print('generating report')
reportgenerate(tracts_finaldf,counties_finaldf,MCD_finaldf,place_finaldf,cd_finaldf,states_finaldf,aia_df)

print('report generated')

#create dataframe from csvs which store names and GEOID. Object is the type pandas uses for strings.
aianames = pd.read_csv('ReportData\\AIAnames.csv',dtype = {"AIANNHCE" : "object"},encoding = 'UTF-8')
placenames = pd.read_csv('ReportData\\PlacesNames.csv',dtype = {"GEOID" : "object"},encoding = 'UTF-8')
mcdnames = pd.read_csv('ReportData\\CountySubNames.csv',dtype = {"GEOID" : "object"}, encoding = 'UTF-8')
countynames = pd.read_csv('ReportData\\CountiesNames.csv', dtype={"GEOID": "object"},encoding = 'UTF-8')
projected = pd.read_csv('ReportData\\ProjectedResponse.csv', dtype={"GEOID": "object"},encoding='UTF-8')

#joining API result dataframes to csvs that contain names, For AIAs, we are joining TO the name file (which contains only the AIAs in the region) since the API gets data for the entire country
MCD_finaldf = MCD_finaldf.merge(mcdnames, how='left', left_on='GEO_ID', right_on='GEOID')
place_finaldf = place_finaldf.merge(placenames, how='left', left_on='GEO_ID', right_on='GEOID')
aia_finaldf = aianames.merge(aia_df, how='left', left_on='AIANNHCE', right_on='american indian area/alaska native area/hawaiian home land')
counties_finaldf = counties_finaldf.merge(countynames, how='left', left_on='GEO_ID', right_on= 'GEOID')

#join county nsmes to tract
#first need to add full county fips code to tract dataframe
tracts_finaldf["countyFIPS"] = tracts_finaldf["state"].astype(str) + tracts_finaldf["county"].astype(str)
tracts_finaldf=tracts_finaldf.merge(countynames, how='left', left_on='countyFIPS', right_on='GEOID')
tracts_finaldf=tracts_finaldf.merge(projected, how='left', left_on='GEO_ID', right_on='GEOID')


#select only certain columns and reorder in more logical order
MCD_finaldf = MCD_finaldf[['state','county subdivision','NAME','GEO_ID','RESP_DATE','DRRINT','DRRALL','CRRINT','CRRALL']]
place_finaldf = place_finaldf[['state','place','NAME','GEO_ID','GEOID','RESP_DATE','DRRINT','DRRALL','CRRINT','CRRALL']]
aia_finaldf = aia_finaldf[['NAME','AIANNHCE','RESP_DATE','DRRINT','DRRALL','CRRINT','CRRALL']]
counties_finaldf = counties_finaldf[['state','county','GEO_ID','NAME','GEOID','RESP_DATE','DRRINT','DRRALL','CRRINT','CRRALL']]
cd_finaldf = cd_finaldf[['state','GEO_ID','congressional district','RESP_DATE','DRRINT','DRRALL','CRRINT','CRRALL']]
tracts_finaldf = tracts_finaldf[['state','NAME','tract','GEO_ID','RESP_DATE','DRRINT','DRRALL','CRRINT','CRRALL','ACO_Code','Projected_Self_Response','Projection_Threshold_Color']]

#replace state codes with states
MCD_finaldf["state"].replace({"09": "Connecticut", "25": "Massachusetts","36":"New York","44":"Rhode Island","23":"Maine","33":"New Hampshire","50":"Vermont","34":"New Jersey","72":"Pueto Rico"}, inplace=True)
place_finaldf["state"].replace({"09": "Connecticut", "25": "Massachusetts","36":"New York","44":"Rhode Island","23":"Maine","33":"New Hampshire","50":"Vermont","34":"New Jersey","72":"Pueto Rico"}, inplace=True)
counties_finaldf["state"].replace({"09": "Connecticut", "25": "Massachusetts","36":"New York","44":"Rhode Island","23":"Maine","33":"New Hampshire","50":"Vermont","34":"New Jersey","72":"Pueto Rico"}, inplace=True)
cd_finaldf["state"].replace({"09": "Connecticut", "25": "Massachusetts","36":"New York","44":"Rhode Island","23":"Maine","33":"New Hampshire","50":"Vermont","34":"New Jersey","72":"Pueto Rico"}, inplace=True)
tracts_finaldf["state"].replace({"09": "Connecticut", "25": "Massachusetts","36":"New York","44":"Rhode Island","23":"Maine","33":"New Hampshire","50":"Vermont","34":"New Jersey","72":"Pueto Rico"}, inplace=True)


#rename columns in dataframe
MCD_finaldf.rename(columns={"state":"State Name",
                            "NAME":"MCD Name",
                            "RESP_DATE":"DATE",
                            'DRRALL':'Daily Response Rate (%)',
                            'CRRINT':'Cumulative Internet Response Rate (%)',
                            'CRRALL':'Cumulative Response Rate (%)',
                            'DRRINT':'Daily Internet Response Rate (%)'},inplace=True)

place_finaldf.rename(columns={"state":"State Name",
                              "NAME":"Place Name",
                              "RESP_DATE":"DATE",
                              'DRRALL':'Daily Response Rate (%)',
                              'CRRINT':'Cumulative Internet Response Rate (%)',
                              'CRRALL':'Cumulative Response Rate (%)',
                              'DRRINT':'Daily Internet Response Rate (%)'},inplace=True)

aia_finaldf.rename(columns={'DRRALL':'Daily Response Rate (%)',
                            "RESP_DATE":"DATE",
                            'CRRINT':'Cumulative Internet Response Rate (%)',
                            'CRRALL':'Cumulative Response Rate (%)',
                            'DRRINT':'Daily Internet Response Rate (%)'},inplace=True)

counties_finaldf.rename(columns={"state":"State Name",
                                 "NAME":"County Name",
                                 "RESP_DATE":"DATE",
                                 'DRRALL':'Daily Response Rate (%)',
                                 'CRRINT':'Cumulative Internet Response Rate (%)',
                                 'CRRALL':'Cumulative Response Rate (%)',
                                 'DRRINT':'Daily Internet Response Rate (%)'},inplace=True)

cd_finaldf.rename(columns={"state":"State Name",
                           "congressional district": "Congressional District",
                           "RESP_DATE":"DATE",'DRRALL':'Daily Response Rate (%)',
                           'CRRINT':'Cumulative Internet Response Rate (%)',
                           'CRRALL':'Cumulative Response Rate (%)',
                           'DRRINT':'Daily Internet Response Rate (%)'},inplace=True)

tracts_finaldf.rename(columns={"state":"State Name",
                               "NAME":"County Name",
                               "RESP_DATE":"DATE",
                               'DRRALL':'Daily Response Rate (%)',
                               'CRRINT':'Cumulative Internet Response Rate (%)',
                               'CRRALL':'Cumulative Response Rate (%)',
                               'DRRINT':'Daily Internet Response Rate (%)'},inplace=True)



tracts_finaldf.to_csv(Tract_APIPull_csv, encoding='utf-8')
counties_finaldf.to_csv(County_APIPull_csv, encoding='utf-8')
place_finaldf.to_csv(Place_APIPull_csv, encoding='utf-8')
MCD_finaldf.to_csv(MCD_APIPull_csv, encoding='utf-8')
cd_finaldf.to_csv(CD_APIPull_csv, encoding='utf-8')
aia_finaldf.to_csv(AIA_APIPull_csv, encoding='utf-8')

newexcelfilename= 'ResponseData_'+date+'.xlsx'

print('moving excel docs to archive old data')
newfilepulled = False
for file in os.listdir("output"):
        if fnmatch.fnmatch(file,'*ResponseData*.xlsx')and(file != newexcelfilename) and (file != "Public"+newexcelfilename):
                try:
                    shutil.move("output"+file, archive_location+file)
                    print()
                    print("moving " + file + " to archive")
                except Exception as e:
                    print()
                    print(e)
                    print(file +"in use. cannot move to archive")
                    continue
print()






print('Data pulled successfully. ')
print('***********************************')
print('')
print('Importing CSV to Geodatabase...')
print "Checking for locks in GDB"
LockList=["file"]
while len(LockList)>0:   
    LockList = [f for f in os.listdir('M:\\08_Geography\\ResponseRateMapper\\ResponseMapper.gdb') if f.endswith('.lock')]
    if len(LockList)>0:
        print "GDB has locks, Lock files: "+str(LockList)
        raw_input("Remove locks then press Enter to check again...")
print ("GDB has no locks")
# Set the current workspace
arcpy.env.workspace = ResponseMapper_gdb
arcpy.env.overwriteOutput = True


#check for extra index on tracts feature class GEOID field and remove prior to
#performing processing within the gdb
indexes = arcpy.ListIndexes('Tracts')
for index in indexes:
    if index.name=='GEOID':
        arcpy.RemoveIndex_management ("Tracts", index.name)

    
# Process: Table to Table
arcpy.TableToTable_conversion(AIA_APIPull_csv, ResponseMapper_gdb, "AIA_ResponseRates", "", "Field1 \"Field1\" true true false 4 Long 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\AIA_APIPull.csv,Field1,-1,-1;NAME \"NAME\" true true false 8000 Text 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\AIA_APIPull.csv,NAME,-1,-1;AIANNHCE \"AIANNHCE\" true true false 8 Text 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\AIA_APIPull.csv,AIANNHCE,-1,-1;DATE \"DATE\" true true false 12 Text 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\AIA_APIPull.csv,DATE,-1,-1;Daily_Internet_Response_Rate____ \"Daily Internet Response Rate (%)\" true true false 8 Double 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\AIA_APIPull.csv,Daily Internet Response Rate (%),-1,-1;Daily_Response_Rate____ \"Daily Response Rate (%)\" true true false 8 Double 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\AIA_APIPull.csv,Daily Response Rate (%),-1,-1;Cumulative_Internet_Response_Rate____ \"Cumulative Internet Response Rate (%)\" true true false 8 Double 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\AIA_APIPull.csv,Cumulative Internet Response Rate (%),-1,-1;Cumulative_Response_Rate____ \"Cumulative Response Rate (%)\" true true false 4 Long 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\AIA_APIPull.csv,Cumulative Response Rate (%),-1,-1", "")
arcpy.TableToTable_conversion(CD_APIPull_csv, ResponseMapper_gdb, "CD_ResponseRates", "", "Field1 \"Field1\" true true false 4 Long 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\CD_APIPull.csv,Field1,-1,-1;State_Name \"State Name\" true true false 8000 Text 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\CD_APIPull.csv,State Name,-1,-1;GEO_ID \"GEO_ID\" true true false 10 Text 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\CD_APIPull.csv,GEO_ID,-1,-1;Congressional_District \"Congressional District\" true true false 4 Text 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\CD_APIPull.csv,Congressional District,-1,-1;DATE \"DATE\" true true false 12 Text 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\CD_APIPull.csv,DATE,-1,-1;Daily_Internet_Response_Rate____ \"Daily Internet Response Rate (%)\" true true false 8 Double 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\CD_APIPull.csv,Daily Internet Response Rate (%),-1,-1;Daily_Response_Rate____ \"Daily Response Rate (%)\" true true false 8 Double 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\CD_APIPull.csv,Daily Response Rate (%),-1,-1;Cumulative_Internet_Response_Rate____ \"Cumulative Internet Response Rate (%)\" true true false 8 Double 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\CD_APIPull.csv,Cumulative Internet Response Rate (%),-1,-1;Cumulative_Response_Rate____ \"Cumulative Response Rate (%)\" true true false 8 Double 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\CD_APIPull.csv,Cumulative Response Rate (%),-1,-1", "")
arcpy.TableToTable_conversion(County_APIPull_csv, ResponseMapper_gdb, "County__ResponseRates", "", 'State_Name "State Name" true true false 8000 Text 0 0 ,First,#,M:\08_Geography\ResponseRateMapper\Counties__APIPull.csv,State Name,-1,-1;county "county" true true false 4 Text 0 0 ,First,#,M:\08_Geography\ResponseRateMapper\Counties__APIPull.csv,county,-1,-1;GEO_ID "GEO_ID" true true false 12 Text 0 0 ,First,#,M:\08_Geography\ResponseRateMapper\Counties__APIPull.csv,GEO_ID,-1,-1;County_Name "County Name" true true false 8000 Text 0 0 ,First,#,M:\08_Geography\ResponseRateMapper\Counties__APIPull.csv,County Name,-1,-1;GEOID "GEOID" true true false 12 Text 0 0 ,First,#,M:\08_Geography\ResponseRateMapper\Counties__APIPull.csv,GEOID,-1,-1;DATE "DATE" true true false 12 Text 0 0 ,First,#,M:\08_Geography\ResponseRateMapper\Counties__APIPull.csv,DATE,-1,-1;Daily_Internet_Response_Rate____ "Daily Internet Response Rate (%)" true true false 8 Double 0 0 ,First,#,M:\08_Geography\ResponseRateMapper\Counties__APIPull.csv,Daily Internet Response Rate (%),-1,-1;Daily_Response_Rate____ "Daily Response Rate (%)" true true false 8 Double 0 0 ,First,#,M:\08_Geography\ResponseRateMapper\Counties__APIPull.csv,Daily Response Rate (%),-1,-1;Cumulative_Internet_Response_Rate____ "Cumulative Internet Response Rate (%)" true true false 8 Double 0 0 ,First,#,M:\08_Geography\ResponseRateMapper\Counties__APIPull.csv,Cumulative Internet Response Rate (%),-1,-1;Cumulative_Response_Rate____ "Cumulative Response Rate (%)" true true false 8 Double 0 0 ,First,#,M:\08_Geography\ResponseRateMapper\Counties__APIPull.csv,Cumulative Response Rate (%),-1,-1', config_keyword="")
arcpy.TableToTable_conversion(MCD_APIPull_csv, ResponseMapper_gdb, "MCD_ResponseRates", "", 'Field1 "Field1" true true false 4 Long 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\MCD_APIPulll.csv,Field1,-1,-1;State_Name "State Name" true true false 8000 Text 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\MCD_APIPulll.csv,State Name,-1,-1;county_subdivision "county subdivision" true true false 10 Text 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\MCD_APIPulll.csv,county subdivision,-1,-1;MCD_Name "MCD Name" true true false 8000 Text 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\MCD_APIPulll.csv,MCD Name,-1,-1;GEO_ID "GEO_ID" true true false 15 Text 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\MCD_APIPulll.csv,GEO_ID,-1,-1;DATE "DATE" true true false 12 text 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\MCD_APIPulll.csv,DATE,-1,-1;Daily_Internet_Response_Rate____ "Daily Internet Response Rate (%)" true true false 8 Double 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\MCD_APIPulll.csv,Daily Internet Response Rate (%),-1,-1;Daily_Response_Rate____ "Daily Response Rate (%)" true true false 8 Double 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\MCD_APIPulll.csv,Daily Response Rate (%),-1,-1;Cumulative_Internet_Response_Rate____ "Cumulative Internet Response Rate (%)" true true false 8 Double 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\MCD_APIPulll.csv,Cumulative Internet Response Rate (%),-1,-1;Cumulative_Response_Rate____ "Cumulative Response Rate (%)" true true false 8 Double 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\MCD_APIPulll.csv,Cumulative Response Rate (%),-1,-1', config_keyword="")
arcpy.TableToTable_conversion(Place_APIPull_csv, ResponseMapper_gdb, "IncPlace_ResponseRates", "", "Field1 \"Field1\" true true false 4 Long 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\Place_APIPull.csv,Field1,-1,-1;GEOID \"GEOID\" true true false 10 Text 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\Place_APIPull.csv,GEOID,-1,-1;Place_Name \"Place Name\" true true false 8000 Text 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\Place_APIPull.csv,Place Name,-1,-1;CRRALL \"CRRALL\" true true false 8 Double 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\Place_APIPull.csv,CRRALL,-1,-1;DRRALL \"DRRALL\" true true false 8 Double 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\Place_APIPull.csv,DRRALL,-1,-1;FSRR2010 \"FSRR2010\" true true false 8 Double 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\Place_APIPull.csv,FSRR2010,-1,-1;DATE \"DATE\" true true false 12 Text 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\Place_APIPull.csv,DATE,-1,-1;Daily_Internet_Response_Rate____ \"Daily Internet Response Rate (%)\" true true false 8 Double 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\Place_APIPull.csv,Daily Internet Response Rate (%),-1,-1;Daily_Response_Rate____ \"Daily Response Rate (%)\" true true false 8 Double 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\Place_APIPull.csv,Daily Response Rate (%),-1,-1;Cumulative_Internet_Response_Rate____ \"Cumulative Internet Response Rate (%)\" true true false 8 Double 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\Place_APIPull.csv,Cumulative Internet Response Rate (%),-1,-1;Cumulative_Response_Rate____ \"Cumulative Response Rate (%)\" true true false 8 Double 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\Place_APIPull.csv,Cumulative Response Rate (%),-1,-1", "")
arcpy.TableToTable_conversion(Tract_APIPull_csv, ResponseMapper_gdb, "Tract_ResponseRates", "", "Field1 \"Field1\" true true false 4 Long 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\Tract_APIPull.csv,Field1,-1,-1;County_Name \"County Name\" true true false 8000 Text 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\Tract_APIPull.csv,County Name,-1,-1;tract \"tract\" true true false 10 Text 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\Tract_APIPull.csv,tract,-1,-1;GEO_ID \"GEO_ID\" true true false 15 Text 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\Tract_APIPull.csv,GEO_ID,-1,-1;DATE \"DATE\" true true false 12 Text 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\Tract_APIPull.csv,DATE,-1,-1;DRRALL \"DRRALL\" true true false 8 Double 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\Tract_APIPull.csv,DRRALL,-1,-1;Daily_Internet_Response_Rate____ \"Daily Internet Response Rate (%)\" true true false 8 Double 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\Tract_APIPull.csv,Daily Internet Response Rate (%),-1,-1;Daily_Response_Rate____ \"Daily Response Rate (%)\" true true false 8 Double 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\Tract_APIPull.csv,Daily Response Rate (%),-1,-1;Cumulative_Internet_Response_Rate____ \"Cumulative Internet Response Rate (%)\" true true false 8 Double 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\Tract_APIPull.csv,Cumulative Internet Response Rate (%),-1,-1;Cumulative_Response_Rate____ \"Cumulative Response Rate (%)\" true true false 4 Double 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\Tract_APIPull.csv,Cumulative Response Rate (%),-1,-1;ACO_Code \"ACO_Code\" true true false 8 Double 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\Tract_APIPull.csv,ACO_Code,-1,-1;Projected_Self_Response \"Projected_Self_Response\" true true false 8 Double 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\Tract_APIPull.csv,Projected_Self_Response,-1,-1;Projection_Threshold_Color \"Projection_Threshold_Color\" true true false 8000 Text 0 0 ,First,#,M:\\08_Geography\\ResponseRateMapper\\Tract_APIPull.csv,Projection_Threshold_Color,-1,-1", "")
tracttable = ResponseMapper_gdb+'/Tract_ResponseRates'
countytable = ResponseMapper_gdb+'/County__ResponseRates'
placetable = ResponseMapper_gdb+'/IncPlace_ResponseRates'
mcdtable = ResponseMapper_gdb+"/MCD_ResponseRates"
cdtable =  ResponseMapper_gdb+'/CD_ResponseRates'
aiatable =  ResponseMapper_gdb+'/AIA_ResponseRates'



print("data imported.")
print('')
print('adding leading zeros to GEOID field...')

# add leading zeros to tract GEOID
with arcpy.da.UpdateCursor(tracttable, "GEO_ID") as cursor:
    for row in cursor:
        if len(row[0]) == 10:
            row[0] = "0"+ row[0]
        cursor.updateRow(row)
with arcpy.da.UpdateCursor(countytable, "GEO_ID") as cursor:
    for row in cursor:
        if len(row[0]) == 4:
            row[0] = "0"+ row[0]
        cursor.updateRow(row)
with arcpy.da.UpdateCursor(placetable, "GEOID") as cursor:
    for row in cursor:
        if row[0] is not None:
            if len(row[0]) == 6:
                row[0] = "0"+ row[0]
            cursor.updateRow(row)
with arcpy.da.UpdateCursor(mcdtable, "GEO_ID") as cursor:
    for row in cursor:
        if row[0] is not None:
            if len(row[0]) == 9:
                row[0] = "0"+ row[0]
            cursor.updateRow(row)
with arcpy.da.UpdateCursor(cdtable, "GEO_ID") as cursor:
    for row in cursor:
        if row[0] is not None:        
            if len(row[0]) == 3:
                row[0] = "0"+ row[0]
            cursor.updateRow(row)




print('')
print('joining data to featureclass...')


print('making layers from tract fc')
arcpy.MakeFeatureLayer_management(ResponseMapper_gdb+"/"+Tracts_featureclass,"Tracts")
print('makiing layers from tract response data')
arcpy.MakeTableView_management(tracttable, "TractResponseData")

print('making layer from counties fc')
arcpy.MakeFeatureLayer_management(ResponseMapper_gdb+"/"+Counties_featureclass,"Counties")
print('making layer from county response data')
arcpy.MakeTableView_management(countytable, "CountyResponseData")

print('making layer from mcd fc')
arcpy.MakeFeatureLayer_management(ResponseMapper_gdb+"/"+CountySub_featureclass,"MCDs")
print('making layer from mcd response table')
arcpy.MakeTableView_management(mcdtable, "MCDResponseData")

print('making place fc layer')
arcpy.MakeFeatureLayer_management(ResponseMapper_gdb+"/"+IncPlaces_featureclass,"IncPlaces")
print('making place table layer')
arcpy.MakeTableView_management(placetable, "PlaceResponseData")

print('making cd fc layer')
arcpy.MakeFeatureLayer_management(ResponseMapper_gdb+"/"+CD_featureclass,"CongressionalDistricts")
print('making cd response table layer')
arcpy.MakeTableView_management(cdtable, "CDResponseData")

print('making aia fc layer')
arcpy.MakeFeatureLayer_management(ResponseMapper_gdb+"/"+AIA_featureclass,"AIAs")
print('making aia table layer')
arcpy.MakeTableView_management(aiatable, "AIAResponseData")

inlayer = "Tracts"
in_Field = "GEOID"
joinTable = "TractResponseData"
joinField = "GEO_ID"

print('joining tracts')
arcpy.AddJoin_management (inlayer, in_Field, joinTable, joinField)
print('deleting old tracts layer')
if arcpy.Exists("Tracts_Joined"):
    arcpy.Delete_management("Tracts_Joined")
print('copying to new tracts_joined fc')

arcpy.CopyFeatures_management(inlayer,ResponseMapper_gdb+"/Tracts_Joined" )
##arcpy.TruncateTable_management(ResponseMapper_gdb+"/Tracts_Joined")
##arcpy.Append_management(inlayer,ResponseMapper_gdb+"/Tracts_Joined",schema_type = "NO_TEST")
##

print('remove join management')
arcpy.RemoveJoin_management (inlayer)

inlayer = "Counties"
in_Field = "GEOID"
joinTable = "CountyResponseData"
joinField = "GEO_ID"
print('joining counties')
arcpy.AddJoin_management (inlayer, in_Field, joinTable, joinField)

print('deleting old couties_joined')
if arcpy.Exists("Counties_Joined"):
    arcpy.Delete_management("Counties_Joined")

print('copying joined data to new counties_joined fc')    
arcpy.CopyFeatures_management(inlayer,ResponseMapper_gdb+"/Counties_Joined" )

arcpy.RemoveJoin_management (inlayer)

inlayer = "MCDs"
in_Field = "GEOID"
joinTable = "MCDResponseData"
joinField = "GEO_ID"
print('joining mcd')
arcpy.AddJoin_management (inlayer, in_Field, joinTable, joinField)

print('deletingold mcd_jioned')
if arcpy.Exists("MCDs_Joined"):
    arcpy.Delete_management("MCDs_Joined")

print('copying new mcd joined')    
arcpy.CopyFeatures_management(inlayer,ResponseMapper_gdb+"/MCDs_Joined" )

arcpy.RemoveJoin_management (inlayer)

inlayer = "IncPlaces"
in_Field = "GEOID"
joinTable = "PlaceResponseData"
joinField = "GEOID"
print('joining inc plc')
arcpy.AddJoin_management (inlayer, in_Field, joinTable, joinField)

print('deleting old incplce_joined')
if arcpy.Exists("IncPlaces_Joined"):
    arcpy.Delete_management("IncPlaces_Joined")

print('copying new incplce joined')    
arcpy.CopyFeatures_management(inlayer,ResponseMapper_gdb+"/IncPlaces_Joined" )

arcpy.RemoveJoin_management (inlayer)


inlayer = "CongressionalDistricts"
in_Field = "GEOID"
joinTable = "CDResponseData"
joinField = "GEO_ID"

arcpy.AddJoin_management (inlayer, in_Field, joinTable, joinField)


if arcpy.Exists("CongressionalDistricts_Joined"):
    arcpy.Delete_management("CongressionalDistricts_Joined")

    
arcpy.CopyFeatures_management(inlayer,ResponseMapper_gdb+"/CongressionalDistricts_Joined" )

arcpy.RemoveJoin_management (inlayer)

inlayer = "AIAs"
in_Field = "AIANNHCE"
joinTable = "AIAResponseData"
joinField = "AIANNHCE"

arcpy.AddJoin_management (inlayer, in_Field, joinTable, joinField)


if arcpy.Exists("AIAs_Joined"):
    arcpy.Delete_management("AIAs_Joined")

    
arcpy.CopyFeatures_management(inlayer,ResponseMapper_gdb+"/AIAs_Joined" )

arcpy.RemoveJoin_management (inlayer)
f = open('ReportData/pulldate.p', 'wb')
pickle.dump(apidate, f )
f.close()
sys.exit(0)
print('')
print('Feature data updated successfully.')







