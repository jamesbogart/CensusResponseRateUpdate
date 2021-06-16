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
import sys

def reportgenerate(tracts_finaldf,counties_finaldf,MCD_finaldf,place_finaldf,cd_finaldf,states_finaldf,aia_df):
    date = cd_finaldf.at[1,'RESP_DATE']
    date = date.replace('-','_')
    
    #create dataframe from csvs which store names and GEOID. Object is the type pandas uses for strings.
    aianames = pd.read_csv('ReportData//AIAnames.csv',dtype = {"AIANNHCE" : "object"},encoding = 'UTF-8')
    placedata = pd.read_csv('ReportData//IncPlcData.csv',dtype = "object",encoding = 'UTF-8')
    mcddata = pd.read_csv('ReportData//MCDData_new.csv',dtype = "object", encoding = 'UTF-8')
    countydata = pd.read_csv('ReportData//CountyData.csv', dtype= "object",encoding = 'UTF-8')
    tractdata = pd.read_csv('ReportData//TractDataCopy2.csv', dtype= 'object', encoding='UTF-8')
    aiadata = pd.read_csv("ReportData//AIA_Data.csv", dtype={"AIANNHCE":"object"},encoding = 'UTF-8')
    cddata = pd.read_csv("ReportData//CDTEA.csv", dtype=object, encoding = 'UTF-8')
    statedata = pd.read_csv('ReportData//StateData.csv',dtype=object, encoding='UTF-8')

    #joining API result dataframes to csvs that contain names, For AIAs, we are joining TO the name file (which contains only the AIAs in the region) since the API gets data for the entire country
    MCD_finaldf = MCD_finaldf.merge(mcddata, how='left', left_on='GEO_ID', right_on='MCDGEOID')
    place_finaldf = place_finaldf.merge(placedata, how='left', left_on='GEO_ID', right_on='GEOID')
    aia_finaldf = aianames.merge(aia_df, how='left', left_on='AIANNHCE', right_on='american indian area/alaska native area/hawaiian home land')
    counties_finaldf = counties_finaldf.merge(countydata, how='left', left_on='GEO_ID', right_on= 'GEOID')
    tracts_finaldf = tracts_finaldf.merge(tractdata, how='left', left_on='GEO_ID', right_on='GEOID')
    cd_finaldf = cd_finaldf.merge(cddata, how='left',left_on='GEO_ID',right_on='GEOID')
    states_finaldf = states_finaldf.merge(statedata, how='left', left_on='state',right_on='STATEFP')
    #join county nsmes to tract
    #first need to add full county fips code to tract dataframe


    reportdate = datetime.strptime(date,'%Y_%m_%d')
    lastprojday = datetime.strptime('2020-10-07','%Y-%m-%d')
    if lastprojday < reportdate:
        datematch = '_10_7_2020'
    else:
        datematch = '_'+date[5:] + '_' + date[:4]
    ##2020-05-03 becomes '_5_03_2020'
    if datematch[4] == '0':
        datematch = datematch[:4] + datematch[5:]
        
    countyproj = pd.read_csv(r'ReportData\ResponseProjections\CountyProjection.csv',dtype={'GEOID':object})

    countyproj.GEOID = countyproj.GEOID.str.pad(width=5, side='left', fillchar='0')
    countyproj['DailyProjectedResponse'] = countyproj.loc[:,'_3_12_2020':datematch].sum(axis=1)
    countyproj = countyproj[['GEOID','DailyProjectedResponse']]
    countyproj = countyproj.round({'DailyProjectedResponse':2})
    counties_finaldf = counties_finaldf.merge(countyproj,how='left',left_on='GEO_ID',right_on='GEOID')

    tractproj = pd.read_csv(r'ReportData\ResponseProjections\TractProjections.csv',dtype={'GEOID':object})

    tractproj['DailyProjectedResponse'] = tractproj.loc[:,'_3_12_2020':datematch].sum(axis=1)
    tractproj = tractproj[['GEOID','DailyProjectedResponse']]
    tractproj = tractproj.round({'DailyProjectedResponse':2})
    tracts_finaldf = tracts_finaldf.merge(tractproj,how='left',left_on='v73_GEOID',right_on='GEOID')

    stateproj = pd.read_csv(r'ReportData\ResponseProjections\Stateprojections.csv',dtype={'BCUSTATEFP':object})

    stateproj['DailyProjectedResponse'] = stateproj.loc[:,'_3_12_2020':datematch].sum(axis=1)
    stateproj = stateproj[['BCUSTATEFP','DailyProjectedResponse']]
    stateproj = stateproj.round({'DailyProjectedResponse':2})
    states_finaldf = states_finaldf.merge(stateproj,how='left',left_on='state',right_on='BCUSTATEFP')

    ###select only certain columns and reorder in more logical order
    MCD_finaldf = MCD_finaldf[['StateName', 'CongressionalDistrict','CountyCode','CountyName', 'CountySub Code', 'MCDGEOID', 'NAME',  'RESP_DATE', 'DRRALL','CRRALL', 'SelfResponseHU', 'UpdateEnumerateHU', 'UpdateLeaveHU', 'Per_TotalHU_SR', 'Per_TotalHU_UE', 'Per_TotalHU_UL','ENRFU_HU','Per_TotalHU_ENRFU','TotalHU']]
    place_finaldf = place_finaldf[['state', 'County', 'County Name', 'Congressional District', 'PlaceFP', 'GEOID', 'Place Name','RESP_DATE','DRRALL','CRRALL', 'SelfResponseHU','UpdateLeaveHU', 'Per_TotalHU_SR','Per_TotalHU_UL','ENRFU_HU','Per_TotalHU_ENRFU','TotalHU']]
    aia_finaldf = aia_finaldf[['NAME','AIANNHCE','RESP_DATE','DRRALL','CRRALL']]
    counties_finaldf = counties_finaldf[['StateName', 'CountyCode', 'NAME', 'Congressional District', 'GEO_ID','RESP_DATE', 'DRRALL', 'CRRALL','DailyProjectedResponse','SelfResponseHU', 'UpdateEnumerateHU', 'UpdateLeaveHU', 'Per_TotalHU_SR', 'Per_TotalHU_UE', 'Per_TotalHU_UL','ENRFU_HU','Per_TotalHU_ENRFU','TotalHU']]
    cd_finaldf = cd_finaldf[['State', 'Congressional District','RESP_DATE','DRRALL',  'CRRALL', 'SelfResponseHU', 'UpdateLeaveHU', 'Per_TotalHU_UL', 'Per_TotalHU_SR','TotalHU']]
    tracts_finaldf = tracts_finaldf[['state','CountyCode', 'CountyName', 'MCDGEOID', 'MCD Name','Place Name', 'IncPlcGEOID', 'Congressional District','ACOCE','CFM','CFS','NYCNeighborhood','BostonNeighborhood', 'Tract', 'GEO_ID', 'V73','RESP_DATE','DRRALL', 'CRRALL', 'DailyProjectedResponse', 'SelfResponseHU', 'UpdateLeaveHU','UpdateEnumerateHU', 'Per_TotalHU_SR', 'Per_TotalHU_UL', 'Per_TotalHU_UE','ENRFU_HU','Per_TotalHU_ENRFU', 'TotalHU']]
                              
    #join to TEA data
    aia_finaldf = aia_finaldf.merge(aiadata, how='left', left_on='AIANNHCE', right_on='AIANNHCE')

    states_finaldf = states_finaldf[['state','RESP_DATE','DRRALL','CRRALL','DailyProjectedResponse','TotalHU', 'UpdateLeaveHU', 'SelfResponseHU', 'Per_TotalHU_UL', 'Per_TotalHU_SR' ]]


        
    ###replace state codes with states
    cd_finaldf["State"].replace({"09": "Connecticut", "25": "Massachusetts","36":"New York","44":"Rhode Island","23":"Maine","33":"New Hampshire","50":"Vermont","34":"New Jersey","72":"Puerto Rico"}, inplace=True)
    states_finaldf['state'].replace({"09": "Connecticut", "25": "Massachusetts","36":"New York","44":"Rhode Island","23":"Maine","33":"New Hampshire","50":"Vermont","34":"New Jersey","72":"Puerto Rico"}, inplace=True)
    tracts_finaldf['state'].replace({"09": "Connecticut", "25": "Massachusetts","36":"New York","44":"Rhode Island","23":"Maine","33":"New Hampshire","50":"Vermont","34":"New Jersey","72":"Puerto Rico"}, inplace=True)
    place_finaldf['state'].replace({"09": "Connecticut", "25": "Massachusetts","36":"New York","44":"Rhode Island","23":"Maine","33":"New Hampshire","50":"Vermont","34":"New Jersey","72":"Puerto Rico"}, inplace=True)


    ###rename columns in dataframe
    MCD_finaldf.rename(columns={'StateName':"State Name",
     'CountyCode':"County Code",
     'CountyName':"County Name",
     'CountySub Code':"MCD Code",
     'MCDGEOID':"MCD GEOID",
     'NAME':"MCD Name",
     'CongressionalDistrict':"Congressional District",
     'RESP_DATE':"DATE",
     'DRRALL':'Daily Response Rate (%)',
     'CRRALL':'Cumulative Response Rate (%)'}, inplace=True)
    place_finaldf.rename(columns={'State':"State Name",
     'County':"County Code",
     'County Name':"County Name",
     'PlaceFP':"Place Code",
     'GEOID':"Place GEOID",
     'RESP_DATE':"DATE",
     'DRRALL':'Daily Response Rate (%)',
     'CRRALL':'Cumulative Response Rate (%)'}, inplace=True)

    aia_finaldf.rename(columns={'NAME_x':'NAME',
                                'DRRALL':'Daily Response Rate (%)',
                                "RESP_DATE":"DATE",
                                'CRRALL':'Cumulative Response Rate (%)'},inplace=True)

    counties_finaldf.rename(columns={'StateName':"State Name",
     'NAME':"County Name",
     'GEO_ID':'County GEOID',
     'RESP_DATE':'DATE',
     'DRRALL':'Daily Response Rate (%)',
     'CRRALL':'Cumulative Response Rate (%)'},inplace=True)

    cd_finaldf.rename(columns={'State':"State Name",
     'RESP_DATE':'DATE',
     'DRRALL':'Daily Response Rate (%)',
     'CRRALL':'Cumulative Response Rate (%)'},inplace=True)

    tracts_finaldf.rename(columns={'StateCode':"State Name",
     'CountyCode':'County Code',
     'CountyName':"County Name",
      'Place Name':'Inc. Place',
     'Tract':'Tract Code',
     'V73':'2010_GEOID(s)',
     'GEO_ID':'Tract GeoID',
     'RESP_DATE':'DATE',
     'DRRALL':'Daily Response Rate (%)',
     'CRRALL':'Cumulative Response Rate (%)'},inplace=True)

    states_finaldf.rename(columns={'DRRALL':'Daily Response Rate (%)',
                                   'CRRALL':'Cumulative Response Rate (%)',
                                   'state':'State',
                                   'RESP_DATE':'DATE'},inplace=True)

    aia_finaldf.DATE.fillna('Tribe Response Data Not Reported', inplace=True)
    print('writing excel doc to share folder')
    newexcelfilename= 'ResponseData_'+date+'.xlsx'
    exceloutput = "output"+newexcelfilename
    print("Writing to Excel file...")
    # Given a dict of dataframes, for example:
    dfs = {'Tracts': tracts_finaldf, 'Place':place_finaldf,'MCDs':MCD_finaldf,'Counties': counties_finaldf, 'Congressional Districts':cd_finaldf,'AIAs':aia_finaldf,'States':states_finaldf}

    writer = pd.ExcelWriter(exceloutput, engine='xlsxwriter')
    workbook  = writer.book
    header_format = workbook.add_format({
        'bold': True,
        'text_wrap': False,
        'align': 'left',
        })
    for sheetname, df in dfs.items():
         for idx, col in enumerate(df):
            if col in ['Daily Internet Response Rate (%)',
                       'Daily Response Rate (%)',
                       'Cumulative Internet Response Rate (%)',
                       'Cumulative Response Rate (%)',
                       'Per_TotalHU_SR','Per_TotalHU_UL','Per_TotalHU_UE',
                       'Per_TotalHU_ENRFU']:
                        df[col] = df[col].astype(float)
            if col in ['TotalHU','SelfResponseHU','UpdateLeaveHU','UpdateEnumerateHU','ENRFU_HU']:
                df[col] = pd.to_numeric(df[col])
    sheets = ['Tracts','MCDs','Place','Counties', 'Congressional Districts','AIAs','States']

    for i in sheets:# loop through `dict` of dataframes
        sheetname = i
        df = dfs[i]
        df.to_excel(writer, sheet_name=sheetname, startrow=2,header=False,index=False)  # send df to writer
        worksheet = writer.sheets[sheetname]  # pull worksheet object
        end_row = len(df.index) + 1 
        end_column = len(df.columns) -1
        cell_range = xlsxwriter.utility.xl_range(1, 0, end_row, end_column)
        df.reset_index(inplace=True)
        header = [{'header': di} for di in df.columns.tolist()]
        header = header[1:]

    ##    for col_num, value in enumerate(df.columns.values):
    ##        worksheet.write(1, col_num, value, header_format)
        for idx, col in enumerate(df):
            idx = idx-1 # loop through all columns
            series = df[col]
            col_len = len(series.name)  # len of column name/header
            worksheet.set_column(idx,idx,col_len + 2)

            if col in ['ACOCE','CFM','CFS']:
                worksheet.set_column(idx,idx,7)            
            if col == 'DATE':
                worksheet.set_column(idx,idx,10)
            if col in ['UL_HU','SR_HU','Per_UL','Per_SR']:
                worksheet.set_column(idx,idx,7)            
            if col == 'State Name' or col=='state' or col=='State':
                worksheet.set_column(idx,idx,14)
            if col == 'County Name':
                worksheet.set_column(idx,idx,12)
            if col == 'MCD Name':
                worksheet.set_column(idx,idx,15)
            if col == 'MCDGEOID':
                worksheet.set_column(idx,idx,11)
            if col == 'Inc. Place':
                worksheet.set_column(idx,idx,15)
            if col == 'County Name':
                worksheet.set_column(idx,idx,12)
            if sheetname == 'Tracts':
                if col == 'Tract GeoID':
                    worksheet.set_column(idx,idx,12)
                if col == 'GEO_ID':
                    worksheet.set_column(idx,idx,13)
            if sheetname == 'MCDs':
                if col == 'MCD Code':
                    worksheet.set_column(idx,idx,11)
            if sheetname == 'AIAs':
                if col == 'NAME':
                    worksheet.set_column(idx,idx,25)
            if sheetname == 'Place':
                if col == 'GEOID':
                    worksheet.set_column(idx,idx,8)
            if worksheet == 'Counties':
                if col == 'GEOID':
                    worksheet.set_column(idx,idx,6)
        caption = False              
        if sheetname == 'Tracts':
            caption = "* Tracts may not nest within only 1 CFM,CFS,Place and Congressional District. In this case, the value shown is the geography that has the most overlap."
            worksheet.conditional_format('S2:S11983', {'type':'3_color_scale',
                                        'min_type': 'num',
                                        'min_value': 0,
                                        'mid_type':'num',
                                        'mid_value':50,
                                        'max_type':'num',
                                        'max_value': 100})
        if sheetname == 'Congressional Districts':
            worksheet.conditional_format('E2:E62', {'type':'3_color_scale',
                                        'min_type': 'num',
                                        'min_value': 0,
                                        'mid_type':'num',
                                        'mid_value':50,
                                        'max_type':'num',
                                        'max_value': 100})
        if sheetname == 'MCDs':
            caption = 'MCDs may not nest within only 1 Congressional District. In this case, the value shown is the Congressional District with which it has the most overlap'
            worksheet.conditional_format('J2:J2570', {'type':'3_color_scale',
                                        'min_type': 'num',
                                        'min_value': 0,
                                        'mid_type':'num',
                                        'mid_value':50,
                                        'max_type':'num',
                                        'max_value': 100})
        if sheetname == 'AIAs':
            worksheet.conditional_format('E2:E34', {'type':'3_color_scale',
                                        'min_type': 'num',
                                        'min_value': 0,
                                        'mid_type':'num',
                                        'mid_value':50,
                                        'max_type':'num',
                                        'max_value': 100})
        if sheetname == 'Place':
            caption = '*Incorporated Places may not nest within only 1 County or Congressional District. In this case, the value shown is the geography that has the most overlap'
            worksheet.conditional_format('J2:J1094', {'type':'3_color_scale',
                                        'min_type': 'num',
                                        'min_value': 0,
                                        'mid_type':'num',
                                        'mid_value':50,
                                        'max_type':'num',
                                        'max_value': 100})
        if sheetname == 'Counties':
            caption = '*Counties may not nest within only 1 Congressional District. In this case, the value shown is the Congressional District with which it has the most overlap'
            worksheet.conditional_format('H2:H230', {'type':'3_color_scale',
                                        'min_type': 'num',
                                        'min_value': 0,
                                        'mid_type':'num',
                                        'mid_value':50,
                                        'max_type':'num',
                                        'max_value': 100})
        if sheetname == 'States':
            worksheet.conditional_format('D2:D11', {'type':'3_color_scale',
                                        'min_type': 'num',
                                        'min_value': 0,
                                        'mid_type':'num',
                                        'mid_value':50,
                                        'max_type':'num',
                                        'max_value': 100})
        worksheet.add_table(cell_range,{'header_row': True,'columns':header,'style': 'Table Style Medium 4'})
        worksheet.freeze_panes(2, 0)
        if caption:
            worksheet.write('A1',caption)
    writer.save()
    print('wrote main excel doc. writing public-facing doc...')

    tracts_finaldf = tracts_finaldf[['state','County Name','MCD Name','Inc. Place','Congressional District','NYCNeighborhood','BostonNeighborhood','Tract Code','Tract GeoID','DATE','Daily Response Rate (%)','Cumulative Response Rate (%)','Per_TotalHU_SR','Per_TotalHU_UL','Per_TotalHU_UE']]
    MCD_finaldf = MCD_finaldf[['State Name','Congressional District','County Name','MCD GEOID','MCD Name','DATE','Daily Response Rate (%)', 'Cumulative Response Rate (%)','Per_TotalHU_SR','Per_TotalHU_UL','Per_TotalHU_UE']]
    place_finaldf = place_finaldf[['state','County Name','Congressional District','Place GEOID','Place Name','DATE','Daily Response Rate (%)','Cumulative Response Rate (%)','Per_TotalHU_SR','Per_TotalHU_UL']]
    counties_finaldf = counties_finaldf[['State Name','County Name','Congressional District','County GEOID','DATE','Daily Response Rate (%)','Cumulative Response Rate (%)','Per_TotalHU_SR','Per_TotalHU_UL','Per_TotalHU_UE']]
    cd_finaldf = cd_finaldf[['State Name','Congressional District','DATE','Daily Response Rate (%)','Cumulative Response Rate (%)','Per_TotalHU_SR','Per_TotalHU_UL']]
    aia_finaldf = aia_finaldf[['NAME','AIANNHCE','DATE','Daily Response Rate (%)', 'Cumulative Response Rate (%)','Per_TotalHU_SR','Per_TotalHU_UL']]
    states_finaldf = states_finaldf[['State','DATE','Daily Response Rate (%)','Cumulative Response Rate (%)','Per_TotalHU_SR','Per_TotalHU_UL']]

    newexcelfilename= 'PublicResponseData_'+date+'.xlsx'
    exceloutput = "output"+newexcelfilename
    dfs = {'Tracts': tracts_finaldf, 'Place':place_finaldf,'MCDs':MCD_finaldf,'Counties': counties_finaldf, 'Congressional Districts':cd_finaldf,'AIAs':aia_finaldf,'States':states_finaldf}

    writer = pd.ExcelWriter(exceloutput, engine='xlsxwriter')
    workbook  = writer.book
    header_format = workbook.add_format({
        'bold': True,
        'text_wrap': False,
        'align': 'left',
        })
    for sheetname, df in dfs.items():
         for idx, col in enumerate(df):
            if col in ['Daily Internet Response Rate (%)',
                       'Daily Response Rate (%)',
                       'Cumulative Internet Response Rate (%)',
                       'Cumulative Response Rate (%)']:
                        df[col] = df[col].astype(float)
    sheets = ['Tracts','MCDs','Place','Counties', 'Congressional Districts','AIAs','States']

    for i in sheets:# loop through `dict` of dataframes
        sheetname = i
        df = dfs[i]
        df.to_excel(writer, sheet_name=sheetname, startrow=2,header=False,index=False)  # send df to writer
        worksheet = writer.sheets[sheetname]  # pull worksheet object
        end_row = len(df.index) + 1 
        end_column = len(df.columns) -1
        cell_range = xlsxwriter.utility.xl_range(1, 0, end_row, end_column)
        df.reset_index(inplace=True)
        header = [{'header': di} for di in df.columns.tolist()]
        header = header[1:]

    ##    for col_num, value in enumerate(df.columns.values):
    ##        worksheet.write(1, col_num, value, header_format)
        for idx, col in enumerate(df):
            idx = idx-1 # loop through all columns
            series = df[col]
            col_len = len(series.name)  # len of column name/header
            worksheet.set_column(idx,idx,col_len + 2)

            if col in ['ACOCE','CFM','CFS']:
                worksheet.set_column(idx,idx,7)            
            if col == 'DATE':
                worksheet.set_column(idx,idx,10)
            if col in ['UL_HU','SR_HU','Per_UL','Per_SR']:
                worksheet.set_column(idx,idx,7)            
            if col == 'State Name' or col=='state' or col=='State':
                worksheet.set_column(idx,idx,14)
            if col == 'County Name':
                worksheet.set_column(idx,idx,12)
            if col == 'MCD Name':
                worksheet.set_column(idx,idx,15)
            if col == 'MCDGEOID':
                worksheet.set_column(idx,idx,11)
            if col == 'Inc. Place':
                worksheet.set_column(idx,idx,15)
            if col == 'County Name':
                worksheet.set_column(idx,idx,12)
            if sheetname == 'Tracts':
                if col == 'Tract GeoID':
                    worksheet.set_column(idx,idx,12)
                if col == 'GEO_ID':
                    worksheet.set_column(idx,idx,13)
            if sheetname == 'MCDs':
                if col == 'MCD Code':
                    worksheet.set_column(idx,idx,11)
            if sheetname == 'AIAs':
                if col == 'NAME':
                    worksheet.set_column(idx,idx,25)
            if sheetname == 'Place':
                if col == 'GEOID':
                    worksheet.set_column(idx,idx,8)
            if worksheet == 'Counties':
                if col == 'GEOID':
                    worksheet.set_column(idx,idx,6)
        caption = False              
        if sheetname == 'Tracts':
            caption = "* Tracts may not nest within only 1 Place or Congressional District. In this case, the value shown is the geography that has the most overlap."
            worksheet.conditional_format('L2:L11983', {'type':'3_color_scale',
                                        'min_type': 'num',
                                        'min_value': 0,
                                        'mid_type':'num',
                                        'mid_value':50,
                                        'max_type':'num',
                                        'max_value': 100})
        if sheetname == 'Congressional Districts':
            worksheet.conditional_format('E2:E62', {'type':'3_color_scale',
                                        'min_type': 'num',
                                        'min_value': 0,
                                        'mid_type':'num',
                                        'mid_value':50,
                                        'max_type':'num',
                                        'max_value': 100})
        if sheetname == 'MCDs':
            caption = 'MCDs may not nest within only 1 Congressional District. In this case, the value shown is the Congressional District with which it has the most overlap'
            worksheet.conditional_format('H2:H2570', {'type':'3_color_scale',
                                        'min_type': 'num',
                                        'min_value': 0,
                                        'mid_type':'num',
                                        'mid_value':50,
                                        'max_type':'num',
                                        'max_value': 100})
        if sheetname == 'AIAs':
            worksheet.conditional_format('E2:E34', {'type':'3_color_scale',
                                        'min_type': 'num',
                                        'min_value': 0,
                                        'mid_type':'num',
                                        'mid_value':50,
                                        'max_type':'num',
                                        'max_value': 100})
        if sheetname == 'Place':
            caption = '*Incorporated Places may not nest within only 1 County or Congressional District. In this case, the value shown is the geography that has the most overlap'
            worksheet.conditional_format('H2:H1094', {'type':'3_color_scale',
                                        'min_type': 'num',
                                        'min_value': 0,
                                        'mid_type':'num',
                                        'mid_value':50,
                                        'max_type':'num',
                                        'max_value': 100})
        if sheetname == 'Counties':
            caption = '*Counties may not nest within only 1 Congressional District. In this case, the value shown is the Congressional District with which it has the most overlap'
            worksheet.conditional_format('G2:G230', {'type':'3_color_scale',
                                        'min_type': 'num',
                                        'min_value': 0,
                                        'mid_type':'num',
                                        'mid_value':50,
                                        'max_type':'num',
                                        'max_value': 100})
        if sheetname == 'States':
            worksheet.conditional_format('D2:D11', {'type':'3_color_scale',
                                        'min_type': 'num',
                                        'min_value': 0,
                                        'mid_type':'num',
                                        'mid_value':50,
                                        'max_type':'num',
                                        'max_value': 100})
        worksheet.add_table(cell_range,{'header_row': True,'columns':header,'style': 'Table Style Medium 4'})
        worksheet.freeze_panes(2, 0)
        if caption:
            worksheet.write('A1',caption)
    writer.save()


    print('Excel Outputs Created.')
    print('***********************************')
    print('')
