import requests as rq
from pprint import pprint
import json
from pandas.io.json import json_normalize
import pandas as pd
import datetime

#Zip code variable
zips = ["02118", 
        "02119", 
        "02120", 
        "02130", 
        "02134", 
        "02135", 
        "02445", 
        "02446",
        "02447",
        "02467",
        "02108",
        "02114",
        "02115",
        "02116",
        "02215",
        "02128",
        "02129",
        "02150",
        "02151",
        "02152",
        "02124",
        "02126",
        "02131",
        "02132",
        "02136",
        "02109",
        "02110",
        "02111",
        "02113",
        "02121",
        "02122",
        "02124",
        "02125",
        "02127",
        "02210"]     


#Load API key (will probs need to change this to an environmental variable once we get a location down)
akey = ['API KEY HERE']

#Create empty data frame to append everything to
m_data = pd.DataFrame()

#get today's date for the scrape date variable
dt = datetime.datetime.now()

#For loop that iterates through a list of zips and makes api calls
for i in zips:
    #build the url using the predetermined end point as well as out iterated zip and the api key
    ur = "http://api.openweathermap.org/data/2.5/weather?units=imperial&zip=" + i + ",us" + "&appid=" + akey
    
    #send the request for the api data
    resp = rq.get(ur)
    
    #parse the JSON data we just grabbed
    data = resp.json()    
    
    #Extracting the various tables built into the JSON file and parse to pandas df
    gen_weath = json_normalize(data['main'])
    
    #get a list of the column names of this df so that we can add the prefix to the name
    gen_nms = list(gen_weath)

    #sub loop that iterates through all the column names and adds the appropriate prefix
    for t in range(len(gen_nms)):
        gen_nms[t] = 'gen.'_+ gen_nms[t]
    
    #write the new names to the generated df
    gen_weath.columns = gen_nms
    
    ##Repeating this procesdure for the other tables we want to grab from these data
    #'system weather' table
    sys_weath = json_normalize(data['sys'])
    sys_nms = list(sys_weath)
    for r in range(len(sys_nms)):
        sys_nms[r] = 'sys_' + sys_nms[r]
    sys_weath.columns = sys_nms
    
    #'weather weather' table
    w_weath = json_normalize(data['weather'])
    w_nms = list(w_weath)
    for y in range(len(w_nms)):
        w_nms[y] = 'weath_' + w_nms[y]
    w_weath.columns = w_nms
    
    #'wind weather' table
    wind_weath = json_normalize(data['wind'])
    wind_nms = list(wind_weath)
    for z in range(len(wind_nms)):
        wind_nms[z] = 'wind_' + wind_nms[z]
    wind_weath.columns = wind_nms
    
    #Checking for optionally added rain and snow information
    pr_chk = list(data)
    if 'snow' in pr_chk:
       #If it's present, grab whatever it has
        sn_weath = json_normalize(data['snow'])
        #Check to see if it includes both of the two possible variables
        #If not, it checks and returns the missing one as an NA
        if len(list(sn_weath)) != 2:
            if '1h' in list(sn_weath):
                s_h1 = 'Y'
            else:
                s_h1 = 'N'
            if '3h' in list(sn_weath):
                s_h3 = 'Y'
            else:
                s_h3 = 'N'
            if s_h1 == 'N':
                sn_weath['1h'] = 'NA'
            if s_h3 =='N':
                sn_weath['3h'] = 'NA'
        else:
            sn_weath = json_normalize(data['snow'])
        
    else:
        #If it doesn't find either snow or rain info, it creates the df as a blank, NA filled one
        nosnow = {'1h':['NA'], '3h':['NA']}
        sn_weath = pd.DataFrame(nosnow)
    if 'rain' in pr_chk:
        #Same deal with rain information. Look for table, fill in possible blanks, or just pass a blank set of values
        rn_weath = json_normalize(data['rain'])
        if len(list(rn_weath)) != 2:
            if '1h' in list(rn_weath):
                r_h1 = 'Y'
            else:
                r_h1 = 'N'
            if '3h' in list(rn_weath):
                r_h3 = 'Y'
            else:
                r_h3 = 'N'
            if r_h1 == 'N':
                rn_weath['1h'] = 'NA'
            if r_h3 =='N':
                rn_weath['3h'] = 'NA'
        else:
            rn_weath = json_normalize(data['rain'])
        
    else:
        
        norain = {'1h':['NA'], '3h':['NA']}
        rn_weath = pd.DataFrame(norain)
       

    sn_nms = list(sn_weath)
    for a in range(len(sn_nms)):
        sn_nms[a] = 'snow_' + sn_nms[a]
    sn_weath.columns = sn_nms
    
    rn_nms = list(rn_weath)
    for j in range(len(rn_nms)):
        rn_nms[j] = 'rain_' + rn_nms[j]
    rn_weath.columns = rn_nms
    
    
    #glue these single row dfs together to form a single row
    g_data = pd.concat([gen_weath, sys_weath, sn_weath, rn_weath, w_weath, wind_weath],axis = 1, sort = False)
    
    #add the zip code
    g_data['zip'] = i
    
    #add the date scraped
    g_data['scrape_date'] = dt.strftime("%Y-%m-%d %H:%M")
    
    #append this to the original dummy data frame
    m_data = m_data.append(g_data, sort=False)
    print("Finished with zip " + i + " --- " + str((zips.index(i)+1)) + " of " + str(len(zips)))

#Saving the resulting file as a csv.
#later we will want to add this to civis/pass it directly to a weather table in civis

m_data.to_csv('PATH TO DIR', index = False)