
 
This script automates daily updating of internal reports and ArcGIS Portal webmaps used to track operational success of self-response operation. The decennial self-response rate data (the percentage of households who have responded to the Census survey) was provided publicly on the Census Bureau’s API and made available by various levels of geography. The New York Regional Census Center needed an efficient way to capture this data daily and make it useable to the various departments in the office that work on the self-response operation. 

 

This script pulls data from the API for various levels of geography and stores them in memory within a Pandas dataframe. Internal operational data is then joined to each of the API response rate tables, and because this data is used by various departments with unique specific duties, many different data inputs were provided to on an ad-hoc basis and were incorporated in the script  on-the-fly. Internal data also includes predicted daily projected response rates from a massive csv file which must be handled by streaming in pandas. Once this data is and formatted, it is then handled by the reportGenerate function to generate multiple reports, each with varying levels of data relevant to the intended users. The reports are then output to specific locations on the network, and previous versions are archived. Once a week a script is ran which generates a weekly report using the archived reports to showcase growth in response rate around the region over the week period. 

 

After the reports have been created, the script updates featureclasses in a geodatabase which are sources in an MXD on the network. The script uses this MXD to update a service definition file, and subsequently, updates are pushed to an ArcGIS Portal map and dashboard which Is shared on the network.  

