import subprocess
import time

##
##print ("Opening ArcMap MXD")
##p=subprocess.Popen([r"C:\Program Files (x86)\ArcGIS\Desktop10.5\bin\ArcMap.exe",r"M:\08_Geography\ResponseRateMapper\Untitled.mxd"])
##time.sleep(120)

import arcpy, os, sys
import xml.dom.minidom as DOM
arcpy.env.overwriteOutput = True


mapDoc = arcpy.mapping.MapDocument("M:/08_Geography/ResponseRateMapper/DailyResponseData_Account1.mxd") 
service = 'CensusResponseRate_Account1'
sddraft = 'M:/08_Geography/ResponseRateMapper/ServiceDefinition/{}.sddraft'.format(service)
newSDdraft = 'M:/08_Geography/ResponseRateMapper/ServiceDefinition/{}updated.sddraft'.format(service)
sd = 'M:/08_Geography/ResponseRateMapper/ServiceDefinition/{}.sd'.format(service)

print ("Creating Service Definition Draft")
# create service definition draft
arcpy.SignInToPortal_server ('NewYorkGeo2020','SJs#*3MaqDj4V!3','https://www.arcgis.com/')
print('signed in to arc online')
arcpy.mapping.CreateMapSDDraft(mapDoc, sddraft, service, 'MY_HOSTED_SERVICES')
doc = DOM.parse(sddraft)

print ("Editing definition")
#edit service definition draft for overwriting service
tagsType = doc.getElementsByTagName('Type')
for tagType in tagsType:
    if tagType.parentNode.tagName == 'SVCManifest':
        if tagType.hasChildNodes():
            tagType.firstChild.data = "esriServiceDefinitionType_Replacement"

tagsState = doc.getElementsByTagName('State')
for tagState in tagsState:
    if tagState.parentNode.tagName == 'SVCManifest':
        if tagState.hasChildNodes():
            tagState.firstChild.data = "esriSDState_Published"
            
# Change service type from map service to feature service
typeNames = doc.getElementsByTagName('TypeName')
for typeName in typeNames:
    if typeName.firstChild.data == "MapServer":
        typeName.firstChild.data = "FeatureServer"
# Turn off caching
configProps = doc.getElementsByTagName('ConfigurationProperties')[0]
propArray = configProps.firstChild
propSets = propArray.childNodes
for propSet in propSets:
    keyValues = propSet.childNodes
    for keyValue in keyValues:
        if keyValue.tagName == 'Key':
            if keyValue.firstChild.data == "isCached":
                keyValue.nextSibling.firstChild.data = "false"
# Turn on feature access capabilities
configProps = doc.getElementsByTagName('Info')[0]
propArray = configProps.firstChild
propSets = propArray.childNodes
for propSet in propSets:
    keyValues = propSet.childNodes
    for keyValue in keyValues:
        if keyValue.tagName == 'Key':
            if keyValue.firstChild.data == "WebCapabilities":
                keyValue.nextSibling.firstChild.data = "Query"

f = open(newSDdraft, 'w')
doc.writexml( f )
f.close()

# Analyze the service
analysis = arcpy.mapping.AnalyzeForSD(newSDdraft)

print ("Staging Service...")
if analysis['errors'] == {}:
    #Stage the service
    arcpy.StageService_server(newSDdraft, sd)

    # Upload the service. The OVERRIDE_DEFINITION parameter allows you to override the
    # sharing properties set in the service definition with new values. In this case,
    # the feature service will be shared to everyone on ArcGIS.com by specifying the
    # SHARE_ONLINE and PUBLIC parameters. Optionally you can share to specific groups
    # using the last parameter, in_groups.
    #arcpy.SignInToPortal_server ('NewYorkGeo2020','SJs#*3MaqDj4V!3','https://www.arcgis.com/')
    print ("Publishing Service...")
    arcpy.UploadServiceDefinition_server(sd, "My Hosted Services", service,
                                         "", "", "", "", "OVERRIDE_DEFINITION", "SHARE_ONLINE",
                                         "PUBLIC", "SHARE_ORGANIZATION", "")

    print "Uploaded and overwrote service"
else:
    # If the sddraft analysis contained errors, display them and quit.
    print analysis['errors']

#print "Closing ArcMap MXD"
#p.terminate()


