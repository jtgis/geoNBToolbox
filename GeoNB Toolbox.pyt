################################################################################
#
# GeoNB LiDAR Downloader and Processor
# Version: 1.0
#
# Download and process LiDAR from GeoNB using ArcMap
# 
# Developed and tested with Python 2.7 and ArcMap 10.8
#
# Author: https://github.com/jtgis
#
# Date: 21 mar 2021
#
################################################################################

#import the required libraries
import arcpy
from arcpy.sa import *
import os
import urllib2
import json
import requests
import shutil
import tempfile

#variables for stuff that is needed to make this tool work. if anything changes
#changes location it can be updated here. LiDAR datasets can be added to the list
#in order of increasing precedence.
laszipURL = "https://www.cs.unc.edu/~isenburg/lastools/download/laszip.exe"

pidMapService = "http://geonb.snb.ca/arcgis/rest/services/GeoNB_SNB_Parcels/MapServer/0/query"
pidID = "PID"

lidarMapServiceList = [["lidar_index_2015","http://geonb.snb.ca/arcgis/rest/services/GeoNB_SNB_LidarIndex/MapServer/4/query"],
                       ["lidar_index_2016","http://geonb.snb.ca/arcgis/rest/services/GeoNB_SNB_LidarIndex/MapServer/3/query"],
                       ["lidar_index_2017","http://geonb.snb.ca/arcgis/rest/services/GeoNB_SNB_LidarIndex/MapServer/2/query"],
                       ["lidar_index_2018","http://geonb.snb.ca/arcgis/rest/services/GeoNB_SNB_LidarIndex/MapServer/1/query"]]
lidarDownloadID = "[FILE_URL]"

#here we have all of the functions made to make this tool go
def createDirectory(dir,name):
    """
    simple folder creating func
    give it a folder for your new folder and a name
    checks to see if the requested dir exists before proceeding
    """
    createdDirectory = r"{}\{}".format(dir,name)
    exists = os.path.exists(createdDirectory)
    if exists == False:
        os.mkdir(createdDirectory)
        arcpy.AddMessage("Created directory {}.".format(createdDirectory))
        return createdDirectory
    else:
        arcpy.AddMessage("Did not create directory {}, it already exists.".format(createdDirectory))
        return createdDirectory

def unique_values(table,field):
    """
    https://gis.stackexchange.com/questions/208430/trying-to-extract-a-list-of-unique-values-from-a-field-using-python/208431
    This one spits out a list of values from the specified attribute of a
    feature class. returns a sorted list of those values.
    """
    with arcpy.da.SearchCursor(table, [field]) as cursor:
        return sorted({row[0] for row in cursor})

def downloadFile(url,downloadToFolder):
    """
    #https://stackoverflow.com/questions/22676/how-to-download-a-file-over-http
    mostly taken from stack with minor changes. Downloads a file to a dir with
    download progress, throws an exception if the url is wonky
    uses urllib2, should move to requests or somethings
    """
    try:
        file_name = url.split('/')[-1]
        downloadedFile = r"{}\{}".format(downloadToFolder,file_name)
        exists = os.path.exists(downloadedFile)
        if exists == False:
            u = urllib2.urlopen(url)
            f = open(downloadedFile,'wb')
            meta = u.info()
            file_size = int(meta.getheaders("Content-Length")[0])
            file_sizeMB = file_size/1e+6
            arcpy.AddMessage("Downloading {} {} MB".format(file_name, file_sizeMB))
            file_size_dl = 0
            block_sz = file_size/20
            while True:
                buffer = u.read(block_sz)
                if not buffer:
                    break
                file_size_dl += len(buffer)
                f.write(buffer)
                status = r"[%3.2f%%]" % (file_size_dl * 100. / file_size)
                arcpy.AddMessage(status,)
            f.close()
            arcpy.AddMessage("Downloaded {}".format(file_name))
        else:
            arcpy.AddMessage("{} is already downloaded.".format(file_name))
        return downloadedFile

    except:
        arcpy.AddMessage("Error downloading {}, check the link in a browser and try again".format(url))

def downloadRestFeatures(url,queryLayer,query,outName):
    """
    #https://gis.stackexchange.com/questions/324513/converting-rest-service-to-file-geodatabase-feature-class
    can export a map service to fc optionally add a query or selection layer
    to limit ouput or leave those as "" to get the whole thing
    returns the new fc
    """
    #set the env vars, we want to overwrite any existing stuff and use a fresh
    #in_memory workspace to store working data
    arcpy.env.overwriteOutput = True
    arcpy.env.workspace = "in_memory"
    arcpy.Delete_management("in_memory")
    #the parameters for the map service query
    if not query:
        query = '1=1'
    params = {'where': query, 'outFields': '*', 'f': 'pjson', 'returnGeometry': True}
    if queryLayer:
        spatial_ref = arcpy.Describe(queryLayer).spatialReference
        dissolved = arcpy.Dissolve_management(queryLayer,"dissolved")
        arcpy.AddGeometryAttributes_management(dissolved,"EXTENT")
        with arcpy.da.SearchCursor(dissolved, ["OID@", "EXT_MIN_X", "EXT_MIN_Y","EXT_MAX_X", "EXT_MAX_Y"]) as sCur:
            for row in sCur:
                minX, minY, maxX, maxY = row[1], row[2], row[3], row[4]
        extent = (str(minX) +","+ str(minY) +","+ str(maxX) +","+ str(maxY))
        params = {'where': query, 'geometry': extent, 'geometryType': 'esriGeometryEnvelope ', 'inSR': spatial_ref, 'spatialRel': 'esriSpatialRelIntersects', 'outFields': '*', 'f': 'pjson', 'returnGeometry': True}
    #making the request
    r = requests.get(url, params)
    #read the data from the request to a json and write it to a file in a temp
    #directory
    data = r.json()
    dirpath = tempfile.mkdtemp()
    json_path = r"{}\mapService.json".format(dirpath)
    with open(json_path, 'w') as f:
        json.dump(data, f)
    f.close()
    r.close()
    #turn that json into a feature class!
    arcpy.JSONToFeatures_conversion(json_path,outName)
    shutil.rmtree(dirpath)
    
    return outName

def mostRecentLidar(inputFeatureClass):
    """
    takes the input fc and uses it to download the lidar index and then makes
    the index the newest data
    """
    #set the env vars, we want to overwrite any existing stuff and use a fresh
    #in_memory workspace to store working data
    arcpy.env.overwriteOutput = True
    arcpy.env.workspace = "in_memory"
    arcpy.Delete_management("in_memory")
    arcpy.AddMessage("Calculating recent LiDAR tiles.")
    for mapService in lidarMapServiceList:
        downloadRestFeatures(mapService[1],inputFeatureClass,"",mapService[0])
    lidarDownloadAtttribute = "DOWNLOAD"
    outlidarFC = "LIDAR"
    tempList = []
    mergeList = []
    for layerName in lidarMapServiceList:
        tempList.append(layerName[0])
    for fc in lidarMapServiceList:
        unionedLidar = "unionedLidar"
        currFC = fc[0]
        arcpy.analysis.Union(tempList,unionedLidar)
        arcpy.management.AddField(unionedLidar,lidarDownloadAtttribute,"TEXT",field_length=150)
        tempList.remove(fc[0])
        query = """FID_{} >= 0""".format(fc[0])
        for i in tempList:
            query += """ AND FID_{} < 0""".format(i)
        arcpy.analysis.Select(unionedLidar,currFC,query)
        arcpy.CalculateField_management(currFC,lidarDownloadAtttribute,lidarDownloadID,"VB")
        mergeList.append(currFC)
    arcpy.Merge_management(mergeList,outlidarFC)
    arcpy.AddMessage("Done calculating recent LiDAR tiles.")

    return outlidarFC

def unzipAndDelLAZ(lazList):
    """
    give it a list of laz files with full path and it turns them into las
    """
    dirpath = tempfile.mkdtemp()
    laszipdownloaded = downloadFile(laszipURL,dirpath)
    for i in lazList:
        unziplaz = "{} {}".format(laszipdownloaded,i)
        arcpy.AddMessage(unziplaz)
        os.system(unziplaz)
        os.remove(i)
    shutil.rmtree(dirpath)

class Toolbox(object):
    def __init__(self):
        self.label = "GeoNB Toolbox"
        self.tools = [downloadLiDAR]

class downloadLiDAR(object):
    def __init__(self):
        self.label = "Download LiDAR Data"
        self.description = ""
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""
        inputType = arcpy.Parameter(
                                    displayName="Input Type",
                                    name="inputType",
                                    datatype="GPString",
                                    parameterType="Required",
                                    direction="Input"
                                   )

        inputType.filter.type = "ValueList"
        inputType.filter.list = ["PID","Feature Class"]
        inputType.value = "PID"

        inputPID = arcpy.Parameter(
                                    displayName="Input PID",
                                    name="inputPID",
                                    datatype="GPValueTable",
                                    parameterType="Optional",
                                    direction="Input",
                                    enabled = False
                                   )

        inputPID.columns = [['GPString', 'PID']]

        inputFeatureClass = arcpy.Parameter(
                                    displayName="Input Feature Class",
                                    name="inputFeatureClass",
                                    datatype="DEFeatureClass",
                                    parameterType="Optional",
                                    direction="Input",
                                    enabled = False
                                   )

        projectName = arcpy.Parameter(
                                    displayName="Project Name",
                                    name="projectName",
                                    datatype="GPString",
                                    parameterType="Optional",
                                    direction="Input"
                                   )

        projectDir = arcpy.Parameter(
                                      displayName="Project Folder",
                                      name="projectDir",
                                      datatype="DEFolder",
                                      parameterType="Optional",
                                      direction="Input"
                                      )

        createLidarProducts = arcpy.Parameter(
                                      displayName="Create LiDAR Products",
                                      name="createLidarProducts",
                                      datatype="GPBoolean",
                                      parameterType="Optional",
                                      direction="Input"
                                      )

        lidarDir = arcpy.Parameter(
                                      displayName="LiDAR Folder",
                                      name="lidarDir",
                                      datatype="DEFolder",
                                      parameterType="Required",
                                      direction="Input"
                                      )

        parameters = [inputType,inputPID,inputFeatureClass,projectName,projectDir,createLidarProducts,lidarDir]

        return parameters

    def updateParameters(self, parameters):

        if parameters[0].value:
            if str(parameters[0].value) == "Feature Class":
                parameters[2].enabled = True
            else:
                parameters[2].enabled = False

            if str(parameters[0].value) == "PID":
                parameters[1].enabled = True
            else:
                parameters[1].enabled = False

        return

    def execute(self, parameters, messages):
        """
        """
        inputType = parameters[0].value
        inputPID = parameters[1].value
        inputFeatureClass = parameters[2].value
        projectName = parameters[3].value
        projectDir = str(parameters[4].value)
        createLidarProducts = parameters[5].value
        lidarDir = str(parameters[6].value)

        arcpy.AddMessage(inputType)

        #set the env vars, we want to overwrite any existing stuff and use a fresh
        #in_memory workspace to store working data
        arcpy.env.overwriteOutput = True
        arcpy.env.workspace = "in_memory"
        arcpy.Delete_management("in_memory")

        if inputType == "PID":
            inputPID = str(inputPID).replace("[","")
            inputPID = str(inputPID).replace("]","")
            inputPID = str(inputPID).replace("u","")
            arcpy.AddMessage(inputPID)
            query = "{} in ({})".format(pidID,inputPID)
            selectFeatrures = downloadRestFeatures(pidMapService,"",query,"selectFeatrures")
        else:
            selectFeatrures = inputFeatureClass

        lidarFC = mostRecentLidar(selectFeatrures)

        lidarFCLayer = arcpy.MakeFeatureLayer_management(lidarFC,"lidarFCLayer")

        arcpy.SelectLayerByLocation_management(lidarFCLayer,"INTERSECT",selectFeatrures,"","NEW_SELECTION")

        downloadURLs = unique_values(lidarFCLayer,"DOWNLOAD")

        count = len(downloadURLs)

        lazList = []
        lasList = []
        downloadLimitCount = 0
        tileProgressCount = 1
        for i in downloadURLs:
            arcpy.AddMessage("Downloading {} of {} laz files.".format(tileProgressCount,count))
            tileProgressCount +=1
            if downloadLimitCount < 101:
                las = "{}\{}".format(lidarDir,i.split("/")[-1].replace("laz","las"))
                lasList.append(las)
                if os.path.exists(las) == False:
                    laz = downloadFile(i,lidarDir)
                    lazList.append(laz)
                    downloadLimitCount +=1
                else:
                    arcpy.AddMessage("LAS tile already downloaded.")
            else:
                arcpy.AddMessage("Exceeded download limit of 100 tiles, run again with same parameters to download remaing tiles.")

        unzipAndDelLAZ(lazList)

        lasDataset = arcpy.management.CreateLasDataset(lasList,"{}\{}.lasd".format(projectDir,projectName),compute_stats="COMPUTE_STATS")

        if createLidarProducts == True:

            arcpy.env.workspace = projectDir

            arcpy.MakeLasDatasetLayer_management(lasDataset,"DEM",class_code=[2])
            outDEM = r"{}\{}_dem.tif".format(projectDir,projectName)
            arcpy.conversion.LasDatasetToRaster("DEM",outDEM,"ELEVATION","BINNING AVERAGE LINEAR","FLOAT","CELLSIZE", 1, 1)

            arcpy.MakeLasDatasetLayer_management(lasDataset,"DSM")
            outDSM = r"{}\{}_dsm.tif".format(projectDir,projectName)
            arcpy.conversion.LasDatasetToRaster("DSM",outDSM,"ELEVATION","BINNING AVERAGE LINEAR","FLOAT","CELLSIZE", 1, 1)

            outSlope = "{}\{}_slope.tif".format(projectDir,projectName)
            arcpy.Slope_3d(outDEM,outSlope)

            outAspect = "{}\{}_aspect.tif".format(projectDir,projectName)
            arcpy.Aspect_3d(outDEM,outAspect)

            outContours = "{}\{}_contours.shp".format(projectDir,projectName)
            arcpy.Contour_3d(outDEM,outContours,"1")

            outHillshade = "{}\{}_hillshade.tif".format(projectDir,projectName)
            arcpy.HillShade_3d(outDEM,outHillshade)

            outCHM = "{}\{}_chm.tif".format(projectDir,projectName)
            arcpy.Minus_3d(outDSM,outDEM,outCHM)

        arcpy.Delete_management("in_memory")

        return

