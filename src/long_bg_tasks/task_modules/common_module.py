
import pandas as pd
import json
import uuid
from urllib.parse import urlparse
from urllib.request import urlopen
import ifcopenshell

import logging
log = logging.getLogger(__name__)

import data.files as file_store

# Get the Ifc file from the URL source
def getIfcModel(fileURL):
    # replace with use of httpx at some point in time
    try:
        response = urlopen(fileURL)
        response_content = response.read().decode('utf-8')
        ifcModel = ifcopenshell.file.from_string(response_content)
        return ifcModel
    except Exception as e:
        log.error(f'Error in getIfcModel: {e}')
        raise Exception(f'Error in getIfcModel: {e}')

# Get the IfcTypes from the reference csv
def get_IfcTypes_from_ref_csv():
    ifctypes_csv_path = '../db/csv/ifc-types-ref.csv'
    ifcTypes_df_0 = pd.read_csv(ifctypes_csv_path, delimiter=';') 
    ifcTypes_df = ifcTypes_df_0[['type','category']] ## don't need here the description and attributes
    return ifcTypes_df

# Get the Ifc Relationships from the reference csv
def get_relationhips_from_ref_csv():
    relationships_csv_path = '../db/csv/ifc-relationships-ref.csv'
    ifcRelationships_df= pd.read_csv(relationships_csv_path, delimiter=';')
    rel_dict = {}
    for index, row in ifcRelationships_df.iterrows():
        rel_dict[row['relationship']] = list([row['relating'], row['related'], row['optionalRelated'], row['optionalRelating']])
    return rel_dict

# Get the Ifc types that are in the filter specified by the request
def get_ifcTypes_in_filter_df(ifcTypes_df, filter):
#    transform_keep_cat = filter['categoryList']
#    transform_exclude_types = filter['excludeTypeList']
    transform_keep_cat = filter.categoryList
    transform_exclude_types = filter.excludeTypeList

    ifcTypes_in_filter_df_0 = ifcTypes_df[['type','category']][ifcTypes_df['category'].isin(transform_keep_cat)]
    ifcTypes_in_filter_df = ifcTypes_in_filter_df_0[~ifcTypes_in_filter_df_0['type'].isin(transform_exclude_types)]
    return ifcTypes_in_filter_df

# Set the filePath from the sourceFileURL
def setFilePath(sourceFileURL:str, BASE_PATH:str) -> str:
    parsed_url = urlparse(sourceFileURL)
    if parsed_url.scheme == 'http' or parsed_url.scheme == 'https':
        filePath = sourceFileURL
    else:
        filePath = f'{BASE_PATH}{sourceFileURL}'            
    return filePath

# get the content of a file with a givenFilePath
def getFileBytesContent(filePath:str) -> str:
    try:
        parsed_url = urlparse(filePath)
        if parsed_url.scheme == 'http' or parsed_url.scheme == 'https':
            fileBytesContent = urlopen(filePath).read()
        else: # local file
            fileBytesContent = file_store.read_file(filePath)
        return fileBytesContent
    except Exception as e:
        raise Exception(f'exception in getFileContent: {e}')

# get the json from the file
def get_ifcJson(filePath):
    try:
        parsed_url = urlparse(filePath)
        if parsed_url.scheme == 'http' or parsed_url.scheme == 'https':
            response = urlopen(filePath)
            ifcJsonData = response.read().decode('utf-8')
        else: # local file
            ifcJsonData = file_store.read_file(filePath)
        # with open(filePath) as ifcJsonFile:    
        ifcJson = json.loads(ifcJsonData)
        header = dict()
        header["type"] = ifcJson["type"]
        header["version"] = ifcJson["version"]
        header["schemaIdentifier"] = ifcJson["schemaIdentifier"]
        header["originatingSystem"]= ifcJson["originatingSystem"]
        header["preprocessorVersion"] = ifcJson["preprocessorVersion"]
        header["timeStamp"] = ifcJson["timeStamp"]  
        return ifcJson, header
    except Exception as e:
        raise Exception(f'exception in get_ifcJson: {e}')
    
# get the Json Model Data from the json 
def get_jsonModelData(ifcJson):
    try:
        jsonModelData = ifcJson['data']
        return jsonModelData
    except Exception as e:
        raise Exception(f'exception in get_jsonModel: {e}')

# get the model data in a dataframe
def get_modelData_df(jsonModelData, ifcTypes_df):
    try:
        modelData_df = pd.DataFrame()
        modelData_df['data'] = jsonModelData
        modelData_df['type'] = modelData_df['data'].apply(pd.Series)['type']
        modelData_df['uuid'] = modelData_df['data'].apply(pd.Series)['globalId']
        # add the category to the modelData
        modelData_df = modelData_df.merge(ifcTypes_df, on='type', how='inner')
        return modelData_df
    except Exception as e:
        raise Exception(f'exception in get_model_df: {e}') 


#   add the representationIds to the objects
def add_representationIds_to_objects(obje_df):
    obje_df.insert(0, 'representationIds', pd.Series(dtype=str)) # create a new column with empty list
    for i in obje_df.index:
        try:
            representationIds = list()
            representations = obje_df.at[i, 'data']['representation']['representations']
            for representation in representations:
                representationIds.append(representation['ref'])  
        except KeyError:
            representationIds = None
        obje_df.at[i, 'representationIds'] = representationIds                
    return obje_df

#   add relatingType, relatingId and relatedTypeAndIds to relationships
def add_relating_and_related_to_relationships(rela_df, rel_dict):
    count, count_Error = 0, 0
    rela_df.insert(0, 'relating_type', pd.Series(dtype=str)) # create a new column with empty list
    rela_df.insert(0, 'relating_ref', pd.Series(dtype=str))
    rela_df.insert(0, 'related_types_and_refs', pd.Series(dtype=str))

    for i in rela_df.index:
        typeOfRelationship = rela_df['type'][i]
        relating_key = rel_dict[typeOfRelationship][0]
        related_key = rel_dict[typeOfRelationship][1]
        rela_df_row = rela_df['data'][i]
        try:
            # handle the relating element
            relatingDetail = rela_df_row[relating_key]
            relating_type = relatingDetail['type']
            rela_df.loc[i,'relating_type']= relating_type
            if typeOfRelationship in ['IfcRelAssociatesMaterial','IfcRelAssociatesClassification']:
                # the relating_ref is not present in IfcRelAssociatesMaterial or IfcRelAssociatesClassification
                pass
            else:
                relating_ref = relatingDetail['ref']
                rela_df.loc[i,'relating_ref']= relating_ref
            # handle the related element
            if typeOfRelationship == 'IfcRelSpaceBoundary':
                if rela_df_row['physicalOrVirtualBoundary'] == 'VIRTUAL':
                    # there is no relatedBuildingElement for a virtual boundary
                    # hence no relatedDetail!
                    pass
                else:
                    relatedDetail = rela_df_row[related_key]
            else:
                # there is a related element
                relatedDetail = rela_df_row[related_key]
            if relatedDetail != None:
                # the relatedDetail is not always a list, so we make it a list !
                # it is a list in most cases but not for all IfcRelSpaceBoundary
                if type(relatedDetail) != list:
                    relatedDetail = [relatedDetail]
                related_types_and_refs = []
                for item in relatedDetail:
                    if type(item) == dict:
                        related_type = item['type']
                        related_ref = item['ref']
                        related_types_and_refs.append(json.dumps({  
                            'related_type': related_type,
                            'related_ref': related_ref
                        }))
                        count += 1
                rela_df.loc[i,'related_types_and_refs']= ';'.join(related_types_and_refs)
        except KeyError as e: 
            message = f'KeyError: {e}'
            log.info(message)
            message = f'Error occured in row {rela_df_row}'
            log.error(message)
            count_Error += 1
        if relating_type == None:
            log.info(f"relating_type is null in row {rela_df_row}")
        if count_Error > 5:
            break 
    return rela_df

#
# There are duplicates in IFC_schependomlaan for uuid='b0b6074f-4c85-c523-ca2b-ac8b7f820ec3'
# and type='IfcRelConnectsPathElements'
# 2 times the same relating and related elements but 1 with ATEND an the other with ATSTART
#
def process_relationship_duplicate_uuids(rela_df):
    rela_df_duplicates = pd.DataFrame()
    try:
        rela_df_duplicates = pd.concat(g for _, g in rela_df.groupby("uuid") if len(g) > 1)
    except Exception as e:
        pass
    if rela_df_duplicates.empty:
        return rela_df
    else:       
        log.info(f"Found {len(rela_df_duplicates)} duplicate relationships")
        # Iterate through the items in the groups of the dataframe groupby
        for name, group in rela_df_duplicates.groupby("uuid"):
            log.info(f"Processing group: {name}")
            counter = 0
            for index, row in group.iterrows():
                log.info(f"Row index: {index}, Row data: {row.to_dict()}")
                if counter > 0:
                    # replace the uuid with a new uuid
                    rela_df.at[index, 'uuid'] = uuid.uuid4()
                    log.info(f"New UUID: {rela_df.at[index, 'uuid']}") 
                counter += 1           
    return rela_df

