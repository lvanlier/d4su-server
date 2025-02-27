##
# Filter the IfcJson model data based on the filter provided
##

import uuid
import pandas as pd
import json
from time import perf_counter
from urllib.parse import urlparse


import long_bg_tasks.task_modules.common_module as common

from pydantic import BaseModel
from model.transform import FilterIfcJson_Instruction, FilterIfcJson_Result

import os

# Set up the logging
import logging
log = logging.getLogger(__name__)

class FilterIfcJson():
    def __init__(self, task_dict:dict):
        try:
            self.task_dict = task_dict
            instruction = FilterIfcJson_Instruction(**self.task_dict['FilterIfcJson_Instruction'])        
            self.sourceFileURL = instruction.sourceFileURL
            self.filter = instruction.filter
            self.BASE_PATH = task_dict['BASE_PATH']
            self.IFCJSON_PATH = task_dict['IFCJSON_PATH']
            self.PRINT = task_dict['debug']
            if self.PRINT:
                log.info(f'>>>>> In FilterIfcJson with instruction: {instruction}')  
        except Exception as e:
            log.error(f'Error in init of FilterIfcJson: {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = f'Error in init of FilterIfcJson: {e}'
    
    def filterJson(self):
        try:
            ifcJsonFilePath = common.setFilePath(self.sourceFileURL, self.BASE_PATH)
            result_rel_path = f'{self.IFCJSON_PATH}{uuid.uuid4()}_FIL.json' 
            result_path = f'{self.BASE_PATH}{result_rel_path}'       
            # Step 0: Get the reference IfcTypes
            ifcTypes_df = common.get_IfcTypes_from_ref_csv()
            # Get the reference Relationships
            rel_dict = common.get_relationhips_from_ref_csv()
            # Step 1: Get the filter from the API request
            # Step 2: Set the IfcTypes to keep to filter the model data
            ifcTypes_in_filter_df = common.get_ifcTypes_in_filter_df(ifcTypes_df, self.filter)
            #   Step 3.1: Get the model (from the json)
            ifcJson, header = common.get_ifcJson(ifcJsonFilePath)
            jsonModelData = common.get_jsonModelData(ifcJson)
            modelData_df = common.get_modelData_df(jsonModelData, ifcTypes_df)                        
            nbrRows = modelData_df.shape[0]
            #   Step 3.2: Filter the model data
            t1_start = perf_counter()
            modelData_f_df = modelData_df[modelData_df['type'].isin(ifcTypes_in_filter_df['type'])]
            nbrRows_f1 = modelData_f_df.shape[0]
            t1_stop = perf_counter()
            t_filterModel = round(t1_stop - t1_start, 3)
            message = f'Applied filter dropping {nbrRows-nbrRows_f1} from {nbrRows} in {t_filterModel} seconds'
            log.info(message)
            #  Step 4 = Split the model data in the different top-level categories
            # excluded elements
            elem_out_df = modelData_df[~modelData_df['uuid'].isin(modelData_f_df['uuid'])]
            pset_f_df = modelData_f_df[modelData_f_df['category'] == 'propertySet']
            repr_f_df = modelData_f_df[modelData_f_df['category'] == 'representation']
            rela_f_df = modelData_f_df[modelData_f_df['category'] == 'relationship']
            # keep only the element that are not in the categories propertySet, relationship, representation
            obje_f_df = modelData_f_df[~modelData_f_df['category'].isin(['propertySet','relationship','representation'])]

            totRows = pset_f_df.shape[0] + repr_f_df.shape[0] + rela_f_df.shape[0] + obje_f_df.shape[0] + elem_out_df.shape[0]
            assert totRows == nbrRows, f'Error: {totRows} != {nbrRows}'

            # Step 5 = Drop representations that are not related to objects in the model
            # get the list of representation references for the excluded elements elem_out_df

            refs_of_representation_to_exclude = self.get_list_of_representation_refs_for_excluded_elements(elem_out_df)
            # drop the representations that are not related to objects in the model
            repr_f_df = repr_f_df[~repr_f_df['uuid'].isin(refs_of_representation_to_exclude)]

            # Step 6 = Drop the relationships that are not related to objects in the model
            # keep only the relationships with a relating element's type that is in the filter
            rela_f_df = self.keep_only_relationships_for_types_in_the_filter(rela_f_df, rel_dict, ifcTypes_in_filter_df, PRINT=False)

            # Step 7 = Prune the related members in the relationship; 
            # if there is no related left, then:
            #   - if the relating is a property_set:  drop the property_set
            #   - in all cases: drop the relationship
            relat_f_df,list_pSet_to_drop  = self.prune_related_members_in_relationships(rela_f_df, rel_dict, ifcTypes_in_filter_df, PRINT=False)

            nbr_propertySetsBeforePruning = pset_f_df['uuid'].count()
            pset_f_df = pset_f_df[~pset_f_df['uuid'].isin(list_pSet_to_drop)]
            nbr_propertySetsAfterPruning = pset_f_df['uuid'].count()
            message = f'Pruned {nbr_propertySetsBeforePruning-nbr_propertySetsAfterPruning} propertySets'
            log.info(message)

            #
            #  Step 8: Save the filtered model data to a json file
            #

            filtered_model_data = pd.concat([obje_f_df['data'], pset_f_df['data'], repr_f_df['data'], rela_f_df['data']])
            header = dict()
            header["type"] = "ifcJSON"
            header["version"]= "0.0.2"
            header["schemaIdentifier"] = ifcJson["schemaIdentifier"]
            header["originatingSystem"]= "IFC2JSON_python Version 0.0.1"
            header["preprocessorVersion"] = "IfcOpenShell 0.8.0"
            from datetime import datetime
            header["timeStamp"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            rootObjectId = ""
            
            outJsonModel = dict(header)
            outJsonModel["data"] = filtered_model_data.to_list()
            
            for item in outJsonModel["data"]:
                if item["type"] == "IfcProject":    
                    rootObjectId = item["globalId"]
                    rootObjectType = item["type"]
                    rootObjectName = item["name"]
                    break
            if not rootObjectId:
                raise Exception("No root object found")

            indent=2
            with open(result_path, 'w') as outJsonFile:
                json.dump(outJsonModel, outJsonFile, indent=indent)
            result = FilterIfcJson_Result(
                rootObjectId = rootObjectId,
                rootObjectType = rootObjectType,
                rootObjectName = rootObjectName,
                resultPath = result_rel_path
            )    
            self.task_dict['result']['FilterIfcJson_Result'] = result.dict()    
        except Exception as e:
            log.error(f'Error in FilterIfcJson.filterExec(): {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = str(e)
        return self.task_dict       

    # get the list of representation references for the excluded elements in elem_out_df
    def get_list_of_representation_refs_for_excluded_elements(self, elem_out_df):
        refs_of_representation = []
        for i in elem_out_df.index:
            row = elem_out_df['data'][i]
            if type(row) == dict:
                try:
                    representation_val = row['representation'] 
                    representations_val = representation_val['representations'] # is a list
                    for item in representations_val:
                        ref = item['ref']
                        refs_of_representation.append(ref)
                except KeyError as e:
                    pass
        return refs_of_representation

    # Get the type of the relating and related elements in the relationships
    def get_relating_and_related_Elements_from_relationships(self, rela_f_df, rel_dict, PRINT=False):
        # add the relating element to the relationship dataframe in series rela_f_df['relating_type']
        count=0
        count_Error=0
        rela_f_df['relating_type'] = pd.Series(dtype='str')
        rela_f_df['relating_ref'] = pd.Series(dtype='str')
        rela_f_df['related_types'] = pd.Series(dtype='str')
        for i in rela_f_df.index:
            typeOfRelationship = rela_f_df['type'][i]
            relating_key = rel_dict[typeOfRelationship][0]
            related_key = rel_dict[typeOfRelationship][1]
            rela_f_df_row = rela_f_df['data'][i]
            try:
                relatingDetail = rela_f_df_row[relating_key]
                relatedDetail = rela_f_df_row[related_key]
                relating_type = relatingDetail['type']
                if relating_type == 'IfcPropertySet' : relating_ref = relatingDetail['ref']
                rela_f_df.loc[i,'relating_type']= relating_type
                if relating_type == 'IfcPropertySet' : rela_f_df.loc[i,'relating_ref']= relating_ref
                # the relatedDetail is not always a list, so we make it a list !
                # it is a list in most cases but not for all IfcRelSpaceBoundary
                if type(relatedDetail) != list:
                    relatedDetail = [relatedDetail]
                related_types = []
                for item in relatedDetail:
                    if type(item) == dict:
                        related_type = item['type']
                        related_types.append(related_type)
                        count += 1
                # message = f'related_types: {related_types}'
                # log.info(message)
                rela_f_df.loc[i,'related_types']= ','.join(related_types)
            except KeyError as e:
                if typeOfRelationship == 'IfcRelSpaceBoundary' and related_key == 'relatedBuildingElement':
                    # the relatedBuildingElement is not always present in IfcRelSpaceBoundary
                    pass
                else:
                    message = f'KeyError: {e}'
                    log.info(message)
                    message = f'Error occured in row {rela_f_df_row}'
                    log.info(message)
                    count_Error += 1
            if count_Error > 5:
                break 
        if PRINT:
            message = f'count of processed relatedDetails: {count}'
            log.info(message)
            message = f'Found the following relatingElements types in the relationships:'
            log.info(message)
            message = f'rela_f_df["relating_type"].unique() before pruning: {rela_f_df["relating_type"].unique()}'
            log.info(message) 
        
        return rela_f_df 

    # Keep only the relationships with a relating element's type that is in the filter
    # i.e., drop the other relationships
    def keep_only_relationships_for_types_in_the_filter(self, rela_f_df, rel_dict,ifcTypes_in_filter_df,PRINT=False):
        t1_start = perf_counter()
        rela_f_df = self.get_relating_and_related_Elements_from_relationships(rela_f_df, rel_dict, PRINT)
        nbr_RelationshipsBeforePruning = rela_f_df['relating_type'].count()
        rela_f_df = rela_f_df[rela_f_df['relating_type'].isin(ifcTypes_in_filter_df['type'])]
        nbr_RelationshipsBeAfterPruning = rela_f_df['relating_type'].count()
        t1_stop = perf_counter()
        t_drop = round(t1_stop - t1_start, 3)
        if PRINT:
            message = f'Pruned {nbr_RelationshipsBeforePruning-nbr_RelationshipsBeAfterPruning } relationships in {t_drop} seconds'
            log.info(message)
            message = f'rela_f_df["relating_type"].unique() after pruning: {rela_f_df["relating_type"].unique()}'
            log.info(message)
        return rela_f_df

    # Prune the related members in the relationship:
    def prune_related_members_in_relationships(self, rela_f_df, rel_dict, ifcTypes_in_filter_df, PRINT=False):
        t1_start = perf_counter()
        count, countEmpty,countDropped,countUpdated,countUnchanged,countPropertySetToDrop = 0,0,0,0,0,0
        list_empty = []
        list_dropped = []
        list_updated = []
        list_unchanged = []
        list_pSet_to_drop = []
        for i in rela_f_df.index:
            relatedStr = rela_f_df['related_types'][i] # is a string
            if relatedStr == '':
                countEmpty +=1
                list_empty.append(rela_f_df["type"][i]+' | '+rela_f_df['relating_type'][i])
                continue
            relatedList_before = relatedStr.split(',') # is a list
            list_length_before = len(relatedList_before)
            # keep only the relatedElements'type that are in the filter
            relatedList_after = [item for item in relatedList_before if item in ifcTypes_in_filter_df['type'].to_list()]
            list_length_after = len(relatedList_after)
            if list_length_after == 0:
                # drop the relationship that has only related types not retained
                # the relatedElements have expectedly been pruned already 
                list_dropped.append(rela_f_df["type"][i]+' | '+rela_f_df['relating_type'][i]+' / '+str(relatedList_before))
                relating_type = rela_f_df['relating_type'][i]
                if relating_type == 'IfcPropertySet':
                    # add ref to list of pset to drop
                    countPropertySetToDrop +=1
                    list_pSet_to_drop.append(rela_f_df['relating_ref'][i])
                rela_f_df.drop(i, inplace=True)
                countDropped += 1
            elif list_length_before != list_length_after:
                # update the related_types and the data for the list of relatedElements
                list_updated.append(rela_f_df["type"][i]+' | '+rela_f_df['relating_type'][i])
                rela_f_df.loc[i,'related_types'] = ','.join(relatedList_after)
                # update the data related - Must be implemented !!!!!
                typeOfRelationship = rela_f_df['type'][i]
                related_key = rel_dict[typeOfRelationship][1]
                # to be checked that it makes an in place update
                rela_f_df['data'][i][related_key] = [item for item in rela_f_df['data'][i][related_key] if item['type'] in relatedList_after]
                countUpdated += 1
            else:
                # nothing to do
                list_unchanged.append(rela_f_df["type"][i]+' | '+rela_f_df['relating_type'][i])
                countUnchanged += 1
            count += 1
        t1_stop = perf_counter()
        t_process = round(t1_stop - t1_start, 3)
        if PRINT:
            message = f'Processed {count} relationships in {t_process} seconds'
            log.info(message)
            message = f'Count of relationships without relatedElements: {countEmpty}'
            log.info(message)
            message = f'Found for the following relationships: {pd.Series(list_empty).unique()}'
            log.info(message)
            message = f'Count of relationships dropped (all their relatedElements have types excluded by the filter): {countDropped}'
            log.info(message)
            message = f'of which {countPropertySetToDrop} were for relating propertySets'
            log.info(message)
            message = f'Found for the following relationships: {pd.Series(list_dropped).unique()}'
            log.info(message)
            message = f'Count of relationships updated (some of their relatedElements have types excluded by the filter): {countUpdated}'
            log.info(message)
            message = f'Found for the following relationships: {pd.Series(list_updated).unique()}'
            log.info(message)
            message = f'Count of relationships kept without change (all their relatedElements have types included in the filter): {countUnchanged}'
            log.info(message)
            message = f'Found for the following relationships: {pd.Series(list_unchanged).unique()}'
            log.info(message)
        return rela_f_df, list_pSet_to_drop


