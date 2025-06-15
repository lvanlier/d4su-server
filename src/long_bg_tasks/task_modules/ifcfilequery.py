import ifcopenshell
# import ifcopenshell.util.selector

from collections import defaultdict
from typing import Literal

import uuid
import pandas as pd

import sys


# Set up the logging
import logging
log = logging.getLogger(__name__)
from time import perf_counter

from long_bg_tasks.task_modules import common_module as common

from model.admin import IfcFileQuery_Instruction, IfcFileQuery_Result


class IfcFileQuery():
    def __init__(self, task_dict:dict):
        try:
            self.task_dict = task_dict
            instruction = IfcFileQuery_Instruction(**self.task_dict['IfcFileQuery_Instruction'])
            self.sourceFileURL = instruction.sourceFileURL
            self.queryType = instruction.queryType
            self.queryString = instruction.queryString
            self.BASE_PATH = task_dict['BASE_PATH']
            self.TEMP_PATH = task_dict['TEMP_PATH']
            self.start = perf_counter()
            self.PRINT = self.task_dict['debug']
            if self.PRINT:
                print(f'>>>>> In IfcFileQuery.init: {self.sourceFileURL}') 
        except Exception as e:
            log.error(f'ListOfElemntTypes.init : {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = f'Error in IfcFileQuery.init : {e}'
    
    
    def query(self):
        try:
            if self.queryType == 'listTypes':
                df = self.listTypes(self.sourceFileURL[0])
                result_rel_path = f'{self.TEMP_PATH}CSV/{uuid.uuid4()}_listTypes.csv'
            elif self.queryType == 'compareListTypes':
                df1 = self.listTypes(self.sourceFileURL[0])
                df2 = self.listTypes(self.sourceFileURL[1])
                df3 = pd.merge(df1, df2, on='IfcType', how='outer', suffixes=('_file1', '_file2'))
                df = df3
                result_rel_path = f'{self.TEMP_PATH}CSV/{uuid.uuid4()}_compareListTypes.csv'
            elif self.queryType == 'compareIfcJsonListTypes':
                df1 = self.listTypes(self.sourceFileURL[0])
                df2 = self.listTypes(self.sourceFileURL[1])
                df3 = pd.merge(df1, df2, on='IfcType', how='outer', suffixes=('_file1', '_file2'))
                df = df3
                result_rel_path = f'{self.TEMP_PATH}CSV/{uuid.uuid4()}_compareIfcJsonListTypes.csv'
            else:
                errorMsg = f'Unsupported queryType: {self.queryType}'
                self.task_dict['status'] = 'failed'
                self.task_dict['error'] = f'Error in IfcFileQuery.query : {errorMsg}'
                raise ValueError(errorMsg)
            result_path = result_rel_path 
            # for some strange reason I got a float in Counts_file2, convert to int was not enough so convert to str              
            if 'Counts_file1' in df.columns:
                df['Counts_file1'] = df['Counts_file1'].apply(lambda x: str(int(x)) if pd.notnull(x) else x)
            if 'Counts_file2' in df.columns:
                df['Counts_file2'] = df['Counts_file2'].apply(lambda x: str(int(x)) if pd.notnull(x) else x)
            df = df[['IfcType', 'Counts_file1', 'Counts_file2']]
            self.write_df_in_csv(df, result_path)
            result = IfcFileQuery_Result(
                resultPath = result_rel_path,
                runtime = round(perf_counter() - self.start, 2)
            )
            self.task_dict['result']['IfcFileQuery_Result'] = result.dict()
        except Exception as e:
            log.error(f'Error IfcFileQuery.listTypes: {e}')
            self.task_dict['status'] = 'failed'
            self.task_dict['error'] = f'Error IfcFileQuery.listType: {e}'
        finally:
            return self.task_dict  
    
          
    def listTypes(self, sourceFileURL):
        srcFilePath = common.setFilePath(sourceFileURL, self.BASE_PATH)
        fileExt = srcFilePath.split('.')[-1]
        l = list()
        if fileExt == 'ifc':       
            ifcModel = common.getIfcModel(srcFilePath)  
            for item in ifcModel:
                s = str(item)
                ifctype = s.split("=")[1].split("(")[0]
                l.append(ifctype)
        elif fileExt == 'json':
            ifcJson, header = common.get_ifcJson(srcFilePath)
            jsonModelData = common.get_jsonModelData(ifcJson)
            # Recursively find all 'type' values in each item
            for item in jsonModelData:
                l.extend(self.find_all_types(item))
        else:
            raise ValueError(f'Unsupported file extension in: {sourceFileURL}')
        oc = self.find_term_occurrences(l)
        df = pd.DataFrame()
        df['IfcType'] = list(oc.keys())
        df['Indices'] = list(oc.values())
        df['Counts'] = df['Indices'].apply(len)
        df = df[['IfcType', 'Counts', 'Indices']]
        df = df.sort_values(by='IfcType', ascending=True)
        return df    
    
    def find_all_types(self, d):
        types = []
        if isinstance(d, dict):
            for k, v in d.items():
                if k == 'type':
                    types.append(v)
                if isinstance(v, dict):
                    types.extend(self.find_all_types(v))
                elif isinstance(v, list):
                    for elem in v:
                        types.extend(self.find_all_types(elem))
        elif isinstance(d, list):
            for elem in d:
                types.extend(self.find_all_types(elem))
        return types
        
    def find_term_occurrences(self,term_list):
        occurrences = defaultdict(list)
        for idx, term in enumerate(term_list):
            occurrences[term].append(idx)
        return dict(occurrences)
    
    def write_df_in_csv(self, df, filePath):
        df.to_csv(filePath, sep=';', index=False, encoding='utf-8')
       
