"""
Python Utility for converting flat text cryptic files to meaningful insight data
Created Date :  25-07-2024
Modified Date : 25-09-2024
Original Author : Saurabh Sanyal
Modified By : Abhinav Bansal / Prakhar Srivastava
"""
import os
import pandas as pd
from pandas import isnull, notnull
from fuzzywuzzy import fuzz
import spacy
from pathlib import Path
import traceback
nlp = spacy.load("en_core_web_md")
msh_8_2 = "ERROR"
msh_6 = "ERROR"
msh_3 = "ERROR"
directory_path = "ERROR"
in1_4 = "ERROR"
first_4_fields_NK1_5= "ERROR"
first_4_fields_NK1_6 = "ERROR"
pid_13_2_list = [ 'PRN','ORN','WPN','VHN','ASN','EMR','NET','BPN'  ]
DIRECTORY_PATH = os.path.dirname(os.path.realpath(__file__))
df_source_F_code = pd.read_excel(DIRECTORY_PATH+"\\Test Data"+"\\facility_code"+"\\"+"enterprise_facility_config"+".xlsx")
SOURCE_DICTIONARY_ = { }
'''
#################################################################
read_source_facility : reads the source and facility code 
The file must be an excel with name enterprise_facility_config.xlsx

#################################################################
'''
def read_source_facility():
        global SOURCE_DICTIONARY_
        global df_source_F_code
        for index, row in df_source_F_code.iterrows():
                keystore = row['Source']
                lookupvalue = row['Facility Code']
                SOURCE_DICTIONARY_[keystore] = lookupvalue
read_source_facility()
SOURCE_DICTIONARY_ = {str(key) : str(value) for key, value in SOURCE_DICTIONARY_.items()}
'''
#################################################################
applies rule to each row of pandas data frame
returns rows after pandas built in method apply()

#################################################################
'''
def apply_rules(row):
        global msh_8_2
        global msh_6
        global msh_3
        global pid_13_2_list
        global in1_4
        global first_4_fields_NK1_5
        global first_4_fields_NK1_6
        if row['Cols'] == 'MSH-3' and isinstance(row['Input-Series'], str):
                for key, value in SOURCE_DICTIONARY_.items():
                        if row['Input-Series'].startswith(key):
                                print('Rule => MSH-3 Facility Code' + key+ " Will be Applied " )
                                msh_3 = value
                                row['Output-Series'] = msh_3
                                row['validate'] = 1
                                row['rule']='Rule => MSH-3 Facility Code' + key
                for key, value in SOURCE_DICTIONARY_.items():     
                        if row['Input-Series'].startswith(key):
                                print('Rule => MSH-3 Facility Code' + key+ " Will be Applied " )
                                msh_3 = value
                                row['Output-Series'] = value
                                row['validate'] = 1
                                row['rule']=row['rule']='Rule => MSH-3 Facility Code' + key
        if row['Cols'] == 'MSH-11' and isinstance(row['Input-Series'], str):
                print("Rule => MSH|11 to 2.5 will be applied for " +  row['Input-Series'])
                row['Output-Series'] = '2.5' 
                row['validate'] = 1
                row['rule']='Rule => MSH|11 to 2.5'
        if row['Cols'] == 'MSH-8' and notnull(row['Input-Series']):
                msh8_parts = row['Input-Series'].split('^')
                if len(msh8_parts) >= 2:                        
                        msh_8_2 = msh8_parts[1]
        if row['Cols'] == 'MSH-6' and notnull(row['Input-Series']):
                msh_6 = row['Input-Series']

        if row['Cols'] == 'EVN-1' and isnull(row['Input-Series']):
                print("Rule => MSH|8.2 to EVN|1 will be applied for ")
                row['Output-Series'] =  msh_8_2 
                row['validate'] = 1
                row['rule']='Rule => MSH|8.2 to EVN|1'
        if row['Cols'] == 'EVN-2':
                print("Rule => MSH|6 to EVN|2 will be applied for ")
                row['Output-Series'] =  msh_6
                row['validate'] = 1
                row['rule']='Rule => MSH|6 to EVN|2'
                

        if row['Cols'] == 'MSH-8' and notnull(row['Input-Series']):
                msh8_parts = row['Input-Series'].split('^')
                if len(msh8_parts) >= 2:                        
                        msh_8_2 = msh8_parts[1]

        if row['Cols'] == 'PID-3' and notnull(row['Input-Series']):
                dessicate = row['Input-Series'].split('^')

                if dessicate[0] is None:
                        row['rule']='PID 3.1 Cannot Be Null'
                        row['validate'] = 0
                else:
                        print("Rule => MSH|6 to EVN|2 will be applied for " +  row['Input-Series'])
                        row['Output-Series'] = dessicate[0]+"^"+"^"+"^"+msh_3+"^MR"
                        row['validate'] = 1
                        row['rule']='Rule => PID|3.4 to MSH|3 FACILITY-CODE => PID|3.5 to MR'
        if row['Cols'] == 'PID-2' and notnull(row['Input-Series']):
                dessicate = row['Input-Series'].split('^')
                print("Rule => PID|2.4 to MSH|3 FACILITY-CODE => PID|3.5 to MR")
                row['Output-Series'] = dessicate[0]+"^"+"^"+"^"+msh_3+"^MR"
                row['validate'] = 1
                row['rule']='Rule => PID|2.4 to MSH|3 FACILITY-CODE => PID|3.5 to MR'
        if row['Cols'] == 'PID-13' and isinstance(row['Input-Series'], str) and notnull(row['Input-Series']):
                dessicate_input = row['Input-Series'].split('^')
                dessicate_output = row['Output-Series'].split('^')
                if len(dessicate_input)> 1 and dessicate_input[1] not in pid_13_2_list:
                        print('Rule => PID|13.2 to Null  will be applied')
                        dessicate_output [1] = ''
                        row['Output-Series'] = '^'.join(dessicate_output)
                        row['validate'] = 1
                        row['rule']='Rule => PID|13.2 to Null '
                if len (dessicate_input) >= 6 and dessicate_input[0] == '' and dessicate_input[5] is not None:
                        print('Rule => PID|13.1 = 13.6 + 13.7  will be applied')
                        dessicate_output[0] = dessicate_input[5] + dessicate_input[6]
                        row['Output-Series'] = '^'.join(dessicate_output)
                        row['validate'] = 1
                        row['rule']='Rule => PID|13.1 = 13.6 + 13.7'
        if row['Cols'] == 'PID-14' and isinstance(row['Input-Series'], str) and notnull(row['Input-Series']):
                dessicate_input = row['Input-Series'].split('^')
                dessicate_output = row['Output-Series'].split('^')
                dessicate_output[1] = "WPN"
                row['Output-Series'] = '^'.join(dessicate_output)
                row['validate'] = 1
                row['rule']='Rule => PID|14.1 to 14.2 as WPN'
        if ((row['Cols'] == 'PV1-7' and isinstance(row['Input-Series'], str)  and notnull(row['Input-Series']) or
              (row['Cols'] == 'PV1-8' and isinstance(row['Input-Series'], str)  and notnull(row['Input-Series']) or
               (row['Cols'] == 'PV1-9' and isinstance(row['Input-Series'], str)  and notnull(row['Input-Series']) or
                (row['Cols'] == 'PV1-17' and isinstance(row['Input-Series'], str)  and notnull(row['Input-Series']) or
                 (row['Cols'] == 'PV1-52' and isinstance(row['Input-Series'], str)  and notnull(row['Input-Series']))))))):
                dessicate_input = row['Input-Series'].split('^')
                dessicate_output = row['Output-Series'].split('^') if isinstance(row['Output-Series'], str) else ''
                if len(dessicate_input[0]) == 10:
                        print('Rule => PV1 to  NPI  will be applied')
                        if "NPI" not in dessicate_output:
                                dessicate_output.append("NPI")
                        
                        row['Output-Series'] = '^'.join(dessicate_output)
                        row['validate'] = 1
                        row['rule']='Rule => PV1 to  NPI'
                else:
                        print('Rule => PV1 to  LOCAL  will be applied')
                        dessicate_output.append("LOCAL") if isinstance(dessicate_output, list) else ''
                        row['Output-Series'] = '^'.join(dessicate_output)
                        row['validate'] = 1
                        row['rule']='Rule => PV1 to  LOCAL'
        if row['Cols'] == 'PV1-19' and not (isinstance(row['Input-Series'], str)):
            print('Rule => PV1-19 should not be Null Rule will be applied')
            row['validate'] = 0
            row['rule']='Rule => PV1-19  | NULL Found'
        if row['Cols'] == 'PV1-2' and (isinstance(row['Input-Series'], str)) and not(row['Input-Series'] == 'O' or row['Input-Series'] == 'S'):
            print('Rule => PV1-19 should Not be other than O or S Rule will be applied')
            row['validate'] = 0
            row['rule']='Rule => PV1-19  | Non O or S Found'
        if row['Cols'] == 'PV1-2' and (isinstance(row['Input-Series'], str)) and (row['Input-Series'] == 'O' or row['Input-Series'] == 'S'):
            print('Rule => PV1-2 should be O or S Rule will be applied')
            row['validate'] = 1
            row['rule']='Rule => PV1-2  | O or S Found'
        if row['Cols'] == 'PD1-4' and isinstance(row['Input-Series'], str)  and notnull(row['Input-Series']):
            dessicate_input = row['Input-Series'].split('^')
            dessicate_output = row['Output-Series'].split('^')
            if len(dessicate_input[0]) == 10:
                print('Rule => PD1-4 to  NPI  will be applied')
                if "NPI" not in dessicate_output:
                    dessicate_output.append("NPI")
                row['Output-Series'] = '^'.join(dessicate_output)
                row['validate'] = 1
                row['rule']='Rule => PD1-4 to  NPI'
            else:
                print('Rule => PD1-4 to  LOCAL  will be applied')
                dessicate_output.append("LOCAL")
                row['Output-Series'] = '^'.join(dessicate_output)
                row['validate'] = 1
                row['rule']='Rule => PD1-4 to  LOCAL'
        if row['Cols'] == 'PD1-4' and isinstance(row['Input-Series'], str)  and notnull(row['Input-Series']):
            dessicate_input = row['Input-Series'].split('^')
            dessicate_output = row['Output-Series'].split('^')
            if len(dessicate_input)> 1 and dessicate_input[0] == '' and dessicate_input[1] is not None and len(dessicate_input [1]) >= 3 and len(dessicate_input [2]) >= 3:
                print('Rule => PD1-4 | Concatenate 4.2 and 4.3  will be applied')
                first_three_characters_A = dessicate_input[1][:3]
                first_three_characters_B = dessicate_input[2][:3]
                dessicate_output[0] = first_three_characters_A + first_three_characters_B
                if "LOCAL" not in dessicate_output:
                    dessicate_output[9] = "LOCAL"
                row['Output-Series'] = '^'.join(dessicate_output)
                row['validate'] = 1
                row['rule']='Rule => PD1-4 to  LOCAL & Rule => PD1-4 =  4.2 + 4.3 First Three Characters'
        if row['Cols'] == 'IN1-4' and isinstance(row['Input-Series'], str)  and notnull(row['Input-Series']):
            dessicate_input = row['Input-Series'].split('^')
            in1_4 = dessicate_input[0]
        if row['Cols'] == 'IN1-2' and isinstance(row['Input-Series'], str)  and notnull(row['Input-Series']):
            dessicate_input = row['Input-Series'].split('^')
            dessicate_output = row['Output-Series'].split('^')
            if len(dessicate_input)> 1 and dessicate_input[1] == '':
                dessicate_output[1] = in1_4
                print('Rule => IN1|4.1 to IN1|2.2  will be applied')
                row['Output-Series'] = '^'.join(dessicate_output)
                row['validate'] = 1
                row['rule']='Rule => IN1|4.1 to IN1|2.2'
        if row['Cols'] == 'IN1-2' and isinstance(row['Input-Series'], str)  and notnull(row['Input-Series']):
                dessicate_input = row['Input-Series'].split('^')
                dessicate_output = row['Output-Series'].split('^')
                if dessicate_input[0] == '' and in1_4 != '':
                        print('Rule => IN1|4 to IN1|2.1 will be applied')
                        first_four_characters_A = in1_4[:4]
                        dessicate_output[0] = first_four_characters_A
                        row['Output-Series'] = '^'.join(dessicate_output)
                        row['validate'] = 1
                        row['rule']='Rule => IN1|4 to IN1|2.1'
        if row['Cols'] == 'IN1-3' and isinstance(row['Input-Series'], str)  and notnull(row['Input-Series']):
                dessicate_input = row['Input-Series'].split('^')
                dessicate_output = row['Output-Series'].split('^')
                if len(dessicate_input)> 1 and dessicate_input[0] == '' and dessicate_input[1] != '':
                        print('Rule => IN1|3.2 to IN1|3.1 will be applied')
                        dessicate_output[0] = dessicate_input[1]
                        row['Output-Series'] = '^'.join(dessicate_output)
                        row['validate'] = 1
                        row['rule']='Rule => IN1|3.2 to IN1|3.1'
        if row['Cols'] == 'IN1-3' and not isinstance(row['Input-Series'], str):
                print('Rule => IN1|4 first 4 characters to IN1|3 will be applied')
                row['Input-Series'] = in1_4[:4]
                row['Output-Series'] = row['Input-Series']
                row['validate'] = 1
                row['rule']='Rule => IN1|4 first 4 characters to IN1|3'
        # Validate >==> Abhinav
        if row['Cols'] == 'IN1-49' and isinstance(row['Input-Series'], str):
                print('Rule => IN1|49 should be NULL will be applied')
                row['Input-Series'] = ''
                row['Output-Series'] = ''
                row['validate'] = 1
                row['rule']='Rule => IN1|49 should be NULL'
        if row['Cols'] == 'IN2' and isinstance(row['Input-Series'], str):
                print('Rule => IN2 should be NULL will be applied')
                row['Input-Series'] = ''
                row['Output-Series'] = ''
                row['validate'] = 1
                row['rule']='Rule => IN2 should be NULL'
        # pending to incorporate
        # DG1--> If DG1|3 and DG1|4 are null remove DG1 segment
        if (row['Cols'] == 'DG1-3' and not isinstance(row['Input-Series'], str))and (row['Cols'] == 'DG1-4' and not isinstance(row['Input-Series'], str)):
                pass
        # Validate >==> Abhinav
        if row['Cols'] == 'GT1-6' and isinstance(row['Input-Series'], str):
                dessicate_input_A = row['Input-Series'].split('^')
                first_4_characters_A = dessicate_input_A[0][:4]
                if row['Cols'] == 'GT1-7' and isinstance(row['Input-Series'], str):
                        dessicate_input_B = row['Input-Series'].split('^')
                        first_4_characters_B = dessicate_input_B[0][:4]
                        concatenate = first_4_characters_A + first_4_characters_B
                        if row['Cols'] == 'GT1-1' and not isinstance(row['Input-Series'], str):
                                print('Rule => FIRST FOUR FIELDS will be applied')
                                row['Input-Series'] = concatenate
                                row['Output-Series'] = row['Input-Series'] 
                                row['validate'] = 1
                                row['rule']='Rule => GT1 > only assign first 4 fields of GT1|6 and 7'
        # Check >==> Abhinav
        #GT1--> only map 2.1 to 2(Exclude all and keep only 2.1)
        if row['Cols'] == 'NK1-5' and isinstance(row['Input-Series'], str):
                dessicate_input_A = row['Input-Series'].split('^')
                first_4_fields_NK1_5 = dessicate_input_A[0][:4]
        if row['Cols'] == 'NK1-6' and isinstance(row['Input-Series'], str):
                dessicate_input_B = row['Input-Series'].split('^')
                first_4_fields_NK1_6 = dessicate_input_B[0][:4]
        if (row['Cols'] == 'NK1-1' and isinstance(row['Input-Series'], str)) and (row['Cols'] == 'NK1-5' and isinstance(row['Input-Series'], str)) and (row['Cols'] == 'NK1-6' and isinstance(row['Input-Series'], str)):
                row['Input-Series'] = row['Input-Series'] + first_4_fields_NK1_5 + first_4_fields_NK1_6
                row['Output-Series'] = row['Input-Series'] 
                row['validate'] = 1
                row['rule']='Rule => NK1--> only assign first 4 fields of NK1|5 and 6'
        return row
'''
#################################################################
create custom index
for example MSH-1| MSH-2 and so on

#################################################################
'''
def custom_index(label, start, end):
    return [f'{label}-{i}' for i in range(start, end+1)]
'''
#################################################################
main process method
takes parameter directory_path inputfile outputfile

#################################################################
'''
def process(directory_path, inputfile, outputfile):
    keywords = [    "MSH",  "EVN",  "PID",  "PD1", 'NK1',  "PV1",  "GT1",  "DG1",  "PR1",  "IN1",  "IN2" , "AL1", "DB1",  "IN3", ]
    global DIRECTORY_PATH
    DIRECTORY_PATH = directory_path
    '''
    #################################################################
    process input file 
    and store in pandas input series
    
    #################################################################
    '''
    input_filename = DIRECTORY_PATH+inputfile
    text_file = open(input_filename,'r')
    data_input = text_file.read()
    splitted_data_input = data_input.split('|')
    #                  Commented for space inclusion
    #                  str_list_input = [x for x in splitted_data_input if x!=""]
    #
    result_input = []
    for x in splitted_data_input:
        if "\n" in x:
            result_input.append(x.split("\n"))
        else:
            result_input.append(x)
    flattened_list = []
    for item in result_input:
        if isinstance(item, list):
            flattened_list.extend(item)
        else:
            flattened_list.append(item)
    data_input = {}
    current_key = None
    for item in flattened_list:
        if item in keywords:
            current_key = item
            data_input[current_key] = []
        else:
            if current_key:
                data_input[current_key].append(item)
    indexed_data_input = {}
    indexed_data_key_IN = []
    for key, values in data_input.items():
        for i, value in enumerate(values):
            indexed_key_input = f"{key}-{i+1}"
            indexed_data_input[indexed_key_input] = value if i <len(values) else None
            indexed_data_key_IN.append(indexed_key_input)
    inputseries = pd.Series(data=indexed_data_input, index=indexed_data_key_IN, name="Data-Series-Input")
    inputseries = inputseries.str.upper()
    inputseries.index.name = "Index-Input"
    '''
    #################################################################
    process output file 
    and store in pandas output series
    
    #################################################################
    '''
    output_filename = directory_path+outputfile
    text_file = open(output_filename,'r')
    data_output = text_file.read()
    splitted_data_output = data_output.split('|')
    #                       Commented for space inclusion
    #                       str_list_output = [x for x in splitted_data_output if x!=""]
    #
    result_output = []
    for x in splitted_data_output:
        if "\n" in x:
            result_output.append(x.split("\n"))
        else:
            result_output.append(x)
    flattened_list = []
    for item in result_output:
        if isinstance(item, list):
            flattened_list.extend(item)
        else:
            flattened_list.append(item)

    data_output = {}
    current_key = None
    for item in flattened_list:
        if item in keywords:
            current_key = item
            data_output[current_key] = []
        else:
            if current_key:
                data_output[current_key].append(item)
    indexed_data_output = {}
    indexed_data_key_OUT = []
    for key, values in data_output.items():
        for i, value in enumerate(values):
            indexed_data_key_output = f"{key}-{i+1}"
            indexed_data_output[indexed_data_key_output] = value if i < len(values) else None
            indexed_data_key_OUT.append(indexed_data_key_output)
    outputseries = pd.Series(data=indexed_data_output, index=indexed_data_key_OUT , name="Data-Series-Output")
    outputseries = outputseries.str.upper()
    outputseries.index.name = 'Index-Output'
    try:
            df_validate = pd.DataFrame({"Input-Series" : inputseries, "Output-Series" : outputseries})
            df_validate['validate'] = df_validate['Input-Series'] == df_validate['Output-Series']
            df_validate['rule'] = ''
            df_validate.index.name = "Cols"
            '''
            #################################################################
            custom_index('MSH',1,1000) is intentionally kept as 1000  if 
            we have more data that spans till 1000 for example MSH-1000
            we can customize the value more than or less than what it is now
            This has negligible impact on performance
            
            #################################################################
            '''
            new_index = (custom_index('MSH',1,1000)
                         + custom_index('EVN',1,1000)
                         + custom_index('PID',1,1000)
                         + custom_index('PD1',1,1000)
                         + custom_index('NK1',1,1000)
                         + custom_index('PV1',1,1000)
                         + custom_index('GT1',1,1000)
                         + custom_index('DG1',1,1000)
                         + custom_index('PR1',1,1000)
                         + custom_index('IN1',1,1000)
                         + custom_index('IN2',1,1000)
                         + custom_index('AL1',1,1000)
                         + custom_index('DB1',1,1000)
                         + custom_index('IN3',1,1000))
            Path(directory_path+"\\RawData\\").mkdir(parents=True, exist_ok = True)
            Path(directory_path+"\\CleanedData\\").mkdir(parents=True, exist_ok = True)
            Path(directory_path+"\\RulesData\\").mkdir(parents=True, exist_ok = True)
            df_validate = df_validate.reindex(new_index)
            df_validate.to_excel(directory_path+"\\RawData\\"+inputfile+"-RAW-"+outputfile+"-"+".xlsx")
            df_cleanup = pd.read_excel(directory_path+"\\RawData\\"+inputfile+"-RAW-"+outputfile+"-"+".xlsx")
            df_cleanup = df_cleanup[(df_cleanup['validate'] == True) | (df_cleanup['validate'] == False) ]
            df_cleanup.to_excel(directory_path+"\\CleanedData\\"+inputfile+"-CLEAN-"+outputfile+"-"+".xlsx")
            df_rule = pd.read_excel(directory_path+"\\CleanedData\\"+inputfile+"-CLEAN-"+outputfile+"-"+".xlsx")
            df_rule_ = df_rule.apply(apply_rules, axis=1)
            df_rule_.to_excel(directory_path+"\\RulesData\\"+inputfile+"-RULES-"+outputfile+"-"+".xlsx")
    except Exception as e:
        print(traceback.format_exc())
        pass
'''
#################################################################
entry method to begin execution

#################################################################
'''
def main():
    cwd = os.path.dirname(os.path.realpath(__file__))
    Path(cwd+"\\Test Data\\").mkdir(parents=True, exist_ok=True)
    directory_path = cwd + "\\Test Data\\"
    print('Test Data Path is   :-)   ' + directory_path)
    all_files = os.listdir(directory_path)
    input_files = [f for f in all_files if f.endswith('_Input.txt')]
    output_files = [f for f in all_files if f.endswith('_Output.txt')]
    print("                                                                                                     ")
    print("PROCESSING STARTED")
    print("                                                                                                     ")
    for i, input_file in enumerate(input_files):
        output_file = output_files[i]
        print(f"Processing File  :-)  Input {input_file} :-) Output {output_file}")
        print("================================================")
        process(directory_path, input_file, output_file)
        print("                                                                                                     ")
    print("PROCESSING COMPLETED")
    print("                                                                                                     ")
'''
#################################################################
python __main__ method follows from here 
this is the internal python main function 
main execution point that 
calls the user main() function

#################################################################
'''
if __name__ == '__main__':
    main()
    input('Press Enter to Exit')
