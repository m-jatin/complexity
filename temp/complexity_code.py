#Output in a excel file
#working on sub-query code embedment
#Making a code little robust - uppercase/lowercase
#Assigning scores to each file
#Create a ascending list as per effort required

import re, os,os.path 
import pip
pip.main(['install', 'Pandas'])
import pandas as pd
import working_github_subfolder

# Replace with your actual values
repo_link = "https://api.github.com/repos/m-jatin/complexity"
token = "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"
local_path = os.getcwd()
subfolder_name = "complexity"
ETL_N_Workflows_folder_loc = local_path

working_github_subfolder.github_to_local(repo_link , token ,local_path)


def count_subqueries_in_sql(query):
    """Counts the number of subqueries (nested SELECT statements) in a SQL query string.

    Args:
        query (str): The SQL query string to analyze.

    Returns:
        int: The number of subqueries found in the query.
    """

    subquery_pattern = r"""\b(?:SELECT\b|\bFROM\b)\s*\([^;)]*\)\b"""  # Improved pattern for capturing subqueries
    subqueries = re.findall(subquery_pattern, query, re.IGNORECASE)
    return len(subqueries)

def count_statements(file_path):
    """Counts the occurrences of SQL statements and subqueries in a file.

    Args:
        file_path (str): The path to the file containing SQL statements.

    Returns:
        dict: A dictionary containing counts for each statement and subquery.

    Raises:
        FileNotFoundError: If the file is not found.
    """

    total_lines = 0
    statements = [
        "create volatile", "create table", "insert", "update", "merge", "delete", "select", "export", "import",
        "with", "rank", "dense_rank", "add_months", "months_between", "last_day", "next_day", "cast", "coalesce",
        "zeroiffull", "min", "max", "sum", "avg", "count", "substr", "length", "upper", "lower", "trim", "like",
        "case", "distinct", "row_number", "top", "qualify", "locate", "grouping", "group by", "order by", "join",
        "where", "del", "sel"
    ]
    #initilizing with 0 for all statements
    statement_counts = dict.fromkeys(statements + ['total_lines', 'subquery_count'], 0)

    try:
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read().lower()
            
            
            lines = content.splitlines()
            content = "\n".join(line for line in lines if not line.startswith("--"))
            
            for statement in statements:
                statement_counts[statement] = content.count(statement)
            statement_counts['total_lines'] = sum(1 for line in content.splitlines() if line.strip())
            statement_counts['subquery_count'] = count_subqueries_in_sql(content)

    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")

    return statement_counts

def process_folder_count_statement(folder_path,output_file_name,path):
    """Counts the occurrences of SQL statements and subqueries in each file in a folder.

    Args:
        folder_path (str): The path to the folder containing SQL script files.

    Returns:
        dict: A nested dictionary containing counts for each statement and subquery in each file.

    Raises:
        FileNotFoundError: If the folder is not found.
    """

    count_statement_results = {}

    if not os.path.isdir(folder_path):
        raise FileNotFoundError(f"Folder not found: {folder_path}")

    for root, dirs, files in os.walk(folder_path):
        for file_name in files:
            if file_name.endswith('.txt'):  # Handle only .txt files
                file_path = os.path.join(root, file_name)
                count_statement_results[file_path] = count_statements(file_path)

    dataframe_list =[]
    for key,value in count_statement_results.items() :
        # print(f'key = {key}')
        # print(f'ETL_N_Workflows_folder_loc = {ETL_N_Workflows_folder_loc}')
        value["01. Filepath"] = key[len(path) + 1:]
        dataframe_list.append(value)
        
    df = pd.DataFrame(dataframe_list)
    
    #bringing filename to first
    last_column = df.columns[-1]
    df = df[[last_column] + list(df.columns[:-1])]
    
    # Export the DataFrame to a CSV file
    # df.transpose().to_csv(output_file_name, header=False)   
    df.to_csv(output_file_name, index=False) 
    return df.transpose() 
    
          
                


# =============================================================================
# Modify below as required
# =============================================================================




#The folder that conatins the folders ETL Scripts and Workflows
etl_scripts_path = os.path.join(ETL_N_Workflows_folder_loc, "2. Input_ETL_Scripts")
workflow_path = os.path.join(ETL_N_Workflows_folder_loc, "3. Input_Workflows_Scripts")


Output_Dir = os.path.join(ETL_N_Workflows_folder_loc,"4. Output_Complexity_Scores")
if os.path.exists(Output_Dir)==False:
    os.mkdir(Output_Dir)
    
# Overall_Score_Dir = os.path.join(Output_Dir,"1.Overall_Scores")
# if os.path.exists(Overall_Score_Dir)==False:
#     os.mkdir(Overall_Score_Dir)    
    
Occurence_Count_Dir = os.path.join(Output_Dir,"1.Occurence_Counts")
if os.path.exists(Occurence_Count_Dir)==False:
    os.mkdir(Occurence_Count_Dir)    
    
Multiplied_Dir = os.path.join(Output_Dir,"2.Multiplied_Scores")
if os.path.exists(Multiplied_Dir)==False:
    os.mkdir(Multiplied_Dir)    

etl_output = os.path.join(Occurence_Count_Dir, "etl_output.csv")
workflows_output = os.path.join(Occurence_Count_Dir, "workflow_output.csv")

#Delete if exists
if os.path.exists(etl_output):
    os.remove(etl_output)
if os.path.exists(workflows_output):
    os.remove(workflows_output)

# Count number of files in below folder - ETL_Scripts and Workflows
files_in_etl = len([name for name in os.listdir(etl_scripts_path) if os.path.isfile(os.path.join(etl_scripts_path, name))])
files_in_workflows=len([name for name in os.listdir(workflow_path) if os.path.isfile(os.path.join(workflow_path, name))])    

print(f"Total_Files in ETL Folder --- {files_in_etl}")
print(f"Total_Files in Workflows Folder --- {files_in_workflows}")

etl_statement_summary = process_folder_count_statement(etl_scripts_path, etl_output,path = etl_scripts_path )
workflow_staement_summary = process_folder_count_statement(workflow_path,workflows_output,path = workflow_path )





# =============================================================================
# Assigning Scores to each file
# =============================================================================


lookup_df= pd.read_csv(os.path.join(Output_Dir,'lookup_scores.csv'),index_col='Statement')

def assign_score(df_name):
    statement_with_lookup = df_name.join(lookup_df, how ='left')  

    first_row = statement_with_lookup.iloc[0]
    statement_with_lookup = statement_with_lookup.iloc[1:]
    # Convert numeric columns to numeric data types
    for col in statement_with_lookup.columns:
        try:
            statement_with_lookup[col] = pd.to_numeric(statement_with_lookup[col])
        except Exception as e:
            print(e)    


    statement_with_lookup['Score'] = statement_with_lookup['Score'].astype(float)
    column_to_multiply = statement_with_lookup['Score']
    
    for col in statement_with_lookup.columns:
      if col != column_to_multiply.name:
          statement_with_lookup[col] *= column_to_multiply * 0.1

    # Update the row in the DataFrame
    statement_with_lookup.loc['02. Total Score'] = statement_with_lookup.sum()
    
    # statement_with_lookup.loc['total_lines'] = statement_with_lookup.loc['total_lines'] * 10
    # statement_with_lookup.loc['subquery_count'] = statement_with_lookup.loc['subquery_count'] * 10

    
    # Re-add the computed row
    statement_with_lookup = pd.concat([statement_with_lookup, first_row.to_frame().T])
    
    replacements = {'total_lines': '03. Total_Lines',
                    'subquery_count' : '04. Subquery_Count',
                    'create table':'05. Create Table',
                    'create volatile':'06. Create Volatile',
                    'insert':'07. Insert',
                    'update':'08. Update',
                    'del':'09. Del',
                    'delete':'10. Delete',
                    'merge':'11. Merge',
                    'avg':'12. Avg',
                    'count':'13. Count',
                    'max':'14. Max',
                    'min':'15. Min',
                    'substr':'16. Sum',
                    'sum':'17. Substr',
                    'select':'18. Select',
                    'sel' : '19. Sel',
                    'cast':'20. Cast',
                    'coalesce':'21. Coalesce',
                    'dense_rank':'22. Dense_Rank',
                    'distinct':'23. Distinct',
                    'group by':'24. Group By',
                    'grouping':'25. Grouping',
                    'last_day':'26. Last_Day',
                    'length':'27. Length',
                    'like':'28. Like',
                    'locate':'29. Locate',
                    'lower':'30. Lower',
                    'months_between':'31. Months_Between',
                    'next_day':'32. Next_Day',
                    'order by':'33. Order By',
                    'rank':'34. Rank',
                    'row_number':'35. Row_Number',
                    'trim':'36. Trim',
                    'upper':'37. Upper',
                    'add_months' : '38. Add_months',
                    'export' : '39. Export',
                    'import' : '40. Import',
                    'join' : '41. Join',
                    'qualify' : '42. Qualify',
                    'case':'43. Case',
                    'top' : '44. Top',
                    'where' : '45. Where',
                    'with' : '46. With',
                    'zeroiffull' : '47. Zeroiffull'}
                    
    statement_with_lookup = statement_with_lookup.rename(replacements,axis = 0)
    statement_with_lookup = statement_with_lookup.sort_index()
       
    inverse_replacements = {
    '01. Filepath':'Filepath',
    '02. Total Score':'Total Score',
    '03. Total_Lines':'Total_Lines',
    '04. Subquery_Count':'Subquery_Count',
    '05. Create Table':'Create Table',
    '06. Create Volatile':'Create Volatile',
    '07. Insert':'Insert',
    '08. Update':'Update',
    '09. Del':'Del',
    '10. Delete':'Delete',
    '11. Merge':'Merge',
    '12. Avg':'Avg',
    '13. Count':'Count',
    '14. Max':'Max',
    '15. Min':'Min',
    '16. Sum':'Sum',
    '17. Substr':'Substr',
    '18. Select':'Select',
    '19. Sel':'Sel',
    '20. Cast':'Cast',
    '21. Coalesce':'Coalesce',
    '22. Dense_Rank':'Dense_Rank',
    '23. Distinct':'Distinct',
    '24. Group By':'Group By',
    '25. Grouping':'Grouping',
    '26. Last_Day':'Last_Day',
    '27. Length':'Length',
    '28. Like':'Like',
    '29. Locate':'Locate',
    '30. Lower':'Lower',
    '31. Months_Between':'Months_Between',
    '32. Next_Day':'Next_Day',
    '33. Order By':'Order By',
    '34. Rank':'Rank',
    '35. Row_Number':'Row_Number',
    '36. Trim':'Trim',
    '37. Upper':'Upper',
    '38. Add_months':'Add_months',
    '39. Export':'Export',
    '40. Import':'Import',
    '41. Join':'Join',
    '42. Qualify':'Qualify',
    '43. Case':'Case',
    '44. Top':'Top',
    '45. Where':'Where',
    '46. With':'With',
    '47. Zeroiffull':'Zeroiffull'}

    
    statement_with_lookup = statement_with_lookup.rename(inverse_replacements,axis = 0)    
    
    
    statement_with_lookup =statement_with_lookup.transpose()
    statement_with_lookup = statement_with_lookup.sort_values(by='Total Score')
    statement_with_lookup =statement_with_lookup.drop('Score').transpose()
    return statement_with_lookup
    


ETL_Scored_df =assign_score(etl_statement_summary)
Workflow_Scored_df =assign_score(workflow_staement_summary)



# ETL_Scored_df.transpose().to_csv((os.path.join(Multiplied_Dir,'Complexity_Score_ETL.csv')), index=False)  
# Workflow_Scored_df.transpose().to_csv((os.path.join(Multiplied_Dir,'Complexity_Score_Workflow.csv')), index=False)  


# ETL_Scored_df.iloc[0:2].transpose().to_csv((os.path.join(Overall_Score_Dir,'Overall_ETL.csv')), index=False)  
# Workflow_Scored_df.iloc[0:2].transpose().to_csv((os.path.join(Overall_Score_Dir,'Overall_Workflow.csv')), index=False)  



ETL_Scored_df.transpose().to_csv((os.path.join(Multiplied_Dir,'Complexity_Score_ETL.csv')), index=False)  
Workflow_Scored_df.transpose().to_csv((os.path.join(Multiplied_Dir,'Complexity_Score_Workflow.csv')), index=False)  





