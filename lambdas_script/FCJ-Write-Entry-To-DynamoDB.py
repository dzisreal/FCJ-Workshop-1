import boto3
import json

# Khởi tạo client CodeCommit
codecommit = boto3.client('codecommit')
dynamodb = boto3.resource('dynamodb')

def read_file_write_to_dynamodb(path_entry_folder,repository_name):
    lst_files_path = [] #list file path of folder
    lst_table_name = [] #list name file + entry of its
    table_name = "FCJ-Entry-Table"
    table = dynamodb.Table(table_name)
    
    responseGetFolder = codecommit.get_folder(
        repositoryName=repository_name,
        folderPath=path_entry_folder
        )
    
    
    for file in responseGetFolder["files"]:
        lst_files_path.append(file["absolutePath"])
        lst_table_name.append(file["relativePath"])
        
    for i in range(len(lst_files_path)) :
        # Lấy file từ CodeCommit
        responseGetFile = codecommit.get_file(
            repositoryName=repository_name,
            filePath=lst_files_path[i]
        )
        file_content = responseGetFile['fileContent']
        
        #convert str to json
        # lst_entry[i].append(file_content.decode('utf-8')) #json.loads(file_content.decode('utf-8'))
        
        item = {
            "table_name": lst_table_name[i][6:-5],
            "table_entry": file_content.decode('utf-8')
        }
        try:
            response = table.put_item(Item=item)
        except Exception as e:
            print(f"ERROR: {e}")
            raise e
    
def lambda_handler(event, context):
    # Thông tin repository và file
    repository_name = 'FCJ-Workshop-1'
    folder_path = 'jobs_entry/'
    
    read_file_write_to_dynamodb(folder_path,repository_name)
