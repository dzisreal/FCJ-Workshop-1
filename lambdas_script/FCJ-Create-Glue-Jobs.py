import boto3
import json

# Khởi tạo client CodeCommit
codecommit = boto3.client('codecommit')
glue_client = boto3.client('glue')
codepipeline = boto3.client('codepipeline')
    
def get_files_jobs_name(folder_path,repository_name):
    lst_files_name = []
    lst_jobs_name = []
    
    responseGetFolder = codecommit.get_folder(
        repositoryName=repository_name,
        folderPath=folder_path
        )
    
    for file in responseGetFolder["files"]:
        lst_files_name.append(file["relativePath"])
        lst_jobs_name.append(file["relativePath"][:-3])
        
    return lst_jobs_name,lst_files_name

def check_exist_and_create(lst_jobs_name):
    for job_name in lst_jobs_name:
        try:
            # Gọi get_job với tên job cụ thể
            response = glue_client.get_job(JobName=job_name)
    
            # Nếu job tồn tại, update job với script mới nhất
            if 'Job' in response:
                continue
        except glue_client.exceptions.EntityNotFoundException:
            # Nếu job không tồn tại, create job với script
            create_job(job_name)

def create_job(job_name):
    folder_config_path = 'jobs_config/'
    repository_name = 'FCJ-Workshop-1'
    responseGetFile = codecommit.get_file(
            repositoryName=repository_name,
            filePath=f"{folder_config_path}cfg_{job_name}.json"
        )
    file_content = responseGetFile['fileContent']
        
    #convert str to json
    job_config = json.loads(file_content.decode('utf-8'))
    
    try:
        response = glue_client.create_job(
                Name=job_name,
                Role="arn:aws:iam::039178755962:role/FCJ-Glue-Role",
                Command={
                    'Name': 'glueetl',
                    'ScriptLocation': f"s3://fcj-project-bucket/workshop1/jobs_script/{job_name}.py",
                    'PythonVersion': '3'
                },
                GlueVersion = job_config["Glue_Version"],
                WorkerType = job_config["Worker_Type"],
                NumberOfWorkers = job_config["Number_Of_Workers"],
                ExecutionProperty = {
                    'MaxConcurrentRuns': job_config["Max_Concurrent_Runs"]
                }
            )
    except Exception as e:
        print(f"Error creating Glue job: {e}")
        raise e

def lambda_handler(event, context):
    # Thông tin repository và file
    repository_name = 'FCJ-Workshop-1'
    folder_script_path = 'jobs_script/'
    
    
    lst_jobs_name,lst_files_name = get_files_jobs_name(folder_script_path,repository_name)
    
    check_exist_and_create(lst_jobs_name)
    job_id = event['CodePipeline.job']['id']
    codepipeline.put_job_success_result(jobId = job_id)