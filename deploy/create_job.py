import boto3


def create_ingestion_job(config, region=None):
    """
    Creates ingestion job on glue
    """
    region = config["primary_region"] if region is None else region
    fm_prefix = config["fm_prefix"]
    project = config["project_name"]
    client = boto3.client("glue", region_name=region)
    job_name = f"{fm_prefix}-data-ingestion"
    # delete the previous glue job
    client.delete_job(JobName=job_name)
    code_bucket = f"s3://{fm_prefix}-code-{region}"
    # location of the glue job in the code bucket
    script_location = f"{code_bucket}/{project}/ingestion/dataIngestion.py"
    default_args = {
        "--extra-py-files": f"{code_bucket}/{project}/dependencies/utils.zip,{code_bucket}/{project}/dependencies/connector.zip",
        "--extra-files": f"{code_bucket}/{project}/ingestion/config/globalConfig.json",
        "--TempDir": f"{code_bucket}/temporary/",
        "--additional-python-modules": "psycopg2-binary"
    }
    # create the new glue job
    response = client.create_job(
        Name=job_name,
        Description="Data Ingestion Job",
        Role="dl-fmwrk-glue-role",
        Command={
            "Name": "glueetl",
            "ScriptLocation": script_location,
            "PythonVersion": "3",
        },
        DefaultArguments=default_args,
        Timeout=15,
        GlueVersion="2.0",
        NumberOfWorkers=10,
        WorkerType="G.2X",
    )
    return response


def create_glue_jobs(config, region=None):
    """
    Main entry method to create multiple glue jobs
    """
    try:
        create_ingestion_job(config, region)
        return True
    except Exception as e:
        print(e)
        return False
