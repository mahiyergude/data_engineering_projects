import json
import os

def list_relative_files(folder_path):
    file_paths = []
    # Walk through the folder recursively
    for root, _, files in os.walk(folder_path):
        for file in files:
            # Get the relative file path
            relative_path = os.path.relpath(os.path.join(root, file), start=folder_path)
            file_paths.append(relative_path)
    return file_paths


def generate_glue_job_config(local_folder_with_scripts, path_project_in_bucket, temp_dir, glue_job_name, glue_role_arn, glue_version="3.0"):
    """
    Generate a Glue job configuration dynamically.
    
    Args:
        local_folder_with_scripts(str): local folder with scripts to use
        path_project_in_bucket (str): Prefix in the S3 bucket where the scripts are stored.
        temp_dir (str): S3 path for temporary Glue files.
        glue_job_name (str): Name of the Glue job.
        glue_role_arn (str): ARN of the Glue role.
        glue_version (str): Version of Glue (default: 3.0).
        
    Returns:
        dict: Glue job configuration JSON.
    """
    # List all files in the S3 path
    # response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=script_prefix)
    files = list_relative_files(local_folder_with_scripts)

    files_py = [path_project_in_bucket+"/"+file for file in files if ".py" in file]
    if not files:
        raise ValueError("No .py files found in the specified S3 path.")

    # Assume the first script is the main script
    main_script = path_project_in_bucket+"/"+"main.py"
    extra_files_py = files_py.copy().remove(main_script)
    extra_py_files = ",".join(extra_files_py) if (extra_files_py!=None) else ""

    # Create the Glue job configuration
    glue_job_config = {
        "Name": glue_job_name,
        "Role": glue_role_arn,
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": main_script
        },
        "DefaultArguments": {
            "--TempDir": temp_dir,
            "--job-bookmark-option": "job-bookmark-enable",
            "--enable-metrics": "",
            "--extra-py-files": extra_py_files
        },
        "MaxRetries": 1,
        "Timeout": 2880,
        "GlueVersion": glue_version
    }

    return glue_job_config

# Example usage
if __name__ == "__main__":
    local_folder_with_scripts = "/home/claudiocm/Git/data_engineering_projects/AWS/etl-flight-data"
    path_project_in_bucket = "scripts-personal-projects/aws_glue/etl-flight-data"
    temp_dir = "s3://your-bucket-name/temp/"
    glue_job_name = "etl-flight-data"
    glue_role_arn = "arn:aws:iam::123456789012:role/GlueRole"
    glue_version = 5.0

    try:
        glue_job_config = generate_glue_job_config(
            local_folder_with_scripts,
            path_project_in_bucket, 
            temp_dir, 
            glue_job_name, 
            glue_role_arn,
            glue_version
            )
        
        glue_job_config_file_path = os.path.join(local_folder_with_scripts,"glue_job_config.json")

        with open(glue_job_config_file_path, "w") as json_file:
            json.dump(glue_job_config, json_file, indent=4)

    except Exception as e:
        print(f"Error: {e}")
