#!/bin/bash


# Define workspace location and AWS profile (customize as needed)
WORKSPACE_LOCATION=/home/$USER/Git/data_engineering_projects_temp/AWS  # Adjust this if needed
PROFILE_NAME=default        # Set your AWS profile name
DATALAKE_FORMATS=iceberg



docker run -it -v ~/.aws:/home/glue_user/.aws \
 -v $WORKSPACE_LOCATION:/home/glue_user/workspace/ \
 -e AWS_PROFILE=$PROFILE_NAME \
 -e DATALAKE_FORMATS=$DATALAKE_FORMATS \
 -e DISABLE_SSL=true \
 --rm -p 4040:4040 \
 -p 18080:18080 \
 --name glue_pyspark amazon/aws-glue-libs:glue_libs_4.0.0_image_01 pyspark