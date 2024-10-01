
import sys
import os
import os.path as path

parent_directory = os.path.abspath(path.join(__file__ ,"../../"))
sys.path.append(parent_directory)


# Importing the ingestion pipeline
from pipelines.stage_01_data_ingestion_pipeline import DataIngestionPipeline
from pipelines.stage_02_data_cleaning_pipeline import DataCleaningPipeline
from pipelines.stage_03_data_transformation import DataTransformationPipeline

STAGE_NAME = "Data Ingestion"

try:
   print(">>>>>> Stage {STAGE_NAME} started <<<<<<") 
   data_ingestion = DataIngestionPipeline()
   data_ingestion.main()
   print(">>>>>> Stage {STAGE_NAME} completed <<<<<<\n\nx==========x")
except Exception as e:
        print((e))
        raise e
     
STAGE_NAME = "DATA CLEANING"
try:
   print(">>>>>> Stage {STAGE_NAME} started <<<<<<",STAGE_NAME) 
   data_cleaning = DataCleaningPipeline()
   data_cleaning.main()
   print(">>>>>> Stage {STAGE_NAME} completed <<<<<<",STAGE_NAME)
except Exception as e:
        print(e)
        raise e

STAGE_NAME = "DATA TRANSFORMATION"

try:
   print(">>>>>> Stage {STAGE_NAME} started <<<<<<",STAGE_NAME) 
   data_transformation = DataTransformationPipeline()
   data_transformation.main()
   print(">>>>>> Stage {STAGE_NAME} completed <<<<<<",STAGE_NAME)
except Exception as e:
        print(e)
        raise e