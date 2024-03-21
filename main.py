import sys
import os
from sensorfabric.mdh import MDH
from Controller import Controller
from dotenv import load_dotenv
from glob import glob
import zipfile
import re

if __name__ == '__main__':

   load_dotenv()

   schema_file = sys.argv[1]
   storage_config = sys.argv[2]

   # Get account secret, account name and project ID from environment variables
   mdh_account_secret = os.environ.get('MDH_SECRET')
   mdh_account_name = os.environ.get('MDH_ACCOUNT_NAME')
   mdh_project_id = os.environ.get('MDH_PROJECT_ID')

   # MDH fields are required.
   if mdh_account_name is None or mdh_account_secret is None or mdh_project_id is None:
      print('Fatal: Environment variables related MDH must be set')
      exit(0)

   # Get the start and end dates from environment variables.
   start_date = os.environ.get('START_DATE')
   end_date = os.environ.get('END_DATE')

   # For now the base path to store the raw exports is just a local directory.
   base_path = './mdh_exports'
   if not os.path.isdir(base_path):
      os.mkdir(base_path)

   # Create an instance of the MDH class
   mdh = MDH(mdh_account_secret, mdh_account_name, mdh_project_id)

   if start_date==None and end_date==None:
      pass
      mdh.getExportData(base_path=base_path)

   elif start_date is not None and end_date is not None:
      # Calling this function will save all exports between the specified dates to the base path
      pass
      mdh.getExportData(date_range=(start_date, end_date), base_path=base_path)

   """
   If the AWS environment variables are correctly set then we will go ahead and enable
   uploading to AWS directly.
   """
   method = ['local']
   if not(os.getenv('AWS_ACCESS_KEY_ID') is None or os.getenv('AWS_SECRET_ACCESS_KEY') is None or os.getenv('AWS_DEFAULT_REGION') is None):
      print("Ready to use AWS")
      method.append('aws')
   else:
      print("AWS backend is not being used")

   print(method)

   folders = glob(f"{base_path}/*")
   zpattern = r'\.zip$'
   for folder in folders:
      if not re.search(zpattern, folder):
         continue
      efolderName = folder.split('.zip')[0]
      os.makedirs(efolderName, exist_ok=True)
      with zipfile.ZipFile(folder, 'r') as zfile:
         zfile.extractall(efolderName)

         Controller(schema_file, efolderName, storage_config, method)
