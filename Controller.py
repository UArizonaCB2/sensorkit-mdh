"""
    Date : 10-26-2023
    Author : Shravan Aras
    Email : shravanaras@arizona.edu

    This is where is all starts. The main controller that runs the ingestion loop.
    The input for this module will the high level data directory for a single export
    from MDH. These directories when unzipped have the format
    RK.[orgid].[ProjectName]_[StartDate]-[EndDate]

    Inside this, we follow the following path.
    table-name -> device -> participantidentifier -> internalidentifier -> datafiles.
"""

import os
import re
import pandas
import json
from sensorfabric.json.Flatten import flatten
from sensorfabric.json.Raw import scanJsonFile
from athena import df_to_athena_table

whitelist = ['sensorkit-accelerometer']

def Controller(schema : str, path : str, aws : str):
    """
    Description
    -----------
    The main controller method. Start by calling this.
    Parameters
    ----------
    schema : str
        Path to the schema file.
    path : str
        Path to the export directory from MDH.
    aws : str
        Path to the AWS json file.
    """
    dirs = os.listdir(path)
    for dir in dirs:
        if dir in whitelist:
            _snake(schema, '/'.join([path, dir]), aws)

def _snake(schema : str, path : str, aws : str):
    """
    Because this recursively snakes and finds its way
    through the depths of hell .. I mean directories.
    """

    # If we hit a compressed json file we stop and call the ingester.
    pattern = r"\.json.gz$"
    if re.search(pattern, path):
        pathbuff = path.split('/')
        participantidentifier = pathbuff[-3]
        table = pathbuff[-5].split('-')[-1]
        _ingestData(schema, aws, table, participantidentifier, path)
    else:
        # If it is not the end we must snake!
        if os.path.isdir(path):
            for dir in os.listdir(path):
                _snake(schema, '/'.join([path, dir]), aws)

def _ingestData(schema : str, aws : str, table : str, participantidentifier : str, path : str):
    """
    This method scans path, for all the data files (.)
    It also takes the schema file so it can enforce the schema before uploading it to AWS.
    """

    print(path)
    (ret, json_buffer) = scanJsonFile(path)
    if not ret:
        print(f"Malformed json at {path}")
        return
    frame = flatten(json_buffer[0])
    # Add the participantID to this frame.
    pframe = pandas.DataFrame({'participantidentifier':[participantidentifier] * frame.shape[0]})
    frame = pandas.concat([frame, pframe], axis=1)

    # Open the schema file and parse the JSON contents.
    sf = open(schema, 'r')
    if sf is None:
        print('Fatal Error : Could not read schema file')
    schema_data = json.loads(sf.read())
    for col in frame.columns:
        if not(frame.dtypes[col] == schema_data[col]):
            frame[col] = frame[col].astype(schema_data[col])
    sf.close()

    # Go ahead load the AWS configuration and send the file over.
    af = open(aws, 'r')
    if af is None:
        print('Fatal Error : Could not read AWS configuration file')
    aws_data = json.loads(af.read())

    res = df_to_athena_table(frame, aws_data['s3_path'], aws_data['database'], table, partition_cols=['participantidentifier'])
    print(res)

    frame.to_csv('tempdump.csv', index=False)
