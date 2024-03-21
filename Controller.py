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
import duckdb
from sensorfabric.json.Flatten import flatten
from sensorfabric.json.Raw import scanJsonFile
from sensorfabric.json.Raw import prettyPrintSchema
from athena import df_to_athena_table

#whitelist = ['sensorkit-accelerometer', 'sensorkit-rotation-rate', 'sensorkit-ambient-light-sensor']
whitelist = ['sensorkit-ambient-light-sensor']

def Controller(schema : str, path : str, storage : str, method=['local']):
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
    storage : str
        Path to the storage json file.
    """
    dirs = os.listdir(path)
    for dir in dirs:
        if dir in whitelist:
            _snake(schema, '/'.join([path, dir]), storage, method)

def _snake(schema : str, path : str, storage : str, method=['local']):
    """
    Because this recursively snakes and finds its way
    through the depths of hell .. I mean directories.
    """

    # If we hit a compressed json file we stop and call the ingester.
    pattern = r"\.json.gz$"
    if re.search(pattern, path):
        pathbuff = path.split('/')
        participantidentifier = pathbuff[-3]
        table = pathbuff[-5].replace('-', '_')
        _ingestData(schema, storage, table, participantidentifier, path, method)
    else:
        # If it is not the end we must snake!
        if os.path.isdir(path):
            for dir in os.listdir(path):
                _snake(schema, '/'.join([path, dir]), storage, method)

def _ingestData(schema : str, storage : str, table : str, participantidentifier : str, path : str, method=['local']):
    """
    This method scans path, for all the data files (.)
    It also takes the schema file so it can enforce the schema before uploading it to AWS.
    """

    print(path)
    (ret, json_buffer) = scanJsonFile(path)
    if not ret:
        print(f"Malformed json at {path}")
        return
    #prettyPrintSchema(json_buffer[0])
    frame = flatten(json_buffer[0], fill=True)
    #for k in frame.keys():
    #    print(f"{k} - {len(frame[k])}")
    frame = pandas.DataFrame(frame)
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

    storage = _loadStorageConfig(storage)
    if storage is None:
        return

    """
    Based on the method we check if we need to add to AWS
    """
    if 'aws' in method:
        res = df_to_athena_table(frame, storage['s3_path'], storage['database'], table, partition_cols=['participantidentifier'])
        print(res)

    """
    Add to local duckdb if the method indicates local
    """
    if 'local' in method:
        frame.to_csv(f"{table}.csv", index=False)
        with duckdb.connect(storage['localdb']) as conn:
            # First check and see if the table is present.
            # If it is then we just append the data to that table.
            try:
                conn.sql(f"describe {table}")
                # If we are here it means the exception was not raised and we
                # can copy data into the existing table.
                conn.sql(f"INSERT INTO {table} BY NAME SELECT * FROM frame")
            except duckdb.duckdb.CatalogException:
                # We don't have the table. So let us create it and populate it with
                # the new data.
                conn.sql(f"CREATE TABLE {table} AS SELECT * FROM frame")

def _loadStorageConfig(storage : str) -> dict:
    af = open(storage, 'r')
    if af is None:
        print('Fatal Error : Could not read storage configuration file')
        return None
    data = json.loads(af.read())

    return data
