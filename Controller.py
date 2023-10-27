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
import duckdb
import re
import pandas
from sensorfabric.json.Flatten import flatten
from sensorfabric.json.Raw import scanJsonFile

whitelist = ['sensorkit-accelerometer']

def Controller(dbpath : str, path : str):
    """
    Description
    -----------
    The main controller method. Start by calling this.
    Parameters
    ----------
    path : str
        Path to the export directory from MDH.
    """

    dirs = os.listdir(path)
    for dir in dirs:
        if dir in whitelist:
            _snake(dbpath, '/'.join([path, dir]))

def _snake(dbpath : str, path : str):
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
        _ingestData(dbpath, table, participantidentifier, path)
    else:
        # If it is not the end we must snake!
        if os.path.isdir(path):
            for dir in os.listdir(path):
                _snake(dbpath, '/'.join([path, dir]))

def _ingestData(dbpath : str, table : str, participantidentifier : str, path : str):
    """
    This method scans path, for all the data files (.)
    Method takes the table name, participantidentifier and the path to the parent
    directory with all the data files (can be .json or .json.gz)
    """

    print(path)
    with duckdb.connect(dbpath) as conn:
        (ret, json_buffer) = scanJsonFile(path)
        if not ret:
            print(f"Malformed json at {path}")
            return
        frame = flatten(json_buffer[0])
        # Add the participantID to this frame.
        pframe = pandas.DataFrame({'participantidentifier':[participantidentifier] * frame.shape[0]})
        frame = pandas.concat([frame, pframe], axis=1)
        frame.to_csv('tempdump.csv', index=False)
        # Check to see the table is present.
        try:
            conn.sql(f"describe {table}")
            # If we are here it means the exception was not raised and the table
            # is present and we just copy the data into it.
            conn.sql(f"COPY {table} FROM 'tempdump.csv' (AUTO_DETECT true)")
        except duckdb.duckdb.CatalogException:
            # We don't have the table. So let us go ahead and make the table.
            conn.sql(f"CREATE TABLE {table} AS SELECT * FROM 'tempdump.csv'")
