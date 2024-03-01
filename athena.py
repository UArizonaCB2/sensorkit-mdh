import pandas as pd
import awswrangler as wr
import boto3

"""
Author : YuanJea Hew
Org : University of Arizona
Created On : 2-29-2024
"""

def df_to_athena_table(df, s3_path, database, table, partition_cols=None, mode="append"):
    """
    Function to write DataFrame to S3 in parquet format and create/update an Athena table in the AWS Glue Catalog.

    Parameters:
    df (str) - Pandas DataFrame
    s3_path (str) - S3 path where the parquet files will be stored
    database (str) - AWS Glue/Athena database name
    table (str) - AWS Glue/Athena table name
    partition_cols (list) - (Default) "None" if no need for partioning. List of columns to be partioned
    mode (str) - (Default) "append" to keep any possible existing table or  "overwrite" to recreate any possible existing table
    """

    # Initialize variables to track parquet files writing status
    parquet_success, parquet_status = False , ""
    # Initialize variable to track Athena table creation/update status
    athena_table_success, athena_table_status = False, ""

    try:
        # Save the DataFrame to S3 in parquet format
        wr.s3.to_parquet(
            df=df,
            path=s3_path,
            dataset=True,
            database=database,
            table=table,
            partition_cols=partition_cols
        )
        parquet_success = True
        parquet_status = "Successfully written parquet files"

    except Exception as e:
         parquet_status = f"Error in writing parquet files: {repr(e)}"

    # Run this if parquet files has been written 
    if parquet_success:
        try:
            # Dynmaically infer column types from df
            columns_types = {col: pd.api.types.infer_dtype(df[col]) for col in df.columns}

            # TODO: If the table already exists then we don't create a new table.

            # Create a parquet table in AWS Glue Catalog
            wr.catalog.create_parquet_table(
                database=database,
                table=table,
                path=s3_path,
                columns_types=columns_types,
                mode=mode
            )
            athena_table_success = True
            athena_table_status = "Succesfully created/updated Athena table"

        except Exception as e:
            athena_table_status = f"Error in creating/updating Athena table: {repr(e)}"
    else:
        athena_table_status = "Athena table not created/updated"

    return parquet_status, athena_table_status

if __name__ == "__main__":

    # Example pandas DataFrame
    df = pd.DataFrame({"id": ["1", "2", "3", "1", "2", "3"],
                    "value": ["A", "B", "C", "D", "E", "F"]})

    # Specify inputs
    s3_path = "s3://cb2-yuanjea-development/dataset/test3/"
    database = "cb2-yuanjea-development"
    table = "test3_table_1"

    # Call function
    parquet_status, athena_table_status = df_to_athena_table(df=df, s3_path=s3_path, database=database, table=table)

    # Check parquet and Athena table status
    print(parquet_status)
    print(athena_table_status)
