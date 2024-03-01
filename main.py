import sys
from Controller import Controller

if __name__ == '__main__':
   export_directory = sys.argv[1]
   schema_file = sys.argv[2]
   aws_file = sys.argv[3]
   Controller(schema_file, export_directory, aws_file)
