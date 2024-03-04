import pandas as pd
import os
import csv
import pyarrow as pa
import pyarrow.parquet as pq


# Set the directory 
os.chdir('/home/onyxia/Visualisation-Parcoursup/')

# read the data 
TestData = pd.read_csv('test.csv',delimiter=';')

# Specify the output Parquet file
parquet_file = 'output_data.parquet'

# Convert the DataFrame to a PyArrow table
table = pa.Table.from_pandas(TestData)

# Write the PyArrow table to Parquet
pq.write_table(table, parquet_file)

print(f"CSV data has been converted to Parquet format and saved to {parquet_file}.")

# Specify the Parquet file path
parquet_file = 'output_data.parquet'

# Read the Parquet file into a PyArrow table
table = pq.read_table(parquet_file)

print(table.schema)

# You can also iterate through the column names and their types
for name, dtype in zip(table.schema.names, table.schema.types):
    print(f"Column: {name}, Type: {dtype}")

# Get the number of rows in the Parquet file
num_rows = table.num_rows

print(f"Number of observations (rows) in the Parquet file: {num_rows}")

# Read the Parquet file into a Pandas DataFrame
ParqData = pd.read_parquet(parquet_file)

# Group by 'Enseignements de spécialité' and sum the counts
specialite_counts = TestData.groupby('Enseignements de spécialité').sum()

# Plot the data
specialite_counts.plot(kind='bar', y='Nombre de candidats bacheliers ayant accepté une proposition d admission', figsize=(10, 6))
plt.xlabel('Enseignements de spécialité')
plt.ylabel('Nombre de candidats bacheliers ayant accepté une proposition d admission')
plt.title('Nombre de candidats bacheliers ayant accepté une proposition d admission par enseignement de spécialité')
plt.xticks(rotation=45, ha='right')  # Rotate x-axis labels for better visibility
plt.tight_layout()  # Adjust layout to prevent clipping of labels
plt.show()




#TestData = table.to_pandas()

# Now you can work with the DataFrame
# For example, you can use the head() method to display the first few rows
# print(TestData.head())