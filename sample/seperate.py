import pandas as pd

# Read the CSV file
df = pd.read_csv('/home/pragnakalp-l-12/Desktop/viren_sir/gitHub/csv_to_rdf_converter/sample/20240408-171638.csv')

# Extract 'date' and 'time' columns
df['date'] = df['date time'].str.split().str[0]
df['time'] = df['date time'].str.split().str[1]

# Create a new DataFrame with only 'date' and 'time' columns
new_df = df[['date', 'time']]

# Write the new DataFrame to a new CSV file
new_df.to_csv('/home/pragnakalp-l-12/Desktop/viren_sir/gitHub/csv_to_rdf_converter/sample/modified_file.csv', index=False)
