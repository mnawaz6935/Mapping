import polars as pl
from rapidfuzz.fuzz import ratio


### Helper function to perform fuzzy matching
def fuzzy_match(s1, s2, threshold=80):
    if s1 and s2:  # Ensure both strings are not None
        return ratio(s1.lower(), s2.lower()) >= threshold
    return False


### Main logic
def main(csv1_path, csv2_path, output_path):
    # Load CSV files into Polars dataframes
    df1 = pl.read_csv(csv1_path, ignore_errors=True).rename({
        "Physical Address": "Address",
        "Physical City": "City",
        "Physical State": "State",
        "Physical ZIP": "ZIP",
    })
    df2 = pl.read_csv(csv2_path, ignore_errors=True).rename({
        "Entity Name": "EntityName",
        "Physical Address": "Address",
        "STREET ADDRESS": "StreetAddress",
        "CITY": "CITY",
        "ZIP": "ZIP",
    })

    # Convert dataframes to a list of dictionaries for custom logic
    d1 = df1.to_dicts()
    d2 = df2.to_dicts()

    matches = []
    all_matches = []

    # Iterate through Dataset 2
    for dt2 in d2:
        # Filter rows in Dataset 1 by ZIP code
        z_data = filter(lambda x: x["ZIP"] and x["ZIP"] == dt2["ZIP"], d1)

        # Filter by City (case-insensitive)
        c_data = filter(lambda x: x["City"] and x["City"].lower() == dt2["CITY"].lower(), z_data)

        # Fuzzy filter by Address similarity
        a_data = filter(
            lambda x: x["Address"] and fuzzy_match(x["Address"], dt2["StreetAddress"]),
            c_data,
        )

        # If there's exactly one match based on ZIP, City, and Address
        a_data = list(a_data)
        if len(a_data) == 1:
            matches.append(dict(
                Entityname=dt2["EntityName"],
                StreetAddress=dt2['StreetAddress'],
                City=dt2['CITY'],
                Zip=dt2['ZIP'],
                CorporateBusinessName=a_data[0]['Business Name'],
                CorporateAddress=a_data[0]['Address'],
                CorporateCity=a_data[0]['City'],
                CorporateZip=a_data[0]['ZIP']
            )
            )
        # If there are multiple matches, perform fuzzy comparison on Business Name
        elif len(a_data) > 1:
            n_data = list(filter(
                lambda x: fuzzy_match(x["Business Name"], dt2["EntityName"]),
                a_data,
            ))
            for d in n_data:
                matches.append(dict(
                    Entityname=dt2["EntityName"],
                    StreetAddress=dt2['StreetAddress'],
                    City=dt2['CITY'],
                    Zip=dt2['ZIP'],
                    CorporateBusinessName=d['Business Name'],
                    CorporateAddress=d['Address'],
                    CorporateCity=d['City'],
                    CorporateZip=d['ZIP']
                )
                )
        if len(matches) >= 100:
            all_matches.extend(matches)
            matches.clear()
            print("Saving Records")
            pl.DataFrame(all_matches).write_csv(output_path)
    # Convert matches to Polars DataFrame and save to CSV
    pl.DataFrame(all_matches).write_csv(output_path)


# Run the main function
if __name__ == "__main__":
    csv1_path = "VIRGINIA AtoZ CORRECTED ADDRESSES 11.28.2024.csv"  # Path to first CSV
    csv2_path = "EFGH CORRECTED ADDRESSES.csv"  # Path to second CSV
    output_path = "output.csv"  # Path to save the resulting matches CSV
    main(csv1_path, csv2_path, output_path)

# import dask.dataframe as dd
# import hashlib
#
# # File paths
# file1_path = 'VIRGINIA AtoZ CORRECTED ADDRESSES 11.28.2024.csv'  # Path to the first CSV file
# file2_path = 'EFGH CORRECTED ADDRESSES.csv'  # Path to the second CSV file
# output_path = 'matched_records.csv'  # Path to save matched results
#
# # Columns to match and normalize
# key_columns_file1 = ['Physical Address', 'Physical City', 'Physical ZIP']
# key_columns_file2 = ['Physical Address', 'CITY', 'ZIP']
#
#
# # Function to normalize and hash keys (case-insensitive and consistent)
# def generate_hashed_key(df, cols):
#     """
#     Combine specific columns into a single normalized key and hash it for efficient comparisons.
#     """
#     # Normalize (lowercase + strip whitespace) and concatenate
#     key_series = df[cols].astype(str).apply(lambda row: '|'.join(row.str.strip().str.lower()), axis=1)
#     # Apply MD5 hashing
#     return key_series.apply(hashlib.md5).apply(lambda x: x.hexdigest())
#
#
# # Load CSV files as Dask DataFrames
# csv1 = dd.read_csv(file1_path, dtype=str)  # Ensure consistent dtype (string)
# csv2 = dd.read_csv(file2_path, dtype=str)  # Ensure consistent dtype (string)
#
# # Normalize column names to avoid case sensitivity issues
# csv1 = csv1.rename(columns={
#     'Business Name': 'business_name',
#     'Physical Address': 'physical_address',
#     'Physical City': 'physical_city',
#     'Physical State': 'physical_state',
#     'Physical ZIP': 'physical_zip'
# })
# csv2 = csv2.rename(columns={
#     'Entity Name': 'entity_name',
#     'Physical Address': 'physical_address',
#     'STREET ADDRESS': 'street_address',
#     'CITY': 'physical_city',
#     'ZIP': 'physical_zip'
# })
#
# # Create hashed keys for matching
# csv1['key'] = generate_hashed_key(csv1, ['physical_address', 'physical_city', 'physical_zip'])
# csv2['key'] = generate_hashed_key(csv2, ['physical_address', 'physical_city', 'physical_zip'])
#
# # Perform an inner join on the hashed key
# matches = dd.merge(csv1, csv2, on='key', how='inner', suffixes=('_file1', '_file2'))
#
# # Filter matches based on the business name (case-insensitive)
# matches = matches[
#     matches['business_name'].str.strip().str.lower() == matches['entity_name'].str.strip().str.lower()
#     ]
#
# # Save results to a CSV file
# print("Performing computations and saving results...")
# matches.compute().to_csv(output_path, index=False)
#
# print(f"Matching complete. Results saved to {output_path}.")
