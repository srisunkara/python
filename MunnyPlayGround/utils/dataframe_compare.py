import pandas as pd

# Function to compare two DataFrames with customization
def compare_dataframes(df1, df2, keys, exclude_cols=None):
    if exclude_cols is None:
        exclude_cols = []

    # Identify non-key columns excluding excluded ones
    non_key_cols = [col for col in df1.columns if col not in keys + exclude_cols]

    # Merge with suffixes
    merged = df1.merge(df2, on=keys, how='outer', suffixes=('_left', '_right'))

    # Determine match mask
    def row_matches(row):
        for col in non_key_cols:
            val_left = row[f"{col}_left"]
            val_right = row[f"{col}_right"]
            if pd.isna(val_left) and pd.isna(val_right):
                continue
            if val_left != val_right:
                return False
        return True

    match_mask = merged.apply(row_matches, axis=1)

    # Create reordered columns: keys first, then each col with left/right
    ordered_cols = keys.copy()
    for col in non_key_cols + exclude_cols:
        if f"{col}_left" in merged.columns and f"{col}_right" in merged.columns:
            ordered_cols.extend([f"{col}_left", f"{col}_right"])

    matching_df = merged[match_mask][ordered_cols].copy()
    differences_df = merged[~match_mask][ordered_cols].copy()

    # Add a column listing which columns differ
    def diff_cols(row):
        diffs = []
        for col in non_key_cols:
            val_left = row[f"{col}_left"]
            val_right = row[f"{col}_right"]
            if not (pd.isna(val_left) and pd.isna(val_right)) and val_left != val_right:
                diffs.append(col)
        return diffs

    differences_df['diff_columns'] = differences_df.apply(diff_cols, axis=1)

    return matching_df, differences_df


# Test
df1 = pd.DataFrame({
    'id': [1, 2, 3, 4],
    'name': ['Alice', 'Bob', 'Charlie', 'David'],
    'score': [85, 90, 88, 92],
    'note': ['ok', 'ok', 'ok', 'ok']
})

df2 = pd.DataFrame({
    'id': [1, 2, 3, 5],
    'name': ['Alice', 'Bob', 'Charles', 'Eve'],
    'score': [85, 90, 88, 95],
    'note': ['ok', 'ok', 'changed', 'ok']
})

matching_df, differences_df = compare_dataframes(df1, df2, keys=['id', 'name'], exclude_cols=['note'])

print("Matching Rows:")
print(matching_df)

print("******************************************************************")
print("Differences:")
print(differences_df)
