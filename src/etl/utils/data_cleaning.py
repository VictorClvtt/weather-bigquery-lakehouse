import pyspark.sql.functions as F

def remove_null_values(df):
    print('\nRemoving null values from each column:')
    
    for col, dtype in df.dtypes:
        if dtype not in ['string', 'date', 'timestamp']:
            null_count = df.filter(F.col(col).isNull() | F.isnan(F.col(col))).count()
            
            if null_count > 0:
                df = df.filter(~(F.col(col).isNull() | F.isnan(F.col(col))))
                print(f"✅ {col}: {null_count} null/NaN values removed.")
            else:
                print(f"☑️ {col}: No null/NaN values found.")
        else:
            null_count = df.filter(F.col(col).isNull()).count()
            
            if null_count > 0:
                df = df.filter(F.col(col).isNotNull())
                print(f"✅ {col}: {null_count} null values removed.")
            else:
                print(f"☑️ {col}: No null values found.")
    
    return df

def remove_whitespace(df):
    print('\nRemoving whitespace in values from each column:')

    string_columns = [col for col, dtype in df.dtypes if dtype == 'string']
    for col in string_columns:
        changed_count = df.filter(
            F.col(col) != F.trim(F.col(col))
        ).count()

        if changed_count > 0:
            df = df.withColumn(col, F.trim(F.col(col)))

            print(f"✅ {col}: {changed_count} lines trimmed")
        else:
            print(f"☑️ {col}: No lines to trim")
    return df

def drop_duplicates(df):
    print('\nDropping duplicate lines:')

    initial_count = df.count()
    df = df.dropDuplicates()
    final_count = df.count()

    dropped_lines = max(initial_count - final_count, 0)

    print(f"✅ {dropped_lines} duplicate lines dropped.")
    return df

def remove_columns(df, columns_to_remove: list):
    print('\nRemoving unnecessary columns:')

    for col in columns_to_remove:
        df = df.drop(col)
        print(f'✅ Column "{col}" removed successfully.')

    return df