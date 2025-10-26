def check_unique_values(df):
    import pyspark.sql.functions as F

    print('\nChecking unique values from each column:')

    for col in df.columns:
        print(f'Unique value count: {df.select(F.col(col)).distinct().count()}')
        df.select(col).distinct().show(truncate=False)

def basic_data_profiling(df):
    import pyspark.sql.functions as F

    print('\nBasic data Profiling:')
    
    for col, dtype in df.dtypes:
        print(f"\nColumn: {col}")
        df.select(
            F.count(F.col(col)).alias("count"),
            F.countDistinct(F.col(col)).alias("distinct"),
            F.min(F.col(col)).alias("min"),
            F.max(F.col(col)).alias("max")
        ).show(truncate=False)