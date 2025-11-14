"""
Classic Word Count Example in PySpark
Demonstrates basic transformations and actions.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, regexp_replace, col

def create_spark_session():
    """Create and return a SparkSession."""
    return SparkSession.builder \
        .appName("Word Count Example") \
        .master("local[*]") \
        .getOrCreate()

def word_count_rdd(spark, text_file):
    """
    Word count using RDD API (traditional approach).

    Args:
        spark: SparkSession
        text_file: Path to text file

    Returns:
        RDD with (word, count) pairs
    """
    # Read text file as RDD
    lines = spark.sparkContext.textFile(text_file)

    # Split lines into words, filter empty, and convert to lowercase
    words = lines.flatMap(lambda line: line.split()) \
                .filter(lambda word: len(word) > 0) \
                .map(lambda word: word.lower().strip())

    # Remove punctuation
    words_clean = words.map(lambda word: ''.join(char for char in word if char.isalnum()))

    # Create (word, 1) pairs and reduce by key
    word_counts = words_clean.map(lambda word: (word, 1)) \
                            .reduceByKey(lambda a, b: a + b)

    # Sort by count descending
    sorted_counts = word_counts.sortBy(lambda x: x[1], ascending=False)

    return sorted_counts

def word_count_dataframe(spark, text_file):
    """
    Word count using DataFrame API (modern approach - recommended).

    Args:
        spark: SparkSession
        text_file: Path to text file

    Returns:
        DataFrame with word and count columns
    """
    # Read text file as DataFrame
    df = spark.read.text(text_file)

    # Split lines into words and explode to separate rows
    words_df = df.select(
        explode(split(col("value"), " ")).alias("word")
    )

    # Clean words: lowercase and remove punctuation
    clean_words = words_df.select(
        lower(regexp_replace(col("word"), "[^a-zA-Z]", "")).alias("word")
    ).filter(col("word") != "")

    # Group by word and count
    word_counts = clean_words.groupBy("word").count() \
                            .orderBy(col("count").desc())

    return word_counts

def main():
    """Main execution function."""
    # Create SparkSession
    spark = create_spark_session()

    # Sample text for demonstration
    sample_text = """
    Apache Spark is a unified analytics engine for large-scale data processing.
    Spark provides high-level APIs in Java, Scala, Python and R.
    PySpark is the Python API for Apache Spark.
    Spark runs on Hadoop, Apache Mesos, Kubernetes, standalone, or in the cloud.
    """

    # Create temporary file with sample text
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
        f.write(sample_text)
        temp_file = f.name

    print("=" * 60)
    print("WORD COUNT COMPARISON: RDD vs DataFrame")
    print("=" * 60)

    # Method 1: RDD approach
    print("\n--- Method 1: RDD API ---")
    rdd_result = word_count_rdd(spark, temp_file)
    print("Top 10 words (RDD):")
    for word, count in rdd_result.take(10):
        print(f"  {word:15} : {count}")

    # Method 2: DataFrame approach (recommended)
    print("\n--- Method 2: DataFrame API (Recommended) ---")
    df_result = word_count_dataframe(spark, temp_file)
    print("Top 10 words (DataFrame):")
    df_result.show(10, truncate=False)

    # Performance comparison
    print("\n--- Performance Insights ---")
    print("✅ DataFrame API is recommended because:")
    print("  - Optimized by Catalyst optimizer")
    print("  - Better memory management (Tungsten)")
    print("  - More readable code")
    print("  - Typically 2-5x faster than RDD")

    # Cleanup
    import os
    os.unlink(temp_file)

    # Stop SparkSession
    spark.stop()
    print("\n✅ Word count example completed!")

if __name__ == "__main__":
    main()
