# PySpark Beginner Exercises

## Exercise Set 1: Basic DataFrame Operations

### Exercise 1: Create and Explore DataFrames

**Task:** Create a DataFrame with information about 5 books including title, author, price, and rating.

**Requirements:**
1. Create the DataFrame with proper schema
2. Display the DataFrame
3. Print the schema
4. Show the row count

**Expected Output:**
```
+-------------------+----------------+------+------+
|title              |author          |price |rating|
+-------------------+----------------+------+------+
|Python Programming |John Smith      |45.99 |4.5   |
|...                |...             |...   |...   |
+-------------------+----------------+------+------+

Total books: 5
```

**Solution:** See `solutions/exercise_01_solution.py`

---

### Exercise 2: Filtering and Selection

**Task:** Using the books DataFrame from Exercise 1:

1. Filter books with price > $30
2. Select only title and price columns
3. Filter books with rating >= 4.0
4. Count how many books meet each criteria

**Starter Code:**
```python
# Your code here
expensive_books = books_df.filter(...)
high_rated = books_df.filter(...)
```

---

### Exercise 3: Adding Calculated Columns

**Task:** Add the following columns to the books DataFrame:

1. `discounted_price`: 20% discount on original price
2. `category`: "Premium" if price > $40, else "Standard"
3. `rating_category`: "Excellent" if rating >= 4.5, "Good" if >= 4.0, else "Average"

**Hints:**
- Use `withColumn()` method
- Use `when()` for conditional logic
- Use `round()` for decimal places

---

### Exercise 4: Aggregations

**Task:** Create a DataFrame of student grades:

| Student | Subject | Score |
|---------|---------|-------|
| Alice   | Math    | 85    |
| Alice   | Science | 92    |
| Bob     | Math    | 78    |
| Bob     | Science | 88    |
| Charlie | Math    | 95    |
| Charlie | Science | 89    |

Calculate:
1. Average score per student
2. Highest score in each subject
3. Overall class average
4. Number of students per subject

---

### Exercise 5: Joins

**Task:** You have two DataFrames:

**Customers:**
| customer_id | name    | city      |
|-------------|---------|-----------|
| 1           | Alice   | New York  |
| 2           | Bob     | London    |
| 3           | Charlie | Paris     |

**Orders:**
| order_id | customer_id | amount |
|----------|-------------|--------|
| 101      | 1           | 250    |
| 102      | 2           | 180    |
| 103      | 1           | 320    |
| 104      | 3           | 150    |

**Requirements:**
1. Join customers with orders
2. Calculate total order amount per customer
3. Find customers with total orders > $300
4. Add customer city to the result

---

## Exercise Set 2: Data Cleaning

### Exercise 6: Handling Missing Values

**Task:** Given this DataFrame with missing values:

```python
data = [
    ("Alice", 25, 50000),
    ("Bob", None, 60000),
    ("Charlie", 35, None),
    (None, 28, 55000),
    ("David", 32, 70000)
]
df = spark.createDataFrame(data, ["name", "age", "salary"])
```

**Requirements:**
1. Count nulls in each column
2. Drop rows where name is null
3. Fill missing ages with the average age
4. Fill missing salaries with 0

---

### Exercise 7: Deduplication

**Task:** Remove duplicate records from this DataFrame:

```python
data = [
    ("Alice", "alice@email.com"),
    ("Bob", "bob@email.com"),
    ("Alice", "alice@email.com"),  # Duplicate
    ("Charlie", "charlie@email.com"),
    ("Bob", "bob@email.com"),  # Duplicate
]
```

**Requirements:**
1. Remove duplicates based on email
2. Count records before and after deduplication
3. Show the clean DataFrame

---

## Exercise Set 3: String Operations

### Exercise 8: Text Processing

**Task:** Clean and process customer names:

```python
data = [
    (" alice smith  ", "alice.smith@email.com"),
    ("BOB JONES", "bob@email.com"),
    ("  Charlie Brown", "charlie@email.com"),
]
```

**Requirements:**
1. Trim whitespace from names
2. Convert names to title case (First Letter Caps)
3. Extract first name and last name into separate columns
4. Convert email to lowercase

**Expected Output:**
```
+-------------+---------+--------+---------------------+
|original_name|first_name|last_name|email               |
+-------------+---------+--------+---------------------+
|Alice Smith  |Alice    |Smith   |alice.smith@email.com|
|Bob Jones    |Bob      |Jones   |bob@email.com        |
|Charlie Brown|Charlie  |Brown   |charlie@email.com    |
+-------------+---------+--------+---------------------+
```

---

## Exercise Set 4: Real-World Scenarios

### Exercise 9: E-commerce Sales Analysis

**Task:** Analyze sales data:

```python
sales_data = [
    ("2024-01-01", "Product A", "Electronics", 5, 100.00),
    ("2024-01-01", "Product B", "Books", 3, 15.00),
    ("2024-01-02", "Product A", "Electronics", 2, 100.00),
    ("2024-01-02", "Product C", "Electronics", 1, 250.00),
    ("2024-01-03", "Product B", "Books", 5, 15.00),
]
columns = ["date", "product", "category", "quantity", "unit_price"]
```

**Requirements:**
1. Calculate total_amount (quantity * unit_price) for each row
2. Find total sales per product
3. Find total sales per category
4. Find which day had the highest sales
5. Calculate average order value per category

---

### Exercise 10: Customer Segmentation

**Task:** Segment customers based on purchase behavior:

```python
customer_purchases = [
    ("CUST001", 1200, 5),   # customer_id, total_spent, num_orders
    ("CUST002", 350, 2),
    ("CUST003", 800, 4),
    ("CUST004", 150, 1),
    ("CUST005", 2500, 12),
]
```

**Create segments:**
- **Premium**: total_spent > $1000 OR num_orders > 10
- **Regular**: total_spent > $500 OR num_orders > 3
- **Occasional**: Everyone else

**Additional calculations:**
1. Count customers in each segment
2. Calculate average spending per segment
3. Calculate percentage of customers in each segment

---

## Challenge Exercises

### Challenge 1: Complex Transformation Pipeline

Build a data pipeline that:
1. Reads data with schema validation
2. Handles missing values appropriately
3. Adds calculated columns
4. Filters invalid records
5. Performs aggregations
6. Writes output in Parquet format

### Challenge 2: Performance Optimization

Given a slow query:
```python
df.rdd.map(lambda x: (x[0], x[1].upper())).toDF()
```

Rewrite it using DataFrame API for better performance.

### Challenge 3: Window Functions

Calculate running totals and moving averages for a sales dataset. Use window functions to:
1. Calculate cumulative sales per product
2. Calculate 7-day moving average
3. Rank products by sales within each category

---

## Solutions

All solutions are available in the `solutions/` directory:
- `exercise_01_solution.py`
- `exercise_02_solution.py`
- ... and so on

---

## Tips for Success

1. **Read the documentation:** https://spark.apache.org/docs/latest/api/python/
2. **Use built-in functions:** Check `pyspark.sql.functions`
3. **Test incrementally:** Test each transformation step by step
4. **Use .show():** Verify intermediate results
5. **Check Spark UI:** Monitor performance at http://localhost:4040

---

## Next Steps

After completing these exercises:
1. Move to intermediate exercises
2. Practice with larger datasets
3. Explore Spark SQL
4. Learn about window functions
5. Study performance optimization

**Happy Learning! 🚀**
