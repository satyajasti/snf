from snowflake_connection import get_athena_connection

def test_athena_connection():
    try:
        print("🔌 Testing Athena connection...")

        conn, schema = get_athena_connection("config.json")
        cursor = conn.cursor()

        # Replace this query with a real table if you want
        test_query = f"SELECT 1 AS test_column"
        cursor.execute(test_query)
        result = cursor.fetchall()

        print(f"✅ Query ran successfully. Result: {result}")

        if result:
            print("🎉 Athena connection and query test passed.")
        else:
            print("⚠️ Query executed but returned no rows.")

    except Exception as e:
        print(f"❌ Athena connection test failed: {e}")

if __name__ == "__main__":
    test_athena_connection()
