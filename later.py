def fetch_asin_tokeyword(asin):
    conn = None
    try:
        db_config = config.get_database_config()
        # Connect to your database
        conn = psycopg2.connect(
            dbname=db_config["dbname"],
            user=db_config["user"],
            password=db_config["password"],
            host=db_config["host"],
        )
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # Execute a query
        cur.execute(
            """
            SELECT a.*
            FROM products_smartscount a
            LEFT JOIN products_relevant_smartscounts b
            ON a.asin = b.asin_relevant AND a.sys_run_date = b.sys_run_date
            WHERE a.sys_run_date = %s AND b.asin = %s
            ORDER BY a.estimated_monthly_revenue DESC
            LIMIT 20
            """,
            (
                str(current_time_gmt7.strftime("%Y-%m-%d")),
                asin,
            ),
        )

        # Fetch all results
        results = cur.fetchall()
        # Extract the asin values from the results
        asins = [item["asin"] for item in results]
        subset_size = 10
        subsets = [
            ", ".join(asins[i : i + subset_size])
            for i in range(0, len(asins), subset_size)
        ]
        asin_parent = asin
        return asin_parent, subsets
    except Exception as e:
        print(f"Database error: {e}")
        return []
    finally:
        if conn:
            conn.close()


# Example usage
asin = "B07VPWR7YY"
asin_parent, subsets = fetch_asin_tokeyword(asin)
print("Subsets:", subsets)
print("B_asin:", asin_parent)
