import duckdb

con = duckdb.connect("watchlist.duckdb")

con.execute("CREATE SCHEMA IF NOT EXISTS raw")

con.execute("""
    CREATE TABLE IF NOT EXISTS raw.raw_prices AS
    SELECT * FROM read_json_auto('raw_prices.json')
""")

con.execute("""
    CREATE TABLE IF NOT EXISTS raw.raw_news AS
    SELECT * FROM read_json_auto('raw_news.json')
""")

con.close()