import sqlite3
import pandas as pd


class InvoiceAnalyzer:
    def __init__(self, db_path):
        self.db_path = db_path

    def get_connection(self):
        return sqlite3.connect(self.db_path)

    def get_top_invoices_by_total_range(self, min_total, max_total, top_n=10):
        try:
            conn = self.get_connection()

            query = """
            SELECT 
                i.InvoiceId,
                i.CustomerId,
                c.FirstName || ' ' || c.LastName as CustomerName,
                i.InvoiceDate,
                i.Total,
                i.BillingCountry
            FROM Invoice i
            JOIN Customer c ON i.CustomerId = c.CustomerId
            WHERE i.Total BETWEEN ? AND ?
            ORDER BY i.Total DESC
            LIMIT ?
            """

            df = pd.read_sql_query(query, conn, params=(min_total, max_total, top_n))
            conn.close()

            return df

        except sqlite3.Error as error:
            print(f'Database error: {error}')
            return pd.DataFrame()

    def get_top_customers_by_invoice_count(self, top_n=10):
        try:
            conn = self.get_connection()

            query = """
            SELECT 
                c.CustomerId,
                c.FirstName || ' ' || c.LastName as CustomerName,
                c.Email,
                c.Country,
                COUNT(i.InvoiceId) as InvoiceCount,
                SUM(i.Total) as TotalSpent
            FROM Customer c
            JOIN Invoice i ON c.CustomerId = i.CustomerId
            GROUP BY c.CustomerId, c.FirstName, c.LastName, c.Email, c.Country
            ORDER BY InvoiceCount DESC, TotalSpent DESC
            LIMIT ?
            """

            df = pd.read_sql_query(query, conn, params=(top_n,))
            conn.close()

            return df

        except sqlite3.Error as error:
            print(f'Database error: {error}')
            return pd.DataFrame()

    def get_top_customers_by_total_value(self, top_n=10):
        try:
            conn = self.get_connection()

            query = """
            SELECT 
                c.CustomerId,
                c.FirstName || ' ' || c.LastName as CustomerName,
                c.Email,
                c.Country,
                COUNT(i.InvoiceId) as InvoiceCount,
                SUM(i.Total) as TotalSpent,
                AVG(i.Total) as AvgInvoiceValue
            FROM Customer c
            JOIN Invoice i ON c.CustomerId = i.CustomerId
            GROUP BY c.CustomerId, c.FirstName, c.LastName, c.Email, c.Country
            ORDER BY TotalSpent DESC, InvoiceCount DESC
            LIMIT ?
            """

            df = pd.read_sql_query(query, conn, params=(top_n,))
            conn.close()

            return df

        except sqlite3.Error as error:
            print(f'Database error: {error}')
            return pd.DataFrame()


# Demo usage
if __name__ == "__main__":
    # Database path
    db_path = "/Users/pniamie/Phương Nghi/Jun - 1st/ML/databases/Chinook_Sqlite.sqlite"

    # Create analyzer
    analyzer = InvoiceAnalyzer(db_path)

    print("=" * 80)
    print("INVOICE DATA ANALYSIS")
    print("=" * 80)

    # Test function 1: TOP 10 Invoices with total value from 5 to 20
    print("\n1. TOP 10 Invoices with total value from 5 to 20:")
    print("-" * 50)
    result1 = analyzer.get_top_invoices_by_total_range(5.0, 20.0, 10)
    if not result1.empty:
        print(result1.to_string(index=False))
    else:
        print("No data found")

    # Test function 2: TOP 10 customers with most invoices
    print("\n\n2. TOP 10 customers with most invoices:")
    print("-" * 50)
    result2 = analyzer.get_top_customers_by_invoice_count(10)
    if not result2.empty:
        print(result2.to_string(index=False))
    else:
        print("No data found")

    # Test function 3: TOP 10 customers with highest total invoice value
    print("\n\n3. TOP 10 customers with highest total invoice value:")
    print("-" * 50)
    result3 = analyzer.get_top_customers_by_total_value(10)
    if not result3.empty:
        print(result3.to_string(index=False))
    else:
        print("No data found")

    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETED")
    print("=" * 80)