import gspread
from oauth2client.service_account import ServiceAccountCredentials
import mysql.connector
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Google Sheets API setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(os.getenv('GOOGLE_CREDENTIALS_PATH'), scope)
client = gspread.authorize(creds)

# Open the Google Sheet
spreadsheet = client.open_by_url(os.getenv('GOOGLE_SHEET_URL'))
sheet = spreadsheet.sheet1

# Fetch all data from the Google Sheet
data = sheet.get_all_values()

# Get column names from the first row of the sheet
columns = data[0]

# Connect to MySQL
mydb = mysql.connector.connect(
    host=os.getenv('MYSQL_HOST'),
    user=os.getenv('MYSQL_USER'),
    password=os.getenv('MYSQL_PASSWORD'),
    database=os.getenv('MYSQL_DATABASE')
)

cursor = mydb.cursor()

# Define the SQL query to check for existing records
check_query = f"SELECT COUNT(*) FROM your_table_name WHERE " + " AND ".join([f"{col} = %s" for col in columns])

# Define the SQL query to insert data
insert_query = f"INSERT INTO your_table_name ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"

# Insert new data into MySQL
for row in data[1:]:  # Skip the header row
    cursor.execute(check_query, row)
    count = cursor.fetchone()[0]
    if count == 0:
        cursor.execute(insert_query, row)

# Commit the transaction
mydb.commit()

# Close the connection
cursor.close()
mydb.close()
