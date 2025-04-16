import subprocess
import pandas as pd
import glob
import re
from sqlalchemy import create_engine

# Initialize global DataFrames
raw_df = pd.DataFrame()
processed_df = pd.DataFrame()


def extract_data(files):

    """
        Extract data from CSV 
    """
    global raw_df
    for file in files:
        if file.lower().endswith(".csv"):  # Fixed case-sensitivity
            df = pd.read_csv(file)
            df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
            raw_df = pd.concat([raw_df, df], ignore_index=True)

def cleaning_and_processing():

    """
        Clean and process the extracted data
    """
    
    global processed_df
    global raw_df

    # Remove duplicates
    raw_df.drop_duplicates(inplace=True)

    # Fill missing values (assignment fixed)
    raw_df['star_rating'] = raw_df['star_rating'].fillna(0)
    raw_df['helpful_votes'] = raw_df['helpful_votes'].fillna(0)
    raw_df['total_votes'] = raw_df['total_votes'].fillna(0)
    raw_df['marketplace'] = raw_df['marketplace'].fillna('Unknown')
    raw_df['review_headline'] = raw_df['review_headline'].fillna('No Title')
    raw_df['review_body'] = raw_df['review_body'].fillna('No Review')

    # Correct data types
    raw_df['review_date'] = pd.to_datetime(raw_df['review_date'], errors='coerce')
    raw_df['star_rating'] = pd.to_numeric(raw_df['star_rating'], downcast='integer', errors='coerce')

    # Standardize fields
    raw_df['marketplace'] = raw_df['marketplace'].str.upper()
    raw_df['product_title'] = raw_df['product_title'].str.title()
    raw_df['review_headline'] = raw_df['review_headline'].str.strip().str.title()

    # Storing in processed_df
    processed_df = raw_df.copy()

def clean_text(text):

    """
    Removing the html tags and special characters
    """
    
    if pd.isna(text):
        return ''
    text = str(text).lower().strip()
    text = re.sub(r'<.*?>', '', text)
    text = re.sub(r'[^a-z0-9\s.,!?]', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text

# Clean the review text
def transform_data():
    global processed_df

    """
        Transforming the cleaned Dataframe
    """

    # Format review date
    processed_df['review_date'] = processed_df['review_date'].dt.strftime('%Y-%m-%d')

    #Normalize text fields 
    processed_df['review_body'] = processed_df['review_body'].apply(clean_text)
    processed_df['review_headline'] = processed_df['review_headline'].apply(clean_text)

    #Create additional features 
    processed_df['review_date'] = pd.to_datetime(processed_df['review_date'], errors='coerce')
    processed_df['review_year'] = processed_df['review_date'].dt.year
    processed_df['review_month'] = processed_df['review_date'].dt.month
    processed_df['review_day'] = processed_df['review_date'].dt.day
    processed_df['review_weekday'] = processed_df['review_date'].dt.day_name()

#main_program

bucket_name = "s3://stg-project-etl1/"
destination_dir = "./sources/"
host = "amz-reviews-db.cfo8ia4sia26.ap-south-1.rds.amazonaws.com"
port = 3306
username = "admin"
password = "7904545874"
database = "reviews_db"

cmd = "/usr/local/bin/aws s3 cp "+ bucket_name +" "+ destination_dir +" --recursive"
result = subprocess.run(cmd,check = True, shell = True,capture_output=True, text=True)

files = glob.glob("./sources/*.csv")

extract_data(files)

cleaning_and_processing()

transform_data()

# Optional: Save cleaned data
processed_df.to_csv('cleaned_reviews.csv', index=False)

engine = create_engine(f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}")

processed_df.to_sql('reviews', con=engine, if_exists='append', index=False)
