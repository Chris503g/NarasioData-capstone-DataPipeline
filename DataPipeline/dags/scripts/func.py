import psycopg2
import numpy as np
import pandas as pd
from google.cloud import storage

def denormalize_artist_revenue():
    # === Establishing the connection ===
    conn = psycopg2.connect(
        database="narasio_pipeline", user='postgres', password='password', host='host.docker.internal', port='5433'
    )
    # === Reating a cursor object using the cursor() method ===
    cursor = conn.cursor()     
    # === Create denormalize Song table ===
    create_table_artist_revenue = ''' 
                create table if not exists Artist_revenue as (
                select
	                a."ArtistId" ,
	                a."Name" as "Artist Name",
	                a2."Title" as "Title Album" ,
	                t."Name" as "Track Name" ,
                    count(il."Quantity") * il."UnitPrice" as "total_price" , 
	                il."InvoiceLineId" ,
	                i."InvoiceId" ,
	                i."InvoiceDate"
                from
	                public."Artist" a
                join public."Album" a2 
                on
	                a."ArtistId" = a2."ArtistId"
                join public."Track" t 
                on
	                a2."AlbumId" = t."AlbumId"
                join public."InvoiceLine" il 
                on
	                t."TrackId" = il."TrackId"
                join public."Invoice" i 
                on 
	                il."InvoiceId" = i."InvoiceId"
                group by 
	                a."ArtistId",
	                a2."Title" ,
	                t."Name" ,
	                il."InvoiceLineId" ,
	                i."InvoiceId"
                order by
	                a."ArtistId" asc
                )
                ''' 
    # === Execute query ===
    cursor.execute(create_table_artist_revenue)
    conn.commit()
    # === Closing the connection ===
    conn.close()
    

def denormalize_song():
    # === Establishing the connection ===
    conn = psycopg2.connect(
        database="narasio_pipeline", user='postgres', password='password', host='host.docker.internal', port='5433'
    )
    # === Reating a cursor object using the cursor() method ===
    cursor = conn.cursor()     
    # === Create denormalize Song table ===
    create_table_song = ''' 
                create table if not exists Song as (
                select 
                    a."ArtistId" ,
                    a."Name" as "Artist_Name" ,
                    a2."AlbumId" ,
                    a2."Title" as "Album_Title" ,
                    t."TrackId" ,
                    t."Name" as "Track_Name" ,
                    t."UnitPrice" ,
                    t."Bytes" ,
                    g."GenreId" ,
                    g."Name" as "Genre_Name"
                from 
                    public."Artist" a
                join
                    public."Album" a2 
                on
                    a."ArtistId" = a2."ArtistId"
                join 
                    public."Track" t 
                on 
                    a2."AlbumId" = t."AlbumId"
                join 
                    public."Genre" g 
                on
                    t."GenreId" = g."GenreId"
                group by 
                    a."ArtistId" ,
                    a2."AlbumId" ,
                    t."TrackId" ,
                    g."GenreId"
                order by
                    a."ArtistId" asc
                )
                ''' 
    # === Execute query ===
    cursor.execute(create_table_song)
    conn.commit()
    # === Closing the connection ===
    conn.close()
    
    
def denormalize_transaction():
    # === Establishing the connection ===
    conn = psycopg2.connect(
        database="narasio_pipeline", user='postgres', password='password', host='host.docker.internal', port='5433'
    )
    # === Reating a cursor object using the cursor() method ===
    cursor = conn.cursor()     
    
    # === Create denormalize Song table ===
    create_table_transaction = ''' 
                create table if not exists Transaction as (
                select 
	                il."TrackId" ,
	                i."InvoiceId" ,
	                i."CustomerId" ,
	                i."InvoiceDate" ,
	                i."Total" , 
	                concat(c."FirstName", ' ', c."LastName") as "Full_Name" ,
	                c."Address" ,
	                c."City" ,
	                c."State" ,
	                c."Country" ,
	                c."PostalCode" ,
	                c."Email"
                from 
	                public."InvoiceLine" il
                join
	                public."Invoice" i 
                on
	                il."InvoiceId" = i."InvoiceId" 
                join 
	                public."Customer" c 
                on 
	                i."CustomerId" = c."CustomerId" 
                )
                ''' 
    # === Execute query ===
    cursor.execute(create_table_transaction)
    conn.commit()
    # === Closing the connection ===
    conn.close()
    
# === Get business questions from denormalized tables ===           
def create_bq_one():
    try:
        # === Establishing the connection === 
        conn = psycopg2.connect(
            database="narasio_pipeline", user='postgres', password='password', host='host.docker.internal', port='5433'
        )
        # === Reating a cursor object using the cursor() method ===
        cursor = conn.cursor()     
        
         # === Get business question one === 
        question_one = ''' 
               select 
                    ar."Artist Name" , 
                    sum(ar.total_price) , 
                    ar."InvoiceDate"
                from 
                    public."artist_revenue" ar
                group by 
                    ar."Artist Name" , 
                    ar."InvoiceDate"
                order by ar."InvoiceDate" asc
        '''
        
        # === Execute First Business Query === 
        cursor.execute(question_one)
        print("1. Berapakah total pendapatan dari penjualan yang didapatkan per harinya")
        records = cursor.fetchall()
        
        # === Transform to DataFrame model & to .csv === 
        df_one = pd.DataFrame(records, columns=['artist_name', 'pendapatan_per_hari', 'InvoiceDate'])
        df_one.index = np.arange(1, len(df_one)+1)
        df_one.to_csv('/opt/airflow/dags/files/bq_one.csv', index=False)
        
        print("bq_one.csv has been created")
        # print("File location using os.getcwd():", os.getcwd())
        # print(f"File location using __file__ variable: {os.path.realpath(os.path.dirname(__file__))}")
        
    finally:
        # === Closing the connection === 
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")


def create_bq_two(): 
    try:
        # === Establishing the connection === 
        conn = psycopg2.connect(
            database="narasio_pipeline", user='postgres', password='password', host='host.docker.internal', port='5433'
        )
        # === Reating a cursor object using the cursor() method ===
        cursor = conn.cursor()     
        
         # === Get business question one === 
        question_two = ''' 
                select 
                    s."Artist_Name" , 
                    count(s."Track_Name") as "total_tracks" ,
                    s."Genre_Name"
                from 
                    public.song s    
                group by 
                    s."Artist_Name" ,
                    s."Genre_Name" 
                order by total_tracks desc
        '''
        
        # === Execute First Business Query === 
        cursor.execute(question_two)
        print("2. Berapakah total pendapatan dari penjualan yang didapatkan per harinya")
        records = cursor.fetchall()
        
        # === Transform to DataFrame model & to .csv === 
        df_two = pd.DataFrame(records, columns=['artist_name', 'total_tracks', 'Genre_Name'])
        df_two.index = np.arange(1, len(df_two)+1)
        df_two.to_csv('/opt/airflow/dags/files/bq_two.csv', index=False)
        
        print("bq_two.csv has been created")
    finally:
        # === Closing the connection === 
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")
  
        
def create_bq_third(): 
    try:
        # === Establishing the connection === 
        conn = psycopg2.connect(
            database="narasio_pipeline", user='postgres', password='password', host='host.docker.internal', port='5433'
        )
        # === Reating a cursor object using the cursor() method ===
        cursor = conn.cursor()     
        
         # === Get business question one === 
        question_third = ''' 
                select 
                    ar."Artist Name" ,
                    sum(ar.total_price) as "total_pendapatan"
                from 
                    public.artist_revenue ar
                group by ar."Artist Name", ar.total_price
                order by total_pendapatan desc
        '''

        # === Execute First Business Query === 
        cursor.execute(question_third)
        print("3. List Artist dengan pendapatan paling tinggi ke paling rendah berdasarkan penjualan track")
        records = cursor.fetchall()
        
        # === Transform to DataFrame model & to .csv === 
        df_third = pd.DataFrame(records, columns=['artist_name', 'total_pendapatan'])
        df_third.index = np.arange(1, len(df_third)+1)
        df_third.to_csv('/opt/airflow/dags/files/bq_third.csv', index=False)
        
        print("bq_third.csv has been created")
        
    finally:
        # === Closing the connection === 
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")


def create_bq_fourth(): 
    try:
        # === Establishing the connection === 
        conn = psycopg2.connect(
            database="narasio_pipeline", user='postgres', password='password', host='host.docker.internal', port='5433'
        )
        # === Reating a cursor object using the cursor() method ===
        cursor = conn.cursor()     
        
         # === Get business question one === 
        question_fourth = ''' 
                select 
                    t."City" ,
                    count(t."City") as "kota_customer"
                from 
                    public."transaction" t
                group by 
                    t."City"
                order by "kota_customer" desc
        '''

        # === Execute First Business Query === 
        cursor.execute(question_fourth)
        print("4. Urutan Basis lokasi kota customer yang sangat sering membeli track ke yang paling jarang")
        records = cursor.fetchall()
        
        # === Transform to DataFrame model & to .csv === 
        df_fourth = pd.DataFrame(records, columns=['city', 'Kota_customer'])
        df_fourth.index = np.arange(1, len(df_fourth)+1)
        df_fourth.to_csv('/opt/airflow/dags/files/bq_fourth.csv', index=False)
        
        print("bq_fourth.csv has been created")
        
    finally:
        # === Closing the connection === 
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")


def upload_file_to_GCS():
    """ Upload data to a bucket"""
    storage_client = storage.Client.from_service_account_json(
        '/opt/airflow/dags/key/tough-chassis.json')
    # GCS Bucket Name
    bucket = storage_client.get_bucket('narasiodata_bucket')  

    file_path = '/opt/airflow/dags/files'
    list_files_to_upload = ['bq_one.csv', 'bq_two.csv', 'bq_third.csv', 'bq_fourth.csv']

    for str_file_name in list_files_to_upload:
        # The name of the file on GCS once uploaded
        blob = bucket.blob(str_file_name)
        # Path of the local file to upload
        blob.upload_from_filename(f'{file_path}/{str_file_name}')
    return blob.public_url