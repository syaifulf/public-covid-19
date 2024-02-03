from datetime import timedelta, datetime
import subprocess
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, inspect, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable


def _install_packages(package_list):
    for package in package_list:
        try:
            subprocess.check_call(['pip3', 'install', package])
            print(f'Successfully installed {package}')
        except subprocess.CalledProcessError:
            print(f'Error installing {package}')

def _get_api():
    import requests
    from sqlalchemy import create_engine
    import os    

    packages_to_install = ['python-dotenv']
    _install_packages(packages_to_install)

    from dotenv import load_dotenv

    url = "http://103.150.197.96:5005/api/v1/rekapitulasi_v2/jabar/harian?level=kab%22"
    response = requests.get(url)

    # Periksa apakah permintaan berhasil (kode status 200)
    if response.status_code == 200:
        # Ubah respons ke dalam format JSON
        data_json = response.json()

        df = pd.json_normalize(data_json['data']['content'])

        dataCase = {'id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
            'status_name': ['closecontact', 'closecontact', 'closecontact', 'confirmation', 'confirmation','probable',
                            'probable', 'probable', 'suspect', 'suspect', 'suspect'],
            'status_detail': ['closecontact_dikarantina', 'closecontact_discarded', 'closecontact_meninggal', 'confirmation_meninggal', 'confirmation_sembuh','probable_diisolasi',
                            'probable_discarded', 'probable_meninggal', 'suspect_diisolasi', 'suspect_discarded', 'suspect_meninggal'],
            }
        dfCase = pd.DataFrame(dataCase)

        # Tampilkan DataFrame
        # print(df)
        load_dotenv()
        nama_database = os.getenv("MYSQL_DATABASE") #'finalproject'
        nama_user = os.getenv("MYSQL_USER") #'syaiful'
        password = os.getenv("MYSQL_PASSWORD") # 'syaiful'
        alamat_server = 'mysql'

        engine = create_engine(f'mysql+mysqlconnector://{nama_user}:{password}@{alamat_server}/{nama_database}')

        # Buat tabel di MySQL
        tabel = 'staging'
        tabel2 = 'case'
        df.to_sql(tabel, con=engine, if_exists='replace', index=False)
        dfCase.to_sql(tabel2, con=engine, if_exists='replace', index=False)
        print('sukses')
    else:
        print(f'Error: {response.status_code}')

def _dim_province():
    from sqlalchemy import create_engine, inspect
    import os
    from dotenv import load_dotenv

    load_dotenv()
    engine_mysql = create_engine(os.getenv("AIRFLOW_CONN_MY_MYSQL_DB"))

    query = "SELECT * FROM staging"

    # Use pandas to read data from MySQL into a DataFrame
    df = pd.read_sql(query, con=engine_mysql)

    distinct_values_ori = df[['kode_prov', 'nama_prov']].drop_duplicates()
    distinct_values = distinct_values_ori.rename(columns={'kode_prov': 'province_id', 'nama_prov': 'province_name'})

    engine_postgre = create_engine(f'postgresql://{os.getenv("POSTGRES_USER")}:{os.getenv("POSTGRES_PASSWORD")}@postgres/{os.getenv("POSTGRES_DB")}')

    inspector = inspect(engine_postgre)
    if inspector.has_table('dim_province'):
        # Delete existing records from the table
        with engine_postgre.connect() as connection:
            connection.execute("DELETE FROM dim_province")

    distinct_values.reset_index(drop=True, inplace=True)
    distinct_values.to_sql('dim_province', engine_postgre, index=False, if_exists='append', index_label='province_id')

def _dim_district():
    from sqlalchemy import create_engine, inspect
    import os
    from dotenv import load_dotenv

    load_dotenv()
    engine_mysql = create_engine(os.getenv("AIRFLOW_CONN_MY_MYSQL_DB"))
    query = "SELECT * FROM staging"

    # Use pandas to read data from MySQL into a DataFrame
    df = pd.read_sql(query, con=engine_mysql)
    
    # Mendapatkan nilai unik dari kolom 'kode_prov' dan 'nama_prov'
    distinct_values_ori = df[['kode_kab', 'kode_prov', 'nama_kab']].drop_duplicates()
    distinct_values = distinct_values_ori.rename(columns={'kode_kab': 'district_id', 'kode_prov':'province_id' ,'nama_kab': 'district_name'})

    engine_postgre = create_engine(f'postgresql://{os.getenv("POSTGRES_USER")}:{os.getenv("POSTGRES_PASSWORD")}@postgres/{os.getenv("POSTGRES_DB")}')

    inspector = inspect(engine_postgre)
    if inspector.has_table('dim_district'):
        # Delete existing records from the table
        with engine_postgre.connect() as connection:
             connection.execute("DELETE FROM dim_district")

    distinct_values.reset_index(drop=True, inplace=True)
    distinct_values.to_sql('dim_district', engine_postgre, index=False, if_exists='append', index_label='district_id')

def _dim_case():
    from sqlalchemy import create_engine, inspect
    import os
    from dotenv import load_dotenv

    load_dotenv()
    engine_mysql = create_engine(os.getenv("AIRFLOW_CONN_MY_MYSQL_DB"))

    query = "SELECT * FROM `case`"
    df = pd.read_sql(query, con=engine_mysql)

    engine_postgre = create_engine(f'postgresql://{os.getenv("POSTGRES_USER")}:{os.getenv("POSTGRES_PASSWORD")}@postgres/{os.getenv("POSTGRES_DB")}')

    inspector = inspect(engine_postgre)
    if inspector.has_table('dim_case'):
        # Delete existing records from the table
        with engine_postgre.connect() as connection:
             connection.execute("DELETE FROM dim_case")

    df.reset_index(drop=True, inplace=True)
    df.to_sql('dim_case', engine_postgre, index=False, if_exists='append', index_label='id')

def _province_daily():
    from dotenv import load_dotenv
    load_dotenv
    engine_mysql = create_engine(os.getenv("AIRFLOW_CONN_MY_MYSQL_DB"))
    query = "SELECT * FROM `case`"
    df = pd.read_sql(query, con=engine_mysql)

    union_queries = []

    # Iterasi melalui baris DataFrame
    for index, row in df.iterrows():
        # Dapatkan nilai id dan detail_name dari setiap baris
        id_value = row['id']
        status_detail = row['status_detail']
        # Bentuk query UNION
        union_query = f'SELECT kode_prov AS province_id, tanggal AS `date`, {id_value} AS `case_id`, SUM({status_detail}) AS total FROM staging  GROUP BY kode_prov, tanggal '
        # Tambahkan query UNION ke list
        union_queries.append(union_query)

    # Gabungkan semua query UNION dengan menggunakan " UNION " sebagai pemisah
    final_query = " UNION ".join(union_queries)

    # Tampilkan hasil akhir
    dffinal = pd.read_sql(final_query, con=engine_mysql)

    engine_postgre = create_engine(f'postgresql://{os.getenv("POSTGRES_USER")}:{os.getenv("POSTGRES_PASSWORD")}@postgres/{os.getenv("POSTGRES_DB")}')

    Base = declarative_base()

    # Definisikan Model Tabel
    class ProvinceDaily(Base):
        __tablename__ = 'province_daily'
        id = Column(Integer, primary_key=True, autoincrement=True)
        province_id = Column(String)
        case_id = Column(Integer)
        date = Column(String)        
        total = Column(Float)

    # Inspeksi untuk mengecek apakah tabel sudah ada
    inspector = inspect(engine_postgre)

    # Jika tabel sudah ada, hapus tabel tersebut
    if inspector.has_table('province_daily'):
        ProvinceDaily.__table__.drop(engine_postgre)

    # Buat Tabel
    Base.metadata.create_all(engine_postgre)

    # Buat sesi SQLAlchemy
    Session = sessionmaker(bind=engine_postgre)
    session = Session()

    # Simpan Data ke Tabel
    dffinal.to_sql('province_daily', engine_postgre, index=False, if_exists='append')

    # Tutup sesi
    session.close()

def _province_monthly():
    from dotenv import load_dotenv
    load_dotenv
    engine_mysql = create_engine(os.getenv("AIRFLOW_CONN_MY_MYSQL_DB"))
    query = "SELECT * FROM `case`"

    # Use pandas to read data from MySQL into a DataFrame
    df = pd.read_sql(query, con=engine_mysql)

    # Simpan hasil query UNION di sini
    union_queries = []

    # Iterasi melalui baris DataFrame
    for index, row in df.iterrows():
        # Dapatkan nilai id dan detail_name dari setiap baris
        id_value = row['id']
        status_detail = row['status_detail']

        # Bentuk query UNION
        union_query = f'SELECT kode_prov AS province_id, DATE_FORMAT(tanggal, "%Y-%m") AS `month`, {id_value} AS `case_id`, SUM({status_detail}) AS total FROM staging GROUP BY kode_prov, `month`'
        
        # Tambahkan query UNION ke list
        union_queries.append(union_query)

    # Gabungkan semua query UNION dengan menggunakan " UNION " sebagai pemisah
    final_query = " UNION ".join(union_queries)

    # Tampilkan hasil akhir
    dffinal = pd.read_sql(final_query, con=engine_mysql)

    engine_postgre = create_engine(f'postgresql://{os.getenv("POSTGRES_USER")}:{os.getenv("POSTGRES_PASSWORD")}@postgres/{os.getenv("POSTGRES_DB")}')

    Base = declarative_base()

    # Definisikan Model Tabel
    class ProvinceMonthly(Base):
        __tablename__ = 'province_monthly'
        id = Column(Integer, primary_key=True, autoincrement=True)
        province_id = Column(String)
        case_id = Column(Integer)
        month = Column(String)        
        total = Column(Float)

    # Inspeksi untuk mengecek apakah tabel sudah ada
    inspector = inspect(engine_postgre)

    # Jika tabel sudah ada, hapus tabel tersebut
    if inspector.has_table('province_monthly'):
        ProvinceMonthly.__table__.drop(engine_postgre)

    # Buat Tabel
    Base.metadata.create_all(engine_postgre)

    # Buat sesi SQLAlchemy
    Session = sessionmaker(bind=engine_postgre)
    session = Session()

    # Simpan Data ke Tabel
    dffinal.to_sql('province_monthly', engine_postgre, index=False, if_exists='append')

    # Tutup sesi
    session.close()

def _province_yearly():
    from dotenv import load_dotenv
    load_dotenv
    engine_mysql = create_engine(os.getenv("AIRFLOW_CONN_MY_MYSQL_DB"))
   
    query = "SELECT * FROM `case`"

    # Use pandas to read data from MySQL into a DataFrame
    df = pd.read_sql(query, con=engine_mysql)

    union_queries = []

    # Iterasi melalui baris DataFrame
    for index, row in df.iterrows():
        # Dapatkan nilai id dan detail_name dari setiap baris
        id_value = row['id']
        status_detail = row['status_detail']

        # Bentuk query UNION
        union_query = f'SELECT kode_prov AS province_id, DATE_FORMAT(tanggal, "%Y") AS `year`, {id_value} AS `case_id`, SUM({status_detail}) AS total FROM staging GROUP BY kode_prov, `year`'
        
        # Tambahkan query UNION ke list
        union_queries.append(union_query)

    # Gabungkan semua query UNION dengan menggunakan " UNION " sebagai pemisah
    final_query = " UNION ".join(union_queries)

    # Tampilkan hasil akhir
    dffinal = pd.read_sql(final_query, con=engine_mysql)

    engine_postgre = create_engine(f'postgresql://{os.getenv("POSTGRES_USER")}:{os.getenv("POSTGRES_PASSWORD")}@postgres/{os.getenv("POSTGRES_DB")}')

    Base = declarative_base()

    # Definisikan Model Tabel
    class ProvinceYearly(Base):
        __tablename__ = 'province_yearly'
        id = Column(Integer, primary_key=True, autoincrement=True)
        province_id = Column(String)
        case_id = Column(Integer)
        year = Column(String)        
        total = Column(Float)

    # Inspeksi untuk mengecek apakah tabel sudah ada
    inspector = inspect(engine_postgre)

    # Jika tabel sudah ada, hapus tabel tersebut
    if inspector.has_table('province_yearly'):
        ProvinceYearly.__table__.drop(engine_postgre)

    # Buat Tabel
    Base.metadata.create_all(engine_postgre)

    # Buat sesi SQLAlchemy
    Session = sessionmaker(bind=engine_postgre)
    session = Session()

    # Simpan Data ke Tabel
    dffinal.to_sql('province_yearly', engine_postgre, index=False, if_exists='append')

    # Tutup sesi
    session.close()

def _district_monthly():
    from dotenv import load_dotenv
    load_dotenv
    engine_mysql = create_engine(os.getenv("AIRFLOW_CONN_MY_MYSQL_DB"))
    query = "SELECT * FROM `case`"

    # Use pandas to read data from MySQL into a DataFrame
    df = pd.read_sql(query, con=engine_mysql)
    
    # Simpan hasil query UNION di sini
    union_queries = []

    # Iterasi melalui baris DataFrame
    for index, row in df.iterrows():
        # Dapatkan nilai id dan detail_name dari setiap baris
        id_value = row['id']
        status_detail = row['status_detail']

        # Bentuk query UNION
        union_query = f'SELECT kode_kab AS district_id, DATE_FORMAT(tanggal,"%Y-%m") AS `month`, {id_value} AS `case_id`, SUM({status_detail}) AS total FROM staging GROUP BY kode_kab, `month`'
        
        # Tambahkan query UNION ke list
        union_queries.append(union_query)

    # Gabungkan semua query UNION dengan menggunakan " UNION " sebagai pemisah
    final_query = " UNION ".join(union_queries)

    # Tampilkan hasil akhir
    dffinal = pd.read_sql(final_query, con=engine_mysql)

    engine_postgre = create_engine(f'postgresql://{os.getenv("POSTGRES_USER")}:{os.getenv("POSTGRES_PASSWORD")}@postgres/{os.getenv("POSTGRES_DB")}')

    Base = declarative_base()

    # Definisikan Model Tabel
    class DistrictMonthly(Base):
        __tablename__ = 'district_monthly'
        id = Column(Integer, primary_key=True, autoincrement=True)
        district_id = Column(String)
        case_id = Column(Integer)
        month = Column(String)        
        total = Column(Float)

    # Inspeksi untuk mengecek apakah tabel sudah ada
    inspector = inspect(engine_postgre)

    # Jika tabel sudah ada, hapus tabel tersebut
    if inspector.has_table('district_monthly'):
        DistrictMonthly.__table__.drop(engine_postgre)

    # Buat Tabel
    Base.metadata.create_all(engine_postgre)

    # Buat sesi SQLAlchemy
    Session = sessionmaker(bind=engine_postgre)
    session = Session()

    # Simpan Data ke Tabel
    dffinal.to_sql('district_monthly', engine_postgre, index=False, if_exists='append')

    # Tutup sesi
    session.close()

def _district_yearly():
    from dotenv import load_dotenv
    load_dotenv
    from sqlalchemy import create_engine
    import os
    
    engine_mysql = create_engine(os.getenv("AIRFLOW_CONN_MY_MYSQL_DB"))
    query = "SELECT * FROM `case`"

    # Use pandas to read data from MySQL into a DataFrame
    df = pd.read_sql(query, con=engine_mysql)

    print(df.columns)
    
    # Simpan hasil query UNION di sini
    union_queries = []

    # Iterasi melalui baris DataFrame
    for index, row in df.iterrows():
        # Dapatkan nilai id dan detail_name dari setiap baris
        id_value = row['id']
        status_detail = row['status_detail']

        # Bentuk query UNION
        union_query = f'SELECT kode_kab AS district_id, DATE_FORMAT(tanggal, "%Y") AS `year`, {id_value} AS `case_id`, SUM({status_detail}) AS total FROM staging GROUP BY kode_kab, `year`'
        
        # Tambahkan query UNION ke list
        union_queries.append(union_query)

    # Gabungkan semua query UNION dengan menggunakan " UNION " sebagai pemisah
    final_query = " UNION ".join(union_queries)

    # Tampilkan hasil akhir
    dffinal = pd.read_sql(final_query, con=engine_mysql)

    engine_postgre = create_engine(f'postgresql://{os.getenv("POSTGRES_USER")}:{os.getenv("POSTGRES_PASSWORD")}@postgres/{os.getenv("POSTGRES_DB")}')

    Base = declarative_base()

    # Definisikan Model Tabel
    class DistrictYearly(Base):
        __tablename__ = 'district_monthly'
        id = Column(Integer, primary_key=True, autoincrement=True)
        district_id = Column(String)
        case_id = Column(Integer)
        year = Column(String)        
        total = Column(Float)

    # Inspeksi untuk mengecek apakah tabel sudah ada
    inspector = inspect(engine_postgre)

    # Jika tabel sudah ada, hapus tabel tersebut
    if inspector.has_table('district_yearly'):
        DistrictYearly.__table__.drop(engine_postgre)

    # Buat Tabel
    Base.metadata.create_all(engine_postgre)

    # Buat sesi SQLAlchemy
    Session = sessionmaker(bind=engine_postgre)
    session = Session()

    # Simpan Data ke Tabel
    dffinal.to_sql('district_yearly', engine_postgre, index=False, if_exists='append')

    # Tutup sesi
    session.close()

def _create_ddl():
    from psycopg2 import sql
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
    import os    
    from dotenv import load_dotenv
    load_dotenv

    connection = None
    print("Connecting to the PostgreSQL database... os.environ.get('POSTGRES_USER')", os.environ.get('POSTGRES_USER'))
    try:
        connection = psycopg2.connect(
            host='localhost',
            port=5433,            
            dbname='postgres',
            user= os.environ.get('POSTGRES_USER'),
            password= os.environ.get('POSTGRES_PASSWORD')
        )
        database_name = 'finalproject'
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        # connection.set_session(autocommit=True)
        with connection.cursor() as cursor:
            # Check if the database already exists
            cursor.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), (database_name,))
            exists = cursor.fetchone()
            
            if not exists:
                # Use sql.SQL and sql.Identifier for proper quoting of identifiers
                create_database_query = sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name))
                cursor.execute(create_database_query)
                print(f"Database '{database_name}' created successfully.")
            else:
                print(f"Database '{database_name}' already exists.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if connection:
            connection.close()
    
with DAG(
    dag_id='dag_api_to_staging_to_postgresql',
    start_date=datetime(2022, 5, 28),
    schedule_interval='00 23 * * *',
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:

    start_task = EmptyOperator(
        task_id='start'
    )

    get_api = PythonOperator(
        task_id='_get_api',
        python_callable=_get_api,
        execution_timeout=timedelta(minutes=5),
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    create_ddl = PythonOperator(
        task_id='create_ddl',
        python_callable=_create_ddl,
        execution_timeout=timedelta(minutes=5),
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    dim_province = PythonOperator(
        task_id='dim_province',
        python_callable=_dim_province,
        execution_timeout=timedelta(minutes=5),
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    dim_district = PythonOperator(
        task_id='dim_district',
        python_callable=_dim_district,
        execution_timeout=timedelta(minutes=5),
        retries=2,
        retry_delay=timedelta(minutes=5),
    )       

    dim_case = PythonOperator(
        task_id='dim_case',
        python_callable=_dim_case,
        execution_timeout=timedelta(minutes=5),
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    province_daily = PythonOperator(
        task_id='province_daily',
        python_callable=_province_daily,
        execution_timeout=timedelta(minutes=5),
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    province_monthly = PythonOperator(
        task_id='province_monthly',
        python_callable=_province_monthly,
        execution_timeout=timedelta(minutes=5),
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    province_yearly = PythonOperator(
        task_id='province_yearly',
        python_callable=_province_yearly,
        execution_timeout=timedelta(minutes=5),
        retries=2,
        retry_delay=timedelta(minutes=5),
    )


    district_monthly = PythonOperator(
        task_id='district_monthly',
        python_callable=_district_monthly,
        execution_timeout=timedelta(minutes=5),
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    district_yearly = PythonOperator(
        task_id='district_yearly',
        python_callable=_district_yearly,
        execution_timeout=timedelta(minutes=5),
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    end_task = EmptyOperator(
        task_id='end'
    )

    start_task >> get_api >> create_ddl >> [dim_province , dim_district]
    [dim_province , dim_district] >> dim_case >>  [ province_daily, district_monthly]
    province_daily >> province_monthly >> province_yearly >> end_task
    district_monthly >> district_yearly >> end_task