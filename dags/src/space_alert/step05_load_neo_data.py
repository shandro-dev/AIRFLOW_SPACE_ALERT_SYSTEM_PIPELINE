# LOAD DATA TO POSTGRES

from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import URL
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()


def load_dataframe_to_postgres(df):
    # --- DB CONFIG ---
    db_user = os.getenv("db_user")
    db_password = os.getenv("db_password")
    db_host = os.getenv("db_host")
    db_port = os.getenv("db_port")
    db_name = os.getenv("db_name")
    schema_name = os.getenv("nasa_schema_name")
    table_name = os.getenv("nasa_table_name")
    audit_table = os.getenv("nasa_audit_table")

    try:
        # --- Extract batch_id from DataFrame ---
        batch_ids = df['batch_id'].unique()
        if len(batch_ids) != 1:
            raise ValueError("DataFrame must contain exactly one unique batch_id for this operation.")
        batch_id = batch_ids[0]

        # --- Build the connection URL ---
        connection_url = URL.create(
            drivername="postgresql+psycopg2",
            username=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
            database=db_name
        )

        # --- Create engine ---
        engine = create_engine(connection_url)

        # --- Ensure schema exists ---
        with engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'))

        # --- Ensure main table exists dynamically ---
        inspector = inspect(engine)
        if not inspector.has_table(table_name, schema=schema_name):
            # Create empty table with same columns as DataFrame
            df.head(0).to_sql(
                name=table_name,
                con=engine,
                schema=schema_name,
                if_exists='replace',  # creates table if missing
                index=False
            )

        # --- Step: Filter out duplicate nasa_id entries ---
        with engine.begin() as conn:
            existing_ids = conn.execute(text(f'''
                SELECT nasa_id FROM "{schema_name}"."{table_name}"
            ''')).fetchall()
            existing_ids_set = set([row[0] for row in existing_ids])

        # Filter DataFrame to only new nasa_id values
        df = df[~df['nasa_id'].isin(existing_ids_set)]

        if df.empty:
            print("🟡 No new records to insert. All nasa_id values already exist.")
            return [True]

        # --- Calculate record count ---
        record_count = len(df)

        # --- Load DataFrame into the database ---
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema_name,
            if_exists='append',
            index=False,
            method='multi'
        )

        # --- Update processing_status only for current batch_id ---
        with engine.begin() as conn:
            conn.execute(text(f'''
                UPDATE "{schema_name}"."{table_name}"
                SET processing_status = 'loaded to db'
                WHERE batch_id = :batch_id
            '''), {'batch_id': batch_id})

        # --- Create audit table if not exists ---
        with engine.begin() as conn:
            conn.execute(text(f'''
                CREATE TABLE IF NOT EXISTS "{schema_name}"."{audit_table}" (
                    batch_id VARCHAR PRIMARY KEY,
                    date DATE NOT NULL,
                    time TIME NOT NULL,
                    record_count INTEGER NOT NULL
                )
            '''))

        # --- Insert audit trail record ---
        now = datetime.now()
        current_date = now.date()
        current_time = now.time()

        with engine.begin() as conn:
            conn.execute(text(f'''
                INSERT INTO "{schema_name}"."{audit_table}" (batch_id, date, time, record_count)
                VALUES (:batch_id, :date, :time, :record_count)
                ON CONFLICT (batch_id) DO NOTHING
            '''), {
                'batch_id': batch_id,
                'date': current_date,
                'time': current_time,
                'record_count': record_count
            })

        print(f"✅ Data loaded and 'processing_status' updated in {schema_name}.{table_name} for batch_id={batch_id} for space alert system")
        print(f"✅ Audit trail updated in schema: '{schema_name}', table: '{audit_table}' for batch_id={batch_id} with record count: {record_count} for space alert system")

        return [True,schema_name,table_name,batch_id,record_count]

    except Exception as e:
        print(f"❌ Failed to load data: {e}")
        return [False]



