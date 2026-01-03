import os
from pyhive import hive

HIVE_HOST = os.getenv('HIVE_HOST', 'hive-server.default.svc.cluster.local')
HIVE_PORT = int(os.getenv('HIVE_PORT', 10000))
USER_NAME = os.getenv('HIVE_USER', 'hdfs_user')
NAMENODE_URL = os.getenv('NAMENODE_URL', 'hdfs://namenode:9000')

PRED_DIR = os.getenv('PRED_PATH', '/topics/processed_data')
ACTUAL_DIR = os.getenv('ACTUAL_PATH', '/topics/energy_data')
REPORT_DIR = os.getenv('REPORT_PATH', '/reports/accuracy_avro')


def main():
    print("Starting Energy Accuracy Job.")
    print(f"Connecting to Hive at: {HIVE_HOST}:{HIVE_PORT}")

    try:
        conn = hive.Connection(host=HIVE_HOST, port=HIVE_PORT, username=USER_NAME)
        cursor = conn.cursor()
        print("Successfully connected to Hive.")

        cursor.execute("SET mapreduce.input.fileinputformat.input.dir.recursive=true")
        cursor.execute("SET hive.mapred.supports.subdirectories=true")

    except Exception as e:
        print(f"ERROR: Could not connect to Hive.")
        raise e

    # 1. Define Prediction Table
    print(f"Defining predictions table...")
    cursor.execute("DROP TABLE IF EXISTS energy_predictions")
    cursor.execute(f"""
        CREATE EXTERNAL TABLE energy_predictions (
            `TimeUTC` STRING,
            `Region` STRING,
            `Predicted` DOUBLE
        )
        ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
        LOCATION '{NAMENODE_URL}{PRED_DIR}'
    """)

    # 2. Define Actuals Table
    print(f"Defining actuals table...")
    cursor.execute("DROP TABLE IF EXISTS energy_actuals")
    cursor.execute(f"""
        CREATE EXTERNAL TABLE energy_actuals (
            `TimeUTC` STRING,
            `TimeDK` STRING,
            `MunicipalityCode` INT,
            `Municipality` STRING,
            `RegionName` STRING,
            `HousingCategory` STRING,
            `HeatingCategory` STRING,
            `ConsumptionkWh` DOUBLE
        )
        ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
        LOCATION '{NAMENODE_URL}{ACTUAL_DIR}'
    """)

    # 3. Define Output Report Table
    print(f"Defining output table...")
    cursor.execute("DROP TABLE IF EXISTS energy_accuracy_report")
    cursor.execute(f"""
        CREATE EXTERNAL TABLE energy_accuracy_report (
            `TimeUTC` STRING,
            `Region` STRING,
            `PredictionkWh` DOUBLE,
            `ActualTotal` DOUBLE,
            `DifferenceKwh` DOUBLE,
            `AccuracyPercent` DOUBLE
        )
        STORED AS AVRO
        LOCATION '{NAMENODE_URL}{REPORT_DIR}'
    """)

	# Debug information
    cursor.execute("SELECT COUNT(*) FROM energy_predictions")
    pred_count = cursor.fetchone()[0]
    print(f"DEBUG: Found {pred_count} rows in predictions table.")

    cursor.execute("SELECT COUNT(*) FROM energy_actuals")
    actual_count = cursor.fetchone()[0]
    print(f"DEBUG: Found {actual_count} rows in actuals table.")

    if pred_count == 0 or actual_count == 0:
        print("WARNING: One of the tables is empty! The Join will likely fail.")

    # 4. Run Calculation
    print("Running calculation...")

    query = """
        INSERT OVERWRITE TABLE energy_accuracy_report
        SELECT
            p.`TimeUTC`,
            p.`Region`,
            p.`Predicted` as `PredictionkWh`,
            sum_actuals.total_consumption,
            ABS(p.`Predicted` - sum_actuals.total_consumption),
            (100 - (ABS(p.`Predicted` - sum_actuals.total_consumption) / sum_actuals.total_consumption * 100))
        FROM energy_predictions p
        JOIN (
            SELECT `TimeUTC`, `RegionName`, SUM(`ConsumptionkWh`) as total_consumption
            FROM energy_actuals
            GROUP BY `TimeUTC`, `RegionName`
        ) sum_actuals
        ON p.`TimeUTC` = sum_actuals.`TimeUTC`
        AND p.`Region` = sum_actuals.`RegionName`
    """

    cursor.execute(query)
    print(f"Job Complete. Report generated at: {NAMENODE_URL}{REPORT_DIR}")

    try:
        conn.close()
    except Exception:
        pass


if __name__ == "__main__":
    main()
