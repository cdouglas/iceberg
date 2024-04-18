from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark import InheritableThread

db_name = "local_original_iceberg.test_db"
table_name = "test_table"
table_name_2 = "test_table_2"
n_threads = 4

from enum import Enum
class IsolationLevel(Enum):
    SNAPSHOT = "snapshot"
    SERIALIZABLE = "serializable"

class ConflictScenario(Enum):
    UPSERT_ROW_CONFLICT = "upsert_row_conflict"
    UPSERT_ROW_NO_CONFLICT = "upsert_row_no_conflict"
    MULTITABLE_UPSERT_ROW_CONFLICT = "multi_table_upsert_row_conflict"
    MULTITABLE_UPSERT_ROW_NO_CONFLICT = "multi_table_upsert_row_no_conflict"

def spark_session(log_level="WARN"):
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("iceberg-concurrent-write-isolation-test")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(log_level)
    return spark

def setup(*, spark, isolation_level: IsolationLevel):
    # Create DataFrame
    df = spark.createDataFrame(
        [
            ("id1", "user1", "20211108"),
            ("id2", "user2", "20211108"),
            ("id3", "user3", "20211109")
        ], 
        schema=StructType(
        [
            StructField("id", StringType(), True),
            StructField("user", StringType(), True),
            StructField("date", StringType(), True)
        ]
    ))

    # Create database
    spark.sql(f"DROP TABLE IF EXISTS {db_name}.{table_name}")
    spark.sql(f"DROP TABLE IF EXISTS {db_name}.{table_name_2}")
    spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")
    spark.sql(f"CREATE DATABASE {db_name}")
    # Write the DataFrame to an Iceberg table
    df.writeTo(f"{db_name}.{table_name}").using("iceberg") \
        .tableProperty("commit.retry.num-retries", "10") \
        .tableProperty("commit.retry.min-wait-ms", "1000") \
        .tableProperty("write.merge.isolation-level", isolation_level.value) \
    .create()
    # Write the DataFrame to an Iceberg table
    df.writeTo(f"{db_name}.{table_name_2}").using("iceberg") \
        .tableProperty("commit.retry.num-retries", "10") \
        .tableProperty("commit.retry.min-wait-ms", "1000") \
        .tableProperty("write.merge.isolation-level", isolation_level.value) \
    .create()

def build_statement(*, scenario: ConflictScenario, id) -> str:

    if scenario == ConflictScenario.UPSERT_ROW_CONFLICT:
        table = f"{db_name}.{table_name}"
        #return f"""MERGE INTO {table} target
        #                USING (
        #                        SELECT 
        #                        'id4' AS id,
        #                        'user4_{id}' AS user,
        #                        '20240327' AS date
        #                        ) source
        #                ON source.id = target.id
        #                WHEN MATCHED THEN UPDATE SET *
        #                WHEN NOT MATCHED THEN INSERT *
        #        """
        table = f"{db_name}.{table_name}"
        if id % 2 == 1:
            # Merge from table 2 to table 1
            return f"""MERGE INTO {table} target
                        USING (
                                SELECT 
                                id,
                                'user_new_{id}' AS user,
                                date
                                FROM {table}
                                ) source
                        ON source.id = target.id
                        WHEN MATCHED THEN UPDATE SET *
                        WHEN NOT MATCHED THEN INSERT *
                """
        else:
            return f"""MERGE INTO {table} target
                        USING (
                                SELECT 
                                'id{id+n_threads*2}' AS id,
                                'user4' AS user,
                                't_{id}' AS date
                                ) source
                        ON source.id = target.id
                        WHEN MATCHED THEN UPDATE SET *
                        WHEN NOT MATCHED THEN INSERT *
            """
    elif scenario == ConflictScenario.UPSERT_ROW_NO_CONFLICT:
        table = f"{db_name}.{table_name}"
        return f"""MERGE INTO {table} target
                        USING (
                                SELECT 
                                'id_new_{id}' AS id,
                                'user4' AS user,
                                '20240327' AS date
                                ) source
                        ON source.id = target.id
                        WHEN MATCHED THEN UPDATE SET *
                        WHEN NOT MATCHED THEN INSERT *
                """
    elif scenario == ConflictScenario.MULTITABLE_UPSERT_ROW_CONFLICT:
        table = f"{db_name}.{table_name}"
        table_2 = f"{db_name}.{table_name_2}"
        if id % 2 == 1:
            # Merge from table 2 to table 1
            return f"""MERGE INTO {table} target
                        USING (
                                SELECT 
                                id,
                                'user_new_{id}' AS user,
                                date
                                FROM {table_2}
                                ) source
                        ON source.id = target.id
                        WHEN MATCHED THEN UPDATE SET *
                        WHEN NOT MATCHED THEN INSERT *
                """
        else:
            return f"""MERGE INTO {table_2} target
                        USING (
                                SELECT 
                                'id{id+n_threads*2}' AS id,
                                'user4' AS user,
                                't_{id}' AS date
                                ) source
                        ON source.id = target.id
                        WHEN MATCHED THEN UPDATE SET *
                        WHEN NOT MATCHED THEN INSERT *
            """
    elif scenario == ConflictScenario.MULTITABLE_UPSERT_ROW_NO_CONFLICT:
        if id % 2 == 0:
            table = f"{db_name}.{table_name}"
            table_2 = f"{db_name}.{table_name_2}"
        else:
            table = f"{db_name}.{table_name_2}"
            table_2 = f"{db_name}.{table_name}"
        
        return f"""MERGE INTO {table} target
                        USING (
                                SELECT 
                                'id_new_{id}' AS id,
                                user,
                                date
                                FROM {table_2}
                                WHERE date = '20211109'
                                ) source
                        ON source.id = target.id
                        WHEN MATCHED THEN UPDATE SET *
                        WHEN NOT MATCHED THEN INSERT *
                """
    else:
        raise ValueError(f"Invalid scenario: {scenario}")

def verify_result(spark, *, isolation_level: IsolationLevel, scenario: ConflictScenario):
    #if scenario in [ConflictScenario.UPSERT_ROW_CONFLICT]:
    #    df = spark.sql(f"SELECT * FROM {db_name}.{table_name} where id = 'id4'")
    #    df.show()
    #    if isolation_level == IsolationLevel.SNAPSHOT:
    #        assert df.count() == n_threads
    #    elif isolation_level == IsolationLevel.SERIALIZABLE:
    #        assert df.count() == 1
    if scenario in [ConflictScenario.UPSERT_ROW_NO_CONFLICT]:
        df = spark.sql(f"SELECT * FROM {db_name}.{table_name} where user = 'user4'")
        df.show()
        assert df.count() == n_threads
    elif scenario in [ConflictScenario.MULTITABLE_UPSERT_ROW_NO_CONFLICT]:
        df = spark.sql(f"SELECT * FROM {db_name}.{table_name} where date = '20211109'")
        df.show()
        df2 = spark.sql(f"SELECT * FROM {db_name}.{table_name_2} where date = '20211109'")
        df2.show()
        assert df2.count() == int(n_threads/2) + 1 and df.count() == int(n_threads/2) + 1
    else:
        print("Table 1:")
        spark.sql(f"SELECT * FROM {db_name}.{table_name}").show()
        print("Table 2:")
        spark.sql(f"SELECT * FROM {db_name}.{table_name_2}").show()

def test_isolation(spark, *, isolation_level: IsolationLevel, scenario: ConflictScenario, parallel=True):

    setup(spark=spark, isolation_level=isolation_level)

    def run_query(spark, query, i):
        spark.sql(query)
        print(f"Thread {i} finished!")

    # Run concurrent merges on the table
    if parallel:
        threads = []
        for i in range(n_threads):
            query = build_statement(scenario=scenario, id=i)
            t = InheritableThread(target=run_query, args=(spark, query, i))
            threads.append(t)
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Verify result
        verify_result(spark, isolation_level=isolation_level, scenario=scenario)
    else:
        for i in range(n_threads):
            query = build_statement(scenario=scenario, id=i)
            print(query)
            run_query(spark, query, i)
        verify_result(spark, isolation_level=None, scenario=None)

if __name__ == "__main__":
    spark = spark_session(log_level="WARN")
    for scenario in [ConflictScenario.UPSERT_ROW_NO_CONFLICT]: #[ConflictScenario.UPSERT_ROW_CONFLICT, ConflictScenario.UPSERT_ROW_NO_CONFLICT]:
        for isolation_level in [IsolationLevel.SERIALIZABLE]: #[IsolationLevel.SNAPSHOT, IsolationLevel.SERIALIZABLE]:
            test_isolation(spark, isolation_level=isolation_level, scenario=scenario, parallel=True)

    spark.stop()
