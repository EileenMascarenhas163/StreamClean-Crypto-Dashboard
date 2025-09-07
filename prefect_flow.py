from prefect import flow, task, get_run_logger
from datetime import timedelta
import subprocess
import time
import os

@task
def wait_for_service(service_name, command, wait_time=60):
    """
    Waits for a service to become available by repeatedly checking a command.
    This is a simple way to ensure dependencies are ready before proceeding.
    """
    logger = get_run_logger()
    logger.info(f"Waiting for {service_name} to become available...")
    start_time = time.time()
    while time.time() - start_time < wait_time:
        try:
            # The command will fail if the service is not ready
            subprocess.run(command, check=True, capture_output=True)
            logger.info(f"{service_name} is now available.")
            return True
        except subprocess.CalledProcessError as e:
            logger.info(f"{service_name} not ready yet. Retrying in 5 seconds...")
            time.sleep(5)
    
    raise TimeoutError(f"Timed out waiting for {service_name} after {wait_time} seconds.")

@task
def run_producer():
    """Starts the Kafka producer in a non-blocking process."""
    subprocess.Popen(["python", "producer.py"])
    get_run_logger().info("Producer launched.")

@task
def run_spark():
    """Starts the Spark streaming job in a non-blocking process."""
    subprocess.Popen(["python", "spark_job.py"])
    get_run_logger().info("Spark streaming job launched.")

@task
def run_historical():
    """Runs the historical ingest script."""
    subprocess.call(["python", "historical_ingest.py"])

@task
def wait_for_parquet_files():
    """Polls for the existence of Parquet files written by Spark."""
    logger = get_run_logger()
    logger.info("Waiting for the first Parquet files to be written...")
    start_time = time.time()
    # Wait up to 5 minutes (300 seconds) for files to appear.
    while time.time() - start_time < 300:
        # Check for any file in the data/parquet directory
        if any(f.endswith('.parquet') for dirpath, dirnames, filenames in os.walk('./data/parquet') for f in filenames):
            logger.info("Parquet files found! Continuing flow.")
            return True
        logger.info("No Parquet files found yet. Sleeping for 10 seconds...")
        time.sleep(10)
    
    raise FileNotFoundError("Timed out waiting for Parquet files to be written.")

@task
def run_duckdb_query():
    """Executes the DuckDB query script."""
    logger = get_run_logger()
    logger.info("Running DuckDB analysis...")
    subprocess.run(["python", "duckdb_query.py"], check=True)
    logger.info("DuckDB analysis completed successfully.")

@flow
def crypto_pipeline():
    """The main flow that orchestrates the data pipeline."""
    # Ingest historical data. This is a blocking call.
    run_historical()
    
    # Start the data streaming processes. Prefect will run these tasks
    # and then immediately move on because they use subprocess.Popen.
    run_producer()
    run_spark()

    # Prefect will automatically wait for this task to complete before
    # moving to the next step.
    wait_for_parquet_files()

    # This task will only run after wait_for_parquet_files has
    # successfully completed.
    run_duckdb_query()


if __name__ == "__main__":
    crypto_pipeline.serve(
        name="Crypto Pipeline",
        interval=timedelta(minutes=5)
    )
