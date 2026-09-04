from pyspark.sql import SparkSession
from ingestion.env.DE_Ingestion_properties import SPARK_APP_NAME, SPARK_MASTER
from ingestion.pyfiles.logger import get_logger

logger = get_logger(__name__)

_SESSION: SparkSession | None = None


def get_spark_session() -> SparkSession:
    global _SESSION
    if _SESSION is not None:
        return _SESSION

    logger.info("Building SparkSession")

    _SESSION = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    _SESSION.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created [appId=%s]", _SESSION.sparkContext.applicationId)
    return _SESSION


def get_active_session() -> SparkSession | None:
    return _SESSION
