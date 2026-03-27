import typer
import shutil
import pandas as pd

import datetime
from pathlib import Path
from loguru import logger
from base.client import BaseClient
from config.mysql import mysql_config
from sqlalchemy import create_engine, text
import pytz

app = typer.Typer()

RAW_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS customer_addresses_raw (
    id BIGINT,
    customer_id BIGINT,
    address VARCHAR(255),
    city VARCHAR(100),
    province VARCHAR(100),
    created_at DATETIME(3),
    source_file VARCHAR(255),
    ingested_at DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)
);
"""

CURATED_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS customer_addresses (
    id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    address VARCHAR(255),
    city VARCHAR(100),
    province VARCHAR(100),
    created_at DATETIME(3),
    updated_at DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)
        ON UPDATE CURRENT_TIMESTAMP(3)
);
"""

MERGE_SQL = """
INSERT INTO customer_addresses (id, customer_id, address, city, province, created_at)
SELECT x.id, x.customer_id, x.address, x.city, x.province, x.created_at
FROM (
    SELECT
        id,
        customer_id,
        address,
        city,
        province,
        created_at,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC, ingested_at DESC) AS rn
    FROM customer_addresses_raw
) x
WHERE x.rn = 1
ON DUPLICATE KEY UPDATE
    customer_id = VALUES(customer_id),
    address = VALUES(address),
    city = VALUES(city),
    province = VALUES(province),
    created_at = VALUES(created_at),
    updated_at = CURRENT_TIMESTAMP(3);
"""


@app.command()
def run(
    input_dir: str = typer.Option(..., help="Directory file CSV harian"),
    archive_dir: str = typer.Option(..., help="Directory archive file selesai proses"),
    pattern: str = typer.Option("customer_addresses_*.csv", help="Pattern file"),
    db_uri: str = typer.Option(None, help="Override URI database jika diperlukan"),
    dry_run: bool = typer.Option(False, help="Validasi file tanpa insert DB")
):
    uri = db_uri or mysql_config.MYSQL_URI
    if not uri:
        raise typer.BadParameter("MYSQL_URI belum tersedia. Set di .env atau kirim --db-uri")

    CustomerAddressIngest(
        db_uri=uri,
        input_dir=input_dir,
        archive_dir=archive_dir,
        pattern=pattern,
        dry_run=dry_run,
    ).consume()


@app.command("run-datamart")
def run_datamart(
    db_uri: str = typer.Option(None, help="Override URI database jika diperlukan"),
    cleaning_sql: str = typer.Option("sql/02_cleaning_views.sql", help="Path file SQL cleaning"),
    report_sql: str = typer.Option("sql/03_datamart_reports.sql", help="Path file SQL report datamart"),
):
    uri = db_uri or mysql_config.MYSQL_URI
    if not uri:
        raise typer.BadParameter("MYSQL_URI belum tersedia. Set di .env atau kirim --db-uri")

    DatamartRunner(
        db_uri=uri,
        cleaning_sql=cleaning_sql,
        report_sql=report_sql,
    ).consume()



class CustomerAddressIngest(BaseClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db_uri = kwargs.get("db_uri")
        self.input_dir = Path(kwargs.get("input_dir"))
        self.archive_dir = Path(kwargs.get("archive_dir"))
        self.pattern = kwargs.get("pattern", "customer_addresses_*.csv")
        self.dry_run = kwargs.get("dry_run", False)
        self.total_rows = 0
        self.total_files = 0

    def _normalize_dataframe(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        required_cols = ["id", "customer_id", "address", "city", "province", "created_at"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing mandatory columns: {missing_cols}")

        normalized = df[required_cols].copy()
        normalized["id"] = pd.to_numeric(normalized["id"], errors="coerce").astype("Int64")
        normalized["customer_id"] = pd.to_numeric(normalized["customer_id"], errors="coerce").astype("Int64")
        normalized["created_at"] = pd.to_datetime(normalized["created_at"], errors="coerce")

        normalized["address"] = normalized["address"].fillna("").astype(str).str.strip()
        normalized["city"] = normalized["city"].fillna("").astype(str).str.strip().str.title()
        normalized["province"] = normalized["province"].fillna("").astype(str).str.strip().str.title()
        normalized["source_file"] = source_file
        normalized["ingested_at"] = datetime.datetime.now(pytz.timezone("Asia/Jakarta")).replace(tzinfo=None)

        normalized = normalized.dropna(subset=["id", "customer_id", "created_at"])
        normalized = normalized.drop_duplicates(subset=["id", "customer_id", "created_at", "address", "city", "province"])
        return normalized

    def _process_file(self, csv_path: Path) -> int:
        logger.info(f"Processing file: {csv_path.name}")
        df = pd.read_csv(csv_path)
        cleaned = self._normalize_dataframe(df, csv_path.name)

        if cleaned.empty:
            logger.warning(f"No valid rows found in {csv_path.name}")
            return 0

        if self.dry_run:
            logger.info(f"Dry run enabled. Skip write DB for {csv_path.name} rows={len(cleaned)}")
            return len(cleaned)

        engine = create_engine(self.db_uri)

        with engine.begin() as conn:
            conn.execute(text(RAW_TABLE_DDL))
            conn.execute(text(CURATED_TABLE_DDL))

        cleaned.to_sql(
            "customer_addresses_raw",
            con=engine,
            if_exists="append",
            index=False,
            chunksize=1000,
            method="multi",
        )

        with engine.begin() as conn:
            conn.execute(text(MERGE_SQL))

        return len(cleaned)

    def consume(self):
        if not self.input_dir.exists():
            raise FileNotFoundError(f"Input directory not found: {self.input_dir}")

        self.archive_dir.mkdir(parents=True, exist_ok=True)
        files = sorted(self.input_dir.glob(self.pattern))

        if not files:
            logger.info(f"No file found for pattern {self.pattern} in {self.input_dir}")
            return

        for file_path in files:
            try:
                written_rows = self._process_file(file_path)
                self.total_rows += written_rows
                self.total_files += 1

                if not self.dry_run:
                    destination = self.archive_dir / file_path.name
                    shutil.move(str(file_path), str(destination))
                    logger.info(f"Moved {file_path.name} to {destination}")
            except Exception as exc:
                logger.error(f"Failed processing file {file_path.name} due to {exc}")

        self.produce()

    def produce(self):
        logger.info(f"Ingest finished. files={self.total_files} rows={self.total_rows} dry_run={self.dry_run}")
        return {
            "total_files": self.total_files,
            "total_rows": self.total_rows,
            "dry_run": self.dry_run,
        }


class DatamartRunner(BaseClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db_uri = kwargs.get("db_uri")
        self.cleaning_sql = kwargs.get("cleaning_sql")
        self.report_sql = kwargs.get("report_sql")
        self.base_dir = Path(__file__).resolve().parents[1]

    def _resolve_sql_path(self, sql_path: str) -> Path:
        path = Path(sql_path)
        if path.is_absolute():
            return path
        return self.base_dir / path

    def _split_statements(self, sql_text: str) -> list[str]:
        statements = []
        buffer = []
        for line in sql_text.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("--"):
                continue
            buffer.append(line)
            if stripped.endswith(";"):
                statements.append("\n".join(buffer).strip().rstrip(";"))
                buffer = []
        if buffer:
            statements.append("\n".join(buffer).strip().rstrip(";"))
        return [stmt for stmt in statements if stmt]

    def _execute_sql_file(self, engine, sql_file: Path) -> int:
        if not sql_file.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file}")

        sql_text = sql_file.read_text(encoding="utf-8")
        statements = self._split_statements(sql_text)
        if not statements:
            logger.warning(f"No SQL statement found in {sql_file}")
            return 0

        executed = 0
        with engine.begin() as conn:
            for stmt in statements:
                conn.execute(text(stmt))
                executed += 1
        logger.info(f"Executed {executed} statements from {sql_file.name}")
        return executed

    def consume(self):
        engine = create_engine(self.db_uri)
        cleaning_path = self._resolve_sql_path(self.cleaning_sql)
        report_path = self._resolve_sql_path(self.report_sql)

        logger.info("Running datamart pipeline: cleaning + report SQL")
        cleaning_count = self._execute_sql_file(engine, cleaning_path)
        report_count = self._execute_sql_file(engine, report_path)

        self.produce(cleaning_count, report_count)

    def produce(self, cleaning_count: int, report_count: int):
        logger.info(
            f"Datamart finished. cleaning_statements={cleaning_count}, report_statements={report_count}"
        )
        return {
            "cleaning_statements": cleaning_count,
            "report_statements": report_count,
        }
