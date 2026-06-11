from __future__ import annotations

import argparse
import os
from pathlib import Path

import matplotlib.pyplot as plt
from databricks.connect import DatabricksSession
from databricks.sdk.core import Config
from pyspark.sql import functions as F


PROFILE = os.getenv("DATABRICKS_CONFIG_PROFILE", "hindman.gmail.com@auth.researchaccelerator.org")
CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID", "0303-193859-1ff54asc")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify Databricks Connect from the local VS Code environment.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print local configuration and import status; do not contact Databricks.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.dry_run:
        print("imports-ok")
        print(f"profile={PROFILE}")
        print(f"cluster_id={CLUSTER_ID}")
        return

    config = Config(profile=PROFILE, cluster_id=CLUSTER_ID)
    spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()

    identity = spark.sql("select current_user() as user, current_timestamp() as ts").toPandas()
    print(identity.to_string(index=False))

    pdf = spark.range(12).withColumn("square", F.col("id") * F.col("id")).toPandas()
    print(pdf.head().to_string(index=False))

    figs_dir = Path("figs")
    figs_dir.mkdir(exist_ok=True)
    out = figs_dir / "databricks_connect_smoke.png"

    ax = pdf.plot(x="id", y="square", marker="o", legend=False)
    ax.set_title("Databricks Connect Smoke Test")
    ax.set_xlabel("id")
    ax.set_ylabel("id squared")
    ax.figure.tight_layout()
    ax.figure.savefig(out, dpi=150)
    plt.close(ax.figure)

    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
