import csv
import json
import os
import re
import sys
import tkinter as tk
from concurrent.futures import ThreadPoolExecutor, as_completed
from tkinter import filedialog

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SKILLS = [
    "python", "sql", "java", "spark", "machine learning", "excel",
    "tableau", "hadoop", "hive", "kafka", "aws", "docker",
    "kubernetes", "mongodb",
]

SUPPORTED_EXTENSIONS = (".txt", ".csv", ".json", ".pdf")


def clean_text(text):
    """Normalize text for matching."""
    text = re.sub(r"[^a-zA-Z0-9+#.\s]", " ", text or "")
    return re.sub(r"\s+", " ", text).strip().lower()


def extract_text(file_path):
    """Extract text from TXT, CSV, JSON, or PDF."""
    ext = os.path.splitext(file_path)[1].lower()

    try:
        if ext == ".pdf":
            from pypdf import PdfReader

            reader = PdfReader(file_path)
            return " ".join(page.extract_text() or "" for page in reader.pages)

        if ext == ".txt":
            with open(file_path, encoding="utf-8", errors="ignore") as file:
                return file.read()

        if ext == ".csv":
            with open(file_path, encoding="utf-8", errors="ignore") as file:
                return " ".join(
                    " ".join(str(value) for value in row.values())
                    for row in csv.DictReader(file)
                )

        if ext == ".json":
            with open(file_path, encoding="utf-8", errors="ignore") as file:
                return json.dumps(json.load(file), ensure_ascii=False)

    except Exception as exc:
        print(f"\nWarning: {os.path.basename(file_path)} -> {exc}")

    return ""


def select_files():
    """Select the job description and resumes."""
    root = tk.Tk()
    root.withdraw()

    print("\nSELECT JOB DESCRIPTION")
    job_path = filedialog.askopenfilename(
        title="Select Job Description",
        filetypes=[
            ("Supported files", "*.txt *.csv *.json *.pdf"),
            ("All files", "*.*"),
        ],
    )

    if not job_path:
        return None, []

    print("\nSELECT RESUMES")
    print("Select multiple files, or click Cancel to choose a folder.")

    resume_paths = list(
        filedialog.askopenfilenames(
            title="Select Resume Files",
            filetypes=[
                ("Supported files", "*.txt *.csv *.json *.pdf"),
                ("All files", "*.*"),
            ],
        )
    )

    if not resume_paths:
        folder = filedialog.askdirectory(title="Select Resume Folder")

        if folder:
            resume_paths = [
                os.path.join(folder, name)
                for name in os.listdir(folder)
                if name.lower().endswith(SUPPORTED_EXTENSIONS)
            ]

    return job_path, resume_paths


def read_resumes_parallel(resume_paths, workers=None):
    """Read resumes in background threads and report progress."""
    total = len(resume_paths)
    workers = workers or min(8, max(2, total))

    print(f"\nProcessing {total} resume(s) in background...")
    print(f"Using {workers} background worker(s).\n")

    results = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(extract_text, path): path
            for path in resume_paths
        }

        for completed, future in enumerate(as_completed(futures), 1):
            path = futures[future]

            try:
                text = future.result()
            except Exception as exc:
                text = ""
                print(f"  ERROR: {os.path.basename(path)} -> {exc}")

            results.append((path, text))

            print(
                f"  [{completed:>3}/{total}] "
                f"Completed: {os.path.basename(path)}"
            )

    # Keep the original file order for predictable output.
    return sorted(results, key=lambda item: resume_paths.index(item[0]))


def analyze_resumes(job_path, resume_paths):
    """Extract text and calculate skill matches."""
    print("\n" + "=" * 60)
    print("ANALYZING JOB DESCRIPTION")
    print("=" * 60)

    print(f"Reading: {os.path.basename(job_path)}")
    job_text = clean_text(extract_text(job_path))

    required_skills = [
        skill for skill in SKILLS
        if skill in job_text
    ]

    print(f"Required skills found: {len(required_skills)}")
    print(f"Skills: {', '.join(required_skills) or 'None'}")

    print("\n" + "=" * 60)
    print("PROCESSING RESUMES")
    print("=" * 60)

    resume_texts = read_resumes_parallel(resume_paths)

    results = []

    for path, text in resume_texts:
        resume_text = clean_text(text)

        matched_skills = [
            skill for skill in required_skills
            if skill in resume_text
        ]

        score = (
            len(matched_skills) / len(required_skills) * 100
            if required_skills
            else 0
        )

        results.append(
            (
                os.path.basename(path),
                len(matched_skills),
                round(score, 2),
                ", ".join(matched_skills),
            )
        )

    return required_skills, results


def create_spark():
    """Create a lightweight local Spark session."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ["HADOOP_HOME"] = ""
    os.environ["HADOOP_CONF_DIR"] = ""

    spark = (
        SparkSession.builder
        .appName("Resume Screening")
        .master("local[1]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main():
    print("=" * 60)
    print("RESUME SCREENING SYSTEM")
    print("=" * 60)

    spark = None

    try:
        print("\nStarting PySpark...")
        spark = create_spark()
        print(f"Spark {spark.version} started.")

        job_path, resume_paths = select_files()

        if not job_path:
            print("\nNo job description selected.")
            return

        if not resume_paths:
            print("\nNo resumes found.")
            return

        print(f"\nJob description: {os.path.basename(job_path)}")
        print(f"Resume files: {len(resume_paths)}")

        required_skills, results = analyze_resumes(
            job_path, resume_paths
        )

        if not results:
            print("\nNo results generated.")
            return

        print("\n" + "=" * 60)
        print("CREATING SPARK DATAFRAME")
        print("=" * 60)

        df = spark.createDataFrame(
            results,
            [
                "Resume",
                "Matched_Skills",
                "Match_Percentage",
                "Skills_Found",
            ],
        ).orderBy(
            F.col("Match_Percentage").desc()
        )

        print("\n" + "=" * 60)
        print("CANDIDATE RANKING")
        print("=" * 60)

        df.show(truncate=False)

        stats = df.select(
            F.avg("Match_Percentage").alias("average"),
            F.max("Match_Percentage").alias("best"),
            F.min("Match_Percentage").alias("lowest"),
        ).first()

        print("=" * 60)
        print("STATISTICS")
        print("=" * 60)
        print(f"Total candidates: {df.count()}")
        print(f"Average match: {stats.average:.2f}%")
        print(f"Best match: {stats.best:.2f}%")
        print(f"Lowest match: {stats.lowest:.2f}%")

        output_dir = os.path.dirname(resume_paths[0]) or os.getcwd()
        output_csv = os.path.join(
            output_dir,
            "resume_ranking_results.csv",
        )

        print("\nSaving results...")
        df.toPandas().to_csv(output_csv, index=False)

        print(f"Results saved to: {output_csv}")
        print("\nAnalysis complete.")

    except Exception as exc:
        print(f"\nERROR: {exc}")

    finally:
        if spark:
            print("\nStopping Spark...")
            spark.stop()
            print("Spark stopped.")


if __name__ == "__main__":
    main()
