# ATS-Friendly-Resume-Screener
A fast, PySpark-powered resume screening system that analyzes job descriptions and PDF resumes, matches required skills, ranks candidates, and exports results—featuring parallel processing and real-time progress tracking.

A Python and PySpark-based resume screening system that analyzes a job description and ranks resumes based on required skills.

The system supports PDF, TXT, CSV, and JSON files and processes multiple resumes in parallel while displaying real-time progress.

## Features

- Analyze job descriptions
- Extract text from PDF resumes
- Support TXT, CSV, JSON, and PDF files
- Detect required skills from the job description
- Match candidate skills against job requirements
- Calculate resume match percentages
- Rank candidates by match score
- Process multiple resumes concurrently
- Display real-time processing progress
- Export results to CSV
- Use PySpark for candidate ranking and data processing

## Technologies

- Python
- PySpark
- PyPDF
- Pandas
- Tkinter
- CSV / JSON

## Project Workflow

text
Job Description
       |
       v
Text Extraction
       |
       v
Required Skill Detection
       |
       v
Resume Selection
       |
       v
Parallel Resume Processing
       |
       v
Skill Matching
       |
       v
Match Percentage
       |
       v
PySpark Ranking
       |
       v
CSV Results

How It Works
1. Select a Job Description

The application opens a file selection window where you can select a:

PDF
TXT
CSV
JSON

The system extracts the text and identifies skills from the predefined skill list.

2. Select Resumes

You can either:

Select multiple resume files
Select a folder containing resumes

Supported formats:

PDF
TXT
CSV
JSON
3. Resume Processing

Multiple resumes are processed concurrently to reduce waiting time.

The terminal displays the current progress:

Processing 20 resume(s) using 8 workers...

[1/20] resume_01.pdf
[2/20] resume_02.pdf
[3/20] resume_03.pdf
...
[20/20] resume_20.pdf
4. Skill Matching

The system compares the skills found in each resume with the skills required by the job description.

For example:

Required Skills:
Python
SQL
Spark
AWS
Docker

A candidate with:

Python
SQL
Spark

would receive a match score based on the number of required skills found.

5. Candidate Ranking

PySpark is used to create and sort the candidate dataset by match percentage.

Example:
Candidate Ranking
────────────────────────────────────────────────────────
Resume              Matched Skills    Match %
────────────────────────────────────────────────────────
candidate1.pdf      5                  100.00%
candidate2.pdf      4                   80.00%
candidate3.pdf      3                   60.00%
candidate4.pdf      2                   40.00%
────────────────────────────────────────────────────────

6. Export Results

The final results are saved as:

resume_ranking_results.csv

The CSV contains:

Resume name
Number of matched skills
Match percentage
Skills found
Installation
Requirements

Python 3.10+ is recommended.
