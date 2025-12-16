# User Session Duration Analysis (SQL)

## 1. Business Problem

The goal of this project is to calculate how many hours users spend on a website per day over the **last 10 days**.

At first glance, the task seems straightforward: pair `open` and `close` events and compute session duration. However, real-world event data contains inconsistencies that can heavily distort the final metrics if not handled properly.

This project demonstrates how **naive session pairing leads to incorrect results** and how to design a **robust SQL-only solution** that accounts for data quality issues.

---

## 2. Dataset Description

The dataset represents user session events with the following fields:

* `id` – unique record identifier
* `id_user` – unique user identifier
* `action` – session event type (`open` or `close`)
* `timestamp_action` – timestamp of the event

The dataset is complete (no NULL values) and contains ~1.8M records.

---

## 3. Key Data Challenges Identified (EDA)

During exploratory data analysis, several critical issues were discovered:

* Duplicate records
* `close` events without a preceding `open`
* Consecutive identical actions (`open → open`, `close → close`)
* Sessions without a closing event
* Extremely long sessions (> 24 hours)

Although unclosed sessions represent less than 1% of the data, they produce extreme outliers (e.g. hundreds of hours per day) and must be handled explicitly.

Detailed analysis is provided in **EDA.pdf**.

---

## 4. Solution Strategy

Instead of simply removing problematic records, the solution follows a **data quality–aware approach**:

1. Deduplicate events
2. Validate logical event order per user
3. Build sessions using window functions
4. Flag problematic sessions instead of blindly dropping them
5. Split sessions across calendar days
6. Aggregate usage per user per day
7. Allow downstream filtering using quality flags

This approach preserves analytical flexibility and makes assumptions explicit.

A full step-by-step explanation is provided in **Case_Study.pdf**.

---

## 5. SQL Techniques Used

* Common Table Expressions (CTEs)
* Window functions (`LAG`, `LEAD`)
* Conditional logic (`CASE`)
* Date and time arithmetic
* Session splitting across days
* Data quality flagging
* Array and date generation

---

## 6. Repository Structure

```
user-session-analysis/
│
├── README.md
│
├── docs/
│   ├── EDA.pdf
│   └── Solution.pdf
│
├── sql/
│   └── user_session_analysis.sql
│
└── results/
    └── result.csv/
```

---

## 7. Final Output

The final result is aggregated **per user per day** and includes:

* Total hours spent on the website
* Flags indicating data quality issues
* A final inclusion indicator (`OK` / `CHECK`)

This allows analysts to flexibly include or exclude questionable data depending on business requirements.

---

## 8. Key Takeaway

This project illustrates that **session-based metrics are highly sensitive to data quality assumptions**.

A technically correct SQL query can still produce meaningless results if real-world edge cases are ignored. Designing robust analytics requires both SQL skills and analytical judgment.

