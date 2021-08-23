---
title: "Orchestrating queries with Airflow"
slug: "Orchestrating queries with Airflow"
parent: "教程"
---

This tutorial walks through the development of Apache Airflow DAG that implements a basic ETL process using Apache Drill.  We'll install Airflow into a Python virtualenv using pip before writing and testing our new DAG.  Consult the [Airflow installation documentation](https://airflow.apache.org/docs/apache-airflow/stable/installation.html) for more information about installing Airflow.

I'll be issuing commands using a shell on a Debian Linux machine in this tutorial but it should be possible with a little translation to follow along on other platforms.

## Prerequisites

1. A Python >= 3.6 installation, including pip and optionally virtualenv.
2. A Drill installation where you have access to run queries and add new storage providers.  I'll be running an embedded mode Drill 1.19.

## (Optional) Set up a virtualenv

Create and activate a new virtualenv called "airflow".  If needed, adjust the Python interpreter path and virtualenv target path arguments for your environment.
```sh
VIRT_ENV_HOME=~/.local/lib/virtualenv
virtualenv -p /usr/bin/python3 $VIRT_ENV_HOME/airflow
. $VIRT_ENV_HOME/airflow/activate
```

## Install Airflow

If you've read their installation guide you'll have seen that the Airflow project provides constraints files the pin the versions of its Python package dependencies to known-good versions.  In many cases things work fine without constraints but, for the sake of reproducibility, we'll apply the constraints file applicable to our Python version using the script 0they provide for the purpose.
```sh
AIRFLOW_VERSION=2.1.2
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-0airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
pip install apache-airflow-providers-apache-drill
```

## Initialise Airflow

We're just experimenting here so we'll have Airflow set up a local SQLite database and add an admin user for ourselves.
```sh
# Optional: change Airflow's data dir from the default of ~/airflow
export0 AIRFLOW_HOME=~/Development/airflow
mkdir -p ~/Development/airflow/

# Create a new SQLite database for Airflow
airflow db init

# Add an admin user
airflow users create \
	--username admin \
	--firstname FIRST_NAME \
	--lastname LAST_NAME \
	--role Admin \
	--email admin@example.org \
	--password admin
```

## Configure a Drill connection

At this point we should have a working Airflow installation. Fire up the web UI with `airflow webserver` and browse to http://localhost:8080.  Click on Admin -> Connections.  Add a new Drill connection called `drill_tutorial`, setting configuration according to your Drill environment.  If you're using embedded mode Drill locally like I am then you'll want the following config.

| Setting   | Value                                                        |
| --------- | ------------------------------------------------------------ |
| Conn Id   | drill_tutorial                                               |
| Conn Type | Drill                                                        |
| Host      | localhost                                                    |
| Port      | 8047                                                         |
| Extra     | {"dialect_driver": "drill+sadrill", "storage_plugin": "dfs"} |

Note that the sqlalchemy-drill dialect and driver information must be specified in the `Extra` field.  See [the sqlalchemy-drill documentation](https://github.com/JohnOmernik/sqlalchemy-drill) for more information about its configuration. 

After you've saved the new connection you can shut the Airflow web UI down with ctrl+c.

## Explore the source data

If you've built ETLs before you know that you can't build anything until you've come to grips with the source data.  Let's obtain a sample of the first 1m rows from the source take a look.

```sh
curl -s https://data.cdc.gov/api/views/vbim-akqf/rows.csv\?accessType\=DOWNLOAD | pv -lSs 1000000 > /tmp/cdc_covid_cases.csvh
```

You can replace `pv -lSs 1000000` above with `head -n1000000` or just drop it if you don't mind fetching the whole file.  Downloading it with a web browser will also work fine.  Note that for a default Drill installation, saving with the file extension `.csvh` does matter for what follows because it will set `extractHeader = true` when this CSV file is queried, and this file does include a header.

It's time to break out Drill.  Instead of dumping my entire interactive SQL session here, I'll just list queries that I ran and the corresponding observations that I made.
```sql
select * from dfs.tmp.`cdc_covid_case.csvh`
-- 1. In date fields, the empty string '' can be converted to SQL NULL
-- 2. Age groups can be split into two numerical fields, with the final
--    group being unbounded above.

select age_group, count() from dfs.tmp.`cdc_covid_case.csvh` group by age_group;
select sex, count() from dfs.tmp.`cdc_covid_case.csvh` group by sex;
select race_ethnicity_combined, count() from dfs.tmp.`cdc_covid_case.csvh` group by race_ethnicity_combined;
-- 3. The string 'Missing' can be transformed to SQL NULL
-- 4. I should really uncover what the difference between 'NA' and 'Missing' is
-- 	  but for this tutorial 'NA' is going to transformed to NULL too
-- 5. race_ethnicity_combined could possibly be split into two fields but we'll
--    leave it as is for this tutorial.

select hosp_yn, count() from dfs.tmp.`cdc_covid_case.csvh` group by hosp_yn;
-- 6. In addition to 'Missing, indicator variables have three possible values
--    so they cannot be transformed to nullable booleans
```

So... this is what it feels like to be a data scientist 😆.  Jokes aside, we learned a lot of neccesary stuff pretty quickly there and it's easy to see that we could have carried on for a long way, testing ranges, casts and regexps and even creating reports if we didn't reign ourselves in.  Let's skip forward to the ETL statement I ended up creating after exploring.

## Develop a CTAS (Create Table As Select) ETL

```sql
drop table if exists dfs.tmp.cdc_covid_cases;

create table dfs.tmp.cdc_covid_cases as
with missing2null as (
select
	nullif(cdc_case_earliest_dt, '') cdc_case_earliest_dt,
	nullif(cdc_report_dt, '') cdc_report_dt,
	nullif(pos_spec_dt, '') pos_spec_dt,
	nullif(onset_dt, '') onset_dt,
	case when current_status not in ('Missing', 'NA') then current_status end current_status,
	case when sex not in ('Missing', 'NA') then sex end sex,
	case when age_group not in ('Missing', 'NA') then age_group end age_group,
	case when race_ethnicity_combined not in ('Missing', 'NA') then race_ethnicity_combined end race_ethnicity_combined,
	case when hosp_yn not in ('Missing', 'NA') then hosp_yn end hosp_yn,
	case when icu_yn not in ('Missing', 'NA') then icu_yn end icu_yn,
	case when death_yn not in ('Missing', 'NA') then death_yn end death_yn,
	case when medcond_yn not in ('Missing', 'NA') then medcond_yn end medcond_yn
from
	dfs.tmp.`cdc_covid_cases.csvh`),
age_parse as (
select 
	*,
	regexp_replace(age_group, '([0-9]+)[ \-\+]+([0-9]*) Years', '$1') age_min_incl,
	regexp_replace(age_group, '([0-9]+)[ \-\+]+([0-9]*) Years', '$2') age_max_excl
from
missing2null)
select
	cast(cdc_case_earliest_dt as date) cdc_case_earliest_dt,
	cast(cdc_report_dt as date) cdc_report_dt,
	cast(pos_spec_dt as date) pos_spec_dt,
	cast(onset_dt as date) onset_dt,
	current_status,
	sex,
	age_group,
	cast(age_min_incl as float) age_min_incl,
	1 + cast(case when age_max_excl = '' then 'Infinity' else age_max_excl end as float) age_max_excl,
	race_ethnicity_combined,
	hosp_yn,
	icu_yn,
	death_yn,
	medcond_yn
from
	age_parse;
```

That's a substantial SQL statement but it covers a fair amount of transformation work and takes us all the way to an output of one (or more) Parquet files, efficient and clean representations of our dataset that are well suited for analytical or ML work.  Consider what we have _not_ done to get this far.

- We have no configuration hidden in the checkboxes and wizards of an ETL package,
- we have not had to add another language to the SQL we used to explore and test trasformations at the outset and
- we have not worried about performance or how to parallelise our data flow because we've left that aspect to Drill.

In addition, while I've yet to hear of SQL winning a language beauty contest, our ETL code feels obvious, self-contained and maintainable.  I'd have no qualms with reviewing a line-by-line diff of this code to isolate a change after a hiatus of months or years, nor any with pointing a SQL-conversant colleague at it with little or even no introduction.  The veteran coder knows that these mundane advantages can swing an extended campaign.

To complete this step, save the CTAS script above into a new file at `$AIRFLOW_HOME/dags/cdc_covid_cases.drill.sql`.  The double file extension is just a little convention I use to indicate both the dialect and the language of my SQL scripts, and entirely optional if it's not to your taste.

## Develop an Airflow DAG

The definition of our DAG will reside in a single Python script.  The complete listing of that script follows immediately, with my commentary continuing as inline source code comments.  You should save this script to a new file at `$AIRFLOW_HOME/dags/drill_tutorial.py`.

```python
'''
Uses the Apache Drill provider to transform, load and report from COVID case
data downloaded from the website of the CDC.

Data source citatation.

Centers for Disease Control and Prevention, COVID-19 Response. COVID-19 Case
Surveillance Public Data Access, Summary, and Limitations.

https://data.cdc.gov/Case-Surveillance/COVID-19-Case-Surveillance-Public-Use-Data/vbim-akqf
'''
from datetime import timedelta

from airflow import DAG
# We'll use a PythonOperator to stage COVID-19 CSV file from the CDC web site
from airflow.operators.python import PythonOperator
# We'll use DrillOperators to kick off queries against the COVID-19 data
from airflow.providers.apache.drill.operators.drill import DrillOperator
from airflow.utils.dates import days_ago
# We can assume requests is present because sqlalchemy-drill requires it
import requests
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Joe Public',
    'depends_on_past': False,
    'email': ['joe@public.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def stage_from_www(src_url, tgt_path):
    '''
    Uses the Requests lib to GET case surveillance data from CDC to a local
    path.  If you're in a distributed environment you'll want to replace the
    local filesystem with HDFS, S3, etc.  Another option is to configure
    Drill's HTTP storage plugin to fetch the data directly from the source.
    '''
    resp = requests.get(
        src_url,
        stream=True  # don't buffer big datasets in memory
    )
    with open(tgt_path) as f:
        f.write(resp.content)


with DAG(
    'drill_tutorial',
    default_args=default_args,
    description='Drill tutorial that loads COVID-19 case data from the CDC.',
    schedule_interval=timedelta(weeks=2),  # source is updated every two weeks
    start_date=days_ago(0),
) as dag:

    # Use this module's docstring for DAG's documentation (visible in the web UI)
    dag.doc_md = __doc__

    # First task is a PythonOperator to GET the CSV data from the CDC website
    stage_from_www_task = PythonOperator(
        task_id='stage_from_www',
        python_callable=stage_from_www,
        op_kwargs= {
            'src_url': 'https://data.cdc.gov/api/views/vbim-akqf/rows.csv?accessType=DOWNLOAD',
            'tgt_path': '/tmp/cdc_covid_cases.csvh'
        }
    )

    stage_from_www.doc = 'Download COVID case CSV data from the CDC using ' \
        'an HTTP GET'

    # Second task is a DrillOperator the executes our CTAS ETL from an external
    # script.  It's also possible to specify inline SQL, and to split  this
    # multi-statement SQL script across tasks e.g. if you prefer to have
    # the inital DROP TABLE be a separate task.
    ctas_etl_task = DrillOperator(
        drill_conn_id='drill_tutorial',
        task_id='ctas_etl',
        sql='cdc_covid_cases.drill.sql'
    )

    ctas_etl_task.doc = 'Recreate dfs.tmp.cdc_covid_cases using CTAS'

    # Third task is a DrillOperator that produces a daily case count report.
    # We just write the report back out to dfs.tmp as human-readable CSV, but
    # you should imagine using Airflow to route and deliver it in any number
    # of ways.
    daily_count_report_task = DrillOperator(
        drill_conn_id='drill_tutorial',
        task_id='drill_report',
        sql='''
        set `store.format` = 'csv';

        drop table if exists dfs.tmp.cdc_daily_counts;

        create table dfs.tmp.cdc_daily_counts as
        select
            cdc_case_earliest_dt,
            count(*) as case_count
        from
            dfs.tmp.cdc_covid_cases
        group by
            cdc_case_earliest_dt
        order by
            cdc_case_earliest_dt;
        '''
    )

    daily_count_report_task.doc = 'Report daily case counts to CSV'

    # Specify the edges of the DAG, i.e. the task dependencies
    stage_from_www_task >> ctas_etl_task >> daily_count_report_task
age_parse;
```

## Manually launch the Airflow DAG

You can harmlessly test the Python syntax of a DAG script by running it through the interpreter.
```sh
python3 $AIRFLOW_HOME/dags/drill-tutorial.py
```

If all is well Python will exit without errors and you can proceed to ensure that your Drillbit is running, then launch a test run of you DAG using airflow.
```sh
airflow dags test drill_tutorial $(date +%Y-%m-%d)
```

After a delay while the COVID case dataset is downloaded to your machine you should start to see all of the queries executed on Drill logged to your console by sqlalchemy-drill.  The DAG execution should have produced two outputs.

1. A Parquet dataset at `$TMPDIR/cdc_covid_cases` at the individual case grain.
2. A CSV daily surveilled case count report at `$TMPDIR/cdc_daily_counts`.

Try some OLAP in Drill with the first and take a look at the second in a spreadsheet or text editor.

Congratulations, you built an ETL using Apache Airflow and Apache Drill!

## Next steps

- [Read about Airflow scheduling](https://airflow.apache.org/docs/apache-airflow/1.10.1/scheduler.html) and run the scheduler as a daemon to have your job run automatically.
- Try adapting the DAG here to work with other data sources.  If you have databases, files and web services in your own environment those will be natural choices, otherwise you can look around online for more public datasets and APIs.
- Instead of replacing the target dataset, try adding new partitions to an existing dataset by aiming CTAS at date-labelled subdirectories.
- Keep an eye out for data crunching steps in existing workflows, including those which are not strictly ETL pipelines, where Drill could shoulder some of the load.

Thanks for joining us for this tutorial and happy Drilling!

