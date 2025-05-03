# CleanAir Nexus: Analyzing the Impact of Air Pollution on Fatalities and Population

This project analyzes how **air pollution** correlates with **fatality rates** and **population size** across countries using real-world datasets. It features a fully automated data pipeline built with **Dagster**, and an interactive **Flask dashboard** that launches automatically when the pipeline is run.

---

## Project Objectives

- Explore how air pollution contributes to death rates from smoking, tuberculosis, and cardiovascular diseases.
- Examine the relationship between population size and air pollution levels.
- Automate the entire ETL + visualization pipeline using Dagster.
- Deliver an interactive dashboard to visualize findings.

---

## Tools & Technologies

| Layer                | Tools Used                                          |
|---------------------|-----------------------------------------------------|
| Data Ingestion       | `pandas`, `json`, `csv`                            |
| Raw Data Storage     | `MongoDB`                                          |
| Data Orchestration   | `Dagster` (assets, materialization, logging)       |
| Clean Data Storage   | `PostgreSQL` via `SQLAlchemy`                      |
| Visualization        | `matplotlib`, `seaborn`                            |
| Dashboard UI         | `Flask` (auto-launched via pipeline)               |

---

## Project Structure

my-dagster-project/

│

├── datasets/

│ ├── gab.csv

│ ├── worldcities.csv

│ └── death.json

│

├── graph/

│ └── *.png ← All generated visualizations

│

├── my_dagster_project/

│ ├── assets.py ← Dagster assets (ETL + plotting + Flask app launch)

│ ├── dash_app.py ← Flask backend (auto-run)

│ └── definitions.py ← Dagster asset registration

│

├── my_dagster_project_tests/

│ └── test_assets.py ← Sample tests

│

├── README.md

├── requirements.txt

└── .gitignore

---

## Datasets Used

1. **gab.csv** – Country-level AQI & pollutant values (CO, NO2, Ozone, PM2.5)
2. **worldcities.csv** – Population and coordinates of cities
3. **death.json** – Deaths caused by smoking, air pollution, TB, and cardiovascular diseases

---

##  How to Run This Project

### 1. Clone the Repository

git clone https://github.com/your-username/my-dagster-project.git
~~~bash

cd my-dagster-project
~~~
### 2. Install Required Packages
~~~bash

pip install -r requirements.txt
~~~
### 3. Ensure MongoDB and PostgreSQL Are Running
MongoDB: localhost:27017

PostgreSQL:

Host: localhost

Port: 5432

User: postgres( use your own user's name)

Password: Admin (use your own password)

Database: dagster (change if you are using different database)

### 4. Start Dagster
~~~bash

dagster dev
~~~
### 5. Launch the Pipeline in Dagster UI
Open: http://localhost:3000

Click "Materialize All"

This triggers the full pipeline:

Loads & cleans data

Stores to PostgreSQL

Generates plots

Stores graph paths

Automatically launches the Flask dashboard at:
http://localhost:5000 

Once materialize all, you can run dash_app.py separately also. 
## How to Create a Dagster Project from Scratch
If you want to build a Dagster pipeline like this one, follow these steps:

### 1. Create a New Dagster Project
~~~ bash

pip install dagster dagster-webserver
dagster project scaffold --name my_dagster_project
~~~

This generates a basic folder structure:


my_dagster_project/

├── my_dagster_project/

│   ├── assets.py         ← Your main data pipeline logic

│   └── definitions.py    ← Register all assets here

├── my_dagster_project_tests/

├── pyproject.toml

### 2. Add Your Data Logic
Write ETL and plotting logic in assets.py using @asset.

Split logic into modular, chainable assets (data loading → cleaning → saving → visualizing).

Use context logging for pipeline monitoring inside Dagster UI.

### 3. Register Assets
Edit definitions.py:

~~~ python
from dagster import Definitions, load_assets_from_modules
from my_dagster_project import assets

all_assets = load_assets_from_modules([assets])
defs = Definitions(assets=all_assets)
~~~

### 4. Start Dagster UI
~~~bash

dagster dev
~~~
Open: http://localhost:3000

Click "Materialize All" to trigger the full pipeline.

## Contributors
Semanta Das 

Muhammad Wasit Shahbaz

Dnyanesh Mohan Bhonde 

