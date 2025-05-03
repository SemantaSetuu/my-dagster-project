from multiprocessing import context
import pandas as pd
import numpy as np
from pymongo import MongoClient
import json
from dagster import asset
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from pathlib import Path
import webbrowser
import seaborn as sns
import pycountry
from fuzzywuzzy import process
import subprocess
import socket
import time
import requests
import os

countries = [country.name for country in pycountry.countries]
client = MongoClient('mongodb://localhost:27017/')
db = client['database']
collection1 = db['gap_collection']
collection2 = db['city_collection']
collection3 = db['deaths_collection']
DB_URI = "postgresql://postgres:Admin@localhost:5432/dagster"
graph = {}


@asset
def load_data_1(context):
    try:
        df = pd.read_csv('datasets/gab.csv')
        context.log.info(f"Data loaded successfully. Here's a preview:\n{df.head()}")
        context.log.info(f"Dataset info:\n{df.info()}")
        df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
        return df
    except Exception as e:
        context.log.error(f"Error loading data: {e}")
        raise Exception(f"Error loading data")


@asset
def insert_data_1(context, load_data_1):
    try:
        df = load_data_1
        context.log.info(f"Data to be inserted into MongoDB:\n{df}")
        records = json.loads(df.to_json(orient='records'))
        if collection1.count_documents({}) == 0:
            collection1.insert_many(records)
            context.log.info("Data successfully inserted into MongoDB.")
        else:
            context.log.info("Data is Already in collection.")
    except Exception as e:
        context.log.error(f"Error connecting to MongoDB: {e}")
        raise Exception(f"Error connecting to MongoDB: {e}")


@asset
def preparing_data_1(context, insert_data_1):
    # collection=insert_data_1['collection']
    try:
        extracted_data = list(collection1.find())
        context.log.info("Data successfully extracted from MongoDB.")
    except Exception as e:
        context.log.error(f"Error extracting data from MongoDB: {e}")
        raise Exception(f"Error extracting data from MongoDB: {e}")
    try:
        df_extracted = pd.DataFrame(extracted_data)
        df_extracted.drop(columns=['_id'], inplace=True, errors='ignore')
        df_extracted = df_extracted.apply(lambda x: x.str.lower() if x.dtype == "object" else x)
        mapping = {
            "good": 0,
            "moderate": 1,
            "unhealthy for sensitive groups": 2,
            "unhealthy": 3,
            "very unhealthy": 4,
            "hazardous": 5
        }
        df_extracted["aqi_category"] = df_extracted["aqi_category"].replace(mapping)
        df_extracted['country'] = df_extracted['country'].fillna('unknown')
        if 'city' in df_extracted.columns:
            mode_value = df_extracted['city'].mode()[0]
            df_extracted['city'] = df_extracted['city'].fillna(mode_value)
        context.log.info("Data cleaning and transformation completed successfully.")
        return df_extracted
    except Exception as e:
        context.log.error(f"Error during data transformation: {e}")
        raise Exception(f"Error during data transformation: {e}")


@asset
def saving_data_postgre_1(context, preparing_data_1):
    df_extracted = preparing_data_1
    engine = create_engine(DB_URI)
    try:
        df_extracted.to_sql('gab_data', engine, if_exists='replace', index=False)
        context.log.info("Transformed data successfully inserted into postgre.")
        return {'table': 'gab_data'}
    except Exception as e:
        context.log.error(f"Error inserting transformed data into postgre: {e}")
        raise Exception(f"Error inserting transformed data into postgre: {e}")


@asset
def average_aqi_by_country(context, saving_data_postgre_1):
    try:
        engine = create_engine(DB_URI)
        query = f"SELECT * FROM {saving_data_postgre_1['table']}"
        df_extracted = pd.read_sql(query, engine)
        avg_per_country = df_extracted.groupby('country')['aqi_value'].mean().reset_index()
        plt.figure(figsize=(18, 12))
        plt.bar(avg_per_country['country'], avg_per_country['aqi_value'])
        plt.title('Average AQI Value per Country')
        plt.xlabel('Country')
        plt.ylabel('Average AQI Value')
        plt.xticks(rotation=90)
        plt.tight_layout()
        graph_path = "C:/Users/seman/my-dagster-project/graph/average_aqi_by_country.png"
        plt.savefig(graph_path)
        plt.close()
        context.log.info("Data visualization for Average AQI Value completed successfully.")
        context.log.info(f"Visualization saved at: {graph_path}")

        graph_paths = [{'graph_name': 'Average AQI by country', 'graph_path': graph_path}]
        df_graph_paths = pd.DataFrame(graph_paths)
        try:
            df_graph_paths.to_sql('Graph', engine, if_exists='append', index=False)
            context.log.info("Graph paths successfully saved into the Graph table in PostgreSQL.")
        except:
            context.log.info(f"Graph path already exsist in PostgreSQL")
        return graph_path
    except Exception as e:
        context.log.error(f"Error during data visualization: {e}")
        raise Exception(f"Error during data visualization: {e}")


@asset
def average_ozone_by_country(context, saving_data_postgre_1):
    try:
        engine = create_engine(DB_URI)
        query = f"SELECT * FROM {saving_data_postgre_1['table']}"
        df_extracted = pd.read_sql(query, engine)
        avg_per_country = df_extracted.groupby('country')['ozone_aqi_value'].mean().reset_index()
        plt.figure(figsize=(18, 12))
        plt.plot(avg_per_country['country'], avg_per_country['ozone_aqi_value'])
        plt.title('Average ozone Value per Country')
        plt.xlabel('Country')
        plt.ylabel('Average ozone Value')
        plt.xticks(rotation=90)
        plt.tight_layout()
        graph_path = "C:/Users/seman/my-dagster-project/graph/average_ozone_by_country.png"
        plt.savefig(graph_path)
        plt.close()
        context.log.info("Data visualization for Average ozone Value completed successfully.")
        context.log.info(f"Visualization saved at: {graph_path}")
        graph_paths = [{'graph_name': 'Average Ozone by Country', 'graph_path': graph_path}]
        df_graph_paths = pd.DataFrame(graph_paths)
        try:
            df_graph_paths.to_sql('Graph', engine, if_exists='append', index=False)
            context.log.info("Graph paths successfully saved into the Graph table in PostgreSQL.")
        except:
            context.log.info(f"Graph path already exsist in PostgreSQL")
        return graph_path
    except Exception as e:
        context.log.error(f"Error during data visualization: {e}")
        raise Exception(f"Error during data visualization: {e}")


@asset
def average_CO_by_country(context, saving_data_postgre_1):
    try:
        engine = create_engine(DB_URI)
        query = f"SELECT * FROM {saving_data_postgre_1['table']}"
        df_extracted = pd.read_sql(query, engine)
        avg_per_country = df_extracted.groupby('country')['co_aqi_value'].mean().reset_index()
        plt.figure(figsize=(18, 12))
        plt.plot(avg_per_country['country'], avg_per_country['co_aqi_value'])
        plt.title('Average CO Value per Country')
        plt.xlabel('Country')
        plt.ylabel('Average CO Value')
        plt.xticks(rotation=90)
        plt.tight_layout()
        graph_path = "C:/Users/seman/my-dagster-project/graph/average_CO_by_country.png"
        plt.savefig(graph_path)
        plt.close()
        context.log.info("Data visualization for Average CO Value completed successfully.")
        context.log.info(f"Visualization saved at: {graph_path}")
        graph_paths = [{'graph_name': 'Average CO by Country', 'graph_path': graph_path}]
        df_graph_paths = pd.DataFrame(graph_paths)
        try:
            df_graph_paths.to_sql('Graph', engine, if_exists='append', index=False)
            context.log.info("Graph paths successfully saved into the Graph table in PostgreSQL.")
        except:
            context.log.info(f"Graph path already exsist in PostgreSQL")
        return graph_path
    except Exception as e:
        context.log.error(f"Error during data visualization: {e}")
        raise Exception(f"Error during data visualization: {e}")


@asset
def average_NO2_by_country(context, saving_data_postgre_1):
    try:
        engine = create_engine(DB_URI)
        query = f"SELECT * FROM {saving_data_postgre_1['table']}"
        df_extracted = pd.read_sql(query, engine)
        avg_per_country = df_extracted.groupby('country')['no2_aqi_value'].mean().reset_index()
        plt.figure(figsize=(18, 12))
        plt.plot(avg_per_country['country'], avg_per_country['no2_aqi_value'])
        plt.title('Average NO2 Value per Country')
        plt.xlabel('Country')
        plt.ylabel('Average NO2 Value')
        plt.xticks(rotation=90)
        plt.tight_layout()
        graph_path = "C:/Users/seman/my-dagster-project/graph/average_NO2_by_country.png"
        plt.savefig(graph_path)
        plt.close()
        context.log.info("Data visualization for Average NO2 Value completed successfully.")
        context.log.info(f"Visualization saved at: {graph_path}")
        graph_paths = [{'graph_name': 'Average NO2 by Country', 'graph_path': graph_path}]
        df_graph_paths = pd.DataFrame(graph_paths)
        try:
            df_graph_paths.to_sql('Graph', engine, if_exists='append', index=False)
            context.log.info("Graph paths successfully saved into the Graph table in PostgreSQL.")
        except:
            context.log.info(f"Graph path already exsist in PostgreSQL")
        return graph_path
    except Exception as e:
        context.log.error(f"Error during data visualization: {e}")
        raise Exception(f"Error during data visualization: {e}")


@asset
def average_PM25_by_country(context, saving_data_postgre_1):
    try:
        engine = create_engine(DB_URI)
        query = f"SELECT * FROM {saving_data_postgre_1['table']}"
        df_extracted = pd.read_sql(query, engine)
        avg_per_country = df_extracted.groupby('country')['pm2.5_aqi_value'].mean().reset_index()
        plt.figure(figsize=(18, 12))
        plt.plot(avg_per_country['country'], avg_per_country['pm2.5_aqi_value'])
        plt.title('Average PM2.5 Value per Country')
        plt.xlabel('Country')
        plt.ylabel('Average PM2.5 Value')
        plt.xticks(rotation=90)
        plt.tight_layout()
        graph_path = "C:/Users/seman/my-dagster-project/graph/average_PM25_by_country.png"
        plt.savefig(graph_path)
        plt.close()
        context.log.info("Data visualization for Average PM2.5 Value completed successfully.")
        context.log.info(f"Visualization saved at: {graph_path}")
        graph_paths = [{'graph_name': 'Average PM2.5 by Country', 'graph_path': graph_path}]
        df_graph_paths = pd.DataFrame(graph_paths)
        try:
            df_graph_paths.to_sql('Graph', engine, if_exists='append', index=False)
            context.log.info("Graph paths successfully saved into the Graph table in PostgreSQL.")
        except:
            context.log.info(f"Graph path already exsist in PostgreSQL")
        return graph_path
    except Exception as e:
        context.log.error(f"Error during data visualization: {e}")
        raise Exception(f"Error during data visualization: {e}")


@asset
def load_data_2(context):
    try:
        df = pd.read_csv('datasets/worldcities.csv')
        context.log.info(df.head())
        context.log.info(df.info())
        df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
        return df
    except Exception as e:
        context.log.info(f"Error loading data: {e}")
        return None


@asset
def insert_data_2(context, load_data_2):
    if load_data_2 is None:
        raise ValueError("load_data_2 is None. Please check the data source or upstream process.")
    else:
        context.log.info("data is here")
    try:
        df = load_data_2
        context.log.info(f"Data to be inserted into MongoDB:\n{df}")
        records = json.loads(df.to_json(orient='records'))
        if collection2.count_documents({}) == 0:
            collection2.insert_many(records)
            context.log.info("Data successfully inserted into MongoDB.")
        else:
            context.log.info("Data is Already in collection.")

    except Exception as e:
        context.log.error(f"Error connecting to MongoDB: {e}")
        raise Exception(f"Error connecting to MongoDB: {e}")


@asset
def preparing_data_2(context, insert_data_2):
    try:
        extracted_data = list(collection2.find())
        context.log.info("Data successfully extracted from MongoDB.")
    except Exception as e:
        context.log.error(f"Error extracting data from MongoDB: {e}")
        raise Exception(f"Error extracting data from MongoDB: {e}")
    try:
        df_extracted = pd.DataFrame(extracted_data)
        df_extracted.drop(columns=['_id'], inplace=True, errors='ignore')
        df_extracted = df_extracted.apply(lambda x: x.str.lower() if x.dtype == "object" else x)
        df_extracted = df_extracted.dropna(subset=["lat", "lng", "population"])
        return df_extracted
    except Exception as e:
        context.log.error(f"Error during data transformation: {e}")
        raise Exception(f"Error during data transformation: {e}")


@asset
def saving_data_postgre_2(context, preparing_data_2):
    df_extracted = preparing_data_2
    engine = create_engine(DB_URI)
    try:
        df_extracted.to_sql('worldcities_data', engine, if_exists='replace', index=False)
        context.log.info("Transformed data successfully inserted into postgre.")
        return {'table': 'worldcities_data'}
    except Exception as e:
        context.log.error(f"Error inserting transformed data into postgre: {e}")
        raise Exception(f"Error inserting transformed data into postgre: {e}")


@asset
def top_10_populated_cities(context, saving_data_postgre_2):
    try:
        engine = create_engine(DB_URI)
        query = f"SELECT * FROM {saving_data_postgre_2['table']}"
        df_extracted = pd.read_sql(query, engine)
        cities = df_extracted.nlargest(10, 'population')
        plt.figure(figsize=(10, 6))
        plt.barh(cities['city'], cities['population'], color='skyblue')
        plt.xlabel('Population')
        plt.ylabel('City')
        plt.title('Top 10 Most Populated Cities')
        plt.gca().invert_yaxis()
        graph_path = "C:/Users/seman/my-dagster-project/graph/top_10_populated_cities.png"
        plt.savefig(graph_path)
        plt.close()
        context.log.info("Data visualization for Top 10 Most Populated Cities completed successfully.")
        context.log.info(f"Visualization saved at: {graph_path}")
        graph_paths = [{'graph_name': 'Top 10 Populated Cities', 'graph_path': graph_path}]
        df_graph_paths = pd.DataFrame(graph_paths)
        try:
            df_graph_paths.to_sql('Graph', engine, if_exists='append', index=False)
            context.log.info("Graph paths successfully saved into the Graph table in PostgreSQL.")
        except:
            context.log.info(f"Graph path already exsist in PostgreSQL")
        return graph_path
    except Exception as e:
        context.log.error(f"Error during data visualization: {e}")
        raise Exception(f"Error during data visualization: {e}")


@asset
def cities_on_map(context, saving_data_postgre_2):
    try:
        engine = create_engine(DB_URI)
        query = f"SELECT * FROM {saving_data_postgre_2['table']}"
        df_extracted = pd.read_sql(query, engine)
        plt.figure(figsize=(12, 8))
        plt.scatter(df_extracted['lng'], df_extracted['lat'], alpha=0.5, c='blue', s=10)
        plt.xlabel('Longitude')
        plt.ylabel('Latitude')
        plt.title('Geographical Distribution of Cities')
        graph_path = "C:/Users/seman/my-dagster-project/graph/cities_on_map.png"
        plt.savefig(graph_path)
        plt.close()
        context.log.info("Data visualization for Geographical Distribution of Cities completed successfully.")
        context.log.info(f"Visualization saved at: {graph_path}")
        graph_paths = [{'graph_name': 'Cities on Map', 'graph_path': graph_path}]
        df_graph_paths = pd.DataFrame(graph_paths)
        try:
            df_graph_paths.to_sql('Graph', engine, if_exists='append', index=False)
            context.log.info("Graph paths successfully saved into the Graph table in PostgreSQL.")
        except:
            context.log.info(f"Graph path already exsist in PostgreSQL")
        return graph_path
    except Exception as e:
        context.log.error(f"Error during data visualization: {e}")
        raise Exception(f"Error during data visualization: {e}")


@asset
def population_by_region_heatmap(context, saving_data_postgre_2):
    try:
        engine = create_engine(DB_URI)
        query = f"SELECT * FROM {saving_data_postgre_2['table']}"
        df_extracted = pd.read_sql(query, engine)
        region_population = df_extracted.groupby('admin_name')['population'].sum().dropna().sort_values(ascending=False)
        region_population_df = region_population.reset_index()
        heatmap_data = pd.pivot_table(region_population_df, values='population', index='admin_name')
        heatmap_data = heatmap_data.sort_values('population', ascending=False)
        plt.figure(figsize=(10, 16))
        sns.heatmap(heatmap_data, cmap='viridis', annot=False, cbar_kws={'label': 'Total Population'}, linewidths=0.5)
        plt.title('Population Heatmap by Region')
        plt.xlabel('Total Population')
        plt.ylabel('Region')
        graph_path = "C:/Users/seman/my-dagster-project/graph/population_by_region_heatmap.png"
        plt.savefig(graph_path)
        plt.close()
        context.log.info("Data visualization for Population Heatmap by Region completed successfully.")
        context.log.info(f"Visualization saved at: {graph_path}")
        graph_paths = [{'graph_name': 'Population by Region Heatmap', 'graph_path': graph_path}]
        df_graph_paths = pd.DataFrame(graph_paths)
        try:
            df_graph_paths.to_sql('Graph', engine, if_exists='append', index=False)
        except:
            context.log.info(f"Graph path already exsist in PostgreSQL")
        return graph_path
    except Exception as e:
        context.log.error(f"Error during data visualization: {e}")
        raise Exception(f"Error during data visualization: {e}")


@asset
def load_data_3(context):
    try:
        df = pd.read_json("datasets/death.json", lines=True)
        context.log.info(df.head())
        context.log.info(df.info())
        df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
        return df
    except Exception as e:
        context.log.info(f"Error loading data: {e}")
        return None


@asset
def insert_data_3(context, load_data_3):
    try:
        df = load_data_3
        context.log.info(f"Data to be inserted into MongoDB:\n{df}")
        records = json.loads(df.to_json(orient='records'))
        if collection3.count_documents({}) == 0:
            collection3.insert_many(records)
            context.log.info("Data successfully inserted into MongoDB.")
        else:
            context.log.info("Data is Already in collection.")
    except Exception as e:
        context.log.error(f"Error connecting to MongoDB: {e}")
        raise Exception(f"Error connecting to MongoDB: {e}")


@asset
def preparing_data_3(context, insert_data_3):
    try:
        extracted_data = list(collection3.find())
        context.log.info("Data successfully extracted from MongoDB.")
    except Exception as e:
        context.log.error(f"Error extracting data from MongoDB: {e}")
        raise Exception(f"Error extracting data from MongoDB: {e}")
    try:
        df_extracted = pd.DataFrame(extracted_data)
        df_extracted.drop(columns=['_id'], inplace=True, errors='ignore')
        df_extracted = df_extracted.apply(lambda x: x.str.lower() if x.dtype == "object" else x)
        df_extracted = df_extracted.dropna(subset=["outdoor_air_pollution", "smoking", "year"])
        return df_extracted
    except Exception as e:
        context.log.error(f"Error during data transformation: {e}")
        raise Exception(f"Error during data transformation: {e}")


@asset
def saving_data_postgre_3(context, preparing_data_3):
    df_extracted = preparing_data_3
    engine = create_engine(DB_URI)
    try:
        df_extracted.to_sql('death_data', engine, if_exists='replace', index=False)
        context.log.info("Transformed data successfully inserted into postgre.")
        return {'table': 'death_data'}
    except Exception as e:
        context.log.error(f"Error inserting transformed data into postgre: {e}")
        raise Exception(f"Error inserting transformed data into postgre: {e}")


@asset
def stacked_area_chart(context, saving_data_postgre_3):
    try:
        engine = create_engine(DB_URI)
        query = f"SELECT * FROM {saving_data_postgre_3['table']}"
        data = pd.read_sql(query, engine)
        required_columns = ["year", "outdoor_air_pollution", "smoking", "tuberculosis_fatalities",
                            "cardiovascular_fatalities"]
        if not all(col in data.columns for col in required_columns):
            raise ValueError(f"Data is missing required columns. Expected columns: {required_columns}")
        grouped_data = data.groupby("year")[["outdoor_air_pollution", "smoking", "tuberculosis_fatalities",
                                             "cardiovascular_fatalities"]].sum().reset_index()
        plt.figure(figsize=(12, 8))
        plt.stackplot(
            grouped_data["year"],
            grouped_data["outdoor_air_pollution"],
            grouped_data["smoking"],
            grouped_data["tuberculosis_fatalities"],
            grouped_data["cardiovascular_fatalities"],
            labels=["outdoor Air Pollution", "smoking", "tuberculosis fatalities", "cardiovascular fatalities"]
        )
        plt.title("Stacked Area Chart: Contribution of Factors to Total Fatalities Over Time", fontsize=14)
        plt.xlabel("Year", fontsize=12)
        plt.ylabel("Fatalities", fontsize=12)
        plt.legend(loc="upper left")
        plt.tight_layout()
        graph_path = "C:/Users/seman/my-dagster-project/graph/stacked_area_chart.png"
        plt.savefig(graph_path)
        plt.close()
        context.log.info("Data visualization for stacked_area_chart completed successfully.")
        context.log.info(f"Stacked area chart saved at: {graph_path}")
        graph_paths = [{'graph_name': 'Stacked Area Chart', 'graph_path': graph_path}]
        df_graph_paths = pd.DataFrame(graph_paths)
        try:
            df_graph_paths.to_sql('Graph', engine, if_exists='append', index=False)
            context.log.info("Graph paths successfully saved into the Graph table in PostgreSQL.")
        except:
            context.log.info(f"Graph path already exsist in PostgreSQL")
        return graph_path
    except Exception as e:
        context.log.error(f"Error during data visualization: {e}")
        raise Exception(f"Error during data visualization: {e}")


@asset
def multi_line_chart(context, saving_data_postgre_3):
    try:
        # Database Connection
        engine = create_engine(DB_URI)
        query = f"SELECT * FROM {saving_data_postgre_3['table']}"
        df_extracted = pd.read_sql(query, engine)

        # Step 1: Log and filter invalid 'outdoor_air_pollution' values
        invalid_rows = df_extracted[
            ~df_extracted["outdoor_air_pollution"].apply(lambda x: str(x).replace('.', '', 1).isdigit())]
        if not invalid_rows.empty:
            context.log.warning(f"Invalid rows detected in 'outdoor_air_pollution':\n{invalid_rows}")

        # Drop invalid rows
        df_extracted = df_extracted[
            df_extracted["outdoor_air_pollution"].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

        # Convert to numeric
        df_extracted["outdoor_air_pollution"] = pd.to_numeric(df_extracted["outdoor_air_pollution"], errors="coerce")

        # Drop any remaining NaN values
        df_extracted = df_extracted.dropna(subset=["year", "entity", "outdoor_air_pollution"])

        # Step 2: Group and aggregate data
        df_aggregated = df_extracted.groupby(["year", "entity"], as_index=False)["outdoor_air_pollution"].mean()

        # Step 3: Pivot the DataFrame
        pivot_data = df_aggregated.pivot(index="year", columns="entity", values="outdoor_air_pollution").reset_index()

        # Check if Pivot Data Contains Values
        if pivot_data.empty:
            raise ValueError("Pivot operation resulted in an empty DataFrame. Check the input data.")

        # Step 4: Plot Multi-Line Chart
        plt.figure(figsize=(12, 8))
        for country in pivot_data.columns[1:]:
            plt.plot(pivot_data["year"], pivot_data[country], label=country)

        # Chart Styling
        plt.title("Multi-Line Chart: Outdoor Air Pollution Trends Across Countries", fontsize=14)
        plt.xlabel("Year", fontsize=12)
        plt.ylabel("Outdoor Air Pollution", fontsize=12)
        plt.legend(title="Country", loc="upper left")
        plt.tight_layout()

        # Step 5: Save the Chart
        graph_path = "C:/Users/seman/my-dagster-project/graph/multi_line_chart.png"
        os.makedirs(os.path.dirname(graph_path), exist_ok=True)
        plt.savefig(graph_path)
        plt.close()

        # Log Success
        context.log.info("Multi-Line Chart visualization for Outdoor Air Pollution completed successfully.")
        context.log.info(f"Visualization saved at: {graph_path}")

        # Save Graph Path to Database
        graph_paths = [{'graph_name': 'Multi Line Chart', 'graph_path': graph_path}]
        df_graph_paths = pd.DataFrame(graph_paths)
        try:
            df_graph_paths.to_sql('Graph', engine, if_exists='append', index=False)
            context.log.info("Graph paths successfully saved into the Graph table in PostgreSQL.")
        except Exception as e:
            context.log.info(f"Graph path already exists in PostgreSQL: {e}")

        return graph_path

    except Exception as e:
        context.log.error(f"Error during multi-line chart visualization: {e}")
        raise Exception(f"Error during multi-line chart visualization: {e}")


@asset
def average_smoking_deaths_by_country(context, saving_data_postgre_3):
    engine = create_engine(DB_URI)
    query = f"SELECT * FROM {saving_data_postgre_3['table']}"
    df = pd.read_sql(query, engine)

    def replace_country_name(country_name, countries):
        closest_match, _ = process.extractOne(country_name, countries)
        return closest_match

    try:
        average_smoking_deaths = df.groupby('entity')['smoking'].mean().reset_index()
        average_smoking_deaths_sorted = average_smoking_deaths.sort_values(by='smoking', ascending=False)
        average_smoking_deaths['entity'] = average_smoking_deaths['entity'].apply(
            lambda x: replace_country_name(x, countries))
        average_smoking_deaths = average_smoking_deaths.dropna(subset=['entity'])
        top_20_countries = average_smoking_deaths_sorted.head(20)
        plt.figure(figsize=(14, 8))
        plt.bar(top_20_countries['entity'], top_20_countries['smoking'], color='blue', alpha=0.7, edgecolor='black')
        plt.title('Top 20 Countries by Average Smoking Deaths', fontsize=16)
        plt.xlabel('Country', fontsize=12)
        plt.ylabel('Average Smoking Deaths', fontsize=12)
        plt.xticks(rotation=45, fontsize=10)
        plt.tight_layout()
        graph_path = "C:/Users/seman/my-dagster-project/graph/average_smoking_deaths_by_country.png"
        plt.savefig(graph_path)
        plt.close()
        context.log.info("average_smoking_deaths_by_country plot completed successfully.")
        context.log.info(f"Visualization saved at: {graph_path}")
        graph_paths = [{'graph_name': 'Average Smoking Deaths by Country', 'graph_path': graph_path}]
        df_graph_paths = pd.DataFrame(graph_paths)
        try:
            df_graph_paths.to_sql('Graph', engine, if_exists='append', index=False)
            context.log.info("Graph paths successfully saved into the Graph table in PostgreSQL.")
        except:
            context.log.info(f"Graph path already exists in PostgreSQL")
        return graph_path

    except Exception as e:
        context.log.error(f"Error during bar chart visualization: {e}")
        raise Exception(f"Error during bar chart visualization: {e}")


@asset
def preparing_data_for_merging_data(context, saving_data_postgre_1, saving_data_postgre_2, saving_data_postgre_3):
    context.log.info("preparing dat for merging in proceess")

    def replace_country_name(country_name, countries):
        closest_match, _ = process.extractOne(country_name, countries)
        return closest_match

    engine = create_engine(DB_URI)
    try:
        query1 = f"SELECT * FROM {saving_data_postgre_1['table']}"
        query2 = f"SELECT * FROM {saving_data_postgre_2['table']}"
        query3 = f"SELECT * FROM {saving_data_postgre_3['table']}"
        gab_df = pd.read_sql(query1, engine)
        worldcities_df = pd.read_sql(query2, engine)
        death_df = pd.read_sql(query3, engine)
    except SQLAlchemyError as e:
        context.log.error(f"Database query error: {e}")
        raise Exception(f"Error querying data from PostgreSQL: {e}")

    try:
        death_df.rename(columns={'entity': 'country'}, inplace=True)
        numeric_columns = ['co_aqi_value', 'ozone_aqi_value', 'no2_aqi_value', 'pm2.5_aqi_value', 'aqi_value']
        gab_df = gab_df.groupby('country')[numeric_columns].mean().reset_index()
        numeric_columns2 = ['outdoor_air_pollution', 'smoking', 'tuberculosis_fatalities', 'cardiovascular_fatalities']
        death_df = death_df.groupby('country')[numeric_columns2].mean().reset_index()
        worldcities_df = worldcities_df.groupby('country')['population'].sum().reset_index()

        gab_df['country'] = gab_df['country'].apply(lambda x: replace_country_name(x, countries))
        worldcities_df['country'] = worldcities_df['country'].apply(lambda x: replace_country_name(x, countries))
        death_df['country'] = death_df['country'].apply(lambda x: replace_country_name(x, countries))
        gab_df['country'] = gab_df['country'].apply(lambda x: replace_country_name(x, countries))

        gab_df = gab_df.drop_duplicates(subset='country', keep='first')

        worldcities_df['country'] = worldcities_df['country'].apply(lambda x: replace_country_name(x, countries))
        worldcities_df = worldcities_df.drop_duplicates(subset='country', keep='first')

        death_df['country'] = death_df['country'].apply(lambda x: replace_country_name(x, countries))
        death_df = death_df.drop_duplicates(subset='country', keep='first')
    except Exception as e:
        context.log.error(f"Error during country preparing data: {e}")
        raise Exception(f"Error during country preparing data: {e}")

    return {'data1': gab_df, 'data2': worldcities_df, 'data3': death_df}


@asset
def merging_data(context, preparing_data_for_merging_data):
    try:
        gab_df = preparing_data_for_merging_data['data1']
        worldcities_df = preparing_data_for_merging_data['data2']
        death_df = preparing_data_for_merging_data['data3']

        merged_df1 = pd.merge(gab_df, worldcities_df, on='country', how='inner')
        final_merged_df = pd.merge(merged_df1, death_df, on='country', how='inner')
        final_merged_df = final_merged_df.dropna(subset=['population'])
        final_merged_df['percentage_tuberculosis'] = (final_merged_df['tuberculosis_fatalities'] / final_merged_df[
            'population']) * 100
        final_merged_df['percentage_smoking'] = (final_merged_df['smoking'] / final_merged_df['population']) * 100
        final_merged_df['percentage_cardiovascular'] = (final_merged_df['cardiovascular_fatalities'] / final_merged_df[
            'population']) * 100
    except Exception as e:
        context.log.error(f"Error in merging data: {e}")
        raise Exception(f"Error in merging data: {e}")
    return {'merge': final_merged_df}


@asset
def save_merged_data(context, merging_data):
    final_merged_df = merging_data['merge']
    engine = create_engine(DB_URI)
    try:
        final_merged_df.to_sql('merged_data', engine, if_exists='replace', index=False)
        context.log.info("Merged data successfully inserted into postgre.")
        return {'table': 'merged_data'}
    except Exception as e:
        context.log.error(f"Error inserting transformed data into postgre: {e}")
        raise Exception(f"Error inserting transformed data into postgre: {e}")


@asset
def Population_vs_AQI(context, save_merged_data):
    engine = create_engine(DB_URI)
    query = f"SELECT * FROM {save_merged_data['table']}"
    merge = pd.read_sql(query, engine)
    merge = merge.sort_values(by='aqi_value', ascending=False).head(10)
    try:
        plt.figure(figsize=(14, 8))
        plt.scatter(merge['population'], merge['aqi_value'], alpha=0.7, edgecolors='black')
        plt.title('Top 10 Most polluted country and their Population', fontsize=15)
        plt.xlabel('Population', fontsize=12)
        plt.ylabel('AQI (Air Quality Index)', fontsize=12)
        plt.grid(alpha=0.3)
        for i, country in enumerate(merge['country']):
            plt.annotate(country, (merge['population'].iloc[i], merge['aqi_value'].iloc[i]), fontsize=8, alpha=0.7)
        plt.tight_layout()
        graph_path = "C:/Users/seman/my-dagster-project/graph/Top_10_Polluted_countries_vs_AQI.png"
        plt.savefig(graph_path)
        plt.close()
        context.log.info("Top 10 Most polluted country and their Population completed successfully.")
        context.log.info(f"Visualization saved at: {graph_path}")
        graph_paths = [{'graph_name': 'Top 10 Most polluted country and their Population', 'graph_path': graph_path}]
        df_graph_paths = pd.DataFrame(graph_paths)
        try:
            df_graph_paths.to_sql('Graph', engine, if_exists='append', index=False)
            context.log.info("Graph paths successfully saved into the Graph table in PostgreSQL.")
        except:
            context.log.info(f"Graph path already exsist in PostgreSQL")
        return graph_path
    except Exception as e:
        context.log.error(f"Error during violin plot visualization: {e}")
        raise Exception(f"Error during violin plot visualization: {e}")


@asset
def Smoking_fatalities_vs_Population(context, save_merged_data):
    engine = create_engine(DB_URI)
    query = f"SELECT * FROM {save_merged_data['table']}"
    merge = pd.read_sql(query, engine)
    try:
        merge['percentage_smoking'] = merge['percentage_smoking'].clip(lower=0.1, upper=1)
        norm = plt.Normalize(0.1, 1)
        colors = plt.cm.viridis(norm(merge['percentage_smoking']))
        fig, ax = plt.subplots(figsize=(20, 8))
        bars = ax.bar(merge['country'], merge['population'], color=colors, edgecolor='black')
        sm = plt.cm.ScalarMappable(cmap="viridis", norm=norm)
        cbar = fig.colorbar(sm, ax=ax)
        cbar.set_label('Percentage of Population Affected by Smoking (%)', rotation=270, labelpad=20)
        ax.set_xticks(range(len(merge['country'])))
        ax.set_xticklabels(merge['country'], rotation=90, fontsize=9)
        ax.set_title('Total Population and Smoking-Related Impact', fontsize=15)
        ax.set_xlabel('Country', fontsize=12)
        ax.set_ylabel('Total Population', fontsize=12)
        plt.tight_layout()
        graph_path = "C:/Users/seman/my-dagster-project/graph/Smoking_fatalities_vs_Population.png"
        plt.savefig(graph_path)
        plt.close()
        context.log.info("Smoking_fatalities_vs_Population plot completed successfully.")
        context.log.info(f"Visualization saved at: {graph_path}")
        graph_paths = [{'graph_name': 'Smoking fatalities vs Population', 'graph_path': graph_path}]
        df_graph_paths = pd.DataFrame(graph_paths)
        try:
            df_graph_paths.to_sql('Graph', engine, if_exists='append', index=False)
            context.log.info("Graph paths successfully saved into the Graph table in PostgreSQL.")
        except:
            context.log.info(f"Graph path already exsist in PostgreSQL")
        return graph_path
    except Exception as e:
        context.log.error(f"Error during violin plot visualization: {e}")
        raise Exception(f"Error during violin plot visualization: {e}")


@asset
def Tuberculosis_fatalities_vs_Population(context, save_merged_data):
    engine = create_engine(DB_URI)
    query = f"SELECT * FROM {save_merged_data['table']}"
    merge = pd.read_sql(query, engine)
    try:
        merge['percentage_tuberculosis'] = (merge['tuberculosis_fatalities'] / merge['population']) * 100
        merge['percentage_tuberculosis'] = merge['percentage_tuberculosis'].clip(lower=0.1, upper=1)
        norm = plt.Normalize(0.1, 1)
        colors = plt.cm.viridis(norm(merge['percentage_tuberculosis']))
        fig, ax = plt.subplots(figsize=(20, 8))
        bars = ax.bar(merge['country'], merge['population'], color=colors, edgecolor='black')
        sm = plt.cm.ScalarMappable(cmap="viridis", norm=norm)
        cbar = fig.colorbar(sm, ax=ax)
        cbar.set_label('Percentage of Population Affected by Tuberculosis (%)', rotation=270, labelpad=20)
        ax.set_xticks(range(len(merge['country'])))
        ax.set_xticklabels(merge['country'], rotation=90, fontsize=9)
        ax.set_title('Total Population and Tuberculosis-Related Impact', fontsize=15)
        ax.set_xlabel('Country', fontsize=12)
        ax.set_ylabel('Total Population', fontsize=12)
        plt.tight_layout()
        graph_path = "C:/Users/seman/my-dagster-project/graph/Tuberculosis_fatalities_vs_Population.png"
        plt.savefig(graph_path)
        plt.close()
        context.log.info("Tuberculosis_fatalities_vs_Population plot completed successfully.")
        context.log.info(f"Visualization saved at: {graph_path}")
        graph_paths = [{'graph_name': 'Tuberculosis fatalities vs Population', 'graph_path': graph_path}]
        df_graph_paths = pd.DataFrame(graph_paths)
        try:
            df_graph_paths.to_sql('Graph', engine, if_exists='append', index=False)
            context.log.info("Graph paths successfully saved into the Graph table in PostgreSQL.")
        except:
            context.log.info(f"Graph path already exsist in PostgreSQL")
        return graph_path
    except Exception as e:
        context.log.error(f"Error during violin plot visualization: {e}")
        raise Exception(f"Error during violin plot visualization: {e}")


@asset
def Cardiovascular_Fatalities_vs_Population(context, save_merged_data):
    engine = create_engine(DB_URI)
    query = f"SELECT * FROM {save_merged_data['table']}"
    merge = pd.read_sql(query, engine)
    try:
        merge['percentage_cardiovascular'] = merge['percentage_cardiovascular'].clip(lower=0.1, upper=2)
        norm = plt.Normalize(0.1, 2)
        colors = plt.cm.viridis(norm(merge['percentage_cardiovascular']))
        fig, ax = plt.subplots(figsize=(20, 8))
        bars = ax.bar(merge['country'], merge['population'], color=colors, edgecolor='black')
        sm = plt.cm.ScalarMappable(cmap="viridis", norm=norm)
        cbar = fig.colorbar(sm, ax=ax)
        cbar.set_label('Percentage of Population Affected by Cardiovascular Fatalities (%)', rotation=270, labelpad=20)
        ax.set_xticks(range(len(merge['country'])))
        ax.set_xticklabels(merge['country'], rotation=90, fontsize=9)
        ax.set_title('Total Population and Cardiovascular Fatalities Impact', fontsize=15)
        ax.set_xlabel('Country', fontsize=12)
        ax.set_ylabel('Total Population', fontsize=12)
        plt.tight_layout()
        graph_path = "C:/Users/seman/my-dagster-project/graph/Cardiovascular_Fatalities_vs_Population.png"
        plt.savefig(graph_path)
        plt.close()
        context.log.info("Cardiovascular_Fatalities_vs_Population plot completed successfully.")
        context.log.info(f"Visualization saved at: {graph_path}")
        graph_paths = [{'graph_name': 'Cardiovascular Fatalities vs Population', 'graph_path': graph_path}]
        df_graph_paths = pd.DataFrame(graph_paths)
        try:
            df_graph_paths.to_sql('Graph', engine, if_exists='append', index=False)
            context.log.info("Graph paths successfully saved into the Graph table in PostgreSQL.")
        except:
            context.log.info(f"Graph path already exsist in PostgreSQL")
        return graph_path
    except Exception as e:
        context.log.error(f"Error during violin plot visualization: {e}")
        raise Exception(f"Error during violin plot visualization: {e}")


@asset
def top_50_polluted_countries_vs_outdoor_deaths(context, save_merged_data):
    engine = create_engine(DB_URI)
    query = f"SELECT * FROM {save_merged_data['table']}"
    merge = pd.read_sql(query, engine)
    try:
        top_50_polluted = merge[['country', 'aqi_value', 'outdoor_air_pollution']].sort_values(by='aqi_value',
                                                                                               ascending=False).head(50)
        top_countries = top_50_polluted['country']
        top_aqi = top_50_polluted['aqi_value']
        outdoor_deaths = top_50_polluted['outdoor_air_pollution']
        plt.figure(figsize=(14, 8))
        plt.bar(top_countries, top_aqi, color='skyblue', label='AQI', alpha=0.7, edgecolor='black')
        plt.plot(top_countries, outdoor_deaths, color='red', marker='o', label='Outdoor Air Pollution Deaths')
        plt.title('Top 50 Most Polluted Countries and Their Outdoor Air Pollution Deaths', fontsize=16)
        plt.xlabel('Country', fontsize=12)
        plt.ylabel('AQI and Outdoor Deaths', fontsize=12)
        plt.xticks(rotation=90, fontsize=10)
        plt.legend()
        plt.tight_layout()
        graph_path = "C:/Users/seman/my-dagster-project/graph/top_50_polluted_countries_vs_outdoor_deaths.png"
        plt.savefig(graph_path)
        plt.close()
        context.log.info("top_50_polluted_countries_vs_outdoor_deaths plot completed successfully.")
        context.log.info(f"Visualization saved at: {graph_path}")
        graph_paths = [{'graph_name': 'Top 50 Polluted Countries vs Outdoor Deaths', 'graph_path': graph_path}]
        df_graph_paths = pd.DataFrame(graph_paths)
        try:
            df_graph_paths.to_sql('Graph', engine, if_exists='append', index=False)
            context.log.info("Graph paths successfully saved into the Graph table in PostgreSQL.")
        except:
            context.log.info(f"Graph path already exsist in PostgreSQL")
        return graph_path
    except Exception as e:
        context.log.error(f"Error during violin plot visualization: {e}")
        raise Exception(f"Error during violin plot visualization: {e}")


@asset
def top_10_populated_countries_pollutants(context, save_merged_data):
    engine = create_engine(DB_URI)
    query = f"SELECT * FROM {save_merged_data['table']}"
    merge = pd.read_sql(query, engine)
    try:
        top_10_populated = merge.nlargest(10, 'population')[
            ['country', 'co_aqi_value', 'ozone_aqi_value', 'no2_aqi_value', 'pm2.5_aqi_value']]
        countries = top_10_populated['country']
        pollutants = ['co_aqi_value', 'ozone_aqi_value', 'no2_aqi_value', 'pm2.5_aqi_value']
        colors = ['black', 'blue', 'green', 'red']
        plt.figure(figsize=(14, 8))
        for pollutant, color in zip(pollutants, colors):
            plt.plot(countries, top_10_populated[pollutant], marker='o', label=pollutant, color=color)
        plt.title('Top 10 Most Populated Countries and Their Pollutant Values', fontsize=16)
        plt.xlabel('Country', fontsize=12)
        plt.ylabel('Pollutant Values (AQI)', fontsize=12)
        plt.xticks(rotation=45, fontsize=10)
        plt.legend(title="Pollutants")
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.tight_layout()
        graph_path = "C:/Users/seman/my-dagster-project/graph/top_10_populated_countries_pollutants.png"
        plt.savefig(graph_path)
        plt.close()
        context.log.info("top_10_populated_countries_pollutants plot completed successfully.")
        context.log.info(f"Visualization saved at: {graph_path}")
        graph_paths = [{'graph_name': 'Top 10 Populated Countries Pollutants', 'graph_path': graph_path}]
        df_graph_paths = pd.DataFrame(graph_paths)
        try:
            df_graph_paths.to_sql('Graph', engine, if_exists='append', index=False)
            context.log.info("Graph paths successfully saved into the Graph table in PostgreSQL.")
        except:
            context.log.info(f"Graph path already exists in PostgreSQL")
        return graph_path

    except Exception as e:
        context.log.error(f"Error during multi-line graph visualization: {e}")
        raise Exception(f"Error during multi-line graph visualization: {e}")


@asset
def run_flask_app_asset(context, average_aqi_by_country,
                        average_ozone_by_country,
                        average_CO_by_country,
                        average_NO2_by_country,
                        average_PM25_by_country,
                        top_10_populated_cities,
                        cities_on_map,
                        population_by_region_heatmap,
                        stacked_area_chart,
                        multi_line_chart,
                        average_smoking_deaths_by_country,
                        Population_vs_AQI,
                        Smoking_fatalities_vs_Population,
                        Tuberculosis_fatalities_vs_Population,
                        Cardiovascular_Fatalities_vs_Population,
                        top_50_polluted_countries_vs_outdoor_deaths,
                        top_10_populated_countries_pollutants
                        ):
    flask_app_path = "C:/Users/seman/my-dagster-project/my_dagster_project/dash_app.py"

    try:
        context.log.info("Starting the Flask application...")
        process = subprocess.Popen(
            ["python", flask_app_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(5)
        flask_url = "http://127.0.0.1:5000"
        context.log.info(f"Flask application is running. Access it at: {flask_url}")
        webbrowser.open(flask_url)
        return {"message": "Flask app is running", "url": flask_url, "pid": process.pid}

    except Exception as e:
        context.log.error(f"Error running Flask app: {e}")
        raise Exception(f"Error running Flask app: {e}")
