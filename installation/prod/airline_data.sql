CREATE OR REPLACE DATABASE &{ database };
DROP SCHEMA PUBLIC;
CREATE SCHEMA &{ schema };

CREATE OR REPLACE PROCEDURE generate_fs_data()
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python','pandas')
HANDLER = 'run'
EXECUTE AS CALLER
AS $$
import pandas as pd
import random
import string
import numpy as np
from datetime import datetime, timedelta
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType

# Function to generate a random tail number
def generate_tail_number():
    return '#' + random.choice(string.ascii_uppercase) + \
           ''.join(random.choices(string.digits, k=3)) + \
           ''.join(random.choices(string.ascii_uppercase, k=3))

# Function to add a random time fluctuation
def add_time_fluctuation(time):
    fluctuation = random.randint(-10, 15)  # Random fluctuation between -10 and +15 minutes
    return time + pd.Timedelta(minutes=fluctuation)

# Function to generate a destination that is not the same as the origin
def generate_destination(exclude_code, used_destinations, all_codes):
    possible_destinations = [code for code in all_codes if code != exclude_code and code not in used_destinations]
    return random.choice(possible_destinations) if possible_destinations else None

# Function to get a shuffled list of destination codes excluding the departure code
def get_shuffled_destinations(exclude_code, all_codes):
    destinations = [code for code in all_codes if code != exclude_code]
    random.shuffle(destinations)
    return destinations

def assign_rain_intensity():
    rand = np.random.random()
    if rand < 0.70:  # Moderate rain 70% of the time
        return np.random.uniform(3.0, 7.5)
    elif rand < 0.95:  # Heavy rain 25% of the time
        return np.random.uniform(7.6, 10.0)
    else:  # Violent rain 5% of the time
        return np.random.uniform(10.1, 15.0)

def generate_tickets_sold(row):

    plane_models_info = [
        {"PLANE_MODEL": "Boeing 737", "SEATING_CAPACITY": 150, "MAX_RANGE_KM": 5700, "FUEL_EFFICIENCY_KM_PER_L": 3.5},
        {"PLANE_MODEL": "Boeing 737 MAX", "SEATING_CAPACITY": 180, "MAX_RANGE_KM": 7050, "FUEL_EFFICIENCY_KM_PER_L": 4.0},
        {"PLANE_MODEL": "Boeing 747", "SEATING_CAPACITY": 400, "MAX_RANGE_KM": 14815, "FUEL_EFFICIENCY_KM_PER_L": 3.0},
        {"PLANE_MODEL": "Boeing 767", "SEATING_CAPACITY": 290, "MAX_RANGE_KM": 9700, "FUEL_EFFICIENCY_KM_PER_L": 3.8},
        {"PLANE_MODEL": "Boeing 777", "SEATING_CAPACITY": 368, "MAX_RANGE_KM": 11095, "FUEL_EFFICIENCY_KM_PER_L": 4.2},
        {"PLANE_MODEL": "Boeing 787 Dreamliner", "SEATING_CAPACITY": 335, "MAX_RANGE_KM": 15700, "FUEL_EFFICIENCY_KM_PER_L": 4.5},
        {"PLANE_MODEL": "Airbus A320", "SEATING_CAPACITY": 150, "MAX_RANGE_KM": 6100, "FUEL_EFFICIENCY_KM_PER_L": 3.6},
        {"PLANE_MODEL": "Airbus A320neo", "SEATING_CAPACITY": 165, "MAX_RANGE_KM": 6850, "FUEL_EFFICIENCY_KM_PER_L": 4.1},
        {"PLANE_MODEL": "Airbus A330", "SEATING_CAPACITY": 277, "MAX_RANGE_KM": 13430, "FUEL_EFFICIENCY_KM_PER_L": 3.7},
        {"PLANE_MODEL": "Airbus A340", "SEATING_CAPACITY": 375, "MAX_RANGE_KM": 13000, "FUEL_EFFICIENCY_KM_PER_L": 3.3},
        {"PLANE_MODEL": "Airbus A350", "SEATING_CAPACITY": 325, "MAX_RANGE_KM": 16700, "FUEL_EFFICIENCY_KM_PER_L": 4.4},
        {"PLANE_MODEL": "Airbus A380", "SEATING_CAPACITY": 555, "MAX_RANGE_KM": 15200, "FUEL_EFFICIENCY_KM_PER_L": 3.6},
    ]

    # Create a DataFrame from the list of plane models
    plane_models_df = pd.DataFrame(plane_models_info)

    if row['DEPARTING_DELAY'] == 0:
        random_number = np.random.uniform(0.6, 0.9)
    else:
        random_number = np.random.uniform(0.8, 1)

    seats_capacity = plane_models_df.loc[plane_models_df['PLANE_MODEL'] == row['PLANE_MODEL'], 'SEATING_CAPACITY'].values[0]

    tickets_sold = int(seats_capacity * random_number + np.random.uniform(0, 3))
    return tickets_sold

def run(session):

    data = [
        ("Hartsfield–Jackson Atlanta International Airport", "ATL"),
        ("Dallas/Fort Worth International Airport", "DFW"),
        ("Denver International Airport", "DEN"),
        ("O'Hare International Airport", "ORD"),
        ("Los Angeles International Airport", "LAX"),
        ("John F. Kennedy International Airport", "JFK"),
        ("Harry Reid International Airport", "LAS"),
        ("Orlando International Airport", "MCO"),
        ("Miami International Airport", "MIA"),
        ("Charlotte Douglas International Airport", "CLT"),
        ("Seattle–Tacoma International Airport", "SEA"),
        ("Phoenix Sky Harbor International Airport", "PHX"),
        ("Newark Liberty International Airport", "EWR"),
        ("San Francisco International Airport", "SFO"),
        ("George Bush Intercontinental Airport", "IAH"),
        ("Logan International Airport", "BOS"),
        ("Fort Lauderdale–Hollywood International Airport", "FLL"),
        ("Minneapolis–Saint Paul International Airport", "MSP"),
    ]

    # Step 2: Create a DataFrame
    airport_df = pd.DataFrame(data, columns=["AIRPORT_NAME", "AIRPORT_CODE"])

    start_date = (pd.Timestamp.today() - pd.Timedelta(days=30))

    # Generate dates and times for flights
    date_range = pd.date_range(start=(pd.Timestamp.today() - pd.Timedelta(days=30)), periods=timedelta(days=30).days)
    times = pd.date_range("06:00", "22:00", periods=16).time

    # Generate airport, dates, and times combinations
    name_to_code = dict(zip(airport_df['AIRPORT_NAME'], airport_df['AIRPORT_CODE']))
    new_df_data = [(airport, name_to_code[airport], pd.Timestamp.combine(date, time))
                   for airport in airport_df['AIRPORT_NAME']
                   for date in date_range
                   for time in times]
    flights_df = pd.DataFrame(new_df_data, columns=["AIRPORT_NAME", "AIRPORT_CODE", "DATETIME"])

    # Additional columns
    flights_df['TAIL_NUMBER'] = [generate_tail_number() for _ in range(len(flights_df))]
    flights_df['SCHEDULED_DEPARTURE_UTC'] = flights_df['DATETIME'].apply(add_time_fluctuation)#.dt.tz_localize('UTC')
    flights_df.drop('DATETIME', axis=1, inplace=True)

    # Mapping airport names to area codes
    airport_area_codes = {
        "Hartsfield–Jackson Atlanta International Airport": "30320",
        "Dallas/Fort Worth International Airport": "75261",
        "Denver International Airport": "80249",
        "O'Hare International Airport": "60666",
        "Los Angeles International Airport": "90045",
        "John F. Kennedy International Airport": "11430",
        "Harry Reid International Airport": "89119",
        "Orlando International Airport": "32827",
        "Miami International Airport": "33126",
        "Charlotte Douglas International Airport": "28208",
        "Seattle–Tacoma International Airport": "98158",
        "Phoenix Sky Harbor International Airport": "85034",
        "Newark Liberty International Airport": "07114",
        "San Francisco International Airport": "94128",
        "George Bush Intercontinental Airport": "77032",
        "Logan International Airport": "02128",
        "Fort Lauderdale–Hollywood International Airport": "33315",
        "Minneapolis–Saint Paul International Airport": "55450"
    }
    flights_df['AIRPORT_ZIP_CODE'] = flights_df['AIRPORT_NAME'].map(airport_area_codes)

    # Destination code generation
    all_codes = list(name_to_code.values())
    flights_df['DESTINATION_CODE'] = None
    for date in flights_df['SCHEDULED_DEPARTURE_UTC'].dt.date.unique():
        for airport_code in flights_df['AIRPORT_CODE'].unique():
            used_destinations = set()
            day_airport_flights = flights_df[(flights_df['SCHEDULED_DEPARTURE_UTC'].dt.date == date) &
                                             (flights_df['AIRPORT_CODE'] == airport_code)]
            for index, row in day_airport_flights.iterrows():
                dest = generate_destination(airport_code, used_destinations, all_codes)
                flights_df.at[index, 'DESTINATION_CODE'] = dest
                if dest:
                    used_destinations.add(dest)

    # Random plane models and carrier codes
    plane_models = ['Boeing 737', 'Boeing 737 MAX', 'Boeing 747', 'Boeing 767', 'Boeing 777', 'Boeing 787 Dreamliner',
                    'Airbus A320', 'Airbus A320neo', 'Airbus A330', 'Airbus A340', 'Airbus A350', 'Airbus A380']
    carrier_codes = ['UA', 'AA', 'DLTA']  # List of carrier codes

    flights_df['PLANE_MODEL'] = [random.choice(plane_models) for _ in range(len(flights_df))]
    flights_df['CARRIER_CODE'] = [random.choice(carrier_codes) for _ in range(len(flights_df))]
    flights_df.rename(columns={'AIRPORT_CODE': 'DEPARTURE_CODE', 'CARRIER_CODE': 'DOMESTIC_CODE'}, inplace=True)

    # Sort and reset index
    flights_df = flights_df.sort_values(by='SCHEDULED_DEPARTURE_UTC').reset_index(drop=True)

    flights_df['DEPARTING_DELAY'] = 0

    # Calculating the number of rows to be marked as delayed (5% of the total rows)
    num_rows = len(flights_df)
    num_delayed = int(num_rows * 0.05)

    # Initialize variables
    delayed_count = 0
    gap = 50
    last_index = -gap - 1  # Start at an index that allows the first selection

    for i in range(num_rows):
        # Check if we can mark this row as delayed
        if delayed_count < num_delayed and i - last_index > gap:
            flights_df.at[i, 'DEPARTING_DELAY'] = 1
            last_index = i
            delayed_count += 1


    flights_df['TICKETS_SOLD'] = flights_df.apply(generate_tickets_sold, axis=1)

    # Reorder columns
    flights_df = flights_df[['AIRPORT_NAME', 'AIRPORT_ZIP_CODE', 'SCHEDULED_DEPARTURE_UTC', 'DEPARTURE_CODE', 'DOMESTIC_CODE', 'TICKETS_SOLD', 'TAIL_NUMBER', 'PLANE_MODEL', 'DESTINATION_CODE', 'DEPARTING_DELAY']]


    session.write_pandas(flights_df, 'US_FLIGHT_SCHEDULES', auto_create_table=True, overwrite=True)

    query = '''
    CREATE OR REPLACE TABLE us_flight_schedules AS
    SELECT
        TO_TIMESTAMP(scheduled_departure_utc,9)::TIMESTAMP_NTZ AS scheduled_departure_utc,
        * EXCLUDE scheduled_departure_utc
    FROM
        us_flight_schedules;
    '''
    session.sql(query).collect()

    # List of zip codes
    zip_codes = ['33315', '85034', '32827', '11430', '02128', '60666', '80249',
                 '90045', '07114', '94128', '30320', '75261', '98158', '77032',
                 '28208', '55450', '33126', '89119']

    # Generating a range of times at minute-level intervals over a 24-hour period
    time_range = pd.date_range("00:00", "23:59", freq='T').time  # 'T' for minute frequency

    # Building the expanded dataset to include minute-level intervals for each date
    weather_dataset_expanded = pd.DataFrame([(zip_code, pd.Timestamp(date.date()).replace(hour=time.hour, minute=time.minute))
                                             for zip_code in zip_codes
                                             for date in date_range
                                             for time in time_range],
                                            columns=['AIRPORT_ZIP_CODE', 'DATETIME'])
    #weather_dataset_expanded['DATETIME'] = weather_dataset_expanded['DATETIME'].dt.tz_localize('UTC')
    # Adding a RAIN_MM column to the weather dataset with default value 0 (no rain)
    weather_dataset_expanded['RAIN_MM'] = 0.0

    num_non_delayed = len(flights_df[flights_df['DEPARTING_DELAY'] == 0])
    num_non_delayed_to_update = int(num_non_delayed * 0.05)
    non_delayed_indices = np.random.choice(flights_df[flights_df['DEPARTING_DELAY'] == 0].index, size=num_non_delayed_to_update, replace=False)

    for idx in non_delayed_indices:
        row = flights_df.loc[idx]
        departure_time = row['SCHEDULED_DEPARTURE_UTC']
        weather_time = departure_time - pd.Timedelta(minutes=35)
        matching_rows = weather_dataset_expanded['DATETIME'] == weather_time
        weather_dataset_expanded.loc[matching_rows, 'RAIN_MM'] = np.random.uniform(0.05, 2.5)

    # Adjusting the code to find matching timestamps in the weather dataset 35 minutes before the departure time
    for i, row in flights_df[flights_df['DEPARTING_DELAY'] == 1].iterrows():
        departure_time = row['SCHEDULED_DEPARTURE_UTC']
        zipcode = row['AIRPORT_ZIP_CODE']
        weather_time = departure_time - pd.Timedelta(minutes=35)
        matching_rows = (weather_dataset_expanded['DATETIME'] == weather_time) & (weather_dataset_expanded['AIRPORT_ZIP_CODE'] == zipcode)
        weather_dataset_expanded.loc[matching_rows, 'RAIN_MM'] = weather_dataset_expanded[matching_rows].apply(lambda _: assign_rain_intensity(), axis=1)

    weather_dataset_expanded = weather_dataset_expanded.rename(columns={'RAIN_MM': 'RAIN_MM_H'})
    weather_dataset_expanded = weather_dataset_expanded[['DATETIME', 'AIRPORT_ZIP_CODE', 'RAIN_MM_H']]

    # Iterating through the dataframe to update the previous 45 rows for each row where RAIN_MM_H > 0
    for index, row in weather_dataset_expanded.iterrows():
        if row['RAIN_MM_H'] > 0:
            # Calculate the range of indices to update (previous 45 rows)
            start_index = 0 if index - 45 < 0 else index - 45
            end_index = index

            # Update the RAIN_MM_H values for the previous 45 rows
            for i in range(start_index, end_index):
                # Randomly choose between +15% or -15%
                change = 1 + np.random.choice([-0.15, 0])
                # Apply the change, ensuring the value does not go below 0
                weather_dataset_expanded.at[i, 'RAIN_MM_H'] =  row['RAIN_MM_H'] * change if row['RAIN_MM_H'] * change > 0 else 0

    session.write_pandas(weather_dataset_expanded, 'AIRPORT_WEATHER_STATION', auto_create_table=True, overwrite=True)

    query = '''
    CREATE OR REPLACE TABLE airport_weather_station AS
    SELECT
        TO_TIMESTAMP(datetime,9) AS datetime_utc,
        * EXCLUDE datetime
    FROM
        airport_weather_station;
    '''
    session.sql(query).collect()

    plane_models_info = [
        {"PLANE_MODEL": "Boeing 737", "SEATING_CAPACITY": 150, "MAX_RANGE_KM": 5700, "FUEL_EFFICIENCY_KM_PER_L": 3.5},
        {"PLANE_MODEL": "Boeing 737 MAX", "SEATING_CAPACITY": 180, "MAX_RANGE_KM": 7050, "FUEL_EFFICIENCY_KM_PER_L": 4.0},
        {"PLANE_MODEL": "Boeing 747", "SEATING_CAPACITY": 400, "MAX_RANGE_KM": 14815, "FUEL_EFFICIENCY_KM_PER_L": 3.0},
        {"PLANE_MODEL": "Boeing 767", "SEATING_CAPACITY": 290, "MAX_RANGE_KM": 9700, "FUEL_EFFICIENCY_KM_PER_L": 3.8},
        {"PLANE_MODEL": "Boeing 777", "SEATING_CAPACITY": 368, "MAX_RANGE_KM": 11095, "FUEL_EFFICIENCY_KM_PER_L": 4.2},
        {"PLANE_MODEL": "Boeing 787 Dreamliner", "SEATING_CAPACITY": 335, "MAX_RANGE_KM": 15700, "FUEL_EFFICIENCY_KM_PER_L": 4.5},
        {"PLANE_MODEL": "Airbus A320", "SEATING_CAPACITY": 150, "MAX_RANGE_KM": 6100, "FUEL_EFFICIENCY_KM_PER_L": 3.6},
        {"PLANE_MODEL": "Airbus A320neo", "SEATING_CAPACITY": 165, "MAX_RANGE_KM": 6850, "FUEL_EFFICIENCY_KM_PER_L": 4.1},
        {"PLANE_MODEL": "Airbus A330", "SEATING_CAPACITY": 277, "MAX_RANGE_KM": 13430, "FUEL_EFFICIENCY_KM_PER_L": 3.7},
        {"PLANE_MODEL": "Airbus A340", "SEATING_CAPACITY": 375, "MAX_RANGE_KM": 13000, "FUEL_EFFICIENCY_KM_PER_L": 3.3},
        {"PLANE_MODEL": "Airbus A350", "SEATING_CAPACITY": 325, "MAX_RANGE_KM": 16700, "FUEL_EFFICIENCY_KM_PER_L": 4.4},
        {"PLANE_MODEL": "Airbus A380", "SEATING_CAPACITY": 555, "MAX_RANGE_KM": 15200, "FUEL_EFFICIENCY_KM_PER_L": 3.6},
    ]

    # Create a DataFrame from the list of plane models
    plane_models_df = pd.DataFrame(plane_models_info)

    session.write_pandas(plane_models_df, 'PLANE_MODEL_ATTRIBUTES', auto_create_table=True, overwrite=True)
    return 'SUCCESS'
$$;

CALL generate_fs_data();

alter table us_flight_schedules set change_tracking=true;
alter table airport_weather_station set change_tracking=true;
alter table plane_model_attributes set change_tracking=true;
