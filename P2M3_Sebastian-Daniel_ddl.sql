-- Create a new table with all columns from the CSV file
CREATE TABLE table_m3 (
    Title TEXT,
    "Location" TEXT,
    "Date" DATE,
    Incident_area TEXT,
    Open_Close_Location TEXT,
    Target TEXT,
    Cause TEXT,
    Summary TEXT,
    Fatalities INTEGER,
    Injured INTEGER,
    Total_victims INTEGER,
    Policeman_killed INTEGER,
    Age TEXT,
	Employeed BOOLEAN,
    Employed_at TEXT,
    Mental_Health_Issues TEXT,
    Race TEXT,
    Gender TEXT,
    Latitude DOUBLE PRECISION,
    Longitude DOUBLE PRECISION
);

-- Copy data from CSV into the new table
COPY table_m3(Title, "Location", "Date", Incident_area, Open_Close_Location, Target, Cause, 
                    Summary, Fatalities, Injured, Total_victims, Policeman_killed, Age, Employeed, Employed_at,
                     Mental_Health_Issues, Race, Gender, Latitude, 
                    Longitude)
FROM 'C:\Users\USER\anaconda3\envs\rmt-23\Milestone3\p2-ftds023-rmt-m3-Legacy453\P2M3_Sebastian_Daniel_data_raw.csv' 
DELIMITER ',' CSV HEADER;