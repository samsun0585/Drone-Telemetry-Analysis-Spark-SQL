This project is about working with drone data using Spark SQL in a simple way.

First, we load two CSV files that contain information about drone missions and drone details. We look at the data to understand its structure and then save it as tables so we can easily query it.

Next, we create another table from scratch to store maintenance records like repairs and costs. After that, we combine all the tables using DroneID so we can see everything in one place.

We then calculate some useful values. For example, we find energy usage based on battery and duration, and we classify missions as high, medium, or low based on how long they run.

We also create a custom function to calculate a stress score for each mission, which tells us how much load the drone experienced.

After that, we summarize the data by grouping it by drone model and mission type. This helps us calculate things like total missions, average duration, average stress score, and total maintenance cost.

Finally, we analyze the results to find which drone model and mission type combination has the highest stress. This helps understand which drones are under more pressure and may need more maintenance.


Spark SQL API Implementation
In this assignment you will analyze telemetry from an autonomous drone fleet. Drones perform missions such as surveys, deliveries, and infrastructure inspections.
You are provided two CSV files:
• drone_missions.csv
• drone_info.csv
Part 1 – Load Data
Using the Spark DataFrame reader, load both CSV files and display the schema and first 10 rows of each dataset.
Part 2 – Create Managed Tables from CSV
Create two managed tables named:
• drone_missions
• drone_info

These tables should be created from the DataFrames generated from the CSV files.
Part 3 – Create a Managed Table from Scratch
Create a new managed table named drone_maintenance using CREATE TABLE syntax.
The schema must include the following columns and data types:
•	MaintenanceID INT
•	DroneID STRING
•	MaintenanceType STRING
•	MaintenanceCost DOUBLE
Insert the following rows into the table:
•	(1, 'DR-1', 'Battery Replacement', 120.0)
•	(2, 'DR-2', 'Motor Repair', 250.0)
•	(3, 'DR-3', 'Propeller Replacement', 75.0)
•	(4, 'DR-1', 'Sensor Calibration', 40.0)
•	(5, 'DR-4', 'Battery Replacement', 110.0)
Part 4 – Join Tables
Join drone_missions with drone_info using DroneID.
Create the following derived column:
EnergyUsage = BatteryUsedPercent / DurationMinutes
Part 5 – CASE Column
Create a new column called MissionIntensity using CASE logic:
• DurationMinutes >= 60 → 'High'
• DurationMinutes between 40 and 59 → 'Medium'
• Otherwise → 'Low'
Part 6 – Create a UDF
Create a Spark UDF called estimate_stress_score using the formula:
(DurationMinutes / MaxFlightMinutes) * WeightKg

Use the UDF to create a column called StressScore.
Part 7 – Create an Aggregated View
Create a view called drone_operations_summary that aggregates data across ALL THREE tables:
drone_missions, drone_info, and drone_maintenance.
•	Model
•	MissionType
•	TotalMissions
•	AverageDuration
•	AverageStressScore
•	TotalMaintenanceCost
The view should:
• Join drone_missions to drone_info using DroneID
• Join drone_info to drone_maintenance using DroneID
• Aggregate results grouped by Model and MissionType
• Compute TotalMaintenanceCost using SUM from the maintenance table
Part 8 – Analytical Query
Using the view, write a query that identifies which drone model and mission type combination produces the highest average stress score.
