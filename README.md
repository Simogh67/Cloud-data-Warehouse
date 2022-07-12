# Cloud-data-Warehouse


# Summary

The goal of this project is to create a database schema and ETL pipeline with AWS Redshift service. <br>
The dataset is comprised of two different datasets called log and song datasets reside in S3 buckets.<br> 
The first dataset is called **song_data**, which is a subset of real data from the Million Song Dataset. <br> 
Each file of the **song_data** dataset is in JSON format and contains metadata about a song and the artist of that song.<br>
The second dataset is called **log_data** consists of log files in JSON files, where each file covers the users activities over a given day.<br>

# Database Schema

To reach the goal of the project, we build a star schema optimized for queries on the given dataset. <br>
First, we extract the data from S3 buckets and stage it in Redshift via tables **staging_songs** and **staging_events**. <br>

 **staging_events**<br>
  -  artist, auth, firstName, gender, itemInSession, lastName, length , level,location ,method, page, <br>
       registration, sessionId, song,status, ts, userAgent, userId <br>
  
  **staging_songs**<br>
  -   num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, <br>
       song_id, title, duration, year<br>
       
Then, we create our star schema via the staging tables as follows: <br>
The fact table is **songplays** - records in log data associated with song plays i.e. records with page nextsong.<br>
  - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent <br>

The dimension tables are: <br> 

   **users** - users in the app <br>
   - user_id, first_name, last_name, gender, level
   **songs** - songs in music database <br>
   - songs - songs in music database
   **artists** - artists in music database <br>
   - artist_id, name, location, latitude, longitude
   **time** - timestamps of records in songplays broken down into specific units <br>
   - start_time, hour, day, week, month, year, weekday
   
   To run the python scripts, first, run create_tables.py and then etl.py to create the database,
   tables and insert data from JSON files into the tables.
 
