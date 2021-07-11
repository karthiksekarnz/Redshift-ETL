## Sparkify Redshift ETL project
### Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

### Objective
<p>As a data engineer, the task is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.</p>
<p>The database and ETL pipeline can be tested by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.</p>

### Datasets
The two datasets that reside in S3. Here are the S3 links for each:
- Song data ``s3://udacity-dend/song_data``
- Log data ``s3://udacity-dend/log_data``

Log data json path: ``s3://udacity-dend/log_json_path.json``

### Song Dataset
The first dataset is a subset of real data from the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. 

```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```

### Log Dataset
The second dataset consists of log files in JSON format.
```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

### Fact Table
- songplays - records in log data associated with song plays i.e. records with page NextSong

### Dimension Tables
- users - users in the app
- songs - songs in music database
- artists - artists in music database
- time - timestamps of records in songplays broken down into specific units

### Assumptions
- The Redshift cluster is already created and endpoint is added to the config.
- The IAM user and role are already attached to the redshift cluster

### Instructions to run the project
- Run ``python3 ./create_tables.py`` command to drop already existing tables and create the tables.
- Then run ``python3 ./etl.py`` command to run the etl script to process the song and log data and import them into the database.

### Analysis queries
Following are some analytical queries run on the redshift cluster

#### Top 5 most played songs
```sql
SELECT s.title AS "Song", COUNT(sp.song_id) AS "Sessions" 
FROM songplays sp 
    JOIN songs s ON sp.song_id = s.song_id 
WHERE sp.song_id IS NOT NULL 
GROUP BY s.title 
ORDER BY sessions DESC LIMIT 5;
```

![Screen Shot 2021-07-11 at 7 07 54 PM](https://user-images.githubusercontent.com/2171885/125186278-9136a900-e27d-11eb-98aa-4d6ae9a786d1.png)


#### Top 5 most listened artists

```sql
SELECT a.name AS "Artist", COUNT(sp.artist_id) AS "Sessions" 
FROM songplays sp 
    JOIN artists a ON sp.artist_id = a.artist_id 
WHERE sp.artist_id IS NOT NULL 
GROUP BY a.name 
ORDER BY sessions DESC LIMIT 5;
```
![Screen Shot 2021-07-11 at 9 38 55 PM](https://user-images.githubusercontent.com/2171885/125190289-77eb2800-e290-11eb-9547-6facac1d6dc3.png)

