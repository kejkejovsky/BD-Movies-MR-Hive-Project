# BD-Movies-MR-Hive-Project
A final project for the Big Data Processing subject

# Desciption
The project analyzes movie ratings by people.

It contains two sets of data - movie ratings and movie information.

The goal is to present the 3 best rated movies of a given year.


Firstly, dataset with ratings is extracted and transformed to show only film id, sum of rating and number of ratings using Hadoop with mapper, combiner and reducer.

Next, we load transformed data and second dataset to database tables using Hive.

Lastly, we create result and save it in new database table.


To run project, we use bash script.

# Technology
- Google Cloud Platform
- Python
- Hive
