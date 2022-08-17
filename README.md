# etlRepository
Creating an ETL function using Pandas 
First, create a database to house the incoming tables and data 
Second, create a user to connect and add table(s) and insert data into this database. I will go ahead and create the same user in SQL Server environment to keep things consistent.
Third, ave the credentials in environment variables. It’s a good habit to store your credentials separately. The goal is to protect the credentials from being exposed in the ETL script. You can use a configuration file or system environment variables.
