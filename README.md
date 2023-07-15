# **lichess-data-warehousing**

### This repo is about data warehouse design, which data source is from lichess open database: https://database.lichess.org/

### I use the 2016 July dataset for this project.

### These images below are my results in hdfs after running the pyspark-etl.py script in hortonwork sandbox:

1. dimGameType
![alt](img/dimGameType-1.png)
![alt](img/dimGameType-2.png)

2. dimTermination
![alt](img/dimTermination-1.png)
![alt](img/dimTermination-2.png)

3. dimDate
![alt](img/dimDate-1.png)
![alt](img/dimDate-2.png)

4. dimTime
![alt](img/dimTime-1.png)
![alt](img/dimTime-2.png)

5. dimPlayer
![alt](img/dimPlayer-1.png)
![alt](img/dimPlayer-2.png)

6. factGame
![alt](img/factGame-1.png)
![alt](img/factGame-2.png)