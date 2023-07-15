from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *

spark = SparkSession.builder.appName("LichessETL").getOrCreate()

global df
df = spark.read.option('header','True').csv("hdfs://172.18.0.2:8020/user/maria_dev/lichess/chess_games.csv")

# 1. Gametype dimension
def dimGameType():
	gameType = df.select(trim('Event').alias('gameType')).distinct()
	gameType = gameType.withColumn('monotonically_increasing_id',monotonically_increasing_id())
	window = Window.orderBy(col('monotonically_increasing_id'))
	gameType = gameType.withColumn('gameTypeId', row_number().over(window))
	gameType = gameType.select('gameTypeId','gameType')
	gameType.write.option("header",True).option("delimiter",",").csv('hdfs://172.18.0.2:8020/user/maria_dev/lichess/dim/dimGameType')

# 2. Date dimension
def dimDate():
	date = df.select('UTCDate').distinct()
	date = date.withColumn('UTCDate', translate('UTCDate','.','-'))
	date = date.withColumn('UTCDate', to_timestamp('UTCDate', 'yyyy-MM-dd'))
	date = date.withColumn('year', year('UTCDate'))
	date = date.withColumn('month', month('UTCDate'))
	date = date.withColumn('day', dayofmonth('UTCDate'))
	date = date.withColumn('weekday', dayofweek('UTCDate'))
	date = date.withColumn('quarter', quarter('UTCDate'))
	date = date.withColumn('date', concat_ws('-','year','month','day'))
	date = date.withColumn('monotonically_increasing_id',monotonically_increasing_id())
	window = Window.orderBy(col('monotonically_increasing_id'))
	date = date.withColumn('dateId', row_number().over(window))
	date = date.select('dateId','date','year','month','day','weekday','quarter')
	date.write.option('header',True).option('delimiter',',').csv('hdfs://172.18.0.2:8020/user/maria_dev/lichess/dim/dimDate')

# 3. Time dimension
def dimTime():
	time = df.select('UTCTime').distinct()
	time = time.withColumn('hour', split(col('UTCTime'),':').getItem(0))
	time = time.withColumn('minute', split(col('UTCTime'),':').getItem(1))
	time = time.withColumn('second', split(col('UTCTime'),':').getItem(2))
	time = time.withColumn('monotonically_increasing_id',monotonically_increasing_id())
	window = Window.orderBy(col('monotonically_increasing_id'))
	time = time.withColumn('timeId', row_number().over(window))
	time = time.withColumnRenamed('UTCTime','time')
	time = time.select('timeId','time','hour','minute','second')
	time.write.option('header',True).option('delimiter',',').csv('hdfs://172.18.0.2:8020/user/maria_dev/lichess/dim/dimTime')

# 4. Termination Dimension
def dimTermination():
	termination = df.select(trim('Termination').alias('terminationReason')).distinct()
	termination = termination.withColumn('monotonically_increasing_id',monotonically_increasing_id())
	window = Window.orderBy(col('monotonically_increasing_id'))
	termination = termination.withColumn('terminationId', row_number().over(window))
	termination = termination.select('terminationId','terminationReason')
	termination.write.option("header",True).option("delimiter",",").csv('hdfs://172.18.0.2:8020/user/maria_dev/lichess/dim/dimTermination')

# 5. Player Dimension
def dimPlayer():
	temp = df.select('White','Black','WhiteElo','BlackElo','WhiteRatingDiff','BlackRatingDiff','UTCDate','UTCTime')
	temp = temp.withColumn('UTCDate', translate('UTCDate','.','-'))
	temp = temp.withColumn('timestamp', concat_ws(' ','UTCDate','UTCTime'))
	temp = temp.withColumn('timestamp', to_timestamp('timestamp','yyyy-MM-dd HH:mm:ss'))
	temp = temp.orderBy('timestamp')
	temp = temp.withColumn('monotonically_increasing_id',monotonically_increasing_id())
	window = Window.orderBy(col('monotonically_increasing_id'))
	temp = temp.withColumn('index', row_number().over(window))
	matches = temp.select('index','White','Black','WhiteElo','BlackElo','WhiteRatingDiff','BlackRatingDiff','timestamp')
	
	whitePlayer = temp.select('White').distinct()
	whitePlayer = whitePlayer.withColumnRenamed('White','playerId')
	blackPlayer = temp.select('Black').distinct()
	blackPlayer = blackPlayer.withColumnRenamed('Black','playerId')
	players = whitePlayer.union(blackPlayer)
	players = players.select('playerId').distinct()
	
	white = matches.groupBy('White').agg(max('index').alias('lastrow'))
	white = white.withColumnRenamed('White','playerId')
	black = matches.groupBy('Black').agg(max('index').alias('lastrow'))
	black = black.withColumnRenamed('Black','playerId')
	playerLastrow = white.union(black)
	playerLastrow = playerLastrow.groupBy('playerId').agg(max('lastrow').alias('lastrow'))
	
	res = playerLastrow.join(matches, playerLastrow.lastrow == matches.index, 'inner')
	
	whiteElo = res.filter('playerId == White')
	whiteElo = whiteElo.withColumn('elo', (col('WhiteElo') + col('WhiteRatingDiff')))
	whiteElo = whiteElo.select('playerId','elo')
	
	blackElo = res.filter('playerId == Black')
	blackElo = blackElo.withColumn('elo', (col('BlackElo') + col('BlackRatingDiff')))
	blackElo = blackElo.select('playerId','elo')
	
	final = whiteElo.union(blackElo)
	final.write.option('header',True).option('delimiter',',').csv('hdfs://172.18.0.2:8020/user/maria_dev/lichess/dim/dimPlayer')

# 6. Games Fact
def factGame():
	games = df.select('*')
	games = games.withColumn('Event',trim('Event'))
	games = games.withColumn('UTCDate', translate('UTCDate','.','-'))
	games = games.withColumn('Termination',trim('Termination'))

	dimGametype = spark.read.option('header',True).csv('hdfs://172.18.0.2:8020/user/maria_dev/lichess/dim/dimGameType')
	dimDate = spark.read.option('header',True).csv('hdfs://172.18.0.2:8020/user/maria_dev/lichess/dim/dimDate')
	dimTime = spark.read.option('header',True).csv('hdfs://172.18.0.2:8020/user/maria_dev/lichess/dim/dimTime')
	dimTermination = spark.read.option('header',True).csv('hdfs://172.18.0.2:8020/user/maria_dev/lichess/dim/dimTermination')
	result = games.join(dimGametype,games.Event == dimGametype.gameType,'inner').join(dimTermination,games.Termination == dimTermination.terminationReason,'inner')
	result = result.select('gameTypeId','White','Black','Result','UTCDate','UTCTime','WhiteElo','BlackElo','WhiteRatingDiff','BlackRatingDiff','ECO','Opening','TimeControl','terminationId','AN')
	result.write.option('header',True).option('delimiter',',').csv('hdfs://172.18.0.2:8020/user/maria_dev/lichess/fact/factGame')	
	
def main():
	dimGameType()
	dimDate()
	dimTime()
	dimTermination()
	dimPlayer()
	factGame()

if __name__ == '__main__':
	main()
