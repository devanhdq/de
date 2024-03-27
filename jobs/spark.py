from pyspark.sql import SparkSession
# <!-- https://mvnrepository.com/artifact/org.mongodb.spark/mongo-spark-connector -->
# <dependency>
#     <groupId>org.mongodb.spark</groupId>
#     <artifactId>mongo-spark-connector_2.11</artifactId>
#     <version>2.1.5</version>
# </dependency>

spark = SparkSession \
    .builder \
    .master('local[*]') \
    .appName('sofascore') \
    .config("spark.sql.legacy.timeParserPolicy", "CORRECTED") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.0') \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/sofascore") \
    .enableHiveSupport() \
    .getOrCreate()

# # lineups process
lineups_raw_df = spark.read \
    .format('com.mongodb.spark.sql.DefaultSource') \
    .option("uri", 'mongodb://localhost:27017/sofascore.lineups') \
    .load()
# lineups_raw_df.show()
# home_players_df = lineups_raw_df \
#     .withColumn('home_players', explode(col('home.players'))) \
#     .selectExpr('home_players.player.*',
#                 'home_players.statistics.*',
#                 'tournament_id',
#                 'country.name as country_name') \
#     .select(col('id'), col('name'), col('dateOfBirthTimestamp'),
#             col('position'), col('jerseyNumber'), col('country_name').alias('country'),
#             col('accurateCross'), col('accurateKeeperSweeper'), col('accurateLongBalls'),
#             col('accuratePass'), col('aerialLost'), col('aerialWon'), col('bigChanceCreated'),
#             col('bigChanceMissed'), col('blockedScoringAttempt'), col('challengeLost'),
#             col('clearanceOffLine'), col('dispossessed'), col('duelLost'), col('duelWon'),
#             col('errorLeadToAGoal'), col('expectedAssists'), col('expectedGoals'), col('fouls'),
#             col('goalAssist'), col('goals'), col('goalsPrevented'), col('goodHighClaim'),
#             col('hitWoodwork'), col('interceptionWon'), col('keyPass'), col('lastManTackle'),
#             col('minutesPlayed'), col('onTargetScoringAttempt'), col('outfielderBlock'), col('ownGoals'),
#             col('penaltyConceded'), col('penaltyMiss'), col('penaltySave'), col('penaltyShootoutGoal'),
#             col('penaltyShootoutMiss'), col('penaltyShootoutSave'), col('penaltyWon'),
#             col('possessionLostCtrl'), col('punches'), col('rating'), col('savedShotsFromInsideTheBox'),
#             col('saves'), col('shotOffTarget'), col('totalClearance'), col('totalContest'),
#             col('totalCross'), col('totalKeeperSweeper'), col('totalLongBalls'), col('totalOffside'),
#             col('totalPass'), col('totalTackle'), col('touches'), col('wasFouled'), col('wonContest'),
#             col('tournament_id')
#             )
# home_players_df.drop_duplicates().write \
#     .format('csv') \
#     .option('header', 'true') \
#     .mode('overwrite') \
#     .option('path', '../output/home-player') \
#     .save()
#
# away_players_df = lineups_raw_df \
#     .withColumn('away_players', explode(col('away.players'))) \
#     .selectExpr('away_players.player.*',
#                 'away_players.statistics.*',
#                 'tournament_id',
#                 'country.name as country_name'
#                 ) \
#     .select(col('id'), col('name'), col('dateOfBirthTimestamp'),
#             col('position'), col('jerseyNumber'), col('country_name').alias('country'),
#             col('accurateCross'), col('accurateKeeperSweeper'), col('accurateLongBalls'),
#             col('accuratePass'), col('aerialLost'), col('aerialWon'), col('bigChanceCreated'),
#             col('bigChanceMissed'), col('blockedScoringAttempt'), col('challengeLost'),
#             col('clearanceOffLine'), col('dispossessed'), col('duelLost'), col('duelWon'),
#             col('errorLeadToAGoal'), col('expectedAssists'), col('expectedGoals'), col('fouls'),
#             col('goalAssist'), col('goals'), col('goalsPrevented'), col('goodHighClaim'),
#             col('hitWoodwork'), col('interceptionWon'), col('keyPass'), col('lastManTackle'),
#             col('minutesPlayed'), col('onTargetScoringAttempt'), col('outfielderBlock'), col('ownGoals'),
#             col('penaltyConceded'), col('penaltyMiss'), col('penaltySave'), col('penaltyShootoutGoal'),
#             col('penaltyShootoutMiss'), col('penaltyShootoutSave'), col('penaltyWon'),
#             col('possessionLostCtrl'), col('punches'), col('rating'), col('savedShotsFromInsideTheBox'),
#             col('saves'), col('shotOffTarget'), col('totalClearance'), col('totalContest'),
#             col('totalCross'), col('totalKeeperSweeper'), col('totalLongBalls'), col('totalOffside'),
#             col('totalPass'), col('totalTackle'), col('touches'), col('wasFouled'), col('wonContest'),
#             col('tournament_id')
#             )
# away_players_df.drop_duplicates().write \
#     .format('csv') \
#     .option('header', 'true') \
#     .mode('overwrite') \
#     .option('path', '../output/away-player') \
#     .save()
#
# lineup_formation_df = lineups_raw_df \
#     .selectExpr('home.formation as home_formation',
#                 'away.formation as away_formation',
#                 'tournament_id')
# lineup_formation_df.drop_duplicates().write \
#     .format('csv') \
#     .option('header', 'true') \
#     .mode('overwrite') \
#     .option('path', '../output/lineup_formation') \
#     .save()
# #
# # # # tournaments process
# tournaments_raw_df = spark.read \
#     .format('com.mongodb.spark.sql.DefaultSource') \
#     .option("uri", 'mongodb://localhost:27017/sofascore.tournaments') \
#     .load()
# tournaments_converter_df = tournaments_raw_df.selectExpr(
#     "id as tournament_id",
#     "tournament.uniqueTournament.name as tournament_name",
#     "tournament.uniqueTournament.category.country.name as tournament_country",
#     "season.name as session_name",
#     "season.year as tournament_year",
#     "roundInfo.round as round",
#     "winnerCode as winner_code",
#
#     "homeTeam.id as home_team_id",
#     "homeTeam.name as home_team_name",
#     "homeTeam.teamColors.primary as home_team_primary_color",
#     "homeTeam.teamColors.secondary as home_team_secondary_color",
#
#     "awayTeam.name as away_team_name",
#     "awayTeam.id as away_team_id",
#     "awayTeam.teamColors.primary as away_team_primary_color",
#     "awayTeam.teamColors.secondary as away_team_secondary_color",
#
#     "homeScore.display as home_score",
#     "homeScore.period1 as home_score_period1",
#     "homeScore.period2 as home_score_period2",
#     "homeScore.normaltime as home_score_normal_time",
#
#     "awayScore.display as away_score",
#     "awayScore.period1 as away_score_period1",
#     "awayScore.period2 as away_score_period2",
#     "awayScore.normaltime as away_score_normal_time",
#
#     "time.currentPeriodStartTimestamp as current_period_start_timestamp",
#     "time.injuryTime1 as injuryTime1",
#     "time.injuryTime2 as injuryTime2"
# )
# tournaments_converter_df.drop_duplicates().write \
#     .format('csv') \
#     .option('header', 'true') \
#     .mode('overwrite') \
#     .option('path', '../output/tournaments') \
#     .save()
# #
# # # statistic process
# statistics_raw_df = spark.read \
#     .format('com.mongodb.spark.sql.DefaultSource') \
#     .option("uri", 'mongodb://localhost:27017/sofascore.statistics') \
#     .load()
# statistics_convert_df = statistics_raw_df \
#     .withColumn('statistics_period', explode('statistics')) \
#     .selectExpr('statistics_period.period', 'statistics_period', 'tournament_id') \
#     .withColumn('groups', explode('statistics_period.groups')) \
#     .withColumn('item', explode('groups.statisticsItems')) \
#     .select(col('tournament_id'), col('groups.groupName').alias('statistic_name'),
#             col('item.name').alias('detail_statistic_name'),
#             col('item.homeValue').alias('home_statistic'),
#             col('item.awayValue').alias('away_statistic'),
#             col('item.compareCode').alias('compareCode'),
#             col('item.statisticsType').alias('statisticsType'),
#             col('period')
#             )
# statistics_convert_df.drop_duplicates().write \
#     .format('csv') \
#     .option('header', 'true') \
#     .mode('overwrite') \
#     .option('path', '../output/statistics') \
#     .save()
# #
# # # best-player process
# best_player_raw_df = spark.read \
#     .format('com.mongodb.spark.sql.DefaultSource') \
#     .option("uri", 'mongodb://localhost:27017/sofascore.best_players') \
#     .load()
#
# best_player_convert_df = best_player_raw_df \
#     .withColumn('home_player', col('bestHomeTeamPlayer.player')) \
#     .withColumn('away_player', col('bestAwayTeamPlayer.player')) \
#     .select(col('tournament_id'),
#             col('bestHomeTeamPlayer.value').alias('home_player_value'),
#
#             col('bestHomeTeamPlayer.player.name').alias('home_player_name'),
#             col('bestHomeTeamPlayer.player.position').alias('home_player_position'),
#             col('bestHomeTeamPlayer.player.jerseyNumber').alias('home_player_jerseyNumber'),
#             col('bestHomeTeamPlayer.player.dateOfBirthTimestamp').alias('home_player_dateOfBirthTimestamp'),
#             col('bestHomeTeamPlayer.player.id').alias('home_player_id'),
#
#             col('bestAwayTeamPlayer.value').alias('away_player_value'),
#
#             col('bestAwayTeamPlayer.player.name').alias('away_player_name'),
#             col('bestAwayTeamPlayer.player.position').alias('away_player_position'),
#             col('bestAwayTeamPlayer.player.jerseyNumber').alias('away_player_jerseyNumber'),
#             col('bestAwayTeamPlayer.player.dateOfBirthTimestamp').alias('away_player_dateOfBirthTimestamp'),
#             col('bestAwayTeamPlayer.player.id').alias('away_player_id'),
#             )
# best_player_convert_df.drop_duplicates().write \
#     .format('csv') \
#     .option('header', 'true') \
#     .mode('overwrite') \
#     .option('path', '../output/best-player') \
#     .save()
# #
# # # h2h process
# h2h_raw_df = spark.read \
#     .format('com.mongodb.spark.sql.DefaultSource') \
#     .option("uri", 'mongodb://localhost:27017/sofascore.h2h') \
#     .load()
#
# h2h_convert_df = h2h_raw_df \
#     .select(col('teamDuel.homeWins').alias('home_team_win'),
#             col('teamDuel.awayWins').alias('away_team_win'),
#             col('teamDuel.draws').alias('draws_team'),
#             col('managerDuel.homeWins').alias('home_manager_win'),
#             col('managerDuel.awayWins').alias('away_manager_win'),
#             col('managerDuel.draws').alias('draws_manager'),
#             col('tournament_id')
#             )
# h2h_convert_df.drop_duplicates().write \
#     .format('csv') \
#     .option('header', 'true') \
#     .mode('overwrite') \
#     .option('path', '../output/h2h') \
#     .save()
