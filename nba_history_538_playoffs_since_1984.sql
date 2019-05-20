/* BigQuery standard SQL query to wrangle 538's NBA history dataset 
   (https://github.com/fivethirtyeight/data/tree/master/nba-elo) 
   for the purpose of analyzing post-1983 playoff series 
*/   
  
#standardSQL
CREATE OR REPLACE TABLE `dnlmc-public.ball_equals_life.nba_history_538_playoffs_since_1984` AS

SELECT *,

       -- create playoff series id
       CONCAT( CAST(year_id AS STRING), team_id, "_", opp_id) AS series_id,
       
       -- aggregate full series results into array in each game row
       ARRAY_AGG(game_result) OVER( PARTITION BY CONCAT( CAST(year_id AS STRING), team_id, opp_id)) AS series_results_array,
       
       -- create field concatenating first 2 game results
       CONCAT(ARRAY_AGG(game_result) OVER( PARTITION BY CONCAT( CAST(year_id AS STRING), team_id, opp_id))[OFFSET(0)],
              "_",
              ARRAY_AGG(game_result) OVER( PARTITION BY CONCAT( CAST(year_id AS STRING), team_id, opp_id))[OFFSET(1)]) as series_first_two,
               
       -- tally series wins
       COUNTIF( game_result = "W" ) OVER( PARTITION BY CONCAT( CAST(year_id AS STRING), team_id, opp_id)) AS series_wins,
       
       -- tally series losses
       COUNTIF( game_result = "L" ) OVER( PARTITION BY CONCAT( CAST(year_id AS STRING), team_id, opp_id)) AS series_losses,
       
       -- determine series result 
       IF( COUNTIF( game_result = "W" ) OVER( PARTITION BY CONCAT( CAST(year_id AS STRING), team_id, opp_id)) >
           COUNTIF( game_result = "L" ) OVER( PARTITION BY CONCAT( CAST(year_id AS STRING), team_id, opp_id)) , "W", "L") AS series_result,
           
       -- determine whether first game was home or away
       FIRST_VALUE(game_location)  OVER( PARTITION BY CONCAT( CAST(year_id AS STRING), team_id, opp_id) ORDER BY game_id) AS series_home_court
            
FROM `dnlmc-public.ball_equals_life.nba_history_538` 

WHERE is_playoffs = 1 
  AND year_id > 1983
  
ORDER BY game_id
