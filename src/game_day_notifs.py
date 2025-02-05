import os
import json
import requests
import boto3
from datetime import datetime, timedelta, timezone

def lambda_handler(event, context):
  # Get environment variables 
  api_key = os.getenv("NBA_API_KEY")
  sns_topic_arn = os.getenv("SNS_TOPIC_ARN")
  sns_client = boto3.client("sns")

  # Get todays date in Central Time
  utc_now = datetime.now(timezone.utc)
  central_time = utc_now - timedelta(hours=6)
  todays_date = central_time.strftime("%Y-%m-%d")
  print(f"{todays_date}")

  # Fetch data from API
  api_url = f"https://api.sportsdata.io/v3/nba/scores/json/GamesByDate/{todays_date}?key={api_key}"
  try: 
    response = requests.get(api_url)
    response.raise_for_status()
    data = response.json()
    print (json.dumps(data, indent=4))
  except Exception as e: 
    print (f"An error occurred: {e}")

  # Get game information
  messages = [format_game_data(game) for game in data]
  final_msg = "\n---\n".join(messages) if messages else "No games today."
  
  # Publish to SNS
  try: 
    sns_client.publish(
      TopicArn=sns_topic_arn,
      Message=final_msg,
      Subject="NBA Game Updates"
    )
    print("Message published to SNS successfully.")
  except Exception as e:
    print(f"Error publlishing to SNS: {e}")
    return {"statusCode": 500, "body": "Error publishing to SNS"}

  return {"statusCode": 200, "body": "Data processed and sent to SNS"}

def format_game_data(game):
  status = game.get("Status", "Unknown")
  away_team = game.get("AwayTeam", "Unknown")
  home_team = game.get("HomeTeam", "Unknown")
  final_score = f"{game.get('AwayTeamScore', 'N/A')}--{game.get('HomeTeamScore', 'N/A')}"
  start_time = game.get("DateTime", "Unknown")
  channel = game.get("Channel", "Unknown")
  quarters = game.get("Quarters", [])

  # Format quarters
  quarter_scores = ', '.join([f"Q{q['Number']}: {q.get('AwayScore', 'N/A')}-{q.get('HomeScore', 'N/A')}" for q in quarters])
  
  if status == "Final":
      return (
          f"Game Status: {status}\n"
          f"{away_team} vs {home_team}\n"
          f"Final Score: {final_score}\n"
          f"Start Time: {start_time}\n"
          f"Channel: {channel}\n"
          f"Quarter Scores: {quarter_scores}\n"
      )
  elif status == "InProgress":
      last_play = game.get("LastPlay", "N/A")
      return (
          f"Game Status: {status}\n"
          f"{away_team} vs {home_team}\n"
          f"Current Score: {final_score}\n"
          f"Last Play: {last_play}\n"
          f"Channel: {channel}\n"
      )
  elif status == "Scheduled":
      return (
          f"Game Status: {status}\n"
          f"{away_team} vs {home_team}\n"
          f"Start Time: {start_time}\n"
          f"Channel: {channel}\n"
      )
  else:
      return (
          f"Game Status: {status}\n"
          f"{away_team} vs {home_team}\n"
          f"Details are unavailable at the moment.\n"
      )
