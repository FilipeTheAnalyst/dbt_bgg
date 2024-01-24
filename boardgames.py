# Import libraries
import csv
from bs4 import BeautifulSoup
import requests
import time
import os
import pandas as pd

# CSV file saving function
def save_to_csv(filename, rows, csv_header):
    with open(filename, 'a', encoding='UTF8') as f:
        dictwriter_object = csv.DictWriter(f, fieldnames=csv_header)
        if f.tell() == 0:
            dictwriter_object.writeheader()
        dictwriter_object.writerows(rows)

# Define request url headers
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Accept-Language": "en-GB, en-US, q=0.9, en"
}

# Define sleep timer value between requests
SLEEP_BETWEEN_REQUEST = 5

# Check if the file exists
file_path = 'bgg_game_ids.csv'
if not os.path.exists(file_path):
    print("File with boardgame_ids doesn't exist!")
else:
    # Read game_ids from the file
    with open(file_path, 'r') as ids_file:
        game_ids = [line.strip() for line in ids_file]

    # Define batch_size
    batch_size = 500
    total_games = len(game_ids)
    # total_games = 10000  # For testing purposes
    base_url = "https://boardgamegeek.com/xmlapi2/thing?id="

    games_header = [
        'name', 'game_id', 'type', 'avg_rating', 'avg_bayes_rating', 'users_rated', 'weight', 'year_published', 'min_players', 'max_players',
        'min_play_time', 'max_play_time', 'min_age', 'owned_by', 'updated_at'
    ]

    # Read existing game IDs from the bgg.csv file
    existing_game_ids = set()
    if os.path.exists('bgg.csv'):
        df_existing = pd.read_csv('bgg.csv')
        existing_game_ids.update(df_existing['game_id'])

    for batch_start in range(0, total_games, batch_size):
        # Extract a batch of game_ids
        batch_ids = game_ids[batch_start:batch_start + batch_size]

        # Join and append to the URL the IDs within batch size
        ids = ",".join(batch_ids)
        url = f"{base_url}{ids}&stats=1"

        # If by any chance there is an error, this will throw the exception and continue to the next batch
        try:
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, features="html.parser")
                items = soup.find_all("item")
                games = {}
                link_type_data = {}

                if not items:
                    print(f">>> No more items. Exiting.")
                    break

                for item in items:
                    if item['type'] == 'boardgame':
                        game_id = int(item['id'])

                        # Check if the game ID is already processed
                        if game_id in existing_game_ids:
                            print(f">>> Skipping game {game_id} as it's already processed.")
                            continue
                        try:
                            # Find values in the XML
                            name = item.find("name")['value'] if item.find("name") is not None else 0
                            year_published = item.find("yearpublished")['value'] if item.find("yearpublished") is not None else 0
                            min_players = item.find("minplayers")['value'] if item.find("minplayers") is not None else 0
                            max_players = item.find("maxplayers")['value'] if item.find("maxplayers") is not None else 0
                            min_play_time = item.find("minplaytime")['value'] if item.find("minplaytime") is not None else 0
                            max_play_time = item.find("maxplaytime")['value'] if item.find("maxplaytime") is not None else 0
                            min_age = item.find("minage")['value'] if item.find("minage") is not None else 0
                            avg_rating = item.find("average")['value'] if item.find("average") is not None else 0
                            avg_bayes_rating = item.find("bayesaverage")['value'] if item.find("bayesaverage") is not None else 0
                            num_users_rated = item.find("usersrated")['value'] if item.find("usersrated") is not None else 0
                            weight = item.find("averageweight")['value'] if item.find("averageweight") is not None else 0
                            owned = item.find("owned")['value'] if item.find("owned") is not None else 0

                            links = item.find_all("link")

                            # Append value(s) for each link type
                            for link in links:
                                link_type_name = link['type']
                                link_type_value = link['value']

                                if link_type_name not in link_type_data:
                                    link_type_data[link_type_name] = []

                                link_type_data[link_type_name].append({
                                    "game_id": item['id'],
                                    "value": link_type_value
                                })

                            game = {
                                "name": name,
                                "game_id": item['id'],
                                "type": item['type'],
                                "avg_rating": avg_rating,
                                "avg_bayes_rating": avg_bayes_rating,
                                "users_rated": num_users_rated,
                                "weight": weight,
                                "year_published": year_published,
                                "min_players": min_players,
                                "max_players": max_players,
                                "min_play_time": min_play_time,
                                "max_play_time": max_play_time,
                                "min_age": min_age,
                                "owned_by": owned,
                                "updated_at": time.strftime("%Y-%m-%d %H:%M:%S")
                            }

                            # Append current item to games dictionary
                            games[item['id']] = game

                        except TypeError:
                            print(">>> NoneType error. Continued on the next item.")
                            continue

                save_to_csv('bgg.csv', list(games.values()), games_header)  # Save the common data to bgg.csv

                # Save to CSV based on link type
                for link_type_name, link_type_values in link_type_data.items():
                    link_type_filename = f'bgg_{link_type_name}.csv'
                    link_type_header = ['game_id', 'value']
                    save_to_csv(link_type_filename, link_type_values, link_type_header)

                print(f">>> Request successful for batch {batch_start}-{batch_start + batch_size - 1}")
            else:
                print(f">>> FAILED batch {batch_start}-{batch_start + batch_size - 1}")
        except Exception as err:
            print(f">>> Error: {err}. Continuing to the next batch.")

        # Pause between requests
        time.sleep(SLEEP_BETWEEN_REQUEST)
