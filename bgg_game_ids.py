from xml.dom import minidom
import requests
import time

filename = 'bgg_game_ids.csv'

# Read existing game_ids from the file
existing_game_ids = set()
try:
    with open(filename, 'r') as existing_file:
        existing_game_ids = set(line.strip() for line in existing_file)
except FileNotFoundError:
    pass  # File doesn't exist yet, ignore

# Open the file in append mode
f = open(filename, 'a')

i = 1
start_time = time.time()  # Record the start time

while True:
    time.sleep(2)
    url = "https://boardgamegeek.com/sitemap_geekitems_boardgame_page_%d.xml" % i
    r = requests.get(url)

    if r.status_code == 200:
        doc = minidom.parseString(r.content)
        locs = doc.getElementsByTagName("loc")
        if not locs:
            print("No more items. Exiting.")
            break
        for loc in locs:
            game_url = loc.firstChild.data
            game_id = game_url.split("/")[4]
            if game_id not in existing_game_ids:
                f.write(game_id + '\n')
                existing_game_ids.add(game_id)
        print("Got %d" % i)
        i += 1
    else:
        print("Didn't get %d" % i)
        break

# Closing the file and printing the completion message outside the loop
f.close()

end_time = time.time()  # Record the end time
elapsed_time = round(end_time - start_time, 2)

print(f"Process complete! Elapsed time: {elapsed_time} seconds.")