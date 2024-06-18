import operator
import string
import random
from datetime import datetime, timedelta
import uuid

from flask import Flask, render_template, request, session, redirect, url_for,make_response
import requests
import io
import json
import http.client
import pandas as pd
from sklearn.metrics import brier_score_loss
from bs4 import BeautifulSoup
import numpy as np
import sqlite3
import mysql.connector
from sklearn.ensemble import RandomForestRegressor
from pybaseball import pitching_stats, team_batting, playerid_reverse_lookup

application = Flask(__name__)
application.secret_key = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))


@application.route("/")
def get_steamer():
    response = requests.get("https://www.fangraphs.com/api/steamer/pitching?key=5sCxU6kRxvCW8VcN", verify=False)
    data = response.json()
    stats = pd.DataFrame(data)
    stats = stats[
        ["steamerid", "mlbamid", "firstname", "lastname", "reliability", "SV", "IP", "ERA", "WHIP", "W", "K", "ER",
         "BB", "H"]]
    stats["Val"] = round((stats["W"] / 10.4) + (stats["K"] / 160) + (stats["SV"] / 26) + (
            ((3.72 - stats["ERA"]) / 3.72) * (stats["IP"] / 150)) + (
                                 ((1.19 - stats["WHIP"]) / 1.19) * (stats["IP"] / 150)), 2)
    stats = stats.sort_values(by='Val', ascending=False)
    stats = stats[stats.IP > 50].round(3)
    stats["POSITION"] = "P"
    stats['Rank'] = stats['Val'].rank(pct=True).round(3)

    response_hitters = requests.get("https://www.fangraphs.com/api/steamer/batting?key=5sCxU6kRxvCW8VcN", verify=False)
    data_hitters = response_hitters.json()
    stats_hitters = pd.DataFrame(data_hitters)
    stats_hitters = stats_hitters[
        ["steamerid", "mlbamid", "firstname", "lastname", "reliability", "HR", "AB", "AVG", "RBI", "R", "H", "SB",
         "POSITION"]]
    stats_hitters["Val"] = round((stats_hitters["R"] / 80) + (stats_hitters["HR"] / 23) + (stats_hitters["SB"] / 15) + (
            stats_hitters["RBI"] / 78) + (((stats_hitters["AVG"] - 0.2692) / 0.2692) * (
            stats_hitters["AB"] / 550)), 2)
    stats_hitters = stats_hitters.sort_values(by='Val', ascending=False)
    stats_hitters = stats_hitters[stats_hitters.AB > 200].round(3)
    stats_hitters['Rank'] = stats_hitters['Val'].rank(pct=True).round(3)
    stats = pd.concat([stats, stats_hitters], axis=0, ignore_index=True)
    stats['Rank_All'] = stats['Val'].rank(pct=True).round(3)
    stats['Val'] = (((stats["Rank"] * 4) + stats["Rank_All"]) / 5) * 100
    stats = stats.sort_values(by='Val', ascending=False).round(3)
    return stats.to_json(orient='records')


@application.route("/target/<string:teams>/<string:pitchers>/<string:hitters>")
def get_targets(teams,pitchers,hitters):
    response = requests.get("https://www.fangraphs.com/api/steamer/pitching?key=5sCxU6kRxvCW8VcN", verify=False)
    data = response.json()
    stats = pd.DataFrame(data)
    stats = stats[
        ["steamerid", "mlbamid", "firstname", "lastname", "reliability", "SV", "IP", "ERA", "WHIP", "W", "K", "ER",
         "BB", "H"]]
    stats = stats.sort_values(by='IP', ascending=False)
    stats = stats[stats.IP > 20]
    stats.WHIP = stats.WHIP * -1
    stats.ERA = stats.ERA * -1
    teams = int(teams)
    pitchers = int(pitchers)
    hitters = int(hitters)
    stats_pitchers_head = stats.head((pitchers+2)*teams)
    stats = stats.sort_values(by='SV', ascending=False)
    stats_pitchers_saves = stats.head(3*teams)
    stats = pd.concat([stats_pitchers_head, stats_pitchers_saves], axis=0, ignore_index=True)
    pitching = stats[["IP", "ERA", "WHIP", "W", "K"]].quantile(.7)
    pitching.WHIP = pitching.WHIP * -1
    pitching.ERA = pitching.ERA * -1
    SV = stats[["SV"]].quantile(.825)
    pitching = pd.concat([pitching, SV], axis=0)
    pitching = pd.DataFrame({'Category': pitching.index, 'Target': pitching.values})
    response_hitters = requests.get("https://www.fangraphs.com/api/steamer/batting?key=5sCxU6kRxvCW8VcN", verify=False)
    data_hitters = response_hitters.json()
    stats_hitters = pd.DataFrame(data_hitters)
    stats_hitters = stats_hitters[
        ["steamerid", "mlbamid", "firstname", "lastname", "reliability", "HR", "AB", "AVG", "RBI", "R", "H", "SB",
         "POSITION"]]
    stats_hitters = stats_hitters.sort_values(by='AB', ascending=False)
    stats_hitters_top = stats_hitters.head((hitters+3)*teams)
    hitting = stats_hitters_top[["HR", "AB", "AVG", "RBI", "R", "SB"]].quantile(.7)
    hitting = pd.DataFrame({'Category': hitting.index, 'Target': hitting.values})
    targets = pd.concat([pitching, hitting], axis=0, ignore_index=True)
    return targets.to_json(orient='records')

@application.route("/score/<string:model_id>")
def get_score(model_id):
    cnx = mysql.connector.connect(user = 'doadmin',password = 'AVNS_Lkaktbc2QgJkv-oDi60',
    host = 'db-mysql-nyc3-89566-do-user-8045222-0.c.db.ondigitalocean.com',
      port = 25060,
    database = 'crowdicate')
    if cnx and cnx.is_connected():

        with cnx.cursor() as cursor:

            result = cursor.execute("SELECT * FROM predictions")

            rows = cursor.fetchall()

        cnx.close()

    else:
        return print("Could not connect")
    results = pd.DataFrame(list(rows), columns=["id", "predictable", "date", "page", "post","prediction","result"])
    results["result"] = pd.to_numeric(results["result"])
    results = results[results[['result']].notnull().all(1)]
    model = results[results.page == model_id]
    brier = brier_score_loss(model["result"], model["prediction"])
    return pd.Series(brier).to_json(orient='records')

@application.route("/template/<string:page_id>")
def get_template(page_id):
    response = requests.get("https://crowdicate.com/api/1.1/obj/page")
    data = response.json()
    results = pd.DataFrame(data["response"]["results"])
    while data["response"]["remaining"] > 0:
        cursor = data["response"]["cursor"] + 100
        response = requests.get(
            "https://crowdicate.com/api/1.1/obj/page" + "?cursor=" + str(
                cursor) + "&limit=100")
        data = response.json()
        test = pd.DataFrame(data["response"]["results"])
        results = pd.concat([results, test])
    response_type = requests.get("https://crowdicate.com/api/1.1/obj/types")
    data = response_type.json()
    results_type = pd.DataFrame(data["response"]["results"])
    while data["response"]["remaining"] > 0:
        cursor = data["response"]["cursor"] + 100
        response_type = requests.get(
            "https://crowdicate.com/api/1.1/obj/types" + "?cursor=" + str(
                cursor) + "&limit=100")
        data = response_type.json()
        test = pd.DataFrame(data["response"]["results"])
        results_type = pd.concat([results_type, test])

    results = results[results._id == page_id]
    types = results["type_list_custom_types"].values[0]
    results_type = results_type[results_type['_id'].isin(types)]
    URL = "https://baseballsavant.mlb.com/probable-pitchers"
    page = requests.get(URL, verify=False)
    soup = BeautifulSoup(page.content, "html.parser")
    links = soup.find_all("a", class_="matchup-link")
    link_list = []
    for link in links:
        test = link["href"]
        splitting = test.split('player_id=')
        link_list.append(splitting[1])
    URL = "https://baseballsavant.mlb.com/probable-pitchers"
    page = requests.get(URL, verify=False)
    soup = BeautifulSoup(page.content, "html.parser")
    links = soup.find_all("div", class_="game-info")
    for link in links:
        link_list.append(link.h2.text.strip())
    URL = "https://baseballsavant.mlb.com/probable-pitchers"
    page = requests.get(URL, verify=False)
    soup = BeautifulSoup(page.content, "html.parser")
    links = soup.find_all("div", class_="game-info")
    for link in links:
        test = link.h2.text.strip()
        splitting = test.split(' @ ')
        link_list.append(splitting[0])
        link_list.append(splitting[1])

    cnx = mysql.connector.connect(user='doadmin', password='AVNS_Lkaktbc2QgJkv-oDi60',
                                  host='db-mysql-nyc3-89566-do-user-8045222-0.c.db.ondigitalocean.com',
                                  port=25060,
                                  database='crowdicate')
    if cnx and cnx.is_connected():

        with cnx.cursor() as cursor:

            result = cursor.execute("SELECT * FROM predictables")

            rows = cursor.fetchall()

        cnx.close()

    else:
        return ("Could not connect")
    results = pd.DataFrame(list(rows), columns=["id", "amount", "player", "player_id", "type"])
    results['date'] = str(datetime.today().strftime("%m/%d/%Y"))
    results['prediction'] = ""
    type_list = results_type['type_text'].tolist()

    results = results[results['type'].isin(type_list)]
    template = results[results['player_id'].isin(link_list) | results['player'].isin(link_list)]
    template = template[
        ["id","amount", "player_id", "player","type", "date","prediction"]]
    template = template.sort_values(["type",'player', 'amount'], ascending=[True, True,True])
    return template.to_json(orient='records')

@application.route("/predictions/<string:post_id>")
def get_predictions(post_id):
    response = requests.get("https://crowdicate.com/api/1.1/obj/posts")
    data = response.json()
    results = pd.DataFrame(data["response"]["results"])
    while data["response"]["remaining"] > 0:
        cursor = data["response"]["cursor"] + 100
        response = requests.get(
            "https://crowdicate.com/api/1.1/obj/posts" + "?cursor=" + str(
                cursor) + "&limit=100")
        data = response.json()
        test = pd.DataFrame(data["response"]["results"])
        results = pd.concat([results, test])
    post = results[results._id == post_id]
    url = "https:" + post["file_file"].values[0]
    data = pd.read_csv(url)
    data["page"] = post["page_text"].values[0]
    data["post"] = post_id
    data["predictable"] = data["id"]
    data = data[["predictable", "prediction", "page", "date", "post"]]
    data["id"] = [uuid.uuid4().hex for _ in range(len(data.index))]
    data = data[["id", "predictable", "date", "page", "post", "prediction"]]
    data = data.loc[data.notna().all(axis='columns')]
    con = sqlite3.connect("Crowdicate.db")
    cnx = mysql.connector.connect(user='doadmin', password='AVNS_Lkaktbc2QgJkv-oDi60',
                                  host='db-mysql-nyc3-89566-do-user-8045222-0.c.db.ondigitalocean.com',
                                  port=25060,
                                  database='crowdicate')
    if cnx and cnx.is_connected():

        with cnx.cursor() as cursor:

            cursor.executemany("""INSERT INTO predictions
                              (id,predictable,date,page,post,prediction) 
                              VALUES (%s,%s,%s,%s,%s,%s);""",list(data.itertuples(index=False, name=None)))

            cnx.commit()

        cnx.close()
        return "success"

    else:
        return "Could not connect"

@application.route("/aggregate/<string:post_id>/<string:page>/<string:type>")
def get_aggregate(post_id,page,type):
    response = requests.get("https://crowdicate.com/api/1.1/obj/types")
    data = response.json()
    results = pd.DataFrame(data["response"]["results"])
    while data["response"]["remaining"] > 0:
        cursor = data["response"]["cursor"] + 100
        response = requests.get(
            "https://crowdicate.com/api/1.1/obj/types" + "?cursor=" + str(
                cursor) + "&limit=100")
        data = response.json()
        test = pd.DataFrame(data["response"]["results"])
        results = pd.concat([results, test])
    name = results[results._id == type]
    name = name["type_text"].values[0]
    cnx = mysql.connector.connect(user='doadmin', password='AVNS_Lkaktbc2QgJkv-oDi60',
                              host='db-mysql-nyc3-89566-do-user-8045222-0.c.db.ondigitalocean.com',
                              port=25060,
                              database='crowdicate')
    if cnx and cnx.is_connected():
        with cnx.cursor() as cursor:
            result = cursor.execute("SELECT * FROM predictions")

            rows = cursor.fetchall()

            result_b = cursor.execute("SELECT * FROM predictables")

            types = cursor.fetchall()

            results = pd.DataFrame(list(rows),columns=["id", "predictable", "date", "page", "post","prediction","result"])
            results1 = results[results['date'] == str(datetime.today().strftime("%m/%d/%Y"))]
            results2 = results[results['date'] == str(datetime.today().strftime("%-m/%-d/%Y").lstrip("0").replace(" 0", " "))]
            results3 = results[results['date'] == str(datetime.today().strftime("%-m/%-d/%y").lstrip("0").replace(" 0", " "))]
            results4 = results[results['date'] == str(datetime.today().strftime("%m/%d/%y"))]
            results = pd.concat([results1,results2,results3,results4])
            results['date'] = str(datetime.today().strftime("%m/%d/%Y"))
            group = results.groupby(['predictable','date'])['prediction'].agg({'mean'}).reset_index()
            predictables = pd.DataFrame(list(types), columns=["id", "amount", "player", "player_id", "type"])
            group = group.merge(predictables[["id","type"]],how='left', left_on='predictable', right_on='id')
            group = group[group.type == name]
            group["id"] = [uuid.uuid4().hex for _ in range(len(group.index))]
            group["post"] = post_id
            group["page"] = page
            group["prediction"] = group["mean"]
            group = group[["id", "predictable", "date", "page", "post","prediction"]]
            cursor.executemany("""INSERT INTO predictions
                                          (id,predictable,date,page,post,prediction) 
                                          VALUES (%s,%s,%s,%s,%s,%s);""", list(group.itertuples(index=False, name=None)))
            cnx.commit()

        cnx.close()
        return "success"

    else:
        return "Could not connect"


@application.route("/export_predictions/<string:post_id>")
def export_predictions(post_id):
    cnx = mysql.connector.connect(user='doadmin', password='AVNS_Lkaktbc2QgJkv-oDi60',
                                  host='db-mysql-nyc3-89566-do-user-8045222-0.c.db.ondigitalocean.com',
                                  port=25060,
                                  database='crowdicate')
    if cnx and cnx.is_connected():
        with cnx.cursor() as cursor:
            result = cursor.execute("SELECT * FROM predictions")

            rows = cursor.fetchall()

            result2 = cursor.execute("SELECT * FROM predictables")

            rows2 = cursor.fetchall()

        cnx.close()

    results = pd.DataFrame(list(rows), columns=["id", "predictable", "date", "page", "post", "prediction", "result"])
    predictions = results[results.post == post_id]
    predictables = pd.DataFrame(list(rows2), columns=["id", "amount", "player", "player_id", "type"])
    predictions = predictions.merge(predictables[["id", "amount", "player", "player_id","type"]], how='left',
                                    left_on='predictable', right_on='id')
    predictions['odds'] = np.where(predictions['prediction'] >= .50, -(100*predictions['prediction'])/(1-predictions['prediction']), (100-(100*predictions['prediction']))/predictions['prediction'])
    template = predictions[
        ["player", "player_id", "amount","type", "date", "prediction","odds"]]
    template = template.sort_values(["type",'player', 'amount'], ascending=[True, True,True])
    return template.to_json(orient='records')

@application.route("/leaderboard/<string:days>")
def generate_leaderboard(days):
    cnx = mysql.connector.connect(user='doadmin', password='AVNS_Lkaktbc2QgJkv-oDi60',
                                  host='db-mysql-nyc3-89566-do-user-8045222-0.c.db.ondigitalocean.com',
                                  port=25060,
                                  database='crowdicate')
    if cnx and cnx.is_connected():
        with cnx.cursor() as cursor:
            result = cursor.execute("SELECT * FROM predictions WHERE STR_TO_DATE(date, '%m/%d/%Y') BETWEEN DATE_SUB(NOW(), INTERVAL "
            + days + " DAY) AND NOW()"
                                    )

            rows = cursor.fetchall()

        cnx.close()

    results = pd.DataFrame(list(rows), columns=["id", "predictable", "date", "page", "post", "prediction", "result"])
    results = results[results[['result']].notnull().all(1)]

    # Grouping DataFrame by 'type' and calculating Brier score for each group
    brier_scores = results.groupby('page').apply(lambda group: brier_score_loss(group['result'], group['prediction']))
    brier_scores = brier_scores.to_frame()
    brier_scores = brier_scores.rename(columns={0: 'score'})
    brier_scores['page'] = brier_scores.index
    brier_scores.reset_index(drop=True, inplace=True)
    preds = results.groupby('page').count()
    preds['page_id'] = preds.index
    brier_scores = brier_scores.merge(preds[["id", "page_id"]], how='left',
                                      left_on='page', right_on='page_id')
    brier_scores = brier_scores.sort_values(by='score', ascending=True)
    brier_scores = brier_scores[["score", "page", "id"]]
    return brier_scores.to_json(orient='records')

@application.route("/score_post/<string:post_id>")
def get_score_post(post_id):
    cnx = mysql.connector.connect(user = 'doadmin',password = 'AVNS_Lkaktbc2QgJkv-oDi60',
    host = 'db-mysql-nyc3-89566-do-user-8045222-0.c.db.ondigitalocean.com',
      port = 25060,
    database = 'crowdicate')
    if cnx and cnx.is_connected():

        with cnx.cursor() as cursor:

            result = cursor.execute("SELECT * FROM predictions")

            rows = cursor.fetchall()

        cnx.close()

    else:
        return print("Could not connect")
    results = pd.DataFrame(list(rows), columns=["id", "predictable", "date", "page", "post","prediction","result"])
    results["result"] = pd.to_numeric(results["result"])
    results = results[results[['result']].notnull().all(1)]
    model = results[results.post == post_id]
    brier = brier_score_loss(model["result"], model["prediction"])
    return pd.Series(brier).to_json(orient='records')

@application.route("/bet_finder/<string:post_id>")
def bet_finder(post_id):
    # API endpoint and key
    url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"
    api_key = "4ba66e1c5d7028fa3011271f95009abc"
    # API endpoint and key
    url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"
    api_key = "4ba66e1c5d7028fa3011271f95009abc"
    params = {
        'apiKey': api_key,
        'regions': 'us',
        ##'markets': 'h2h,spreads',
        'markets': 'h2h',
        'oddsFormat': 'american',
        'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'commenceTimeTo': (datetime.utcnow() + timedelta(days=1)).replace(hour=6, minute=0).strftime(
            '%Y-%m-%dT%H:%M:%SZ')
    }

    # Fetch the data from the API
    response = requests.get(url, params=params)
    data = response.json()

    # Extract and flatten markets data
    flattened_markets = []

    for entry in data:
        game_info = {
            "id": entry["id"],
            "sport_key": entry["sport_key"],
            "sport_title": entry["sport_title"],
            "commence_time": entry["commence_time"],
            "home_team": entry["home_team"],
            "away_team": entry["away_team"]
        }
        for bookmaker in entry["bookmakers"]:
            bookmaker_info = {
                "bookmaker_key": bookmaker["key"],
                "bookmaker_title": bookmaker["title"]
            }
            for market in bookmaker["markets"]:
                market_info = game_info.copy()
                market_info.update(bookmaker_info)
                market_info.update({
                    "market_key": market["key"],
                })
                for outcome in market["outcomes"]:
                    outcome_info = market_info.copy()
                    if "point" in outcome:
                        outcome_info.update({
                            "outcome_name": outcome["name"],
                            "outcome_price": outcome["price"],
                            "outcome_point": outcome["point"]
                        })
                    else:
                        outcome_info.update({
                            "outcome_name": outcome["name"],
                            "outcome_price": outcome["price"],
                            "outcome_point": 0
                        })
                    flattened_markets.append(outcome_info)

    # Convert flattened markets data into a DataFrame
    df = pd.DataFrame(flattened_markets)

    # max_indices = df.groupby(['outcome_name','outcome_point'])['outcome_price'].idxmax()
    # min_indices = df.groupby(['outcome_name','outcome_point'])['outcome_price'].idxmin()

    # Combine the indices and filter the DataFrame
    # unique_indices = max_indices.append(min_indices).unique()
    # filtered_df = df.loc[unique_indices]

    df['Im_Prob'] = np.where(df['outcome_price'] >= 0, 100 / (100 + df['outcome_price']),
                             -df['outcome_price'] / (-df['outcome_price'] + 100))

    cnx = mysql.connector.connect(user='doadmin', password='AVNS_Lkaktbc2QgJkv-oDi60',
                                  host='db-mysql-nyc3-89566-do-user-8045222-0.c.db.ondigitalocean.com',
                                  port=25060,
                                  database='crowdicate')
    if cnx and cnx.is_connected():
        with cnx.cursor() as cursor:
            result = cursor.execute("SELECT * FROM predictions")

            rows = cursor.fetchall()

            result2 = cursor.execute("SELECT * FROM predictables")

            rows2 = cursor.fetchall()

        cnx.close()

    results = pd.DataFrame(list(rows), columns=["id", "predictable", "date", "page", "post", "prediction", "result"])
    predictions = results[results.post == post_id]
    predictables = pd.DataFrame(list(rows2), columns=["id", "amount", "player", "player_id", "type"])
    predictions = predictions.merge(predictables[["id", "amount", "player", "player_id", "type"]], how='left',
                                    left_on='predictable', right_on='id')
    predictions['odds'] = np.where(predictions['prediction'] >= .50,
                                   -(100 * predictions['prediction']) / (1 - predictions['prediction']),
                                   (100 - (100 * predictions['prediction'])) / predictions['prediction'])
    template = predictions[
        ["player", "player_id", "amount", "type", "date", "prediction", "odds"]]
    template = template[template['type'] == 'MLB - Moneyline']
    template = template.sort_values(["type", 'player', 'amount'], ascending=[True, True, True])
    predictions_live = df.merge(template[["player", "prediction"]], how='left',
                                left_on='outcome_name', right_on='player')
    predictions_live['diff'] = predictions_live['prediction'] - predictions_live['Im_Prob']
    predictions_live = predictions_live[predictions_live['diff'] > 0]
    predictions_live = predictions_live.sort_values(["diff"], ascending=[False])
    return predictions_live.to_json(orient='records')

@application.route("/predict_model/<string:post_id>/<string:page_id>/<string:model_id>")
def predict_model(post_id,page_id,model_id):
    response = requests.get("https://crowdicate.com/api/1.1/obj/models")
    data = response.json()
    results = pd.DataFrame(data["response"]["results"])
    while data["response"]["remaining"] > 0:
        cursor = data["response"]["cursor"] + 100
        response = requests.get(
            "https://crowdicate.com/api/1.1/obj/models" + "?cursor=" + str(
                cursor) + "&limit=100")
        data = response.json()
        test = pd.DataFrame(data["response"]["results"])
        results = pd.concat([results, test])

    results = results[results._id == model_id]
    metrics = results["metrics_list_text"].values[0]
    trees = results["trees_number"].values[0]
    hit_number = results["hit_number"].values[0]
    pitch_number = results["pitch_number"].values[0]

    URL = "https://baseballsavant.mlb.com/probable-pitchers"
    page = requests.get(URL, verify=False)
    soup = BeautifulSoup(page.content, "html.parser")
    links = soup.find_all("a", class_="matchup-link")
    link_list = []
    for link in links:
        test = link["href"]
        splitting = test.split('teamPitching=')
        splitting2 = splitting[1].split('&teamBatting=')
        splitting = splitting2[1].split('&player_id=')
        link_list.append([splitting2[0], splitting[0], splitting[1]])

    df = pd.DataFrame(link_list, columns=['Pitch Team', 'Hit Team', 'Pitcher'])
    df = df.drop_duplicates()

    hit = metrics.copy()
    hit.append("Team")
    pitch = metrics.copy()
    pitch.append("IDfg")

    data = pitching_stats(2024, 2024, qual=5)
    data = data[pitch]
    team_data = team_batting(2024, 2024)
    team_data["K-BB%"] = team_data["K%"] - team_data["BB%"]
    team_data["Inn"] = (team_data['PA'] - team_data['H'] - team_data['BB'] - team_data['HBP']) / 3
    team_data['K/9'] = (team_data['SO'] / team_data['Inn']) * 9
    team_data["BB/9"] = (team_data['BB'] / team_data['Inn']) * 9
    team_data["HR/9"] = (team_data['HR'] / team_data['Inn']) * 9
    team_data = team_data[hit]

    teams = pd.read_csv("teams.csv")
    ids = pd.read_csv("razzball.csv")
    df['Pitch Team'] = df['Pitch Team'].astype(str)
    df['Hit Team'] = df['Hit Team'].astype(str)
    df['Pitcher'] = df['Pitcher'].astype(str)
    ids['MLBAMID'] = ids['MLBAMID'].astype(str)
    ids['FanGraphsID'] = ids['FanGraphsID'].astype(str)
    data['IDfg'] = data['IDfg'].astype(str)


    df = df.merge(ids[["MLBAMID", 'FanGraphsID']], how='left',
                  left_on=['Pitcher'], right_on=['MLBAMID'])

    teams["team_id"] = teams["team_id"].astype(str)

    df = df.merge(teams, how='left',
                  left_on=['Pitch Team'], right_on=['team_id'])
    df = df.merge(teams, how='left',
                  left_on=['Hit Team'], right_on=['team_id'])

    df = df.merge(data, how='left',
                  left_on=['FanGraphsID'], right_on=['IDfg'])
    df = df.merge(team_data, how='left',
                  left_on=['team_abv_y'], right_on=['Team'])

    p_list = [x + "_x" for x in metrics]
    h_list = [x + "_y" for x in metrics]

    for i in range(len(metrics)):
        df[metrics[i]] = (df[p_list[i]] * pitch_number) + (df[h_list[i]] * hit_number)

    games = pd.read_csv("games_data.csv")

    x = games.loc[:,
        metrics].values
    y = games.loc[:, 'R'].values

    regressor = RandomForestRegressor(n_estimators=trees, random_state=0, oob_score=True)

    regressor.fit(x, y)

    df = df.dropna()
    predictions = regressor.predict(df[metrics])
    df["pred"] = predictions

    games = df[["Hit Team", "Pitch Team", "pred"]].merge(df[["Hit Team", "Pitch Team", "pred"]], how='left',
                                                         left_on=['Pitch Team'], right_on=['Hit Team'])

    games["WP"] = ((games["pred_x"] - games["pred_y"]) / 10) + 0.5

    moneyline = games[["Hit Team_x", "WP"]].merge(teams[["team_id", "player_id"]], how='left',
                                                  left_on=['Hit Team_x'], right_on=['team_id'])

    URL = "https://baseballsavant.mlb.com/probable-pitchers"
    page = requests.get(URL, verify=False)
    soup = BeautifulSoup(page.content, "html.parser")
    links = soup.find_all("a", class_="matchup-link")
    link_list = []
    for link in links:
        test = link["href"]
        splitting = test.split('player_id=')
        link_list.append(splitting[1])
    URL = "https://baseballsavant.mlb.com/probable-pitchers"
    page = requests.get(URL, verify=False)
    soup = BeautifulSoup(page.content, "html.parser")
    links = soup.find_all("div", class_="game-info")
    for link in links:
        link_list.append(link.h2.text.strip())
    URL = "https://baseballsavant.mlb.com/probable-pitchers"
    page = requests.get(URL, verify=False)
    soup = BeautifulSoup(page.content, "html.parser")
    links = soup.find_all("div", class_="game-info")
    for link in links:
        test = link.h2.text.strip()
        splitting = test.split(' @ ')
        link_list.append(splitting[0])
        link_list.append(splitting[1])

    cnx = mysql.connector.connect(user='doadmin', password='AVNS_Lkaktbc2QgJkv-oDi60',
                                  host='db-mysql-nyc3-89566-do-user-8045222-0.c.db.ondigitalocean.com',
                                  port=25060,
                                  database='crowdicate')
    if cnx and cnx.is_connected():
        with cnx.cursor() as cursor:
            result = cursor.execute("SELECT * FROM predictables")

            rows = cursor.fetchall()

            results = pd.DataFrame(list(rows), columns=["id", "amount", "player", "player_id", "type"])
            results['date'] = str(datetime.today().strftime("%m/%d/%Y"))
            results['prediction'] = ""
            type_list = ["MLB - Moneyline"]

            results = results[results['type'].isin(type_list)]
            template = results[results['player_id'].isin(link_list) | results['player'].isin(link_list)]
            template = template[
                ["id", "amount", "player_id", "player", "type", "date", "prediction"]]
            template = template.sort_values(["type", 'player', 'amount'], ascending=[True, True, True])

            template = template.merge(moneyline, how="left")
            template["prediction"] = template["WP"]
            template = template[template[['prediction']].notnull().all(1)]
            group = template
            group["predictable"] = group["id"]
            group["id"] = [uuid.uuid4().hex for _ in range(len(group.index))]
            group["post"] = post_id
            group["page"] = page_id
            group = group[["id", "predictable", "date", "page", "post", "prediction"]]
            cursor.executemany("""INSERT INTO predictions
                                      (id,predictable,date,page,post,prediction) 
                                      VALUES (%s,%s,%s,%s,%s,%s);""", list(group.itertuples(index=False, name=None)))
            cnx.commit()

            cnx.close()
        return "success"
    else:
        return "Could not connect"

@application.route("/market_predict/<string:post_id>/<string:page>/<string:type>")
def market_predict(post_id,page,type):
    response = requests.get("https://crowdicate.com/api/1.1/obj/types")
    data = response.json()
    results = pd.DataFrame(data["response"]["results"])
    while data["response"]["remaining"] > 0:
        cursor = data["response"]["cursor"] + 100
        response = requests.get(
            "https://crowdicate.com/api/1.1/obj/types" + "?cursor=" + str(
                cursor) + "&limit=100")
        data = response.json()
        test = pd.DataFrame(data["response"]["results"])
        results = pd.concat([results, test])
    name = results[results._id == type]
    name = name["type_text"].values[0]

    URL = "https://baseballsavant.mlb.com/probable-pitchers"
    page = requests.get(URL, verify=False)
    soup = BeautifulSoup(page.content, "html.parser")
    links = soup.find_all("a", class_="matchup-link")
    link_list = []
    for link in links:
        test = link["href"]
        splitting = test.split('player_id=')
        link_list.append(splitting[1])
    URL = "https://baseballsavant.mlb.com/probable-pitchers"
    page = requests.get(URL, verify=False)
    soup = BeautifulSoup(page.content, "html.parser")
    links = soup.find_all("div", class_="game-info")
    for link in links:
        link_list.append(link.h2.text.strip())
    URL = "https://baseballsavant.mlb.com/probable-pitchers"
    page = requests.get(URL, verify=False)
    soup = BeautifulSoup(page.content, "html.parser")
    links = soup.find_all("div", class_="game-info")
    for link in links:
        test = link.h2.text.strip()
        splitting = test.split(' @ ')
        link_list.append(splitting[0])
        link_list.append(splitting[1])

    # API endpoint and key
    url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"
    api_key = "4ba66e1c5d7028fa3011271f95009abc"
    # API endpoint and key
    url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"
    api_key = "4ba66e1c5d7028fa3011271f95009abc"
    params = {
        'apiKey': api_key,
        'regions': 'us',
        ##'markets': 'h2h,spreads',
        'markets': 'h2h',
        'oddsFormat': 'american',
        'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'commenceTimeTo': (datetime.utcnow() + timedelta(days=1)).replace(hour=6, minute=0).strftime(
            '%Y-%m-%dT%H:%M:%SZ')
    }

    # Fetch the data from the API
    response = requests.get(url, params=params)
    data = response.json()

    # Extract and flatten markets data
    flattened_markets = []

    for entry in data:
        game_info = {
            "id": entry["id"],
            "sport_key": entry["sport_key"],
            "sport_title": entry["sport_title"],
            "commence_time": entry["commence_time"],
            "home_team": entry["home_team"],
            "away_team": entry["away_team"]
        }
        for bookmaker in entry["bookmakers"]:
            bookmaker_info = {
                "bookmaker_key": bookmaker["key"],
                "bookmaker_title": bookmaker["title"]
            }
            for market in bookmaker["markets"]:
                market_info = game_info.copy()
                market_info.update(bookmaker_info)
                market_info.update({
                    "market_key": market["key"],
                })
                for outcome in market["outcomes"]:
                    outcome_info = market_info.copy()
                    if "point" in outcome:
                        outcome_info.update({
                            "outcome_name": outcome["name"],
                            "outcome_price": outcome["price"],
                            "outcome_point": outcome["point"]
                        })
                    else:
                        outcome_info.update({
                            "outcome_name": outcome["name"],
                            "outcome_price": outcome["price"],
                            "outcome_point": 0
                        })
                    flattened_markets.append(outcome_info)

        # Convert flattened markets data into a DataFrame
        df = pd.DataFrame(flattened_markets)

        # max_indices = df.groupby(['outcome_name','outcome_point'])['outcome_price'].idxmax()
        # min_indices = df.groupby(['outcome_name','outcome_point'])['outcome_price'].idxmin()

        # Combine the indices and filter the DataFrame
        # unique_indices = max_indices.append(min_indices).unique()
        # filtered_df = df.loc[unique_indices]

        df['Im_Prob'] = np.where(df['outcome_price'] >= 0, 100 / (100 + df['outcome_price']),
                                 -df['outcome_price'] / (-df['outcome_price'] + 100))

    cnx = mysql.connector.connect(user='doadmin', password='AVNS_Lkaktbc2QgJkv-oDi60',
                                  host='db-mysql-nyc3-89566-do-user-8045222-0.c.db.ondigitalocean.com',
                                  port=25060,
                                  database='crowdicate')
    if cnx and cnx.is_connected():
        with cnx.cursor() as cursor:
            result = cursor.execute("SELECT * FROM predictables")

            rows = cursor.fetchall()

            result_b = cursor.execute("SELECT * FROM predictables")

            types = cursor.fetchall()

            results = pd.DataFrame(list(rows), columns=["id", "amount", "player", "player_id", "type"])
            results['date'] = str(datetime.today().strftime("%m/%d/%Y"))
            results['prediction'] = ""
            type_list = [name]

            results = results[results['type'].isin(type_list)]
            template = results[results['player_id'].isin(link_list) | results['player'].isin(link_list)]
            template = template[
                ["id", "amount", "player_id", "player", "type", "date", "prediction"]]
            template = template.sort_values(["type", 'player', 'amount'], ascending=[True, True, True])

            group = df.groupby(['outcome_name'])['Im_Prob'].agg({'mean'}).reset_index()

            template = template.merge(group, how='left', left_on='player', right_on='outcome_name')
            template["post"] = post_id
            template["page"] = page
            template['prediction'] = template['mean']
            template['predictable'] = template["id"]
            template["id"] = [uuid.uuid4().hex for _ in range(len(template.index))]

            template = template[["id", "predictable", "date", "page", "post", "prediction"]]

            cursor.executemany("""INSERT INTO predictions
                                              (id,predictable,date,page,post,prediction) 
                                              VALUES (%s,%s,%s,%s,%s,%s);""",
                               list(template.itertuples(index=False, name=None)))
            cnx.commit()

        cnx.close()
        return "success"
    else:
        return "Could not connect"


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    application.run()