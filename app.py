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
import scipy
from pytz import timezone
import nfl_data_py as nfl
import itertools

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
    tz = timezone('America/New_York')

    URL = "https://baseballsavant.mlb.com/probable-pitchers"
    page = requests.get(URL, verify=False)
    soup = BeautifulSoup(page.content, "html.parser")
    print(type(page.content))
    tester = soup.find_all("div", class_="mod")
    links = soup.find_all("a", class_="matchup-link")
    now = datetime.now(tz)
    current_time = datetime.strptime(now.strftime("%H:%M:%S"), "%H:%M:%S").time()
    link_list = []
    for mod in tester:
        time_list = mod.find_all("span", class_="time")
        for time in time_list:
            test = time.text.strip()
            splitting = test.split(' ET')
            time_object = datetime.strptime(splitting[0], '%I:%M %p').time()
            if time_object > current_time:
                links = mod.find_all("a", class_="matchup-link")
                for link in links:
                    test = link["href"]
                    splitting = test.split('player_id=')
                    link_list.append(splitting[1])
                links = mod.find_all("div", class_="game-info")
                for link in links:
                    link_list.append(link.h2.text.strip())
                links = mod.find_all("div", class_="game-info")
                for link in links:
                    test = link.h2.text.strip()
                    splitting = test.split(' @ ')
                    link_list.append(splitting[0])
                    link_list.append(splitting[1])

    current_week = 10
    type_list = results_type['type_text'].tolist()

    if any("NFL" in s for s in type_list):
        schedule = pd.read_csv("schedule.csv")
        schedule["game"] = schedule["away_team"] + " @ " + schedule["home_team"]
        schedule = schedule[schedule['week'] == current_week]
        games = schedule["game"].tolist()
        games = games + schedule["home_team"].tolist()
        games = games + schedule["away_team"].tolist()
        link_list = link_list + games

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
    results['date'] = np.where(results["type"].str.contains('NFL'),
                            "Week " + str(current_week),
                            str(datetime.today().strftime("%m/%d/%Y")))
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

@application.route("/leaderboard//<string:type_id>")
@application.route("/leaderboard/<string:days>/<string:type_id>")
def generate_leaderboard(days = None,type_id = None):
    if type_id is None:
        cnx = mysql.connector.connect(user='doadmin', password='AVNS_Lkaktbc2QgJkv-oDi60',
                                      host='db-mysql-nyc3-89566-do-user-8045222-0.c.db.ondigitalocean.com',
                                      port=25060,
                                      database='crowdicate')
        if cnx and cnx.is_connected():
            with cnx.cursor() as cursor:
                result = cursor.execute(
                    "select a.id,a.predictable,a.date,a.page,a.prediction,a.result,t.type from `crowdicate`.`predictions` as a left join `crowdicate`.`predictables` as t on a.predictable = t.id"
                    )

                rows = cursor.fetchall()

            cnx.close()

        results = pd.DataFrame(list(rows),
                               columns=['id', "predictable", "date", "page", "prediction", "result", 'type'])
        results = results[results[['result']].notnull().all(1)]

        # Grouping DataFrame by 'type' and calculating Brier score for each group
        brier_scores = results.groupby('page').apply(
            lambda group: brier_score_loss(group['result'], group['prediction']))
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
    name = results[results._id == type_id]
    name = name["type_text"].values[0]
    cnx = mysql.connector.connect(user='doadmin', password='AVNS_Lkaktbc2QgJkv-oDi60',
                                  host='db-mysql-nyc3-89566-do-user-8045222-0.c.db.ondigitalocean.com',
                                  port=25060,
                                  database='crowdicate')
    if cnx and cnx.is_connected():
        with cnx.cursor() as cursor:
            if days is None:
                result = cursor.execute(
                    "select a.id,a.predictable,a.date,a.page,a.prediction,a.result,t.type from `crowdicate`.`predictions` as a left join `crowdicate`.`predictables` as t on a.predictable = t.id"
                )
            else:
                result = cursor.execute("select a.id,a.predictable,a.date,a.page,a.prediction,a.result,t.type from `crowdicate`.`predictions` as a left join `crowdicate`.`predictables` as t on a.predictable = t.id WHERE STR_TO_DATE(date, '%m/%d/%Y') BETWEEN DATE_SUB(NOW(), INTERVAL "
                + days + " DAY) AND NOW()"
                                    )

            rows = cursor.fetchall()

        cnx.close()

    results = pd.DataFrame(list(rows), columns=['id',"predictable", "date", "page", "prediction", "result",'type'])
    results = results[results[['result']].notnull().all(1)]
    results = results[results.type == name]

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

@application.route("/score_post")
def get_score_post():
    cnx = mysql.connector.connect(user='doadmin', password='AVNS_Lkaktbc2QgJkv-oDi60',
                                  host='db-mysql-nyc3-89566-do-user-8045222-0.c.db.ondigitalocean.com',
                                  port=25060,
                                  database='crowdicate')
    if cnx and cnx.is_connected():
        with cnx.cursor() as cursor:
            result = cursor.execute(
                "select a.id,a.predictable,a.date,a.post,a.prediction,a.result,t.type from `crowdicate`.`predictions` as a left join `crowdicate`.`predictables` as t on a.predictable = t.id"
            )

            rows = cursor.fetchall()

        cnx.close()

    results = pd.DataFrame(list(rows),
                           columns=['id', "predictable", "date", "post", "prediction", "result", 'type'])
    results = results[results[['result']].notnull().all(1)]

    # Grouping DataFrame by 'type' and calculating Brier score for each group
    brier_scores = results.groupby('post').apply(
        lambda group: brier_score_loss(group['result'], group['prediction']))
    brier_scores = brier_scores.to_frame()
    brier_scores = brier_scores.rename(columns={0: 'score'})
    brier_scores['post'] = brier_scores.index
    brier_scores.reset_index(drop=True, inplace=True)
    preds = results.groupby('post').count()
    preds['post_id'] = preds.index
    brier_scores = brier_scores.merge(preds[["id", "post_id"]], how='left',
                                      left_on='post', right_on='post_id')
    brier_scores = brier_scores.sort_values(by='score', ascending=True)
    brier_scores = brier_scores[["score", "post", "id"]]
    return brier_scores.to_json(orient='records')
    return pd.Series(brier).to_json(orient='records')

@application.route("/bet_finder/<string:post_id>")
def bet_finder(post_id):
    # API endpoint and key
    url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"
    api_key = "22a6282c9744177b06acb842d34a02cb"
    # API endpoint and key
    url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"
    api_key = "22a6282c9744177b06acb842d34a02cb"
    params = {
        'apiKey': api_key,
        'regions': 'us,us2',
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
    games['WP'] = np.where(games['WP'] >= 0.9, 0.9,
                             games['WP'])
    games['WP'] = np.where(games['WP'] <= 0.1, 0.1,
                             games['WP'])

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

@application.route("/market_predict/<string:post_id>/<string:page_id>/<string:type>")
def market_predict(post_id,page_id,type):
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
    api_key = "22a6282c9744177b06acb842d34a02cb"
    # API endpoint and key
    url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"
    api_key = "22a6282c9744177b06acb842d34a02cb"
    params = {
        'apiKey': api_key,
        'regions': 'us,us2',
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
            template["page"] = page_id
            template['prediction'] = template['mean']
            template['predictable'] = template["id"]
            template["id"] = [uuid.uuid4().hex for _ in range(len(template.index))]
            template = template[template[['prediction']].notnull().all(1)]

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


@application.route("/predict_model_strikeout/<string:post_id>/<string:page_id>/<string:model_id>")
def predict_model_strikeout(post_id,page_id,model_id):
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
    pitch.append("TBF")

    data = pitching_stats(2024, 2024, qual=0)
    data["TBF"] = data['TBF'] / data['G']
    data = data[pitch]
    team_data = team_batting(2024, 2024)
    team_data["K-BB%"] = team_data["K%"] - team_data["BB%"]
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

    games = pitching_stats(2021, 2023, qual=10)
    build_2 = metrics.copy()
    build_2.append("TBF")
    build_2.append("K%")
    games = games[build_2]
    games = games.replace([np.inf, -np.inf], np.nan).dropna(axis=1)

    build = metrics.copy()
    ##build.append("TBF")
    x = games.loc[:,
        build].values
    y = games.loc[:, 'K%'].values
    regressor = RandomForestRegressor(n_estimators=trees, random_state=0, oob_score=True)

    regressor.fit(x, y)
    df = df.replace([np.inf, -np.inf], np.nan).dropna(axis=0).reset_index()
    predictions = regressor.predict(df[build])
    df["pred"] = predictions
    names = []
    for tree in range(trees):
        vals = regressor.estimators_[tree].predict(df[build])
        df[str(tree)] = pd.Series(vals)
        names.append(str(tree))

    df_sub = df[names]
    df['std'] = df_sub.std(axis=1, ddof=1).multiply(df["TBF"])
    df['pred'] = df['pred'].multiply(df["TBF"], axis="index")

    k35 = pd.DataFrame(scipy.stats.norm(df['pred'], df['std'].multiply(2.5)).sf(3.5))
    k35['val'] = 3.5
    k35['player_id'] = df['MLBAMID']

    k45 = pd.DataFrame(scipy.stats.norm(df['pred'], df['std'].multiply(2.5)).sf(4.5))
    k45['val'] = 4.5
    k45['player_id'] = df['MLBAMID']

    k55 = pd.DataFrame(scipy.stats.norm(df['pred'], df['std'].multiply(2.5)).sf(5.5))
    k55['val'] = 5.5
    k55['player_id'] = df['MLBAMID']

    k65 = pd.DataFrame(scipy.stats.norm(df['pred'], df['std'].multiply(2.5)).sf(6.5))
    k65['val'] = 6.5
    k65['player_id'] = df['MLBAMID']

    k75 = pd.DataFrame(scipy.stats.norm(df['pred'], df['std'].multiply(2.5)).sf(7.5))
    k75['val'] = 7.5
    k75['player_id'] = df['MLBAMID']

    k85 = pd.DataFrame(scipy.stats.norm(df['pred'], df['std'].multiply(2.5)).sf(8.5))
    k85['val'] = 8.5
    k85['player_id'] = df['MLBAMID']

    k95 = pd.DataFrame(scipy.stats.norm(df['pred'], df['std'].multiply(2.5)).sf(9.5))
    k95['val'] = 9.5
    k95['player_id'] = df['MLBAMID']

    all_ks = pd.concat([k35, k45, k55, k65, k75, k85, k95])
    all_ks['prob'] = all_ks[0]

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
            type_list = ["MLB - Strikeouts"]

            results = results[results['type'].isin(type_list)]
            template = results[results['player_id'].isin(link_list) | results['player'].isin(link_list)]
            template = template[
                ["id", "amount", "player_id", "player", "type", "date", "prediction"]]
            template = template.sort_values(["type", 'player', 'amount'], ascending=[True, True, True])

            template = template.merge(all_ks, how="left",left_on=['player_id','amount'], right_on=['player_id','val'])
            template["prediction"] = template["prob"]
            template = template[template[['prediction']].notnull().all(1)]
            group = template
            group["predictable"] = group["id"]
            group["id"] = [uuid.uuid4().hex for _ in range(len(group.index))]
            group["post"] = post_id
            group["page"] = page_id
            group = group[["id", "predictable", "date", "page", "post", "prediction"]]
            cursor.executemany("""INSERT INTO predictions
                                                  (id,predictable,date,page,post,prediction) 
                                                  VALUES (%s,%s,%s,%s,%s,%s);""",
                               list(group.itertuples(index=False, name=None)))
            cnx.commit()

            cnx.close()
        return "success"
    else:
            return "Could not connect"

@application.route("/bet_finder_strikeouts/<string:post_id>")
def bet_finder_strikeouts(post_id):
    url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/events/"
    api_key = "22a6282c9744177b06acb842d34a02cb"
    params = {
        'apiKey': api_key,
        'regions': 'us,us2',
        ##'markets': 'h2h,spreads',
        ##'markets': 'pitcher_strikeouts_alternate,batter_total_bases',
        'markets': 'pitcher_strikeouts,pitcher_strikeouts_alternate',
        'oddsFormat': 'american',
        'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'commenceTimeTo': (datetime.utcnow() + timedelta(days=2)).replace(hour=6, minute=0).strftime(
            '%Y-%m-%dT%H:%M:%SZ')
    }
    response = requests.get(url, params=params)
    data = response.json()

    events = []

    for event in data:
        events.append(event['id'])

    flattened_markets = []

    for event in events:
        url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/events/" + event + "/odds"
        api_key = "22a6282c9744177b06acb842d34a02cb"
        params = {
            'apiKey': api_key,
            'regions': 'us,us2',
            ##'markets': 'h2h,spreads',
            'markets': 'pitcher_strikeouts,pitcher_strikeouts_alternate',
            'oddsFormat': 'american',
            'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            'commenceTimeTo': (datetime.utcnow() + timedelta(days=2)).replace(hour=6, minute=0).strftime(
                '%Y-%m-%dT%H:%M:%SZ')
        }
        # x = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

        # x = (datetime.utcnow() + timedelta(days=1)).replace(hour=6, minute=0).strftime('%Y-%m-%dT%H:%M:%SZ')
        # Fetch the data from the API
        response = requests.get(url, params=params)
        data = response.json()

        # Extract and flatten markets data

        for entry in data:
            game_info = {
                "id": data["id"],
                "sport_key": data["sport_key"],
                "sport_title": data["sport_title"],
                "commence_time": data["commence_time"],
                "home_team": data["home_team"],
                "away_team": data["away_team"]
            }
            for bookmaker in data["bookmakers"]:
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
                                "player_name": outcome["description"],
                                "outcome_name": outcome["name"],
                                "outcome_price": outcome["price"],
                                "outcome_point": outcome["point"]
                            })
                        else:
                            outcome_info.update({
                                "player_name": outcome["description"],
                                "outcome_name": outcome["name"],
                                "outcome_price": outcome["price"],
                                "outcome_point": 0
                            })
                        flattened_markets.append(outcome_info)

    # Convert flattened markets data into a DataFrame
    df = pd.DataFrame(flattened_markets)

    #max_indices = df.groupby(["player_name", 'outcome_name', 'outcome_point'])['outcome_price'].idxmax()
    #min_indices = df.groupby(["player_name", 'outcome_name', 'outcome_point'])['outcome_price'].idxmin()

    # Combine the indices and filter the DataFrame
    #unique_indices = max_indices.append(min_indices).unique()
    #df = df.loc[unique_indices]

    df['Im_Prob'] = np.where(df['outcome_price'] >= 0, 100 / (100 + df['outcome_price']),
                             -df['outcome_price'] / (-df['outcome_price'] + 100))

    df = df[df['outcome_name'] == 'Over']

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
    template = template[template['type'] == 'MLB - Strikeouts']
    template = template.sort_values(["type", 'player', 'amount'], ascending=[True, True, True])
    predictions_live = df.merge(template[["player", "amount", "prediction"]], how='left',
                                left_on=['player_name', 'outcome_point'], right_on=['player', "amount"])
    predictions_live['diff'] = predictions_live['prediction'] - predictions_live['Im_Prob']
    predictions_live = predictions_live[predictions_live['diff'] > 0]
    predictions_live = predictions_live.sort_values(["diff"], ascending=[False])

    predictions_live = predictions_live.drop_duplicates()
    table = pd.pivot_table(predictions_live, values=['outcome_price', "diff"], index=['player_name', 'amount'],
                           columns=['bookmaker_title'], aggfunc="mean")

    predictions_live = predictions_live.sort_values(["diff"], ascending=[False])
    return predictions_live.to_json(orient='records')

@application.route("/test_bet_finder/<string:market>/<string:alt>/")
@application.route("/test_bet_finder/<string:market>/<string:alt>/<string:books>")
def test_bet(market,alt,books = None):
    current_week = 10
    if market == "strikeouts":
        # API endpoint and key
        url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/events/"
        api_key = "22a6282c9744177b06acb842d34a02cb"
        if books is not None:
            if alt == 'y':
                params = {
                'apiKey': api_key,
                'regions': 'us',
                ##'markets': 'h2h,spreads',
                ##'markets': 'pitcher_strikeouts_alternate,batter_total_bases',
                'bookmakers': books.replace("-",","),
                'markets': 'pitcher_strikeouts,pitcher_strikeouts_alternate',
                'oddsFormat': 'american',
                'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                'commenceTimeTo': (datetime.utcnow() + timedelta(days=1)).replace(hour=6, minute=0).strftime(
                    '%Y-%m-%dT%H:%M:%SZ')
                }
            else:
                params = {
                'apiKey': api_key,
                'regions': 'us',
                ##'markets': 'h2h,spreads',
                ##'markets': 'pitcher_strikeouts_alternate,batter_total_bases',
                'bookmakers': books.replace("-",","),
                'markets': 'pitcher_strikeouts',
                'oddsFormat': 'american',
                'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                'commenceTimeTo': (datetime.utcnow() + timedelta(days=1)).replace(hour=6, minute=0).strftime(
                    '%Y-%m-%dT%H:%M:%SZ')
                }
        else:
            if alt == 'y':
                params = {
                'apiKey': api_key,
                'regions': 'us',
                ##'markets': 'h2h,spreads',
                ##'markets': 'pitcher_strikeouts_alternate,batter_total_bases',
                'markets': 'pitcher_strikeouts,pitcher_strikeouts_alternate',
                'oddsFormat': 'american',
                'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                'commenceTimeTo': (datetime.utcnow() + timedelta(days=1)).replace(hour=6, minute=0).strftime(
                    '%Y-%m-%dT%H:%M:%SZ')
                }
            else:
                params = {
                'apiKey': api_key,
                'regions': 'us',
                ##'markets': 'h2h,spreads',
                ##'markets': 'pitcher_strikeouts_alternate,batter_total_bases',
                'markets': 'pitcher_strikeouts',
                'oddsFormat': 'american',
                'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                'commenceTimeTo': (datetime.utcnow() + timedelta(days=1)).replace(hour=6, minute=0).strftime(
                    '%Y-%m-%dT%H:%M:%SZ')
                }
        response = requests.get(url, params=params)
        data = response.json()

        events = []

        for event in data:
            events.append(event['id'])

        flattened_markets = []

        for event in events:
            url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/events/" + event + "/odds"
            api_key = "22a6282c9744177b06acb842d34a02cb"
            if books is not None:
                if alt == 'y':
                    params = {
                    'apiKey': api_key,
                    'regions': 'us',
                    ##'markets': 'h2h,spreads',
                    ##'markets': 'pitcher_strikeouts_alternate,batter_total_bases',
                    'bookmakers': books.replace("-", ","),
                    'markets': 'pitcher_strikeouts,pitcher_strikeouts_alternate',
                    'oddsFormat': 'american',
                    'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'commenceTimeTo': (datetime.utcnow() + timedelta(days=1)).replace(hour=0, minute=0).strftime(
                        '%Y-%m-%dT%H:%M:%SZ')
                    }
                else:
                    params = {
                    'apiKey': api_key,
                    'regions': 'us',
                    ##'markets': 'h2h,spreads',
                    ##'markets': 'pitcher_strikeouts_alternate,batter_total_bases',
                    'bookmakers': books.replace("-", ","),
                    'markets': 'pitcher_strikeouts',
                    'oddsFormat': 'american',
                    'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'commenceTimeTo': (datetime.utcnow() + timedelta(days=1)).replace(hour=0, minute=0).strftime(
                        '%Y-%m-%dT%H:%M:%SZ')
                    }
            else:
                if alt == 'y':
                    params = {
                    'apiKey': api_key,
                    'regions': 'us',
                    ##'markets': 'h2h,spreads',
                    ##'markets': 'pitcher_strikeouts_alternate,batter_total_bases',
                    'markets': 'pitcher_strikeouts,pitcher_strikeouts_alternate',
                    'oddsFormat': 'american',
                    'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'commenceTimeTo': (datetime.utcnow() + timedelta(days=1)).replace(hour=0, minute=0).strftime(
                        '%Y-%m-%dT%H:%M:%SZ')
                    }
                else:
                    params = {
                    'apiKey': api_key,
                    'regions': 'us',
                    ##'markets': 'h2h,spreads',
                    ##'markets': 'pitcher_strikeouts_alternate,batter_total_bases',
                    'markets': 'pitcher_strikeouts',
                    'oddsFormat': 'american',
                    'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'commenceTimeTo': (datetime.utcnow() + timedelta(days=1)).replace(hour=0, minute=0).strftime(
                        '%Y-%m-%dT%H:%M:%SZ')
                    }
            # x = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

            # x = (datetime.utcnow() + timedelta(days=1)).replace(hour=6, minute=0).strftime('%Y-%m-%dT%H:%M:%SZ')
            # Fetch the data from the API
            response = requests.get(url, params=params)
            data = response.json()

            # Extract and flatten markets data

            for entry in data:
                game_info = {
                    "id": data["id"],
                    "sport_key": data["sport_key"],
                    "sport_title": data["sport_title"],
                    "commence_time": data["commence_time"],
                    "home_team": data["home_team"],
                    "away_team": data["away_team"]
                }
                for bookmaker in data["bookmakers"]:
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
                                    "player_name": outcome["description"],
                                    "outcome_name": outcome["name"],
                                    "outcome_price": outcome["price"],
                                    "outcome_point": outcome["point"]
                                })
                            else:
                                outcome_info.update({
                                    "player_name": outcome["description"],
                                    "outcome_name": outcome["name"],
                                    "outcome_price": outcome["price"],
                                    "outcome_point": 0
                                })
                            flattened_markets.append(outcome_info)

        # Convert flattened markets data into a DataFrame
        df = pd.DataFrame(flattened_markets)
        max_indices = df.groupby(['outcome_name', 'outcome_point', 'player_name'])['outcome_price'].idxmax()
        filtered_df = df.loc[max_indices]
        filtered_df['Im_Prob'] = np.where(filtered_df['outcome_price'] >= 0, 100 / (100 + filtered_df['outcome_price']),
                                          -filtered_df['outcome_price'] / (-filtered_df['outcome_price'] + 100))

        filtered_df = filtered_df[filtered_df['outcome_name'] == 'Over']

        cnx = mysql.connector.connect(user='doadmin', password='AVNS_Lkaktbc2QgJkv-oDi60',
                                      host='db-mysql-nyc3-89566-do-user-8045222-0.c.db.ondigitalocean.com',
                                      port=25060,
                                      database='crowdicate')
        if cnx and cnx.is_connected():
            with cnx.cursor() as cursor:
                cursor.execute("SET time_zone = 'EST';")
                result = cursor.execute(
                    "SELECT s.predictable,s.date,s.page,s.prediction,t.id,t.type, t.amount,t.player,t.player_id FROM crowdicate.predictions as s left join crowdicate.predictables as t on s.predictable = t.id WHERE STR_TO_DATE(s.date, '%m/%d/%Y') = CURDATE()"
                )

                rows = cursor.fetchall()

            cnx.close()

        results = pd.DataFrame(list(rows),
                               columns=["predictable", "date", "page", "prediction", 'id', 'type', 'amount', 'player',
                                        'player_id'])

        predictions_live = filtered_df.merge(results[["player", "amount", "prediction", "page"]], how='left',
                                             left_on=['player_name', 'outcome_point'], right_on=['player', "amount"])
        #predictions_live['diff'] = (((predictions_live['prediction'] - predictions_live['Im_Prob']) / predictions_live[
        #    'Im_Prob']) * 100).round(1)

        predictions_live['diff'] = ((((1/predictions_live['Im_Prob'])-1) * predictions_live['prediction'])+((1-(predictions_live['prediction']))*-1)).round(4)

        predictions_live = predictions_live.dropna(subset=['prediction', 'page', 'diff'])
        predictions_live = predictions_live.sort_values(["player_name", 'outcome_point'], ascending=[True, True])

        # Group the data
        grouped = predictions_live.groupby(['player_name', 'bookmaker_title', 'outcome_price', 'outcome_point'])

        # Initialize the list to hold the final JSON structure
        bets = []

        # Iterate through the groups and construct the JSON structure
        for (player_name, bookmaker_title, outcome_price, outcome_point), group in grouped:
            predictions = []
            for _, row in group.iterrows():
                predictions.append({
                    "page": row['page'],
                    "diff": str(row['diff'])
                })
            bet = {
                "player_name": player_name,
                "bookmaker_title": bookmaker_title,
                "outcome_price": str(outcome_price),
                "outcome_point": str(outcome_point),
                "predictions": predictions
            }
            bets.append(bet)

        # Create the final JSON structure
        final_json = {
            "bets": bets
        }

        return json.dumps(final_json, indent=2)
    elif market == "moneyline":
        url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"
        api_key = "22a6282c9744177b06acb842d34a02cb"
        # API endpoint and key
        url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"
        api_key = "22a6282c9744177b06acb842d34a02cb"
        if books is not None:
            params = {
                'apiKey': api_key,
                'regions': 'us,us2',
                ##'markets': 'h2h,spreads',
                'bookmakers': books.replace("-",","),
                'markets': 'h2h',
                'oddsFormat': 'american',
                'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                'commenceTimeTo': (datetime.utcnow() + timedelta(days=1)).replace(hour=6, minute=0).strftime(
                    '%Y-%m-%dT%H:%M:%SZ')
            }
        else:
            params = {
                'apiKey': api_key,
                'regions': 'us,us2',
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

        max_indices = df.groupby(['outcome_name','outcome_point'])['outcome_price'].idxmax()
        # min_indices = df.groupby(['outcome_name','outcome_point'])['outcome_price'].idxmin()

        # Combine the indices and filter the DataFrame
        # unique_indices = max_indices.append(min_indices).unique()
        filtered_df = df.loc[max_indices]

        filtered_df['Im_Prob'] = np.where(filtered_df['outcome_price'] >= 0, 100 / (100 + filtered_df['outcome_price']),
                                 -filtered_df['outcome_price'] / (-filtered_df['outcome_price'] + 100))
        cnx = mysql.connector.connect(user='doadmin', password='AVNS_Lkaktbc2QgJkv-oDi60',
                                      host='db-mysql-nyc3-89566-do-user-8045222-0.c.db.ondigitalocean.com',
                                      port=25060,
                                      database='crowdicate')
        if cnx and cnx.is_connected():
            with cnx.cursor() as cursor:
                cursor.execute("SET time_zone = 'EST';")
                result = cursor.execute(
                    "SELECT s.predictable,s.date,s.page,s.prediction,t.id,t.type, t.amount,t.player,t.player_id FROM crowdicate.predictions as s left join crowdicate.predictables as t on s.predictable = t.id WHERE STR_TO_DATE(s.date, '%m/%d/%Y') = CURDATE()"
                )

                rows = cursor.fetchall()

            cnx.close()

        results = pd.DataFrame(list(rows),
                               columns=["predictable", "date", "page", "prediction", 'id', 'type', 'amount', 'player',
                                        'player_id'])

        predictions_live = filtered_df.merge(results[["player", "amount", "prediction", "page"]], how='left',
                                             left_on=['outcome_name'], right_on=['player'])
        # predictions_live['diff'] = (((predictions_live['prediction'] - predictions_live['Im_Prob']) / predictions_live[
        #    'Im_Prob']) * 100).round(1)

        predictions_live['diff'] = ((((1/predictions_live['Im_Prob'])-1) * predictions_live['prediction'])+((1-(predictions_live['prediction']))*-1)).round(4)


        predictions_live["outcome_point"] = ""

        predictions_live = predictions_live.dropna(subset=['prediction', 'page', 'diff'])
        predictions_live = predictions_live.sort_values(["outcome_name", 'outcome_point'], ascending=[True, True])

        # Group the data
        grouped = predictions_live.groupby(['outcome_name', 'bookmaker_title', 'outcome_price', 'outcome_point'])

        # Initialize the list to hold the final JSON structure
        bets = []

        # Iterate through the groups and construct the JSON structure
        for (outcome_name, bookmaker_title, outcome_price, outcome_point), group in grouped:
            predictions = []
            for _, row in group.iterrows():
                predictions.append({
                    "page": row['page'],
                    "diff": str(row['diff'])
                })
            bet = {
                "player_name": outcome_name,
                "bookmaker_title": bookmaker_title,
                "outcome_price": str(outcome_price),
                "outcome_point": str(outcome_point),
                "predictions": predictions
            }
            bets.append(bet)

        # Create the final JSON structure
        final_json = {
            "bets": bets
        }

        return json.dumps(final_json, indent=2)
    elif market == "nfl_moneyline":
        url = "https://api.the-odds-api.com/v4/sports/americanfootball_nfl/odds"
        api_key = "22a6282c9744177b06acb842d34a02cb"
        # API endpoint and key
        url = "https://api.the-odds-api.com/v4/sports/americanfootball_nfl/odds"
        api_key = "22a6282c9744177b06acb842d34a02cb"
        if books is not None:
            params = {
                'apiKey': api_key,
                'regions': 'us,us2',
                ##'markets': 'h2h,spreads',
                'bookmakers': books.replace("-",","),
                'markets': 'h2h',
                'oddsFormat': 'american',
                'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                'commenceTimeTo': (datetime.utcnow() + timedelta(days=3)).replace(hour=6, minute=0).strftime(
                    '%Y-%m-%dT%H:%M:%SZ')
            }
        else:
            params = {
                'apiKey': api_key,
                'regions': 'us,us2',
                ##'markets': 'h2h,spreads',
                'markets': 'h2h',
                'oddsFormat': 'american',
                'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                'commenceTimeTo': (datetime.utcnow() + timedelta(days=3)).replace(hour=6, minute=0).strftime(
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

        max_indices = df.groupby(['outcome_name','outcome_point'])['outcome_price'].idxmax()
        # min_indices = df.groupby(['outcome_name','outcome_point'])['outcome_price'].idxmin()

        # Combine the indices and filter the DataFrame
        # unique_indices = max_indices.append(min_indices).unique()
        filtered_df = df.loc[max_indices]

        filtered_df['Im_Prob'] = np.where(filtered_df['outcome_price'] >= 0, 100 / (100 + filtered_df['outcome_price']),
                                 -filtered_df['outcome_price'] / (-filtered_df['outcome_price'] + 100))
        cnx = mysql.connector.connect(user='doadmin', password='AVNS_Lkaktbc2QgJkv-oDi60',
                                      host='db-mysql-nyc3-89566-do-user-8045222-0.c.db.ondigitalocean.com',
                                      port=25060,
                                      database='crowdicate')
        if cnx and cnx.is_connected():
            with cnx.cursor() as cursor:
                cursor.execute("SET time_zone = 'EST';")
                result = cursor.execute(
                    "SELECT s.predictable,s.date,s.page,s.prediction,t.id,t.type, t.amount,t.player,t.player_id FROM crowdicate.predictions as s left join crowdicate.predictables as t on s.predictable = t.id WHERE s.date = 'Week " + str(current_week) + "'"
                )

                rows = cursor.fetchall()

            cnx.close()

        results = pd.DataFrame(list(rows),
                               columns=["predictable", "date", "page", "prediction", 'id', 'type', 'amount', 'player',
                                        'player_id'])
        results = results[results.type == 'NFL - Moneyline']

        predictions_live = filtered_df.merge(results[["player", "amount", "prediction", "page"]], how='left',
                                             left_on=['outcome_name'], right_on=['player'])
        # predictions_live['diff'] = (((predictions_live['prediction'] - predictions_live['Im_Prob']) / predictions_live[
        #    'Im_Prob']) * 100).round(1)

        predictions_live['diff'] = ((((1/predictions_live['Im_Prob'])-1) * predictions_live['prediction'])+((1-(predictions_live['prediction']))*-1)).round(4)


        predictions_live["outcome_point"] = ""

        predictions_live = predictions_live.dropna(subset=['prediction', 'page', 'diff'])
        predictions_live = predictions_live.sort_values(["outcome_name", 'outcome_point'], ascending=[True, True])

        # Group the data
        grouped = predictions_live.groupby(['outcome_name', 'bookmaker_title', 'outcome_price', 'outcome_point'])

        # Initialize the list to hold the final JSON structure
        bets = []

        # Iterate through the groups and construct the JSON structure
        for (outcome_name, bookmaker_title, outcome_price, outcome_point), group in grouped:
            predictions = []
            for _, row in group.iterrows():
                predictions.append({
                    "page": row['page'],
                    "diff": str(row['diff'])
                })
            bet = {
                "player_name": outcome_name,
                "bookmaker_title": bookmaker_title,
                "outcome_price": str(outcome_price),
                "outcome_point": str(outcome_point),
                "predictions": predictions
            }
            bets.append(bet)

        # Create the final JSON structure
        final_json = {
            "bets": bets
        }

        return json.dumps(final_json, indent=2)
    elif market == "nfl_spread":
        url = "https://api.the-odds-api.com/v4/sports/americanfootball_nfl/odds"
        api_key = "22a6282c9744177b06acb842d34a02cb"
        # API endpoint and key
        url = "https://api.the-odds-api.com/v4/sports/americanfootball_nfl/odds"
        api_key = "22a6282c9744177b06acb842d34a02cb"
        if books is not None:
            params = {
                'apiKey': api_key,
                'regions': 'us,us2',
                ##'markets': 'h2h,spreads',
                'bookmakers': books.replace("-",","),
                'markets': 'spreads',
                'oddsFormat': 'american',
                'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                'commenceTimeTo': (datetime.utcnow() + timedelta(days=3)).replace(hour=6, minute=0).strftime(
                    '%Y-%m-%dT%H:%M:%SZ')
            }
        else:
            params = {
                'apiKey': api_key,
                'regions': 'us,us2',
                ##'markets': 'h2h,spreads',
                'markets': 'spreads',
                'oddsFormat': 'american',
                'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                'commenceTimeTo': (datetime.utcnow() + timedelta(days=3)).replace(hour=6, minute=0).strftime(
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

        max_indices = df.groupby(['outcome_name','outcome_point'])['outcome_price'].idxmax()
        # min_indices = df.groupby(['outcome_name','outcome_point'])['outcome_price'].idxmin()

        # Combine the indices and filter the DataFrame
        # unique_indices = max_indices.append(min_indices).unique()
        filtered_df = df.loc[max_indices]

        filtered_df['Im_Prob'] = np.where(filtered_df['outcome_price'] >= 0, 100 / (100 + filtered_df['outcome_price']),
                                 -filtered_df['outcome_price'] / (-filtered_df['outcome_price'] + 100))
        cnx = mysql.connector.connect(user='doadmin', password='AVNS_Lkaktbc2QgJkv-oDi60',
                                      host='db-mysql-nyc3-89566-do-user-8045222-0.c.db.ondigitalocean.com',
                                      port=25060,
                                      database='crowdicate')
        if cnx and cnx.is_connected():
            with cnx.cursor() as cursor:
                cursor.execute("SET time_zone = 'EST';")
                result = cursor.execute(
                    "SELECT s.predictable,s.date,s.page,s.prediction,t.id,t.type, t.amount,t.player,t.player_id FROM crowdicate.predictions as s left join crowdicate.predictables as t on s.predictable = t.id WHERE s.date = 'Week " + str(current_week) + "'"
                )

                rows = cursor.fetchall()

            cnx.close()

        results = pd.DataFrame(list(rows),
                               columns=["predictable", "date", "page", "prediction", 'id', 'type', 'amount', 'player',
                                        'player_id'])

        results = results[results.type == 'NFL - Spread']

        predictions_live = filtered_df.merge(results[["player", "amount", "prediction", "page"]], how='left',
                                             left_on=['outcome_name',"outcome_point"], right_on=['player',"amount"])
        # predictions_live['diff'] = (((predictions_live['prediction'] - predictions_live['Im_Prob']) / predictions_live[
        #    'Im_Prob']) * 100).round(1)

        predictions_live['diff'] = ((((1/predictions_live['Im_Prob'])-1) * predictions_live['prediction'])+((1-(predictions_live['prediction']))*-1)).round(4)


        predictions_live = predictions_live.dropna(subset=['prediction', 'page', 'diff'])
        predictions_live = predictions_live.sort_values(["outcome_name", 'outcome_point'], ascending=[True, True])

        # Group the data
        grouped = predictions_live.groupby(['outcome_name', 'bookmaker_title', 'outcome_price', 'outcome_point'])

        # Initialize the list to hold the final JSON structure
        bets = []

        # Iterate through the groups and construct the JSON structure
        for (outcome_name, bookmaker_title, outcome_price, outcome_point), group in grouped:
            predictions = []
            for _, row in group.iterrows():
                predictions.append({
                    "page": row['page'],
                    "diff": str(row['diff'])
                })
            bet = {
                "player_name": outcome_name,
                "bookmaker_title": bookmaker_title,
                "outcome_price": str(outcome_price),
                "outcome_point": str(outcome_point),
                "predictions": predictions
            }
            bets.append(bet)

        # Create the final JSON structure
        final_json = {
            "bets": bets
        }

        return json.dumps(final_json, indent=2)
    elif market == "nfl_team_totals":
        url = "https://api.the-odds-api.com/v4/sports/americanfootball_nfl/events/"
        api_key = "22a6282c9744177b06acb842d34a02cb"
        if books is not None:
            if alt == 'y':
                params = {
                    'apiKey': api_key,
                    'regions': 'us',
                    ##'markets': 'h2h,spreads',
                    ##'markets': 'pitcher_strikeouts_alternate,batter_total_bases',
                    'bookmakers': books.replace("-", ","),
                    'markets': 'team_totals',
                    'oddsFormat': 'american',
                    'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'commenceTimeTo': (datetime.utcnow() + timedelta(days=3)).replace(hour=6, minute=0).strftime(
                        '%Y-%m-%dT%H:%M:%SZ')
                }
            else:
                params = {
                    'apiKey': api_key,
                    'regions': 'us',
                    ##'markets': 'h2h,spreads',
                    ##'markets': 'pitcher_strikeouts_alternate,batter_total_bases',
                    'bookmakers': books.replace("-", ","),
                    'markets': 'team_totals',
                    'oddsFormat': 'american',
                    'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'commenceTimeTo': (datetime.utcnow() + timedelta(days=3)).replace(hour=6, minute=0).strftime(
                        '%Y-%m-%dT%H:%M:%SZ')
                }
        else:
            if alt == 'y':
                params = {
                    'apiKey': api_key,
                    'regions': 'us',
                    ##'markets': 'h2h,spreads',
                    ##'markets': 'pitcher_strikeouts_alternate,batter_total_bases',
                    'markets': 'team_totals',
                    'oddsFormat': 'american',
                    'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'commenceTimeTo': (datetime.utcnow() + timedelta(days=3)).replace(hour=6, minute=0).strftime(
                        '%Y-%m-%dT%H:%M:%SZ')
                }
            else:
                params = {
                    'apiKey': api_key,
                    'regions': 'us',
                    ##'markets': 'h2h,spreads',
                    ##'markets': 'pitcher_strikeouts_alternate,batter_total_bases',
                    'markets': 'team_totals',
                    'oddsFormat': 'american',
                    'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'commenceTimeTo': (datetime.utcnow() + timedelta(days=3)).replace(hour=6, minute=0).strftime(
                        '%Y-%m-%dT%H:%M:%SZ')
                }
        response = requests.get(url, params=params)
        data = response.json()

        events = []

        for event in data:
            events.append(event['id'])

        flattened_markets = []

        for event in events:
            url = "https://api.the-odds-api.com/v4/sports/americanfootball_nfl/events/" + event + "/odds"
            api_key = "22a6282c9744177b06acb842d34a02cb"
            if books is not None:
                if alt == 'y':
                    params = {
                        'apiKey': api_key,
                        'regions': 'us',
                        ##'markets': 'h2h,spreads',
                        ##'markets': 'pitcher_strikeouts_alternate,batter_total_bases',
                        'bookmakers': books.replace("-", ","),
                        'markets': 'team_totals',
                        'oddsFormat': 'american',
                        'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                        'commenceTimeTo': (datetime.utcnow() + timedelta(days=3)).replace(hour=6, minute=0).strftime(
                            '%Y-%m-%dT%H:%M:%SZ')
                    }
                else:
                    params = {
                        'apiKey': api_key,
                        'regions': 'us',
                        ##'markets': 'h2h,spreads',
                        ##'markets': 'pitcher_strikeouts_alternate,batter_total_bases',
                        'bookmakers': books.replace("-", ","),
                        'markets': 'team_totals',
                        'oddsFormat': 'american',
                        'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                        'commenceTimeTo': (datetime.utcnow() + timedelta(days=3)).replace(hour=6, minute=0).strftime(
                            '%Y-%m-%dT%H:%M:%SZ')
                    }
            else:
                if alt == 'y':
                    params = {
                        'apiKey': api_key,
                        'regions': 'us',
                        ##'markets': 'h2h,spreads',
                        ##'markets': 'pitcher_strikeouts_alternate,batter_total_bases',
                        'markets': 'team_totals',
                        'oddsFormat': 'american',
                        'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                        'commenceTimeTo': (datetime.utcnow() + timedelta(days=3)).replace(hour=6, minute=0).strftime(
                            '%Y-%m-%dT%H:%M:%SZ')
                    }
                else:
                    params = {
                        'apiKey': api_key,
                        'regions': 'us',
                        ##'markets': 'h2h,spreads',
                        ##'markets': 'pitcher_strikeouts_alternate,batter_total_bases',
                        'markets': 'team_totals',
                        'oddsFormat': 'american',
                        'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                        'commenceTimeTo': (datetime.utcnow() + timedelta(days=3)).replace(hour=6, minute=0).strftime(
                            '%Y-%m-%dT%H:%M:%SZ')
                    }
            # x = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

            # x = (datetime.utcnow() + timedelta(days=1)).replace(hour=6, minute=0).strftime('%Y-%m-%dT%H:%M:%SZ')
            # Fetch the data from the API
            response = requests.get(url, params=params)
            data = response.json()

            # Extract and flatten markets data

            for entry in data:
                game_info = {
                    "id": data["id"],
                    "sport_key": data["sport_key"],
                    "sport_title": data["sport_title"],
                    "commence_time": data["commence_time"],
                    "home_team": data["home_team"],
                    "away_team": data["away_team"]
                }
                for bookmaker in data["bookmakers"]:
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
                                    "player_name": outcome["description"],
                                    "outcome_name": outcome["name"],
                                    "outcome_price": outcome["price"],
                                    "outcome_point": outcome["point"]
                                })
                            else:
                                outcome_info.update({
                                    "player_name": outcome["description"],
                                    "outcome_name": outcome["name"],
                                    "outcome_price": outcome["price"],
                                    "outcome_point": 0
                                })
                            flattened_markets.append(outcome_info)

        # Convert flattened markets data into a DataFrame
        df = pd.DataFrame(flattened_markets)
        df = df[df['outcome_name'] == 'Over']

        max_indices = df.groupby(['player_name', 'outcome_point'])['outcome_price'].idxmax()
        # min_indices = df.groupby(['outcome_name','outcome_point'])['outcome_price'].idxmin()

        # Combine the indices and filter the DataFrame
        # unique_indices = max_indices.append(min_indices).unique()
        filtered_df = df.loc[max_indices]

        filtered_df['Im_Prob'] = np.where(filtered_df['outcome_price'] >= 0, 100 / (100 + filtered_df['outcome_price']),
                                          -filtered_df['outcome_price'] / (-filtered_df['outcome_price'] + 100))

        teams = pd.read_csv("nfl_teams.csv")
        cnx = mysql.connector.connect(user='doadmin', password='AVNS_Lkaktbc2QgJkv-oDi60',
                                      host='db-mysql-nyc3-89566-do-user-8045222-0.c.db.ondigitalocean.com',
                                      port=25060,
                                      database='crowdicate')
        if cnx and cnx.is_connected():
            with cnx.cursor() as cursor:
                cursor.execute("SET time_zone = 'EST';")
                result = cursor.execute(
                    "SELECT s.predictable,s.date,s.page,s.prediction,t.id,t.type, t.amount,t.player,t.player_id FROM crowdicate.predictions as s left join crowdicate.predictables as t on s.predictable = t.id WHERE s.date = 'Week " + str(current_week) + "'"
                )

                rows = cursor.fetchall()

            cnx.close()

        results = pd.DataFrame(list(rows),
                               columns=["predictable", "date", "page", "prediction", 'id', 'type', 'amount', 'player',
                                        'player_id'])
        results = results[results.type == 'NFL - Team Totals']

        predictions_live = filtered_df.merge(results[["player", "amount", "prediction", "page"]], how='left',
                                             left_on=['player_name', "outcome_point"], right_on=['player', "amount"])
        # predictions_live['diff'] = (((predictions_live['prediction'] - predictions_live['Im_Prob']) / predictions_live[
        #    'Im_Prob']) * 100).round(1)

        predictions_live['diff'] = ((((1 / predictions_live['Im_Prob']) - 1) * predictions_live['prediction']) + (
                (1 - (predictions_live['prediction'])) * -1)).round(4)

        predictions_live = predictions_live.dropna(subset=['prediction', 'page', 'diff'])
        predictions_live = predictions_live.sort_values(["player_name", 'outcome_point'], ascending=[True, True])

        # Group the data
        grouped = predictions_live.groupby(['player_name', 'bookmaker_title', 'outcome_price', 'outcome_point'])

        # Initialize the list to hold the final JSON structure
        bets = []

        # Iterate through the groups and construct the JSON structure
        for (player_name, bookmaker_title, outcome_price, outcome_point), group in grouped:
            predictions = []
            for _, row in group.iterrows():
                predictions.append({
                    "page": row['page'],
                    "diff": str(row['diff'])
                })
            bet = {
                "player_name": player_name,
                "bookmaker_title": bookmaker_title,
                "outcome_price": str(outcome_price),
                "outcome_point": str(outcome_point),
                "predictions": predictions
            }
            bets.append(bet)

        # Create the final JSON structure
        final_json = {
            "bets": bets
        }

        return json.dumps(final_json, indent=2)
    else:
        url = "https://api.the-odds-api.com/v4/sports/americanfootball_nfl/odds"
        api_key = "22a6282c9744177b06acb842d34a02cb"
        # API endpoint and key
        url = "https://api.the-odds-api.com/v4/sports/americanfootball_nfl/odds"
        api_key = "22a6282c9744177b06acb842d34a02cb"
        if books is not None:
            params = {
                'apiKey': api_key,
                'regions': 'us,us2',
                ##'markets': 'h2h,spreads',
                'bookmakers': books.replace("-", ","),
                'markets': 'totals',
                'oddsFormat': 'american',
                'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                'commenceTimeTo': (datetime.utcnow() + timedelta(days=3)).replace(hour=6, minute=0).strftime(
                    '%Y-%m-%dT%H:%M:%SZ')
            }
        else:
            params = {
                'apiKey': api_key,
                'regions': 'us,us2',
                ##'markets': 'h2h,spreads',
                'markets': 'totals',
                'oddsFormat': 'american',
                'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                'commenceTimeTo': (datetime.utcnow() + timedelta(days=3)).replace(hour=6, minute=0).strftime(
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

        max_indices = df.groupby(['outcome_name', 'outcome_point'])['outcome_price'].idxmax()
        # min_indices = df.groupby(['outcome_name','outcome_point'])['outcome_price'].idxmin()

        # Combine the indices and filter the DataFrame
        # unique_indices = max_indices.append(min_indices).unique()
        filtered_df = df.loc[max_indices]

        filtered_df['Im_Prob'] = np.where(filtered_df['outcome_price'] >= 0, 100 / (100 + filtered_df['outcome_price']),
                                          -filtered_df['outcome_price'] / (-filtered_df['outcome_price'] + 100))

        teams = pd.read_csv("nfl_teams.csv")
        filtered_df = filtered_df.merge(teams, how='left',
                                        left_on=['home_team'], right_on=['team'])
        filtered_df = filtered_df.merge(teams, how='left',
                                        left_on=['away_team'], right_on=['team'])
        filtered_df["game"] = filtered_df["abbreviation_y"] + " @ " + filtered_df["abbreviation_x"]
        filtered_df = filtered_df[filtered_df['outcome_name'] == 'Over']

        cnx = mysql.connector.connect(user='doadmin', password='AVNS_Lkaktbc2QgJkv-oDi60',
                                      host='db-mysql-nyc3-89566-do-user-8045222-0.c.db.ondigitalocean.com',
                                      port=25060,
                                      database='crowdicate')
        if cnx and cnx.is_connected():
            with cnx.cursor() as cursor:
                cursor.execute("SET time_zone = 'EST';")
                result = cursor.execute(
                    "SELECT s.predictable,s.date,s.page,s.prediction,t.id,t.type, t.amount,t.player,t.player_id FROM crowdicate.predictions as s left join crowdicate.predictables as t on s.predictable = t.id WHERE s.date = 'Week " + str(current_week) + "'"
                )

                rows = cursor.fetchall()

            cnx.close()

        results = pd.DataFrame(list(rows),
                               columns=["predictable", "date", "page", "prediction", 'id', 'type', 'amount', 'player',
                                        'player_id'])
        results = results[results.type == 'NFL - Game Totals']

        predictions_live = filtered_df.merge(results[["player", "amount", "prediction", "page"]], how='left',
                                             left_on=['game',"outcome_point"], right_on=['player',"amount"])
        # predictions_live['diff'] = (((predictions_live['prediction'] - predictions_live['Im_Prob']) / predictions_live[
        #    'Im_Prob']) * 100).round(1)

        predictions_live['diff'] = ((((1/predictions_live['Im_Prob'])-1) * predictions_live['prediction'])+((1-(predictions_live['prediction']))*-1)).round(4)



        predictions_live = predictions_live.dropna(subset=['prediction', 'page', 'diff'])
        predictions_live = predictions_live.sort_values(["game", 'outcome_point'], ascending=[True, True])

        # Group the data
        grouped = predictions_live.groupby(['game', 'bookmaker_title', 'outcome_price', 'outcome_point'])

        # Initialize the list to hold the final JSON structure
        bets = []

        # Iterate through the groups and construct the JSON structure
        for (game, bookmaker_title, outcome_price, outcome_point), group in grouped:
            predictions = []
            for _, row in group.iterrows():
                predictions.append({
                    "page": row['page'],
                    "diff": str(row['diff'])
                })
            bet = {
                "player_name": game,
                "bookmaker_title": bookmaker_title,
                "outcome_price": str(outcome_price),
                "outcome_point": str(outcome_point),
                "predictions": predictions
            }
            bets.append(bet)

        # Create the final JSON structure
        final_json = {
            "bets": bets
        }

        return json.dumps(final_json, indent=2)



@application.route("/strikeout_market/<string:post_id>/<string:page_id>/<string:type>")
def strikeout_market(post_id,page_id,type):
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
    url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/events/"
    api_key = "22a6282c9744177b06acb842d34a02cb"
    params = {
        'apiKey': api_key,
        'regions': 'us',
        ##'markets': 'h2h,spreads',
        ##'markets': 'pitcher_strikeouts_alternate,batter_total_bases',
        'markets': 'pitcher_strikeouts_alternate',
        'oddsFormat': 'american',
        'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'commenceTimeTo': (datetime.utcnow() + timedelta(days=1)).replace(hour=6, minute=0).strftime(
            '%Y-%m-%dT%H:%M:%SZ')
    }
    response = requests.get(url, params=params)
    data = response.json()

    events = []

    for event in data:
        events.append(event['id'])

    flattened_markets = []

    for event in events:
        url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/events/" + event + "/odds"
        api_key = "22a6282c9744177b06acb842d34a02cb"
        params = {
            'apiKey': api_key,
            'regions': 'us',
            ##'markets': 'h2h,spreads',
            'markets': 'pitcher_strikeouts,pitcher_strikeouts_alternate',
            'oddsFormat': 'american',
            'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            'commenceTimeTo': (datetime.utcnow() + timedelta(days=1)).replace(hour=6, minute=0).strftime(
                '%Y-%m-%dT%H:%M:%SZ')
        }
        # x = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

        # x = (datetime.utcnow() + timedelta(days=1)).replace(hour=6, minute=0).strftime('%Y-%m-%dT%H:%M:%SZ')
        # Fetch the data from the API
        response = requests.get(url, params=params)
        data = response.json()

        # Extract and flatten markets data

        for entry in data:
            game_info = {
                "id": data["id"],
                "sport_key": data["sport_key"],
                "sport_title": data["sport_title"],
                "commence_time": data["commence_time"],
                "home_team": data["home_team"],
                "away_team": data["away_team"]
            }
            for bookmaker in data["bookmakers"]:
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
                                "player_name": outcome["description"],
                                "outcome_name": outcome["name"],
                                "outcome_price": outcome["price"],
                                "outcome_point": outcome["point"]
                            })
                        else:
                            outcome_info.update({
                                "player_name": outcome["description"],
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

    df = df[df['outcome_name'] == 'Over']

    group = df.groupby(['player_name', 'outcome_point'])['Im_Prob'].agg({'mean'}).reset_index()

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


            template = template.merge(group, how='left', left_on=['player',"amount"], right_on=['player_name',"outcome_point"])
            template["post"] = post_id
            template["page"] = page_id
            template['prediction'] = template['mean']
            template = template[template[['prediction']].notnull().all(1)]
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


@application.route("/delete_predictions/<string:post_id>")
def delete_predictions(post_id):
    cnx = mysql.connector.connect(user='doadmin', password='AVNS_Lkaktbc2QgJkv-oDi60',
                                  host='db-mysql-nyc3-89566-do-user-8045222-0.c.db.ondigitalocean.com',
                                  port=25060,
                                  database='crowdicate')
    if cnx and cnx.is_connected():
        with cnx.cursor() as cursor:
            result = cursor.execute("delete FROM `crowdicate`.`predictions` where post in ('" + post_id + "')")

        cnx.commit()

        cnx.close()
        return "success"
    else:
        return "Could not connect"

@application.route("/predict_single/<string:page_id>/<string:post_id>/<string:predictable_id>/<string:prediction>")
def predict_single(page_id,post_id,predictable_id,prediction):
    cnx = mysql.connector.connect(user='doadmin', password='AVNS_Lkaktbc2QgJkv-oDi60',
                                  host='db-mysql-nyc3-89566-do-user-8045222-0.c.db.ondigitalocean.com',
                                  port=25060,
                                  database='crowdicate')
    if cnx and cnx.is_connected():
        with cnx.cursor() as cursor:
            result = cursor.execute("SELECT * FROM predictables")

            rows = cursor.fetchall()

            results = pd.DataFrame(list(rows), columns=["id", "amount", "player", "player_id", "type"])
            current_week = 10

            results['date'] = np.where(results["type"].str.contains('NFL'),
                                       "Week " + str(current_week),
                                       str(datetime.today().strftime("%m/%d/%Y")))

            results['prediction'] = ""
            template = results[results.id == predictable_id]
            template = template[
                ["id", "amount", "player_id", "player", "type", "date", "prediction"]]
            template = template.sort_values(["type", 'player', 'amount'], ascending=[True, True, True])

            template["post"] = post_id
            template["page"] = page_id
            template['prediction'] = prediction
            template = template[template[['prediction']].notnull().all(1)]
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

@application.route("/predict_model_nfl/<string:post_id>/<string:page_id>/<string:model_id>")
def predict_model_nfl(post_id,page_id,model_id):
    response = requests.get("https://crowdicate.com/api/1.1/obj/models")
    data = response.json()
    results = pd.DataFrame(data["response"]["results"])
    current_week = 10
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
    types = results["type_custom_types"].values[0]

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
    name = results[results._id == types]
    name = name["type_text"].values[0]

    games_data = pd.read_csv("games_data_nfl.csv")
    games_data["temp"] = games_data["temp"].fillna(70)
    games_data["wind"] = games_data["wind"].fillna(0)
    games_data = pd.get_dummies(data=games_data, columns=["stadium_id"])
    spike_cols = [col for col in games_data.columns if 'stadium_id' in col]
    if "stadium_id" in metrics:
        metrics.remove("stadium_id")
        metrics = metrics + spike_cols

    pbp = pd.read_csv("https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_2024.csv")
    passes = pbp[pbp.play_type == 'pass']
    runs = pbp[pbp.play_type == 'run']

    games_count = pbp[['posteam', "game_id"]]
    games_count = games_count.drop_duplicates()
    games_count = games_count.groupby(['posteam'])['posteam'].agg({'count'}).reset_index()
    games_count = games_count.rename(columns={"count": "games"})
    pass_off = passes.groupby(['posteam'])['epa'].agg({'sum', 'count', 'mean'}).reset_index()
    pass_off = pass_off.rename(
        columns={"posteam": "team", "sum": "total_pass_epa", "count": "pass_plays", "mean": "pass_epa"})
    run_off = runs.groupby(['posteam'])['epa'].agg({'sum', 'count', 'mean'}).reset_index()
    run_off = run_off.rename(
        columns={"posteam": "team", "sum": "total_run_epa", "count": "run_plays", "mean": "run_epa"})
    pass_def = passes.groupby(['defteam'])['epa'].agg({'sum', 'count', 'mean'}).reset_index()
    pass_def = pass_def.rename(
        columns={"defteam": "opponent", "sum": "total_pass_epa_defense", "count": "pass_plays_defense",
                 "mean": "pass_epa_defense"})
    run_def = runs.groupby(['defteam'])['epa'].agg({'sum', 'count', 'mean'}).reset_index()
    run_def = run_def.rename(
        columns={"defteam": "opponent", "sum": "total_run_epa_defense", "count": "run_plays_defense",
                 "mean": "run_epa_defense"})
    offense = pass_off.merge(run_off, how='left',
                             left_on='team', right_on='team')
    offense = offense.merge(games_count, how='left',
                            left_on='team', right_on='posteam')
    defense = pass_def.merge(run_def, how='left',
                             left_on='opponent', right_on='opponent')
    defense = defense.merge(games_count, how='left',
                            left_on='opponent', right_on='posteam')

    offense["pass_share"] = offense["pass_plays"] / (offense["pass_plays"] + offense["run_plays"])
    defense["pass_share_defense"] = defense["pass_plays_defense"] / (
                defense["pass_plays_defense"] + defense["run_plays_defense"])
    offense["pace"] = (offense["pass_plays"] + offense["run_plays"]) / offense["games"]
    defense["pace_defense"] = (defense["pass_plays_defense"] + defense["run_plays_defense"]) / defense["games"]

    schedule = nfl.import_schedules([2024])
    schedule = pd.get_dummies(data=schedule, columns=["stadium_id"])
    spike_cols = [col for col in schedule.columns if 'stadium_id' in col]
    week = schedule[schedule.week == current_week].reset_index()

    week["away_spread_line"] = week['spread_line']
    week["home_spread_line"] = week["spread_line"] * -1
    week["away_implied"] = (week['total_line'] / 2) - (week['spread_line'] / 2)
    week["home_implied"] = (week['total_line'] / 2) + (week['spread_line'] / 2)
    away_cols = ["away_team", "home_team", "away_rest", "away_moneyline", "away_spread_line", "home_rest",
                 "away_spread_odds", "total_line", "under_odds", "over_odds", "div_game", "temp", "wind",
                 "away_implied"] + spike_cols
    home_cols = ["home_team", "away_team", "home_rest", "away_rest", "home_moneyline", "home_spread_line",
                 "home_spread_odds", "total_line", "under_odds", "over_odds", "div_game", "temp", "wind",
                 "home_implied"] + spike_cols
    away = week[away_cols].reset_index()
    home = week[home_cols].reset_index()
    away["home"] = 0
    home["home"] = 1
    away = away.rename(
        columns={"away_team": "team", "home_team": "opponent", "away_rest": "rest", "home_rest": "opponent_rest",
                 "away_moneyline": "moneyline", "away_spread_line": "spread_line", "away_spread_odds": "spread_odds",
                 "away_implied": "implied"})
    home = home.rename(
        columns={"home_team": "team", "away_team": "opponent", "home_rest": "rest", "away_rest": "opponent_rest",
                 "home_moneyline": "moneyline", "home_spread_line": "spread_line", "home_spread_odds": "spread_odds",
                 "home_implied": "implied"})

    games = pd.concat([away, home])
    games = games.merge(offense, how='left',
                        left_on='team', right_on='team')
    games = games.merge(defense, how='left',
                        left_on='opponent', right_on='opponent')
    games["temp"] = games["temp"].fillna(70)
    games["wind"] = games["wind"].fillna(0)

    x = games_data.loc[:,
        metrics].values
    y = games_data.loc[:, 'team_score'].values

    regressor = RandomForestRegressor(n_estimators=500, random_state=0, oob_score=True)

    regressor.fit(x, y)

    for col in metrics:
        if col not in games.columns:
            games[col] = 0

    predictions = regressor.predict(games[metrics])
    games["pred"] = predictions
    games_sub = games[["team", "opponent", "pred"]]
    games_sub = games_sub.merge(games_sub, how='left',
                                left_on='opponent', right_on='team')

    games_sub["WP"] = (games_sub["pred_x"] ** 2.5) / (((games_sub["pred_x"] ** 2.5)) + (games_sub["pred_y"] ** 2.5))
    teams = games["team"].tolist()

    names = []
    for tree in range(500):
        vals = regressor.estimators_[tree].predict(games[metrics])
        games[str(tree)] = pd.Series(vals)
        names.append(str(tree))

    spreads_lines = []
    team_totals_lines = []
    game_totals_lines = []
    for num in games.index:
        team = games['team'].loc[games.index[num]]
        team_scores = games[games.team == team].reset_index()
        team_scores = team_scores[names]
        team_scores = team_scores.loc[0, :].values.tolist()
        opp = games['opponent'].loc[games.index[num]]
        opp_scores = games[games.team == opp].reset_index()
        opp_scores = opp_scores[names]
        opp_scores = opp_scores.loc[0, :].values.tolist()
        team_totals = np.arange(15.5, 32.5, 1).tolist()
        team_scores_df = pd.DataFrame(team_scores, columns=["score"])
        for team_total in team_totals:
            win = team_scores_df[team_scores_df.score > team_total].count().score / len(team_scores_df)
            team_win = (team, win, team_total)
            team_totals_lines.append(team_win)
        combine_scores = list(itertools.product(team_scores, opp_scores))
        combine_scores = pd.DataFrame(combine_scores, columns=("team", "opp"))
        combine_scores["difference"] = combine_scores["opp"] - combine_scores["team"]
        combine_scores["combined_total"] = combine_scores["team"] + combine_scores["opp"]
        spreads = np.arange(-10.5, 11.5, 1).tolist()
        for spread in spreads:
            win = combine_scores[combine_scores.difference < spread].count().team / len(combine_scores.index)
            team_win = (team, win, spread)
            spreads_lines.append(team_win)
        game_totals = np.arange(40.5, 52.5, 1).tolist()
        for total in game_totals:
            win = combine_scores[combine_scores.combined_total > total].count().team / len(combine_scores.index)
            team_win = (team + " @ " + opp, win, total)
            game_totals_lines.append(team_win)
    spreads_all = pd.DataFrame(spreads_lines, columns=['team', 'WP', "spread"])
    game_totals_all = pd.DataFrame(game_totals_lines, columns=['game', 'WP', "total"])
    team_totals_all = pd.DataFrame(team_totals_lines, columns=['team', 'WP', "total"])


    schedule = pd.read_csv("schedule.csv")
    schedule["game"] = schedule["away_team"] + " @ " + schedule["home_team"]
    schedule = schedule[schedule['week'] == current_week]
    games = schedule["game"].tolist()
    games = games + schedule["home_team"].tolist()
    games = games + schedule["away_team"].tolist()
    link_list = games

    cnx = mysql.connector.connect(user='doadmin', password='AVNS_Lkaktbc2QgJkv-oDi60',
                                  host='db-mysql-nyc3-89566-do-user-8045222-0.c.db.ondigitalocean.com',
                                  port=25060,
                                  database='crowdicate')
    if cnx and cnx.is_connected():

        with cnx.cursor() as cursor:

            result = cursor.execute("SELECT * FROM predictables")

            rows = cursor.fetchall()


            results = pd.DataFrame(list(rows), columns=["id", "amount", "player", "player_id", "type"])
            results['date'] = "Week " + str(current_week)
            results['prediction'] = ""

            results = results[results['type'] == name]
            template = results[results['player_id'].isin(link_list) | results['player'].isin(link_list)]
            template = template[
            ["id", "amount", "player_id", "player", "type", "date", "prediction"]]

            if name == "NFL - Moneyline":
                template = template.merge(games_sub, how="left", left_on=['player_id'], right_on=['team_x'])
                template["prediction"] = template["WP"]
                template = template[template[['prediction']].notnull().all(1)]
                group = template
                group["predictable"] = group["id"]
                group["id"] = [uuid.uuid4().hex for _ in range(len(group.index))]
                group["page"] = page_id
                group["post"] = post_id
                group = group[["id", "predictable", "date", "page", "post", "prediction"]]
            elif name == "NFL - Spread":
                template = template.merge(spreads_all, how="left", left_on=['player_id', 'amount'],
                                      right_on=['team', 'spread'])
                template["prediction"] = template["WP"]
                template = template[template[['prediction']].notnull().all(1)]
                group = template
                group["predictable"] = group["id"]
                group["id"] = [uuid.uuid4().hex for _ in range(len(group.index))]
                group["page"] = page_id
                group["post"] = post_id
                group = group[["id", "predictable", "date", "page", "post", "prediction"]]
            elif name == "NFL - Team Totals":
                template = template.merge(team_totals_all, how="left", left_on=['player_id', 'amount'],
                                      right_on=['team', 'total'])
                template["prediction"] = template["WP"]
                template = template[template[['prediction']].notnull().all(1)]
                group = template
                group["predictable"] = group["id"]
                group["id"] = [uuid.uuid4().hex for _ in range(len(group.index))]
                group["page"] = page_id
                group["post"] = post_id
                group = group[["id", "predictable", "date", "page", "post", "prediction"]]
            else:
                template = template.merge(game_totals_all, how="left", left_on=['player_id', 'amount'],
                                      right_on=['game', 'total'])
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
                                                  VALUES (%s,%s,%s,%s,%s,%s);""",
                           list(group.itertuples(index=False, name=None)))
            cnx.commit()

            cnx.close()
            return "success"
    else:
        return "Could not connect"

@application.route("/aggregate_nfl/<string:post_id>/<string:page>/<string:type>")
def get_aggregate_nfl(post_id,page,type):
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
    current_week = 10
    if cnx and cnx.is_connected():
        with cnx.cursor() as cursor:
            result = cursor.execute("SELECT * FROM predictions")

            rows = cursor.fetchall()

            result_b = cursor.execute("SELECT * FROM predictables")

            types = cursor.fetchall()

            results = pd.DataFrame(list(rows),columns=["id", "predictable", "date", "page", "post","prediction","result"])
            results = results[results['date'] == "Week " + str(current_week)]
            results['date'] = "Week " + str(current_week)
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

@application.route("/market_nfl/<string:post_id>/<string:page_id>/<string:type_id>")
def get_market_nfl(post_id,page_id,type_id):
    if type_id == 'moneyline':
        url = "https://api.the-odds-api.com/v4/sports/americanfootball_nfl/odds"
        api_key = "22a6282c9744177b06acb842d34a02cb"
        # API endpoint and key
        url = "https://api.the-odds-api.com/v4/sports/americanfootball_nfl/odds"
        api_key = "22a6282c9744177b06acb842d34a02cb"

        params = {
            'apiKey': api_key,
            'regions': 'us,us2',
            ##'markets': 'h2h,spreads',
            'markets': 'h2h',
            'oddsFormat': 'american',
            'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            'commenceTimeTo': (datetime.utcnow() + timedelta(days=6)).replace(hour=6, minute=0).strftime(
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
        df['Im_Prob'] = np.where(df['outcome_price'] >= 0, 100 / (100 + df['outcome_price']),
                                 -df['outcome_price'] / (-df['outcome_price'] + 100))

        group = df.groupby(['outcome_name'])['Im_Prob'].agg({'mean'}).reset_index()

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
                current_week = 10

                results = pd.DataFrame(list(rows), columns=["id", "amount", "player", "player_id", "type"])
                results['date'] = "Week " + str(current_week)
                results['prediction'] = ""
                type_list = ["NFL - Moneyline"]
                schedule = pd.read_csv("schedule.csv")
                schedule["game"] = schedule["away_team"] + " @ " + schedule["home_team"]
                schedule = schedule[schedule['week'] == current_week]
                games = schedule["game"].tolist()
                games = games + schedule["home_team"].tolist()
                games = games + schedule["away_team"].tolist()
                link_list = games
                results = results[results['type'].isin(type_list)]
                template = results[results['player_id'].isin(link_list) | results['player'].isin(link_list)]
                template = template[
                    ["id", "amount", "player_id", "player", "type", "date", "prediction"]]
                template = template.sort_values(["type", 'player'], ascending=[True, True])

                template = template.merge(group, how='left', left_on=['player'],
                                          right_on=['outcome_name'])
                template["post"] = post_id
                template["page"] = page_id
                template['prediction'] = template['mean']
                template = template[template[['prediction']].notnull().all(1)]
                template['predictable'] = template["id"]
                template["id"] = [uuid.uuid4().hex for _ in range(len(template.index))]

                template = template[["id", "predictable", "date", "page", "post", "prediction"]]

                cursor.executemany("""INSERT INTO predictions
                                                      (id,predictable,date,page,post,prediction) 
                                                      VALUES (%s,%s,%s,%s,%s,%s);""",
                                   list(template.itertuples(index=False, name=None)))
                cnx.commit()

            cnx.close()
    elif type_id == "teamttotals":
        url = "https://api.the-odds-api.com/v4/sports/americanfootball_nfl/events/"
        api_key = "22a6282c9744177b06acb842d34a02cb"
        params = {
            'apiKey': api_key,
            'regions': 'us',
            ##'markets': 'h2h,spreads',
            ##'markets': 'pitcher_strikeouts_alternate,batter_total_bases',
            'markets': 'team_totals',
            'oddsFormat': 'american',
            'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            'commenceTimeTo': (datetime.utcnow() + timedelta(days=6)).replace(hour=6, minute=0).strftime(
                '%Y-%m-%dT%H:%M:%SZ')
        }
        response = requests.get(url, params=params)
        data = response.json()

        events = []

        for event in data:
            events.append(event['id'])

        flattened_markets = []

        for event in events:
            url = "https://api.the-odds-api.com/v4/sports/americanfootball_nfl/events/" + event + "/odds"
            api_key = "22a6282c9744177b06acb842d34a02cb"
            params = {
                'apiKey': api_key,
                'regions': 'us',
                ##'markets': 'h2h,spreads',
                'markets': 'team_totals,alternate_team_totals',
                'oddsFormat': 'american',
                'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                'commenceTimeTo': (datetime.utcnow() + timedelta(days=6)).replace(hour=6, minute=0).strftime(
                    '%Y-%m-%dT%H:%M:%SZ')
            }
            # x = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

            # x = (datetime.utcnow() + timedelta(days=1)).replace(hour=6, minute=0).strftime('%Y-%m-%dT%H:%M:%SZ')
            # Fetch the data from the API
            response = requests.get(url, params=params)
            data = response.json()

            # Extract and flatten markets data

            for entry in data:
                game_info = {
                    "id": data["id"],
                    "sport_key": data["sport_key"],
                    "sport_title": data["sport_title"],
                    "commence_time": data["commence_time"],
                    "home_team": data["home_team"],
                    "away_team": data["away_team"]
                }
                for bookmaker in data["bookmakers"]:
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
                                    "player_name": outcome["description"],
                                    "outcome_name": outcome["name"],
                                    "outcome_price": outcome["price"],
                                    "outcome_point": outcome["point"]
                                })
                            else:
                                outcome_info.update({
                                    "player_name": outcome["description"],
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

        df = df[df['outcome_name'] == 'Over']

        group = df.groupby(['player_name', 'outcome_point'])['Im_Prob'].agg({'mean'}).reset_index()

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
                current_week = 10

                results = pd.DataFrame(list(rows), columns=["id", "amount", "player", "player_id", "type"])
                results['date'] = "Week " + str(current_week)
                results['prediction'] = ""
                type_list = ["NFL - Team Totals"]
                schedule = pd.read_csv("schedule.csv")
                schedule["game"] = schedule["away_team"] + " @ " + schedule["home_team"]
                schedule = schedule[schedule['week'] == current_week]
                games = schedule["game"].tolist()
                games = games + schedule["home_team"].tolist()
                games = games + schedule["away_team"].tolist()
                link_list = games
                results = results[results['type'].isin(type_list)]
                template = results[results['player_id'].isin(link_list) | results['player'].isin(link_list)]
                template = template[
                    ["id", "amount", "player_id", "player", "type", "date", "prediction"]]
                template = template.sort_values(["type", 'player', 'amount'], ascending=[True, True, True])

                template = template.merge(group, how='left', left_on=['player', "amount"],
                                          right_on=['player_name', "outcome_point"])
                template["post"] = post_id
                template["page"] = page_id
                template['prediction'] = template['mean']
                template = template[template[['prediction']].notnull().all(1)]
                template['predictable'] = template["id"]
                template["id"] = [uuid.uuid4().hex for _ in range(len(template.index))]

                template = template[["id", "predictable", "date", "page", "post", "prediction"]]

                cursor.executemany("""INSERT INTO predictions
                                                      (id,predictable,date,page,post,prediction) 
                                                      VALUES (%s,%s,%s,%s,%s,%s);""",
                                   list(template.itertuples(index=False, name=None)))
                cnx.commit()

            cnx.close()
    elif type_id == "gametotals":
        url = "https://api.the-odds-api.com/v4/sports/americanfootball_nfl/events/"
        api_key = "22a6282c9744177b06acb842d34a02cb"
        params = {
            'apiKey': api_key,
            'regions': 'us',
            ##'markets': 'h2h,spreads',
            ##'markets': 'pitcher_strikeouts_alternate,batter_total_bases',
            'markets': 'alternate_totals',
            'oddsFormat': 'american',
            'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            'commenceTimeTo': (datetime.utcnow() + timedelta(days=6)).replace(hour=6, minute=0).strftime(
                '%Y-%m-%dT%H:%M:%SZ')
        }
        response = requests.get(url, params=params)
        data = response.json()

        events = []

        for event in data:
            events.append(event['id'])

        flattened_markets = []

        for event in events:
            url = "https://api.the-odds-api.com/v4/sports/americanfootball_nfl/events/" + event + "/odds"
            api_key = "22a6282c9744177b06acb842d34a02cb"
            params = {
                'apiKey': api_key,
                'regions': 'us',
                ##'markets': 'h2h,spreads',
                'markets': 'totals,alternate_totals',
                'oddsFormat': 'american',
                'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                'commenceTimeTo': (datetime.utcnow() + timedelta(days=6)).replace(hour=6, minute=0).strftime(
                    '%Y-%m-%dT%H:%M:%SZ')
            }
            # x = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

            # x = (datetime.utcnow() + timedelta(days=1)).replace(hour=6, minute=0).strftime('%Y-%m-%dT%H:%M:%SZ')
            # Fetch the data from the API
            response = requests.get(url, params=params)
            data = response.json()

            # Extract and flatten markets data

            for entry in data:
                game_info = {
                    "id": data["id"],
                    "sport_key": data["sport_key"],
                    "sport_title": data["sport_title"],
                    "commence_time": data["commence_time"],
                    "home_team": data["home_team"],
                    "away_team": data["away_team"]
                }
                for bookmaker in data["bookmakers"]:
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

        df = df[df['outcome_name'] == 'Over']
        teams = pd.read_csv("nfl_teams.csv")
        df = df.merge(teams, how='left',
                      left_on=['home_team'], right_on=['team'])
        df = df.merge(teams, how='left',
                      left_on=['away_team'], right_on=['team'])
        df["outcome_name"] = df["abbreviation_y"] + " @ " + df["abbreviation_x"]

        group = df.groupby(['outcome_name', 'outcome_point'])['Im_Prob'].agg({'mean'}).reset_index()

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
                current_week = 10

                results = pd.DataFrame(list(rows), columns=["id", "amount", "player", "player_id", "type"])
                results['date'] = "Week " + str(current_week)
                results['prediction'] = ""
                type_list = ["NFL - Game Totals"]
                schedule = pd.read_csv("schedule.csv")
                schedule["game"] = schedule["away_team"] + " @ " + schedule["home_team"]
                schedule = schedule[schedule['week'] == current_week]
                games = schedule["game"].tolist()
                games = games + schedule["home_team"].tolist()
                games = games + schedule["away_team"].tolist()
                link_list = games
                results = results[results['type'].isin(type_list)]
                template = results[results['player_id'].isin(link_list) | results['player'].isin(link_list)]
                template = template[
                    ["id", "amount", "player_id", "player", "type", "date", "prediction"]]
                template = template.sort_values(["type", 'player', 'amount'], ascending=[True, True, True])

                template = template.merge(group, how='left', left_on=['player', "amount"],
                                          right_on=['outcome_name', "outcome_point"])
                template["post"] = post_id
                template["page"] = page_id
                template['prediction'] = template['mean']
                template = template[template[['prediction']].notnull().all(1)]
                template['predictable'] = template["id"]
                template["id"] = [uuid.uuid4().hex for _ in range(len(template.index))]

                template = template[["id", "predictable", "date", "page", "post", "prediction"]]

                cursor.executemany("""INSERT INTO predictions
                                                      (id,predictable,date,page,post,prediction) 
                                                      VALUES (%s,%s,%s,%s,%s,%s);""",
                                   list(template.itertuples(index=False, name=None)))
                cnx.commit()

            cnx.close()
    else:
        url = "https://api.the-odds-api.com/v4/sports/americanfootball_nfl/events/"
        api_key = "22a6282c9744177b06acb842d34a02cb"
        params = {
            'apiKey': api_key,
            'regions': 'us',
            ##'markets': 'h2h,spreads',
            ##'markets': 'pitcher_strikeouts_alternate,batter_total_bases',
            'markets': 'alternate_spreads',
            'oddsFormat': 'american',
            'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            'commenceTimeTo': (datetime.utcnow() + timedelta(days=6)).replace(hour=6, minute=0).strftime(
                '%Y-%m-%dT%H:%M:%SZ')
        }
        response = requests.get(url, params=params)
        data = response.json()

        events = []

        for event in data:
            events.append(event['id'])

        flattened_markets = []

        for event in events:
            url = "https://api.the-odds-api.com/v4/sports/americanfootball_nfl/events/" + event + "/odds"
            api_key = "22a6282c9744177b06acb842d34a02cb"
            params = {
                'apiKey': api_key,
                'regions': 'us',
                ##'markets': 'h2h,spreads',
                'markets': 'spreads,alternate_spreads',
                'oddsFormat': 'american',
                'commenceTimeFrom': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                'commenceTimeTo': (datetime.utcnow() + timedelta(days=6)).replace(hour=6, minute=0).strftime(
                    '%Y-%m-%dT%H:%M:%SZ')
            }
            # x = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

            # x = (datetime.utcnow() + timedelta(days=1)).replace(hour=6, minute=0).strftime('%Y-%m-%dT%H:%M:%SZ')
            # Fetch the data from the API
            response = requests.get(url, params=params)
            data = response.json()

            # Extract and flatten markets data

            for entry in data:
                game_info = {
                    "id": data["id"],
                    "sport_key": data["sport_key"],
                    "sport_title": data["sport_title"],
                    "commence_time": data["commence_time"],
                    "home_team": data["home_team"],
                    "away_team": data["away_team"]
                }
                for bookmaker in data["bookmakers"]:
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

        # df = df[df['outcome_name'] == 'Over']

        group = df.groupby(['outcome_name', 'outcome_point'])['Im_Prob'].agg({'mean'}).reset_index()

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
                current_week = 10

                results = pd.DataFrame(list(rows), columns=["id", "amount", "player", "player_id", "type"])
                results['date'] = "Week " + str(current_week)
                results['prediction'] = ""
                type_list = ["NFL - Spread"]
                schedule = pd.read_csv("schedule.csv")
                schedule["game"] = schedule["away_team"] + " @ " + schedule["home_team"]
                schedule = schedule[schedule['week'] == current_week]
                games = schedule["game"].tolist()
                games = games + schedule["home_team"].tolist()
                games = games + schedule["away_team"].tolist()
                link_list = games
                results = results[results['type'].isin(type_list)]
                template = results[results['player_id'].isin(link_list) | results['player'].isin(link_list)]
                template = template[
                    ["id", "amount", "player_id", "player", "type", "date", "prediction"]]
                template = template.sort_values(["type", 'player', 'amount'], ascending=[True, True, True])

                template = template.merge(group, how='left', left_on=['player', "amount"],
                                          right_on=['outcome_name', "outcome_point"])
                template["post"] = post_id
                template["page"] = page_id
                template['prediction'] = template['mean']
                template = template[template[['prediction']].notnull().all(1)]
                template['predictable'] = template["id"]
                template["id"] = [uuid.uuid4().hex for _ in range(len(template.index))]

                template = template[["id", "predictable", "date", "page", "post", "prediction"]]

                cursor.executemany("""INSERT INTO predictions
                                                      (id,predictable,date,page,post,prediction) 
                                                      VALUES (%s,%s,%s,%s,%s,%s);""",
                                   list(template.itertuples(index=False, name=None)))
                cnx.commit()

            cnx.close()
    return "Success"
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    application.run()