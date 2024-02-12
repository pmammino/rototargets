import operator
import string
import random
import datetime
import uuid

from flask import Flask, render_template, request, session, redirect, url_for,make_response
import requests
import json
import http.client
import pandas as pd

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
            ((3.72 - stats["ERA"]) / 3.72) * (stats["IP"] / 100)) + (
                                 ((1.19 - stats["WHIP"]) / 1.19) * (stats["IP"] / 100)), 2)
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
            stats_hitters["RBI"] / 78) + (((stats_hitters["AVG"] - 0.260) / 0.2692) * (
            stats_hitters["AB"] / 400)), 2)
    stats_hitters = stats_hitters.sort_values(by='Val', ascending=False)
    stats_hitters = stats_hitters[stats_hitters.AB > 200].round(3)
    stats_hitters['Rank'] = stats_hitters['Val'].rank(pct=True).round(3)
    stats = pd.concat([stats, stats_hitters], axis=0, ignore_index=True)
    stats['Rank_All'] = stats['Val'].rank(pct=True).round(3)
    stats['Val'] = (((stats["Rank"] * 2) + stats["Rank_All"]) / 3) * 100
    stats = stats.sort_values(by='Val', ascending=False).round(3)
    return stats.to_json(orient='records')


@application.route("/target")
def get_targets():
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
    stats_pitchers_head = stats.head(11*12)
    stats = stats.sort_values(by='SV', ascending=False)
    stats_pitchers_saves = stats.head(3*12)
    stats = pd.concat([stats_pitchers_head, stats_pitchers_saves], axis=0, ignore_index=True)
    pitching = stats[["IP", "ERA", "WHIP", "W", "K"]].quantile(.7)
    SV = stats[["SV"]].quantile(.8)
    pitching = pd.concat([pitching, SV], axis=0)
    pitching = pd.DataFrame({'Category': pitching.index, 'Target': pitching.values})
    response_hitters = requests.get("https://www.fangraphs.com/api/steamer/batting?key=5sCxU6kRxvCW8VcN", verify=False)
    data_hitters = response_hitters.json()
    stats_hitters = pd.DataFrame(data_hitters)
    stats_hitters = stats_hitters[
        ["steamerid", "mlbamid", "firstname", "lastname", "reliability", "HR", "AB", "AVG", "RBI", "R", "H", "SB",
         "POSITION"]]
    stats_hitters = stats_hitters.sort_values(by='AB', ascending=False)
    stats_hitters_top = stats_hitters.head(17*12)
    hitting = stats_hitters_top[["HR", "AB", "AVG", "RBI", "R", "SB"]].quantile(.7)
    hitting = pd.DataFrame({'Category': hitting.index, 'Target': hitting.values})
    targets = pd.concat([pitching, hitting], axis=0, ignore_index=True)
    targets.WHIP = targets.WHIP * -1
    targets.ERA = targets.ERA * -1
    return targets.to_json(orient='records')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    application.run()

