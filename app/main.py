import json
import os
from datetime import datetime, timedelta

import aiofiles
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from dotenv import load_dotenv
from numpy.ma.extras import average

load_dotenv()

app = FastAPI()

# Подключаем папку со статикой (css, js, картинки)
app.mount("/static", StaticFiles(directory="static"), name="static")

# Подключаем шаблоны
templates = Jinja2Templates(directory="templates")


async def get_data(data_file: str) -> list | dict:
    async with aiofiles.open(data_file, mode="r", encoding="utf-8") as f:
        content = await f.read()
        return json.loads(content)


async def get_page_info(car_name: str) -> dict:
    match car_name:
        case "avatr":
            clicks_per_day_target = "AVATR"
            duration_target = "Avatr Одесса"
            events_target = "Avatr Одесса"
            traffic_target = "Avatr Одесса"
        case _:
            clicks_per_day_target = "AVATR"
            duration_target = "Avatr Одесса"
            events_target = "Avatr Одесса"
            traffic_target = "Avatr Одесса"
    clicks_per_day = await get_data(os.getenv("GOOGLE_ADS_CLICKS_PER_DAY_FILE"))
    clicks_per_day = next((c for c in clicks_per_day if c["campaign_name"] == clicks_per_day_target), None)
    duration = await get_data(os.getenv("GOOGLE_ANALYST_DURATION_FILE"))
    duration = next((c for c in duration if c["campaign_name"] == duration_target), None)
    events = await get_data(os.getenv("GOOGLE_ANALYST_EVENTS_FILE"))
    events = next((c for c in events if c["campaign_name"] == events_target), None)
    traffic = await get_data(os.getenv("GOOGLE_ANALYST_TRAFFIC_FILE"))
    traffic = next((c for c in traffic if c["campaign_name"] == traffic_target), None)
    return {"clicks_per_day": clicks_per_day, "duration": duration, "events": events, "traffic": traffic}

async def format_number(n: int) -> str:
    s = str(n)
    parts = []

    while s:
        parts.append(s[-3:])
        s = s[:-3]

    return ' '.join(reversed(parts))

async def get_fresh_date(data: list) -> list:
    months = await get_data(os.getenv("MONTH_FILE"))
    target_indexes = [10, 14, 18, 26]
    last_30 = data[-30:]

    result = []

    for i in target_indexes:
        if i - 1 >= len(last_30):
            continue

        item = last_30[i - 1]
        d = datetime.strptime(item["date"], "%Y-%m-%d")

        day = d.day
        month_name = months.get(str(d.month))

        result.append({
            "i": i+1,
            "text": f"{day:02d}\n{month_name}"
        })

    return result

async def get_current_day(date: str) -> str:
    days_of_week = {
        "0": "пн",
        "1": "вт",
        "2": "ср",
        "3": "чт",
        "4": "пт",
        "5": "сб.",
        "6": "вс."
    }
    months = await get_data(os.getenv("MONTH_FILE"))
    day = ''
    d = datetime.strptime(date, "%Y-%m-%d")
    day += f'{days_of_week[str(d.weekday())]} '
    day += f'{d.day} '
    day += months.get(str(d.month))
    return day
@app.get("/", response_class=HTMLResponse)
async def avatr(request: Request):
    now = datetime.now()
    current_year = now.year
    current_month = now.month

    data = await get_page_info("avatr")

    total_clicks = sum(
        item["clicks"]
        for item in data["clicks_per_day"]["data"]
        if datetime.strptime(item["date"], "%Y-%m-%d").year == current_year
        and datetime.strptime(item["date"], "%Y-%m-%d").month == current_month
    )
    total_clicks = await format_number(total_clicks)

    total_impressions = sum(
        item["impressions"]
        for item in data["clicks_per_day"]["data"]
        if datetime.strptime(item["date"], "%Y-%m-%d").year == current_year
        and datetime.strptime(item["date"], "%Y-%m-%d").month == current_month
    )
    total_impressions = await format_number(total_impressions)

    fresh_date = await get_fresh_date(data["clicks_per_day"]["data"])

    duration_graph = [{"label": await get_current_day(d["date"]), "v": d["duration"]} for d in data["duration"]["data"]]
    duration_graph_points = [dur['v'] for dur in duration_graph]
    duration_graph_points = [int(min(duration_graph_points)) - 10 if min(duration_graph_points) - 10 >= 0 else 0,
                           int(average(duration_graph_points)), int(max(duration_graph_points)) + 10]

    clicks_graph = [{"label": await get_current_day(d["date"]), "v": d["clicks"], "imp": d["impressions"]}
                    for d in data["clicks_per_day"]["data"]]
    clicks_graph_points = [click['v'] for click in clicks_graph]
    clicks_graph_points = [min(clicks_graph_points) - 10 if min(clicks_graph_points) - 10 >=0 else 0,
                           int(average(clicks_graph_points)), max(clicks_graph_points) + 10]

    events_by_date = {}

    for d in data["events"]["data"]:
        date_str = d["date"]  # "YYYY-MM-DD"
        name = d["eventName"]
        count = int(d["eventCount"] or 0)

        if date_str not in events_by_date:
            events_by_date[date_str] = {"date": date_str}

        events_by_date[date_str][name] = count

    events_graph = []
    for date_str, row in events_by_date.items():
        row = dict(row)
        row["label"] = await get_current_day(date_str)
        events_graph.append(row)

    events_graph.sort(key=lambda x: x["date"])

    events_graph_points = [e.get("page_view", 0) for e in events_graph]
    events_graph_points = [0, int(average(events_graph_points)), max(events_graph_points) + 10]

    traffic_current_graph = [
        {"date": d["date"], "label": await get_current_day(d["date"]), "v": int(d["totalUsers"])}
        for d in data["traffic"]["data"]
        if datetime.strptime(d["date"], "%Y-%m-%d").year == current_year
           and datetime.strptime(d["date"], "%Y-%m-%d").month == current_month
    ]

    traffic_current_graph.sort(key=lambda x: x["date"])

    traffic_current_graph_percent = []
    prev = None

    calls_mtd = 0
    forms_mtd = 0

    for point in traffic_current_graph:
        date_str = point["date"]
        cur = point["v"]

        if prev is None or prev == 0:
            percent_v = 0
        else:
            percent_v = ((prev - cur) / prev * 100) * -1
        prev = cur

        ev = events_by_date.get(date_str, {})

        calls_day = int(ev.get("binotel_ct_call_details", 0) or 0) + int(ev.get("binotel_ct_call_received", 0) or 0)
        forms_day = int(ev.get("form_start", 0) or 0)

        calls_mtd += calls_day
        forms_mtd += forms_day

        traffic_current_graph_percent.append({
            "date": date_str,
            "label": point["label"],
            "v": percent_v,

            "Звонки": calls_day,
            "Звонки с начала месяца": calls_mtd,

            "Заявки": forms_day,
            "Заявки с начала месяца": forms_mtd,

            "Общая конверсия": calls_mtd + forms_mtd
        })

    # 5) Трафик за ВСЕ время (как traffic_current_graph, только без фильтра по месяцу)
    traffic_all_graph = [
        {"date": d["date"], "label": await get_current_day(d["date"]), "v": int(d["totalUsers"])}
        for d in data["traffic"]["data"]
    ]
    traffic_all_graph.sort(key=lambda x: x["date"])

    # 6) traffic_all_graph_percent (как traffic_current_graph_percent),
    # но накопления считаем "с начала месяца" для КАЖДОГО месяца отдельно
    traffic_all_graph_percent = []
    prev = None

    calls_mtd = 0
    forms_mtd = 0
    prev_month_key = None  # "YYYY-MM"

    for point in traffic_all_graph:
        date_str = point["date"]
        cur = point["v"]

        # % изменение трафика относительно предыдущей точки (как у тебя)
        if prev is None or prev == 0:
            percent_v = 0
        else:
            percent_v = ((prev - cur) / prev * 100) * -1
        prev = cur

        # определяем смену месяца -> сбрасываем month-to-date
        month_key = date_str[:7]  # "YYYY-MM"
        if prev_month_key is None or month_key != prev_month_key:
            calls_mtd = 0
            forms_mtd = 0
            prev_month_key = month_key

        ev = events_by_date.get(date_str, {})

        calls_day = int(ev.get("binotel_ct_call_details", 0) or 0) + int(ev.get("binotel_ct_call_received", 0) or 0)
        forms_day = int(ev.get("form_start", 0) or 0)

        calls_mtd += calls_day
        forms_mtd += forms_day

        traffic_all_graph_percent.append({
            "date": date_str,
            "label": point["label"],
            "v": percent_v,

            "Звонки": calls_day,
            "Звонки с начала месяца": calls_mtd,

            "Заявки": forms_day,
            "Заявки с начала месяца": forms_mtd,

            "Общая конверсия": calls_mtd + forms_mtd
        })


    return templates.TemplateResponse(
        "index.html",
        {"request": request, "impressions": total_impressions, "clicks": total_clicks,
         "fresh_date": fresh_date, "clicks_graph": clicks_graph, "clicks_graph_points": clicks_graph_points,
         "duration_graph": duration_graph, "duration_graph_points": duration_graph_points, "events_graph": events_graph,
         "events_graph_points": events_graph_points, "traffic_current_graph": traffic_current_graph,
         "traffic_current_graph_percent": traffic_current_graph_percent, "traffic_all_graph": traffic_all_graph,
         "traffic_all_graph_percent": traffic_all_graph_percent}
    )

@app.get("/mitsubishi", response_class=HTMLResponse)
async def mitsubishi(request: Request):
    images = [
        "/static/graphs/6751259696_14_01_2026/ads.jpeg",
        "/static/graphs/6751259696_14_01_2026/campaigns.jpeg",
        "/static/graphs/6751259696_14_01_2026/keywords.jpeg",
        "/static/graphs/6751259696_14_01_2026/search_terms.jpeg"
    ]
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "active_tab": "mitsubishi", "images": images, "thought": "/static/thought_mitsubishi.txt"}
    )