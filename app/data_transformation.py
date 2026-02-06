import json
import os
from datetime import datetime

import aiofiles
from numpy import average
from dotenv import load_dotenv
load_dotenv()


class Data:
    def __init__(self):
        self.GOOGLE_ADS_CLICKS_PER_DAY_FILE = os.getenv("GOOGLE_ADS_CLICKS_PER_DAY_FILE")
        self.GOOGLE_ANALYST_DURATION_FILE = os.getenv("GOOGLE_ANALYST_DURATION_FILE")
        self.GOOGLE_ANALYST_EVENTS_FILE = os.getenv("GOOGLE_ANALYST_EVENTS_FILE")
        self.GOOGLE_ANALYST_TRAFFIC_FILE = os.getenv("GOOGLE_ANALYST_TRAFFIC_FILE")

        self.data = None

    async def get_page_info(self, car_name: str) -> dict:
        match car_name:
            case "avatr":
                ads_target = "AVATR"
                analyst_target = "Avatr Одесса"
            case "electro":
                ads_target = "Електро"
                analyst_target = "Autogroup Electro"
            case _:
                ads_target = "AVATR"
                analyst_target = "Avatr Одесса"
        async def get_info(file: str, target: str = analyst_target) -> dict | list:
            temp_data = await Other.get_data(file)
            return next((c for c in temp_data if c["campaign_name"] == target), None)
        clicks_per_day = await get_info(self.GOOGLE_ADS_CLICKS_PER_DAY_FILE, target=ads_target)
        duration = await get_info(self.GOOGLE_ANALYST_DURATION_FILE)
        events = await get_info(self.GOOGLE_ANALYST_EVENTS_FILE)
        traffic = await get_info(self.GOOGLE_ANALYST_TRAFFIC_FILE)
        return {"clicks": clicks_per_day, "duration": duration, "events": events, "traffic": traffic}

    async def get_additional_information(self):
        now = datetime.now()
        current_year = now.year
        current_month = now.month

        total_clicks = sum(
            item["clicks"]
            for item in self.data["clicks"]["data"]
            if datetime.strptime(item["date"], "%Y-%m-%d").year == current_year
            and datetime.strptime(item["date"], "%Y-%m-%d").month == current_month
        )
        total_clicks = await Other.format_number(total_clicks)

        total_impressions = sum(
            item["impressions"]
            for item in self.data["clicks"]["data"]
            if datetime.strptime(item["date"], "%Y-%m-%d").year == current_year
            and datetime.strptime(item["date"], "%Y-%m-%d").month == current_month
        )
        total_impressions = await Other.format_number(total_impressions)

        fresh_date = await Other.get_fresh_date(self.data["clicks"]["data"])
        return total_clicks, total_impressions, fresh_date

    async def chill_info(self, curr_info_name: str) -> (list, list):
        graph = [{"label": await Other.get_current_day(d["date"]), "v": d[curr_info_name]} for d in
                          self.data[curr_info_name]["data"]]
        points = [dur['v'] for dur in graph]
        points = [int(min(points)) - 10 if min(points) - 10 >= 0 else 0,
                          int(average(points)), int(max(points)) + 10]
        return graph, points

    async def get_events(self):
        events_by_date = {}

        for d in self.data["events"]["data"]:
            date_str = d["date"]
            name = d["eventName"]
            count = int(d["eventCount"] or 0)

            if date_str not in events_by_date:
                events_by_date[date_str] = {"date": date_str}

            events_by_date[date_str][name] = count

        events_graph = []
        for date_str, row in events_by_date.items():
            row = dict(row)
            row["label"] = await Other.get_current_day(date_str)
            events_graph.append(row)

        events_graph.sort(key=lambda x: x["date"])

        events_graph_points = [e.get("page_view", 0) for e in events_graph]
        events_graph_points = [0, int(average(events_graph_points)), max(events_graph_points) + 10]

        return events_graph, events_graph_points, events_by_date

    async def get_traffic(self, events_by_date:dict, is_all: bool = False):
        now = datetime.now()
        current_year = now.year
        current_month = now.month

        if is_all:
            traffic_graph = [
                {"date": d["date"], "label": await Other.get_current_day(d["date"]), "v": int(d["totalUsers"])}
                for d in self.data["traffic"]["data"]
            ]
        else:
            traffic_graph = [
                {"date": d["date"], "label": await Other.get_current_day(d["date"]), "v": int(d["totalUsers"])}
                for d in self.data["traffic"]["data"]
                if datetime.strptime(d["date"], "%Y-%m-%d").year == current_year
                   and datetime.strptime(d["date"], "%Y-%m-%d").month == current_month
            ]

        traffic_graph.sort(key=lambda x: x["date"])

        traffic_graph_percent = []
        prev = None

        calls_mtd = 0
        forms_mtd = 0
        prev_month_key = None

        for point in traffic_graph:
            date_str = point["date"]
            cur = point["v"]

            if prev is None or prev == 0:
                percent_v = 0
            else:
                percent_v = ((prev - cur) / prev * 100) * -1
            prev = cur

            if is_all:
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

            traffic_graph_percent.append({
                "date": date_str,
                "label": point["label"],
                "v": percent_v,

                "Звонки": calls_day,
                "Звонки с начала месяца": calls_mtd,

                "Заявки": forms_day,
                "Заявки с начала месяца": forms_mtd,

                "Общая конверсия": calls_mtd + forms_mtd
            })
        return traffic_graph, traffic_graph_percent




class Other:
    def __init__(self):
        pass

    @staticmethod
    async def get_data(data_file: str) -> list | dict:
        try:
            async with aiofiles.open(data_file, mode="r", encoding="utf-8") as f:
                content = await f.read()
                if json.loads(content) is None:
                    print(f"Ошибка извлечения данных для: {data_file}")
                return json.loads(content)
        except Exception as e:
            print(data_file)
            print(e)
            return []

    @staticmethod
    async def format_number(n: int) -> str:
        s = str(n)
        parts = []

        while s:
            parts.append(s[-3:])
            s = s[:-3]

        return ' '.join(reversed(parts))

    @staticmethod
    async def get_fresh_date(data: list) -> list:
        months = await Other.get_data(os.getenv("MONTH_FILE"))
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
                "i": i + 1,
                "text": f"{day:02d}\n{month_name}"
            })

        return result

    @staticmethod
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
        months = await Other.get_data(os.getenv("MONTH_FILE"))
        day = ''
        d = datetime.strptime(date, "%Y-%m-%d")
        day += f'{days_of_week[str(d.weekday())]} '
        day += f'{d.day} '
        day += months.get(str(d.month))
        return day
