import asyncio
import json
import os
import re
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Sequence, Any, Optional, List, Dict

import aiofiles
import asyncpg
from numpy import average
from dotenv import load_dotenv
load_dotenv()


class Data:
    def __init__(self):
        self.GOOGLE_ADS_CLICKS_PER_DAY_FILE = os.getenv("GOOGLE_ADS_CLICKS_PER_DAY_FILE")
        self.GOOGLE_ANALYST_DURATION_FILE = os.getenv("GOOGLE_ANALYST_DURATION_FILE")
        self.GOOGLE_ANALYST_EVENTS_FILE = os.getenv("GOOGLE_ANALYST_EVENTS_FILE")
        self.GOOGLE_ANALYST_TRAFFIC_FILE = os.getenv("GOOGLE_ANALYST_TRAFFIC_FILE")
        self.TARGET_NAMES_FILE = os.getenv("TARGET_NAMES_FILE")

        self.sql = SQL()

        self.data = None

    async def get_page_info(self, car_name: str) -> list:
        data = await self.sql.get_data_from_table(table=car_name.replace("-", "_"))
        for item in data:
            item["date"] = item["date"].isoformat()
        return data

    async def get_top_info(self) -> Dict[str, Dict]:
        path = self.TARGET_NAMES_FILE
        if not path:
            raise RuntimeError("TARGET_NAMES_FILE env var is not set")

        async with aiofiles.open(path, mode="r", encoding="utf-8") as f:
            raw = await f.read()

        try:
            targets = json.loads(raw)
        except json.JSONDecodeError:
            print(f"Ошибка парсинга JSON файла: {path}")
            return {}

        tables = [
            t["vehicle_name"].replace("-", "_")
            for t in targets
            if isinstance(t, dict) and t.get("vehicle_name")
        ]
        if not tables:
            return {}

        sql = SQL()

        pool = await asyncpg.create_pool(dsn=sql.dsn, min_size=1, max_size=10, command_timeout=60)

        sem = asyncio.Semaphore(10)

        async def fetch_one(table: str):
            async with sem:
                try:
                    async with pool.acquire() as conn:
                        data = await sql.get_last_ctr_cost_cpc(conn, table)
                        return table, data
                except Exception as e:
                    print(f"Ошибка получения верхних данных ({table}): {e}")
                    return table, {}

        results = await asyncio.gather(*(fetch_one(t) for t in tables))

        await pool.close()
        return {table: data for table, data in results}


    async def get_additional_information(self):
        now = datetime.now()
        current_year = now.year
        current_month = now.month

        total_clicks = sum(
            item['clicks']
            for item in self.data
            if datetime.strptime(item["date"], "%Y-%m-%d").year == current_year
            and datetime.strptime(item["date"], "%Y-%m-%d").month == current_month
        )
        total_clicks = await Other.format_number(total_clicks)

        total_impressions = sum(
            item["impressions"]
            for item in self.data
            if datetime.strptime(item["date"], "%Y-%m-%d").year == current_year
            and datetime.strptime(item["date"], "%Y-%m-%d").month == current_month
        )
        total_impressions = await Other.format_number(total_impressions)

        return total_clicks, total_impressions

    async def chill_info(self, curr_info_name: str) -> (list, list):
        today = datetime.today().date()
        three_months_ago = today - relativedelta(months=3)
        match curr_info_name:
            case "clicks":
                graph = [
                    {
                        "label": await Other.get_current_day(d["date"]),
                        "v": d["clicks"],
                        "imp": d["impressions"],
                    }
                    for d in self.data
                    if datetime.strptime(d["date"], "%Y-%m-%d").date() >= three_months_ago
                ]
            case _:
                graph = [
                    {"label": await Other.get_current_day(d["date"]), "v": d[curr_info_name]}
                    for d in self.data
                    if datetime.strptime(d["date"], "%Y-%m-%d").date() >= three_months_ago
                ]
        points = [dur['v'] for dur in graph]
        points = [int(min(points)) - 10 if min(points) - 10 >= 0 else 0,
                          int(average(points)), int(max(points)) + 10]
        return graph, points

    async def get_events(self):
        today = datetime.today().date()
        three_months_ago = today - relativedelta(months=3)

        events_by_date = {}

        for d in self.data:
            if datetime.strptime(d["date"], "%Y-%m-%d").date() >= three_months_ago:
                for d_key, d_val in d.items():
                    if d_key not in ('page_view', 'session_start', 'user_engagement', 'first_visit', 'view_item',
                                     'click', 'get_call', 'scroll', 'form_start', 'all_forms', 'binotel_ct_call_details',
                                     'binotel_ct_call_received', 'total_users', 'G-MSGH2BB72V', 'G-3WLWZYJN52', 'G-EKMR3T60Q4'):
                        continue
                    date_str = d["date"]
                    name = d_key
                    count = int(d_val or 0)

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
                {"date": d["date"], "label": await Other.get_current_day(d["date"]), "v": int(d["total_users"])}
                for d in self.data
            ]
        else:
            traffic_graph = [
                {"date": d["date"], "label": await Other.get_current_day(d["date"]), "v": int(d["total_users"])}
                for d in self.data
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

            calls_day = int(ev.get("binotel_ct_call_details", 0) or 0) + int(ev.get("Get_call", 0) or 0)
            forms_day = int(ev.get("all_forms", 0) or 0)

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

class SQL:
    def __init__(self):
        self.dsn = os.getenv("DB_CONNECT")
        self.VALID_TABLE_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

    async def get_data_from_table(self, table: str, columns: Sequence[str] = ("*",), where: str = "",
                                      params: Sequence[Any] = (), limit: Optional[int] = 1000) -> List[Dict[str, Any]]:
        if columns == ("*",) or columns == ["*"]:
            cols_sql = "*"
        else:
            safe_cols = []
            for c in columns:
                if not c.replace("_", "").isalnum():
                    raise ValueError(f"Unsafe column name: {c}")
                safe_cols.append(f'"{c}"')
            cols_sql = ", ".join(safe_cols)

        where_sql = f" WHERE {where}" if where else ""
        limit_sql = f" LIMIT {int(limit)}" if limit is not None else ""

        sql = f"SELECT {cols_sql} FROM {table}{where_sql} ORDER BY date {limit_sql};"

        conn: Optional[asyncpg.Connection] = None
        try:
            conn = await asyncpg.connect(self.dsn)
            rows = await conn.fetch(sql, *params)
            return [dict(r) for r in rows]
        finally:
            if conn is not None:
                await conn.close()

    def _sanitize_table_name(self, table: str) -> str:
        # строго: только безопасные идентификаторы
        if not self.VALID_TABLE_RE.match(table):
            raise ValueError(f"Unsafe table name: {table!r}")
        return table

    async def get_last_ctr_cost_cpc(self, conn: asyncpg.Connection, table: str) -> Dict:
        table = self._sanitize_table_name(table)
        row = await conn.fetchrow(
            f"""
            SELECT ctr, cost_micros, average_cpc
            FROM {table}
            ORDER BY date DESC
            LIMIT 1
            """
        )
        return dict(row) if row else {}


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
