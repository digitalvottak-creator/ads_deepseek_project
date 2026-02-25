import json
import logging
import os
import asyncio
import sys
from datetime import date, timedelta, datetime
from typing import Optional, List, Dict, Tuple

from google.ads.googleads.client import GoogleAdsClient

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (DateRange, Metric, RunReportRequest, Dimension, OrderBy, Filter,
                                                FilterExpression)
from google.oauth2.credentials import Credentials

from dotenv import load_dotenv
import aiofiles

import asyncpg

load_dotenv()

class ColorFormatter(logging.Formatter):
    COLORS = {
        logging.INFO: "\033[94m",  # Синий
        logging.WARNING: "\033[93m",  # Жёлтый
        logging.ERROR: "\033[91m"  # Красный
    }

    RESET = "\033[0m"

    def format(self, record):
        log_color = self.COLORS.get(record.levelno, self.RESET)
        message = super().format(record)
        return f"{log_color}{message}{self.RESET}"


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(ColorFormatter('%(asctime)s - %(levelname)s: %(message)s'))
logger.addHandler(console_handler)

class Google:
    def __init__(self):
        self.manager_id = "5109744025"
        self.service_name = "GoogleAdsService"

        self.ads_client = GoogleAdsClient.load_from_dict(
            {
                "developer_token": os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN"),
                "client_id": os.getenv("GOOGLE_ADS_CLIENT_ID"),
                "client_secret": os.getenv("GOOGLE_ADS_CLIENT_SECRET"),
                "refresh_token": os.getenv("GOOGLE_ADS_REFRESH_TOKEN"),
                "use_proto_plus": "true",
            }
        )

        self.analytics_credentials = Credentials(
            token=None,
            refresh_token=os.getenv("GOOGLE_ANALYTICS_REFRESH_TOKEN"),
            token_uri="https://oauth2.googleapis.com/token",
            client_id=os.getenv("GOOGLE_ANALYTICS_CLIENT_ID"),
            client_secret=os.getenv("GOOGLE_ANALYTICS_CLIENT_SECRET"),
            scopes=["https://www.googleapis.com/auth/analytics.readonly"],
        )
        self.analytics_client = BetaAnalyticsDataClient(credentials=self.analytics_credentials)

    async def gaql_async(self, customer_id: str, query: str):
        def _run():
            service = self.ads_client.get_service(self.service_name)
            response = service.search(
                customer_id=customer_id,
                query=query
            )
            return [row for row in response]

        return await asyncio.to_thread(_run)

    async def get_sub_accounts(self):
        query = """
                SELECT
                    customer_client.client_customer,
                    customer_client.descriptive_name
                FROM customer_client \
                """
        return await self.gaql_async(self.manager_id, query)

    async def get_analyst_data(self, property_id: str) -> list:
        request = RunReportRequest(
            property=f"properties/{property_id}",
            date_ranges=[DateRange(start_date="yesterday", end_date="today")],
            dimensions=[Dimension(name="date")],
            metrics=[Metric(name="averageSessionDuration")],
            order_bys=[
                OrderBy(
                    dimension=OrderBy.DimensionOrderBy(
                        dimension_name="date"
                    )
                )
            ],
        )

        response = self.analytics_client.run_report(request)

        result = []
        for row in response.rows:
            click_date = datetime.strptime(
                row.dimension_values[0].value, "%Y%m%d"
            ).date().isoformat()

            avg_duration = float(row.metric_values[0].value)

            result.append({"date": click_date, "duration": avg_duration})

        return result

    async def get_analyst_events(self, property_id: str, include_date: bool = True, event_names: Optional[List[str]] = None,
                limit: int = 10000) -> List[Dict]:
            dims = []
            if include_date:
                dims.append(Dimension(name="date"))
            dims.append(Dimension(name="eventName"))

            metrics = [Metric(name="eventCount")]

            dimension_filter = None
            if event_names:
                from google.analytics.data_v1beta.types import FilterExpression, Filter, InListFilter

                dimension_filter = FilterExpression(
                    filter=Filter(
                        field_name="eventName",
                        in_list_filter=InListFilter(values=event_names),
                    )
                )

            order_bys = []
            if include_date:
                order_bys.append(OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date")))
            order_bys.append(OrderBy(metric=OrderBy.MetricOrderBy(metric_name="eventCount"), desc=True))

            request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[DateRange(start_date="yesterday", end_date="today")],
                dimensions=dims,
                metrics=metrics,
                dimension_filter=dimension_filter,
                order_bys=order_bys,
                limit=limit,
            )

            response = self.analytics_client.run_report(request)

            results: List[Dict] = []
            for row in response.rows:
                idx = 0
                date_iso = None

                if include_date:
                    raw_date = row.dimension_values[idx].value  # YYYYMMDD
                    date_iso = datetime.strptime(raw_date, "%Y%m%d").date().isoformat()
                    idx += 1

                event_name = row.dimension_values[idx].value
                event_count = int(float(row.metric_values[0].value))

                if include_date:
                    results.append({"date": date_iso, "eventName": event_name, "eventCount": event_count})
                else:
                    results.append({"eventName": event_name, "eventCount": event_count})

            return results

    async def get_analyst_traffic(self, property_id: str, start_date: str = "yesterday",
            end_date: str = "today") -> List[Dict]:
        request = RunReportRequest(
            property=f"properties/{property_id}",
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[Dimension(name="date")],
            metrics=[Metric(name="totalUsers")],
            order_bys=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"))],
        )

        response = self.analytics_client.run_report(request)

        result: List[Dict] = []
        for row in response.rows:
            raw_date = row.dimension_values[0].value
            day = datetime.strptime(raw_date, "%Y%m%d").date().isoformat()
            total_users = int(float(row.metric_values[0].value))

            result.append({"date": day, "total_users": total_users})

        return result


class SQL:
    def __init__(self):
        self.dsn = os.getenv("DB_CONNECT")
        self.pool = None

    async def create_conn(self):
        if self.pool is None:
            self.pool = await asyncpg.create_pool(
                dsn=self.dsn, min_size=1, max_size=10, command_timeout=60
            )

    async def close(self):
        if self.pool is not None:
            await self.pool.close()
            self.pool = None

    async def ensure_schema(self, conn: asyncpg.Connection, info: dict) ->  Optional[Tuple[int, str]]:
        """
        Проверяем существование таблиц и создаём, если их нет.
        """
        targets = await Other.get_data(os.getenv("TARGET_NAMES_FILE"))
        for target in targets:
            if ((target["vehicle_name"] == info["campaign_name"]) or (target["ads_target"] == info["campaign_name"]) or
                    (target["analyst_target"] == info["campaign_name"])):
                table_name = target["vehicle_name"].replace("-", "_")
                break
        else:
            logger.warning(f'Название компании для {info["campaign_name"]} не найдено!')
            return None

        if not await self.table_exists(conn, "companies"):
            companies_table = """CREATE TABLE IF NOT EXISTS companies (
                id                 BIGSERIAL PRIMARY KEY,
                company_name       TEXT NOT NULL UNIQUE,
                google_ads_id      TEXT,
                ga4_property_id    TEXT,
                created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
            );"""
            await conn.execute(companies_table)

        db_id = await self.insert_company(conn, table_name, info['campaign_id'])
        current_table = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            vehicle_id   BIGINT NOT NULL REFERENCES companies(id),
            date         DATE NOT NULL,
            clicks                   INTEGER DEFAULT 0,
            impressions              INTEGER DEFAULT 0,
            duration                 DOUBLE PRECISION DEFAULT 0,
            page_view                INTEGER DEFAULT 0,
            session_start            INTEGER DEFAULT 0,
            user_engagement          INTEGER DEFAULT 0,
            first_visit              INTEGER DEFAULT 0,
            view_item                INTEGER DEFAULT 0,
            click                    INTEGER DEFAULT 0,
            get_call                 INTEGER DEFAULT 0,
            scroll                   INTEGER DEFAULT 0,
            form_start               INTEGER DEFAULT 0,
            g_msgh2bb72v             INTEGER DEFAULT 0,
            g_3wlwzyjn52             INTEGER DEFAULT 0,
            g_ekmr3t60q4             INTEGER DEFAULT 0,
            all_forms                INTEGER DEFAULT 0,
            binotel_ct_call_details  INTEGER DEFAULT 0,
            binotel_ct_call_received INTEGER DEFAULT 0,
            total_users              INTEGER DEFAULT 0,
            
            ctr                      DOUBLE PRECISION DEFAULT 0,
            cost_micros              DOUBLE PRECISION DEFAULT 0,
            average_cpc              DOUBLE PRECISION DEFAULT 0,
        
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        
            PRIMARY KEY (vehicle_id, date)
        );
        """
        table_exist = await self.table_exists(conn, table_name)
#
        if not table_exist:
            await conn.execute(current_table)

        return db_id, table_name

    async def insert_company(self, conn: asyncpg.Connection, company_name: str, company_id: str) -> int:
        # 1) Проверяем, есть ли такая компания, и какие там значения
        row = await conn.fetchrow(
            """
            SELECT id, google_ads_id, ga4_property_id
            FROM companies
            WHERE company_name = $1;
            """,
            company_name,
        )

        if row is not None:
            db_id: int = row["id"]
            google_ads_id: Optional[str] = row["google_ads_id"]
            ga4_property_id: Optional[str] = row["ga4_property_id"]

            def is_empty(v: Optional[str]) -> bool:
                return v is None or v.strip() == ""

            # 2) Если google_ads_id пустой -> пишем company_id туда и выходим
            if is_empty(google_ads_id):
                await conn.execute(
                    """
                    UPDATE companies
                    SET google_ads_id = $2
                    WHERE id = $1;
                    """,
                    db_id,
                    company_id,
                )
                return db_id

            # 3) Если google_ads_id есть, но ga4_property_id пустой -> пишем company_id туда и выходим
            if is_empty(ga4_property_id):
                await conn.execute(
                    """
                    UPDATE companies
                    SET ga4_property_id = $2
                    WHERE id = $1;
                    """,
                    db_id,
                    company_id,
                )
                return db_id

            # 4) Если оба уже заполнены — ничего не меняем
            return db_id

        # 5) Если company_name не существует — создаём и пишем company_id в google_ads_id
        new_id = await conn.fetchval(
            """
            INSERT INTO companies (company_name, google_ads_id)
            VALUES ($1, $2) RETURNING id;
            """,
            company_name,
            company_id,
        )
        return int(new_id)

    async def table_exists(self, conn: asyncpg.Connection, table_name: str, schema: str = "public") -> bool:
        """
        Проверка существования таблицы через information_schema.
        """
        q = """
            SELECT EXISTS (SELECT 1 \
                           FROM information_schema.tables \
                           WHERE table_schema = $1 \
                             AND table_name = $2); \
            """
        return await conn.fetchval(q, schema, table_name)

    async def set_data(self, conn: asyncpg.Connection, info: dict, table_name:str, data_type:str, db_id: int) -> None:
        match data_type:
            case "clicks_per_day":
                for i in info['data']:
                    curr_date = date.fromisoformat(i["date"])
                    clicks = i["clicks"]
                    impressions = i["impressions"]
                    ctr = i["ctr"]
                    cost_micros = i["cost_micros"]
                    average_cpc = i["average_cpc"]
                    query = f"""
                              INSERT INTO {table_name} (vehicle_id, date, clicks, impressions, ctr, cost_micros, average_cpc)
                              VALUES ($1, $2, $3, $4, $5, $6, $7)
                              ON CONFLICT (vehicle_id, date) DO
                              UPDATE SET
                                  clicks = EXCLUDED.clicks,
                                  impressions = EXCLUDED.impressions,
                                  ctr = EXCLUDED.ctr,
                                  cost_micros = EXCLUDED.cost_micros,
                                  average_cpc = EXCLUDED.average_cpc;   
                              """
                    await conn.execute(query, db_id, curr_date, clicks, impressions, ctr, cost_micros, average_cpc)
                else:
                    logger.info(f'Очередь запросов "clicks_per_day" для "{table_name}" cоставлена!')
            case "duration":
                for i in info['data']:
                    curr_date = date.fromisoformat(i["date"])
                    duration = i["duration"]
                    query = f"""
                        INSERT INTO {table_name} (vehicle_id, date, duration)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (vehicle_id, date) DO
                        UPDATE SET
                            duration = EXCLUDED.duration;
                        """
                    await conn.execute(query, db_id, curr_date, duration)
                else:
                    logger.info(f'Очередь запросов "duration" для "{table_name}" cоставлена!')
            case "events":
                counts_by_date = {}
                for row in info["data"]:
                    d = row["date"]
                    e = row["eventName"]
                    if e not in ('page_view', 'session_start', 'user_engagement', 'first_visit', 'view_item',
                                 'click', 'get_call', 'scroll', 'form_start', 'all_forms', 'binotel_ct_call_details',
                                 'binotel_ct_call_received', 'total_users', 'G-MSGH2BB72V', 'G-3WLWZYJN52', 'G-EKMR3T60Q4'):
                        continue
                    if e in ('G-MSGH2BB72V', 'G-3WLWZYJN52', 'G-EKMR3T60Q4'):
                        match e:
                            case 'G-MSGH2BB72V':
                                e = 'g_msgh2bb72v'
                            case 'G-3WLWZYJN52':
                                e = 'g_3wlwzyjn52'
                            case 'G-EKMR3T60Q4':
                                e = 'g_ekmr3t60q4'
                    c = row["eventCount"]
                    counts_by_date.setdefault(d, {})[e] = c

                for curr_date, events_map in counts_by_date.items():
                    curr_date = date.fromisoformat(curr_date)
                    curr_events = list(events_map.keys())
                    columns = ["vehicle_id", "date"] + curr_events
                    placeholders = [f"${i}" for i in range(1, len(columns) + 1)]
                    values = [db_id, curr_date] + [
                        events_map[e] for e in curr_events
                    ]
                    update_set = ",\n        ".join(
                        [f'"{e}" = EXCLUDED."{e}"' for e in curr_events]
                    )
                    query = f"""
                    INSERT INTO {table_name} (
                        {", ".join([f'"{c}"' if c not in ("vehicle_id", "date") else c for c in columns])}
                    )
                    VALUES ({", ".join(placeholders)})
                    ON CONFLICT (vehicle_id, date) DO
                    UPDATE SET
                        {update_set};
                    """
                    await conn.execute(query, *values)
                else:
                    logger.info(f'Очередь запросов "events" для "{table_name}" cоставлена!')
            case "traffic":
                for i in info['data']:
                    curr_date = date.fromisoformat(i["date"])
                    total_users = i["total_users"]
                    query = f"""
                            INSERT INTO {table_name} (vehicle_id, date, total_users)
                            VALUES ($1, $2, $3)
                            ON CONFLICT (vehicle_id, date) DO
                            UPDATE SET
                                total_users = EXCLUDED.total_users;
                            """
                    await conn.execute(query, db_id, curr_date, total_users)
                else:
                    logger.info(f'Очередь запросов "traffic" для "{table_name}" cоставлена!')
            case _:
                logger.warning(f"Неожиданное вхождение данных: {data_type}")
                return

class Functions:
    def __init__(self):
        self.traffic_drop = os.getenv("TRAFFIC_DROP_QUERY")
        self.yesterday = (date.today() - timedelta(days=0)).isoformat()
        self.month_before = (date.today() - timedelta(days=1)).isoformat()

    async def put_current_days(self) -> None:
        self.traffic_drop = self.traffic_drop.replace("day_1", self.yesterday).replace("day_2", self.month_before)
        return

    async def get_traffic(self, traffic_drop_per_day: list) -> list:
        clicks_by_date = [{"date": row.segments.date, "clicks": row.metrics.clicks, "impressions": row.metrics.impressions,
                           "ctr": round(row.metrics.ctr * 100, 1), "cost_micros": round(row.metrics.cost_micros / 1_000_000, 2),
                           "average_cpc": round(row.metrics.average_cpc / 1_000_000, 2)} for row in traffic_drop_per_day]

        return clicks_by_date

class Other:
    @staticmethod
    async def get_data(data_file: str) -> list:
        async with aiofiles.open(data_file, mode="r", encoding="utf-8") as f:
            content = await f.read()
            return json.loads(content)

    @staticmethod
    async def save_data(data: list, data_type: str, sql: SQL = SQL()) -> None:
        await sql.create_conn()
        async with sql.pool.acquire() as conn:
            # Вся логика в транзакции, чтобы было атомарно
            async with conn.transaction():
                for info in data:
                    service_data = await sql.ensure_schema(conn, info)
                    if service_data is not None:
                        db_id, table_name = service_data
                        await sql.set_data(conn=conn, info=info, data_type=data_type, table_name=table_name, db_id=db_id)


async def refresh_data_func():
    google = Google()
    sql = SQL()
    functions = Functions()

    await functions.put_current_days()

    sub_ads_accounts = await google.get_sub_accounts()

    # Google Ads
    traffic_drop_per_day = []
    for sub in sub_ads_accounts:
        sub_id = sub.customer_client.client_customer.removeprefix('customers/')
        sub_name = sub.customer_client.descriptive_name or ""
        logger.info(f"Подчинённый рекламный аккаунт: {sub_name} ({sub_id})")
####
        if sub_id == '5109744025':
            continue
####
        # Google Ads Result
        ads_results = await asyncio.gather(
            google.gaql_async(sub_id, functions.traffic_drop)
        )
        traffic_drop_per_day_temp = ads_results[0]
        traffic_drop_per_day_temp = await functions.get_traffic(traffic_drop_per_day_temp)
        traffic_drop_per_day.append({"campaign_name": sub_name, "campaign_id": sub_id, "data": traffic_drop_per_day_temp})
    await Other.save_data(traffic_drop_per_day, "clicks_per_day", sql)

    # Google Analyst
    sub_analytics_account = await Other.get_data(os.getenv("ANALYTIC_ACCOUNTS_FILE"))
#
    duration_data = []
    events_data = []
    traffic_data = []
    for sub in sub_analytics_account:
        logger.info(f"Текущий обрабатываемый аккаунт аналитики: {sub['account_name']} {sub['account_id']}")
        # Время пребывания на сайте
        duration_temp = await google.get_analyst_data(sub['account_id'])
        duration_data.append({"campaign_name": sub['account_name'], "campaign_id": sub['account_id'], "data": duration_temp})
        # События на сайте
        events_temp = await google.get_analyst_events(property_id=sub['account_id'])
        events_data.append({"campaign_name": sub['account_name'], "campaign_id": sub['account_id'], "data": events_temp})
        # Трафик сайта
        traffic_temp = await google.get_analyst_traffic(property_id=sub['account_id'])
        traffic_data.append(
            {"campaign_name": sub['account_name'], "campaign_id": sub['account_id'], "data": traffic_temp})
    await Other.save_data(duration_data, "duration", sql)
    await Other.save_data(events_data, "events", sql)
    await Other.save_data(traffic_data, "traffic", sql)
    logger.info("Данные успешно сохранены")


if __name__ == "__main__":
    asyncio.run(refresh_data_func())
