import json
import os
import asyncio
from datetime import date, timedelta, datetime
from typing import Optional, List, Dict

from google.ads.googleads.client import GoogleAdsClient

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (DateRange, Metric, RunReportRequest, Dimension, OrderBy, Filter,
                                                FilterExpression)
from google.oauth2.credentials import Credentials

from dotenv import load_dotenv
import aiofiles

load_dotenv()


class Google:
    def __init__(self):
        self.manager_id = "5109744025"
        self.storage_name = os.getenv("GOOGLE_ADS_LOGIN_FILE")
        self.service_name = "GoogleAdsService"

        self.ads_client = GoogleAdsClient.load_from_storage(self.storage_name)

        self.analytics_credentials = Credentials(
            token=None,
            refresh_token=os.getenv("GOOGLE_REFRESH_TOKEN"),
            token_uri="https://oauth2.googleapis.com/token",
            client_id=os.getenv("GOOGLE_CLIENT_ID"),
            client_secret=os.getenv("GOOGLE_CLIENT_SECRET"),
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
            date_ranges=[DateRange(start_date="30daysAgo", end_date="today")],
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
                date_ranges=[DateRange(start_date="30daysAgo", end_date="today")],
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

    async def get_analyst_traffic(self, property_id: str, start_date: str = "120daysAgo",
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

            result.append({"date": day, "totalUsers": total_users})

        return result


class SQL:
    def __init__(self):
        pass

class Functions:
    def __init__(self):
        self.traffic_drop = os.getenv("TRAFFIC_DROP_QUERY")
        self.yesterday = (date.today() - timedelta(days=0)).isoformat()
        self.month_before = (date.today() - timedelta(days=31)).isoformat()

    async def put_current_days(self) -> None:
        self.traffic_drop = self.traffic_drop.replace("day_1", self.yesterday).replace("day_2", self.month_before)
        return

    async def get_traffic(self, traffic_drop_per_day: list) -> list:
        clicks_by_date = [{"date": row.segments.date, "clicks": row.metrics.clicks,
                           "impressions": row.metrics.impressions} for row in traffic_drop_per_day]

        return clicks_by_date

class Other:
    @staticmethod
    async def get_data(data_file: str) -> list:
        async with aiofiles.open(data_file, mode="r", encoding="utf-8") as f:
            content = await f.read()
            return json.loads(content)

    @staticmethod
    async def save_data(data: list, data_file: str) -> None:
        json_str = json.dumps(data, ensure_ascii=False, indent=2)

        async with aiofiles.open(data_file, "w", encoding="utf-8") as f:
            await f.write(json_str)


async def main():
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
        print(f"Подчинённый рекламный аккаунт: {sub_name} ({sub_id})")
##
        if sub_id == '5109744025':
            continue
##
        # Google Ads Result
        ads_results = await asyncio.gather(
            google.gaql_async(sub_id, functions.traffic_drop)
        )
        traffic_drop_per_day_temp = ads_results[0]
        traffic_drop_per_day_temp = await functions.get_traffic(traffic_drop_per_day_temp)
        traffic_drop_per_day.append({"campaign_name": sub_name, "campaign_id": sub_id, "data": traffic_drop_per_day_temp})
    await Other.save_data(traffic_drop_per_day, os.getenv("GOOGLE_ADS_CLICKS_PER_DAY_FILE"))

    # Google Analyst
    sub_analytics_account = await Other.get_data(os.getenv("ANALYTIC_ACCOUNTS_FILE"))

    duration_data = []
    events_data = []
    traffic_data = []
    for sub in sub_analytics_account:
        print(f"Текущий обрабатываемый аккаунт аналитики: {sub['account_name']} {sub['account_id']}")
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
    await Other.save_data(duration_data, os.getenv("GOOGLE_ANALYST_DURATION_FILE"))
    await Other.save_data(events_data, os.getenv("GOOGLE_ANALYST_EVENTS_FILE"))
    await Other.save_data(traffic_data, os.getenv("GOOGLE_ANALYST_TRAFFIC_FILE"))
    print("Done")


if __name__ == "__main__":
    asyncio.run(main())
