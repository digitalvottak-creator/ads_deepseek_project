import asyncio
from contextlib import asynccontextmanager
from zoneinfo import ZoneInfo

from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.get_google_data import refresh_data_func
from data_transformation import Data

@asynccontextmanager
async def lifespan(app: FastAPI):
    loop = asyncio.get_event_loop()
    scheduler = AsyncIOScheduler(event_loop=loop)
    scheduler.add_job(refresh_data, trigger=CronTrigger(hour=9, minute=0, timezone=ZoneInfo("Europe/Kyiv")))
    scheduler.add_job(refresh_data, trigger=CronTrigger(hour=12, minute=0, timezone=ZoneInfo("Europe/Kyiv")))
    scheduler.add_job(refresh_data, trigger=CronTrigger(hour=15, minute=0, timezone=ZoneInfo("Europe/Kyiv")))
    scheduler.start()
    try:
        yield
    finally:
        scheduler.shutdown()

async def refresh_data():
    print("Запуск обновления данных")
    await refresh_data_func()

app = FastAPI(lifespan=lifespan)

app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="templates")

async def get_full_data(data_class: Data) -> dict:
    total_clicks, total_impressions, fresh_date = await data_class.get_additional_information()

    duration_graph, duration_graph_points = await data_class.chill_info("duration")
    clicks_graph, clicks_graph_points = await data_class.chill_info("clicks")

    events_graph, events_graph_points, events_by_date = await data_class.get_events()

    traffic_current_graph, traffic_current_graph_percent = await data_class.get_traffic(events_by_date)
    traffic_all_graph, traffic_all_graph_percent = await data_class.get_traffic(events_by_date, is_all=True)

    return {"impressions": total_impressions, "clicks": total_clicks,
            "fresh_date": fresh_date, "clicks_graph": clicks_graph, "clicks_graph_points": clicks_graph_points,
            "duration_graph": duration_graph, "duration_graph_points": duration_graph_points,
            "events_graph": events_graph,
            "events_graph_points": events_graph_points, "traffic_current_graph": traffic_current_graph,
            "traffic_current_graph_percent": traffic_current_graph_percent, "traffic_all_graph": traffic_all_graph,
            "traffic_all_graph_percent": traffic_all_graph_percent}


@app.get("/", response_class=HTMLResponse)
async def avatr(request: Request):
    data_class = Data()

    car_name = "avatr"

    data_class.data = await data_class.get_page_info(car_name=car_name)

    full_data = await get_full_data(data_class)
    full_data["request"] = request
    full_data["active_tab"] = car_name

    return templates.TemplateResponse(
        "index.html",
        full_data
    )


@app.get("/electro", response_class=HTMLResponse)
async def ag_electro(request: Request):
    data_class = Data()

    car_name = "electro"

    data_class.data = await data_class.get_page_info(car_name=car_name)

    full_data = await get_full_data(data_class)
    full_data["request"] = request
    full_data["active_tab"] = car_name

    return templates.TemplateResponse(
        "index.html",
        full_data
    )


@app.get("/bosh-service", response_class=HTMLResponse)
async def ag_electro(request: Request):
    data_class = Data()

    car_name = "bosh-service"

    data_class.data = await data_class.get_page_info(car_name=car_name)

    full_data = await get_full_data(data_class)
    full_data["request"] = request
    full_data["active_tab"] = car_name

    return templates.TemplateResponse(
        "index.html",
        full_data
    )


@app.get("/autogroup-e-service", response_class=HTMLResponse)
async def ag_electro(request: Request):
    data_class = Data()

    car_name = "autogroup-e-service"

    data_class.data = await data_class.get_page_info(car_name=car_name)

    full_data = await get_full_data(data_class)
    full_data["request"] = request
    full_data["active_tab"] = car_name

    return templates.TemplateResponse(
        "index.html",
        full_data
    )


@app.get("/autogroup-used-cars", response_class=HTMLResponse)
async def ag_electro(request: Request):
    data_class = Data()

    car_name = "autogroup-used-cars"

    data_class.data = await data_class.get_page_info(car_name=car_name)

    full_data = await get_full_data(data_class)
    full_data["request"] = request
    full_data["active_tab"] = car_name

    return templates.TemplateResponse(
        "index.html",
        full_data
    )


@app.get("/citroen", response_class=HTMLResponse)
async def ag_electro(request: Request):
    data_class = Data()

    car_name = "citroen"

    data_class.data = await data_class.get_page_info(car_name=car_name)

    full_data = await get_full_data(data_class)
    full_data["request"] = request
    full_data["active_tab"] = car_name

    return templates.TemplateResponse(
        "index.html",
        full_data
    )


@app.get("/ds", response_class=HTMLResponse)
async def ag_electro(request: Request):
    data_class = Data()

    car_name = "ds"

    data_class.data = await data_class.get_page_info(car_name=car_name)

    full_data = await get_full_data(data_class)
    full_data["request"] = request
    full_data["active_tab"] = car_name

    return templates.TemplateResponse(
        "index.html",
        full_data
    )


@app.get("/ford", response_class=HTMLResponse)
async def ag_electro(request: Request):
    data_class = Data()

    car_name = "ford"

    data_class.data = await data_class.get_page_info(car_name=car_name)

    full_data = await get_full_data(data_class)
    full_data["request"] = request
    full_data["active_tab"] = car_name

    return templates.TemplateResponse(
        "index.html",
        full_data
    )


@app.get("/hyundai", response_class=HTMLResponse)
async def ag_electro(request: Request):
    data_class = Data()

    car_name = "hyundai"

    data_class.data = await data_class.get_page_info(car_name=car_name)

    full_data = await get_full_data(data_class)
    full_data["request"] = request
    full_data["active_tab"] = car_name

    return templates.TemplateResponse(
        "index.html",
        full_data
    )


@app.get("/kia", response_class=HTMLResponse)
async def ag_electro(request: Request):
    data_class = Data()

    car_name = "kia"

    data_class.data = await data_class.get_page_info(car_name=car_name)

    full_data = await get_full_data(data_class)
    full_data["request"] = request
    full_data["active_tab"] = car_name

    return templates.TemplateResponse(
        "index.html",
        full_data
    )


@app.get("/mg", response_class=HTMLResponse)
async def ag_electro(request: Request):
    data_class = Data()

    car_name = "mg"

    data_class.data = await data_class.get_page_info(car_name=car_name)

    full_data = await get_full_data(data_class)
    full_data["request"] = request
    full_data["active_tab"] = car_name

    return templates.TemplateResponse(
        "index.html",
        full_data
    )


@app.get("/mitsubishi", response_class=HTMLResponse)
async def ag_electro(request: Request):
    data_class = Data()

    car_name = "mitsubishi"

    data_class.data = await data_class.get_page_info(car_name=car_name)

    full_data = await get_full_data(data_class)
    full_data["request"] = request
    full_data["active_tab"] = car_name

    return templates.TemplateResponse(
        "index.html",
        full_data
    )


@app.get("/nissan", response_class=HTMLResponse)
async def ag_electro(request: Request):
    data_class = Data()

    car_name = "nissan"

    data_class.data = await data_class.get_page_info(car_name=car_name)

    full_data = await get_full_data(data_class)
    full_data["request"] = request
    full_data["active_tab"] = car_name

    return templates.TemplateResponse(
        "index.html",
        full_data
    )


@app.get("/peugeot", response_class=HTMLResponse)
async def ag_electro(request: Request):
    data_class = Data()

    car_name = "peugeot"

    data_class.data = await data_class.get_page_info(car_name=car_name)

    full_data = await get_full_data(data_class)
    full_data["request"] = request
    full_data["active_tab"] = car_name

    return templates.TemplateResponse(
        "index.html",
        full_data
    )


@app.get("/renault", response_class=HTMLResponse)
async def ag_electro(request: Request):
    data_class = Data()

    car_name = "renault"

    data_class.data = await data_class.get_page_info(car_name=car_name)

    full_data = await get_full_data(data_class)
    full_data["request"] = request
    full_data["active_tab"] = car_name

    return templates.TemplateResponse(
        "index.html",
        full_data
    )


@app.get("/skoda", response_class=HTMLResponse)
async def ag_electro(request: Request):
    data_class = Data()

    car_name = "skoda"

    data_class.data = await data_class.get_page_info(car_name=car_name)

    full_data = await get_full_data(data_class)
    full_data["request"] = request
    full_data["active_tab"] = car_name

    return templates.TemplateResponse(
        "index.html",
        full_data
    )


@app.get("/vag-service", response_class=HTMLResponse)
async def ag_electro(request: Request):
    data_class = Data()

    car_name = "vag-service"

    data_class.data = await data_class.get_page_info(car_name=car_name)

    full_data = await get_full_data(data_class)
    full_data["request"] = request
    full_data["active_tab"] = car_name

    return templates.TemplateResponse(
        "index.html",
        full_data
    )


@app.get("/autogroup-service", response_class=HTMLResponse)
async def ag_electro(request: Request):
    data_class = Data()

    car_name = "autogroup-service"

    data_class.data = await data_class.get_page_info(car_name=car_name)

    full_data = await get_full_data(data_class)
    full_data["request"] = request
    full_data["active_tab"] = car_name

    return templates.TemplateResponse(
        "index.html",
        full_data
    )


@app.get("/electro", response_class=HTMLResponse)
async def ag_electro(request: Request):
    data_class = Data()

    car_name = "electro"

    data_class.data = await data_class.get_page_info(car_name=car_name)

    full_data = await get_full_data(data_class)
    full_data["request"] = request
    full_data["active_tab"] = car_name

    return templates.TemplateResponse(
        "index.html",
        full_data
    )


@app.get("/chery", response_class=HTMLResponse)
async def ag_electro(request: Request):
    data_class = Data()

    car_name = "chery"

    data_class.data = await data_class.get_page_info(car_name=car_name)

    full_data = await get_full_data(data_class)
    full_data["request"] = request
    full_data["active_tab"] = car_name

    return templates.TemplateResponse(
        "index.html",
        full_data
    )

@app.get("/lts", response_class=HTMLResponse)
async def ag_electro(request: Request):
    data_class = Data()

    car_name = "lts"

    data_class.data = await data_class.get_page_info(car_name=car_name)

    full_data = await get_full_data(data_class)
    full_data["request"] = request
    full_data["active_tab"] = car_name

    return templates.TemplateResponse(
        "index.html",
        full_data
    )