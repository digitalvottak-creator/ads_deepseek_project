from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates


from data_transformation import Data


app = FastAPI()

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
         "duration_graph": duration_graph, "duration_graph_points": duration_graph_points, "events_graph": events_graph,
         "events_graph_points": events_graph_points, "traffic_current_graph": traffic_current_graph,
         "traffic_current_graph_percent": traffic_current_graph_percent, "traffic_all_graph": traffic_all_graph,
         "traffic_all_graph_percent": traffic_all_graph_percent}

@app.get("/", response_class=HTMLResponse)
async def avatr(request: Request):
    data_class = Data()

    car_name="avatr"

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
