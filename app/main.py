from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

app = FastAPI()

# Подключаем папку со статикой (css, js, картинки)
app.mount("/static", StaticFiles(directory="static"), name="static")

# Подключаем шаблоны
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
async def avatr(request: Request):
    images = [
        "/static/graphs/1346938215_14_01_2026/ads.jpeg",
        "/static/graphs/1346938215_14_01_2026/campaigns.jpeg",
        "/static/graphs/1346938215_14_01_2026/keywords.jpeg",
        "/static/graphs/1346938215_14_01_2026/search_terms.jpeg"
    ]
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "active_tab": "avatr", "images": images, "thought": "/static/thought_avatr.txt"}
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