import asyncio
import asyncpg
import aiohttp_jinja2
import io
import jinja2
import PIL
import PIL.Image
from aiohttp import web
from asyncpg.exceptions import UniqueViolationError
from pathlib import Path
from pyzbar.pyzbar import decode
from typing import Any, AsyncIterator, Dict


router = web.RouteTableDef()
# Используем декораторы для определения url

@router.get("/")
@aiohttp_jinja2.template("index.html")
async def index(request: web.Request) -> Dict[str, Any]:    
    ret = []
    pool = request.app["pool"]
    async with pool.acquire() as connection:
        async with connection.transaction():
            async for row in connection.cursor("SELECT ticket, qr, photo FROM tickets"):
                    ret.append(
                 {
                     "ticket": row["ticket"],
                     "qr": row["qr"],
                     "photo": row["photo"],
                 }
                    )
    return {"posts": ret}

@router.get("/new")
@aiohttp_jinja2.template("new.html")
async def new_post(request: web.Request) -> Dict[str, Any]:
    return {}

@router.post("/new")
@aiohttp_jinja2.template("edit.html")
async def new_post_apply(request: web.Request) -> Dict[str, Any]:
    pool = request.app['pool']
    post = await request.post()            
    ticket_num = post.get("ticket")
    qr = post.get("qr")    
    image = post.get("image")
    if qr:
        qr_content = qr.file.read()  
        qr_info = await decode_qr(pool, ticket_num, qr_content)
    if image:
        img_content = image.file.read()  
        image_info = await apply_image(pool, ticket_num, img_content)
    try:
        row = await pool.execute("INSERT INTO tickets (ticket, qr, photo) VALUES($1, $2, $3)", ticket_num, qr_info, image_info)
    except UniqueViolationError:
        print('Ooopps. This ticket number is already in a database')
        raise web.HTTPSeeOther(location=f"/")
    else:
        raise web.HTTPSeeOther(location=f"/{ticket_num}")

@router.get("/{post}")
@aiohttp_jinja2.template("view.html")
async def view_post(request: web.Request) -> Dict[str, Any]:
    ticket_num = request.match_info["post"]
    return {"ticket": await fetch_post(request, ticket_num)}

@router.get("/{post}/image")
async def render_post_image(request: web.Request) -> web.Response:
    ticket_num = request.match_info["post"]
    pool = request.app['pool']
    row = await pool.fetchrow("SELECT photo FROM tickets WHERE ticket = $1", ticket_num)
    content = row["photo"]
    return web.Response(body=content, content_type="image/jpeg")

async def decode_qr(
    pool, ticket_num: int, qr_content: bytes) -> None:
    buf = io.BytesIO(qr_content)  
    loop = asyncio.get_event_loop()
    img = PIL.Image.open(buf)
    dec_qr = await loop.run_in_executor(None, decode, img)
    # Создаем отдельный поток для блокирующей функции и декодируем qr код
    info = dec_qr[0].data.decode('ascii')
    return info
    
async def apply_image(
    pool, ticket_num: int, img_content: bytes) -> None:
    buf = io.BytesIO(img_content)  
    loop = asyncio.get_event_loop()
    img = PIL.Image.open(buf)
    new_img = await loop.run_in_executor(None, img.resize, (320, 240), PIL.Image.LANCZOS)
    # Создаем отдельный поток для блокирующей функции и изменяем фото до нужных размеров
    out_buf = io.BytesIO()
    new_img.save(out_buf, format="JPEG")
    image = out_buf.getvalue()
    return image
    
async def fetch_post(request, ticket_num: int) -> Dict[str, Any]:
    pool = request.app['pool']
    row = await pool.fetchrow("SELECT qr, photo FROM tickets WHERE ticket = $1", ticket_num)
    if row is None:
        raise RuntimeError(f"Post {ticket_num} doesn't exist")
    return {
        "ticket": ticket_num,
        "qr": row["qr"],
        "photo": row["photo"],
    }

async def init_db(app: web.Application):
    DSN = 'postgres://postgres:fatcartman@127.0.0.1:5432/tickets'
    db = await asyncpg.connect(DSN)
    yield
    await db.close()

async def init_app() -> web.Application:
    app = web.Application()
    DSN = 'postgres://postgres:fatcartman@127.0.0.1:5432/tickets'
    app['pool'] = await asyncpg.create_pool(DSN)
    app.add_routes(router)
    app.cleanup_ctx.append(init_db)
    aiohttp_jinja2.setup(
        app, loader=jinja2.FileSystemLoader(str(Path(__file__).parent / "templates"))
    )
    return app


if __name__ == '__main__':
    web.run_app(init_app())
