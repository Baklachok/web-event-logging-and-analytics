from aiohttp import web
from aiohttp_swagger3 import SwaggerDocs, SwaggerInfo, SwaggerUiSettings
from pydantic import BaseModel


# Pydantic модель, используем её для валидации в коде
class Item(BaseModel):
    name: str
    price: float


items: list[Item] = []

app = web.Application()

swagger = SwaggerDocs(
    app,
    swagger_ui_settings=SwaggerUiSettings(path="/docs"),
    info=SwaggerInfo(
        title="My API", version="1.0.0", description="Пример API с Pydantic и Swagger"
    ),
    components="components.yaml",  # подключаем файл с описанием схем
)


async def get_items(request: web.Request) -> web.Response:
    """
    Возвращает список товаров.
    ---
    summary: Получить все товары
    responses:
      '200':
        description: Список всех товаров
        content:
          application/json:
            schema:
              type: array
              items:
                $ref: "#/components/schemas/Item"
    """
    # Возвращаем список Pydantic моделей, swagger будет валидировать JSON
    return web.json_response([item.dict() for item in items])


async def add_item(request: web.Request) -> web.Response:
    """
    Создает новый товар.
    ---
    summary: Добавить товар
    requestBody:
      required: true
      content:
        application/json:
          schema:
            $ref: "#/components/schemas/Item"
    responses:
      '201':
        description: Товар создан
        content:
          application/json:
            schema:
              $ref: "#/components/schemas/Item"
    """
    data = await request.json()
    item = Item(**data)  # валидация pydantic
    items.append(item)
    return web.json_response(item.dict(), status=201)


# Регистрируем маршруты
swagger.add_routes([web.get("/items", get_items), web.post("/items", add_item)])

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=8080)
