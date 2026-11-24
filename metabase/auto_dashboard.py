#!/usr/bin/env python3
"""Создать 6 отчётов и дашборд BigDataNiFi через локальный Metabase API.

Запуск из C:\\MAI\\BigDataNiFi: py -3 .\\metabase\\auto_dashboard.py
Библиотека стандартная: дополнительных pip-пакетов не нужно.
"""

from __future__ import annotations

import getpass
import json
import sys
import urllib.error
import urllib.parse
import urllib.request

BASE_URL = "http://localhost:3005"
DB_NAME = "bigdata_nifi"
DASH_NAME = "BigDataNiFi — Sales Analytics"

# Название, тип графика, SQL, поле категорий, поле показателя
REPORTS = [
    (
        "01 — Выручка по месяцам (2021)", "line",
        "SELECT MAKE_DATE(year::int, month::int, 1) AS period, revenue "
        "FROM public.report_time_sales ORDER BY period",
        "period", "revenue",
    ),
    (
        "02 — Топ-10 товаров по продажам", "row",
        "SELECT product_name, SUM(units_sold) AS units_sold "
        "FROM public.report_product_sales GROUP BY product_name "
        "ORDER BY units_sold DESC, product_name LIMIT 10",
        "product_name", "units_sold",
    ),
    (
        "03 — Топ-10 клиентов по затратам", "row",
        "SELECT customer_email, total_spent "
        "FROM public.report_customer_sales ORDER BY total_spent DESC LIMIT 10",
        "customer_email", "total_spent",
    ),
    (
        "04 — Топ-10 магазинов по выручке", "bar",
        "SELECT store_name, revenue "
        "FROM public.report_store_sales ORDER BY revenue DESC LIMIT 10",
        "store_name", "revenue",
    ),
    (
        "05 — Топ-10 поставщиков по выручке", "bar",
        "SELECT supplier_name, revenue "
        "FROM public.report_supplier_sales ORDER BY revenue DESC LIMIT 10",
        "supplier_name", "revenue",
    ),
    (
        "06 — Качество товаров и отзывы", "table",
        "SELECT product_name, product_category, "
        "ROUND(average_rating::numeric, 2) AS average_rating, "
        "review_count, units_sold "
        "FROM public.report_product_quality "
        "ORDER BY average_rating DESC NULLS LAST, "
        "review_count DESC NULLS LAST LIMIT 20",
        None, None,
    ),
]


class ApiError(RuntimeError):
    pass


class MetabaseApi:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.token = None

    def call(self, method: str, path: str, data=None):
        url = self.base_url + path
        headers = {"Accept": "application/json"}
        body = None
        if data is not None:
            headers["Content-Type"] = "application/json"
            body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        if self.token:
            headers["X-Metabase-Session"] = self.token
        req = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=35) as resp:
                payload = resp.read().decode("utf-8")
                return json.loads(payload) if payload else None
        except urllib.error.HTTPError as exc:
            # Не выводить тело запроса: в нём мог быть пароль.
            raw = exc.read().decode("utf-8", errors="replace")[:250]
            # Сообщения сервера иногда включают пароль; не печатаем их для логина.
            if path == "/api/session":
                raw = "Проверь email и пароль от учётной записи Metabase."
            raise ApiError(f"{method} {path}: HTTP {exc.code}; {raw}") from exc
        except urllib.error.URLError as exc:
            raise ApiError(f"Нет подключения к {self.base_url}. Metabase запущен? {exc.reason}") from exc

    def login(self, email: str, password: str):
        response = self.call("POST", "/api/session", {"username": email, "password": password})
        self.token = response.get("id")
        if not self.token:
            raise ApiError("Metabase не вернул токен сессии")


def entries(result):
    """Metabase отдаёт коллекции и списком, и в формате {'data':[...]}"""
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        if isinstance(result.get("data"), list):
            return result["data"]
        if isinstance(result.get("items"), list):
            return result["items"]
    return []


def card_database_id(card):
    query = card.get("dataset_query") or {}
    return query.get("database") or card.get("database_id")


def main():
    print("\nBigDataNiFi Lab 5 — автоматическое создание дашборда\n")
    email = input("Email администратора Metabase (НЕ PostgreSQL): ").strip()
    if not email:
        raise ApiError("Email не может быть пустым")
    password = getpass.getpass("Пароль администратора Metabase (скрыт): ")
    api = MetabaseApi(BASE_URL)
    api.login(email, password)
    del password
    print("[OK] Вход в Metabase выполнен")

    dbs = entries(api.call("GET", "/api/database"))
    matches = [d for d in dbs if d.get("name", "").casefold() == DB_NAME.casefold() and d.get("engine") == "postgres"]
    if not matches:
        raise ApiError(f"В Metabase не найдена база {DB_NAME!r}. Найдены: {[x.get('name') for x in dbs]}")
    db_id = matches[0]["id"]
    print(f"[OK] Подключена PostgreSQL {DB_NAME} (database id={db_id})")

    try:
        all_cards = entries(api.call("GET", "/api/card"))
    except ApiError:
        all_cards = []
    available = [c for c in all_cards if not c.get("archived") and card_database_id(c) == db_id]

    # Предпочесть уже сохранённый вручную график, не создавать дубль.
    monthly = next((c for c in available if "выручка по месяцам" in c.get("name", "").casefold() and c.get("display") == "line"), None)
    if monthly:
        print(f"[OK] Использую твой готовый график: {monthly['name']} (id={monthly['id']})")

    # Коллекция, куда пользователь положил первый график; иначе "Наша аналитика".
    collection_id = monthly.get("collection_id") if monthly else None
    if collection_id == "root":
        collection_id = None
    if collection_id is None:
        try:
            collections = entries(api.call("GET", "/api/collection"))
            found = next((c for c in collections if c.get("name") == "Наша аналитика"), None)
            collection_id = found["id"] if found else None
        except ApiError:
            pass

    # Проверяем, не создавали ли дашборд этим скриптом в прошлый раз.
    try:
        dashboards = entries(api.call("GET", "/api/dashboard"))
    except ApiError:
        dashboards = []
    existing_dash = next((d for d in dashboards if d.get("name") == DASH_NAME), None)
    if existing_dash:
        dash_id = existing_dash["id"]
        print(f"[OK] Использую существующий дашборд (id={dash_id})")
    else:
        dashboard = api.call("POST", "/api/dashboard", {
            "name": DASH_NAME,
            "description": "Лабораторная №5: Kafka → Apache NiFi → PostgreSQL → Metabase. 10 000 записей, 10 CSV, шесть отчётов.",
            "collection_id": collection_id,
        })
        dash_id = dashboard["id"]
        print(f"[OK] Создан дашборд (id={dash_id})")

    card_ids = []
    for idx, (name, display, query, dim, metric) in enumerate(REPORTS):
        if idx == 0 and monthly:
            card = monthly
        else:
            card = next((c for c in available if c.get("name") == name), None)
            if card is None:
                settings = {"graph.dimensions": [dim], "graph.metrics": [metric]} if dim else {}
                card = api.call("POST", "/api/card", {
                    "name": name,
                    "type": "question",
                    "description": "Лабораторная №5: отчёт над PostgreSQL VIEW (данные NiFi).",
                    "display": display,
                    "collection_id": collection_id,
                    "visualization_settings": settings,
                    "dataset_query": {"database": db_id, "type": "native", "native": {"query": query}},
                })
                available.append(card)
                print(f"[OK] Создан отчёт: {name} (id={card['id']})")
            else:
                print(f"[OK] Уже существует: {name} (id={card['id']})")
        card_ids.append(card["id"])

    dashboard = api.call("GET", f"/api/dashboard/{dash_id}")
    old_cards = dashboard.get("dashcards") or dashboard.get("ordered_cards") or []
    existing_card_ids = {item.get("card_id") for item in old_cards}
    # Для повторных запусков сохраняем уже размещённые элементы с их ID.
    updated = [
        {
            "id": c["id"], "card_id": c.get("card_id"),
            "row": c.get("row", 0), "col": c.get("col", 0),
            "size_x": c.get("size_x", 12), "size_y": c.get("size_y", 6),
        }
        for c in old_cards
    ]
    # 24 колонки: один большой линейный график и 5 отчётов в два столбца.
    positions = [
        (0, 0, 24, 7),
        (7, 0, 12, 7), (7, 12, 12, 7),
        (14, 0, 12, 7), (14, 12, 12, 7),
        (21, 0, 24, 9),
    ]
    next_temp_id = -1
    for card_id, (row, col, size_x, size_y) in zip(card_ids, positions):
        if card_id in existing_card_ids:
            continue
        updated.append({
            "id": next_temp_id, "card_id": card_id,
            "row": row, "col": col, "size_x": size_x, "size_y": size_y,
        })
        next_temp_id -= 1

    response = api.call("PUT", f"/api/dashboard/{dash_id}", {"dashcards": updated})
    count = len(response.get("dashcards", [])) if isinstance(response, dict) else len(updated)
    print(f"\n[ГОТОВО] На дашборде {count} карточек")
    print(f"Открывай: {BASE_URL}/dashboard/{dash_id}")
    print("Для GitHub: сохрани скриншот дашборда и скрипт в репозитории.")


if __name__ == "__main__":
    try:
        main()
    except (ApiError, KeyError, ValueError) as exc:
        print(f"\n[ОШИБКА] {exc}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nОтменено")
        sys.exit(130)
