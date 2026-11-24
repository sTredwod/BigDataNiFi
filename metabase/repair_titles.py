#!/usr/bin/env python3
"""Repair Cyrillic report names corrupted by PowerShell 5.1 Get-Content/Set-Content.

Only rename the already created six report cards and dashboard; do not create
new dashboards, modify queries, or touch data.
"""
from __future__ import annotations

import getpass
import json
import sys
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

URL = "http://localhost:3005"
DASHBOARD_ID = 2
CARD_NAMES = {
    41: "01 — Выручка по месяцам (2021)",
    42: "02 — Топ-10 товаров по продажам",
    43: "03 — Топ-10 клиентов по затратам",
    44: "04 — Топ-10 магазинов по выручке",
    45: "05 — Топ-10 поставщиков по выручке",
    46: "06 — Качество товаров и отзывы",
}
DASHBOARD_NAME = "BigDataNiFi — Sales Analytics"


def request(method, route, token=None, payload=None):
    headers = {"Accept": "application/json"}
    if token:
        headers["X-Metabase-Session"] = token
    body = None
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    try:
        with urlopen(Request(URL + route, data=body, headers=headers, method=method), timeout=30) as r:
            raw = r.read().decode("utf-8")
            return json.loads(raw) if raw else None
    except HTTPError as e:
        if route == "/api/session":
            raise RuntimeError("Неверный email или пароль администратора Metabase") from None
        # Do not log credentials or raw responses.
        raise RuntimeError(f"{method} {route}: HTTP {e.code}") from None
    except URLError as e:
        raise RuntimeError(f"Metabase недоступен по адресу {URL}: {e.reason}") from None


def main():
    print("\nВосстановление русских названий в Metabase (без изменения SQL и данных)\n")
    email = input("Email администратора Metabase: ").strip()
    password = getpass.getpass("Пароль Metabase (скрыт): ")
    login = request("POST", "/api/session", payload={"username": email, "password": password})
    del password
    token = login["id"]
    dashboard = request("GET", f"/api/dashboard/{DASHBOARD_ID}", token)
    cells = dashboard.get("dashcards") or dashboard.get("ordered_cards") or []
    on_dashboard = {c.get("card_id") for c in cells}
    missing = set(CARD_NAMES) - on_dashboard
    if missing:
        raise RuntimeError(f"На дашборде {DASHBOARD_ID} нет карточек {sorted(missing)}; названия не изменены")

    for card_id, new_name in CARD_NAMES.items():
        c = request("GET", f"/api/card/{card_id}", token)
        if c.get("id") != card_id or c.get("dataset_query", {}).get("database") != 2:
            raise RuntimeError(f"Карточка {card_id} не относится к ожидаемой базе №2; остановка")

    for card_id, new_name in CARD_NAMES.items():
        request("PUT", f"/api/card/{card_id}", token, {"name": new_name})
        print(f"[OK] Карточка {card_id}: {new_name}")
    request("PUT", f"/api/dashboard/{DASHBOARD_ID}", token, {"name": DASHBOARD_NAME})
    print(f"[OK] Дашборд {DASHBOARD_ID}: {DASHBOARD_NAME}")
    print(f"\nГОТОВО: {URL}/dashboard/{DASHBOARD_ID}")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, KeyError, ValueError) as exc:
        print(f"\n[ОШИБКА] {exc}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nОтменено")
        sys.exit(130)
