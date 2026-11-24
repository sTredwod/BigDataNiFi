BigDataNiFi: исправление русских названий после PowerShell

1) Распаковать в C:\MAI\BigDataNiFi, подтвердив замену файла
   metabase\auto_dashboard.py. Оригинальный auto_dashboard.py здесь
   восстановлен как UTF-8 и содержит исправление collection_id=root.

2) Открыть терминал VS Code в C:\MAI\BigDataNiFi
   py -3 .\metabase\repair_titles.py

3) Ввести учётные данные администратора Metabase. Пароль не сохраняется.
   Скрипт исправит ТОЛЬКО имена уже созданного дашборда id=2 и шести
   карточек id=41..46. Он не изменяет запросы или PostgreSQL.

4) Открыть http://localhost:3005/dashboard/2 и обновить страницу.

ВАЖНО: НЕ запускайте auto_dashboard.py второй раз ради починки текста:
это может создавать новые элементы вместо переименования.
