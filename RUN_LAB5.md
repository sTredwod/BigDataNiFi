# Лабораторная №5 — Low-code ETL с Apache NiFi и Metabase

## Что реализовано

Пайплайн по заданию преподавателя:

1. Python Producer преобразует **10 CSV × 1000 строк** в **10 000 JSON-сообщений Kafka**.
2. **Apache NiFi** (не внешний скрипт) читает Kafka через `ConsumeKafkaRecord_2_6` и записывает необработанные записи в PostgreSQL через `PutDatabaseRecord` в режиме `UPSERT`.
3. SQL-скрипты создают **6 аналитических отчётов** как представления (VIEW) в PostgreSQL.
4. **Metabase** подключается к PostgreSQL для интерактивной визуализации отчетов.
5. Скрипт `nifi-setup/bootstrap.py` автоматически создаёт сам NiFi Process Group через официальный REST API. Описание и связи процессоров сохраняются внутри NiFi, а код автоматической сборки хранится в репозитории.

### Docker-сервисы

| Сервис | Адрес на Windows | Назначение |
|---|---|---|
| Kafka | `localhost:9095` | Приём JSON-событий |
| NiFi | http://localhost:8085/nifi | Визуальный low-code поток |
| PostgreSQL | `localhost:5437` | Хранилище необработанных записей и отчётов |
| Metabase | http://localhost:3005 | Дашборды |
| `kafka-init` | — | Создание топика `lab5_sales` |
| `nifi-setup` | — | Создание и запуск NiFi-процессоров |
| `producer` | — | Отправка 10 000 JSON-событий |

Порты отличаются от предыдущих лабораторных.

## Запуск с нуля

Скачайте проект, перейдите в корень в PowerShell и запустите:

```powershell
docker compose config -q
docker compose up --build -d
```

Первый запуск NiFi и Metabase может потребовать заметного времени. `nifi-setup` ожидает появления NiFi API, создаёт визуальный поток и запускает процессоры. Только **после успешного завершения `nifi-setup`** Docker запускает `producer`.

Проверка контейнеров:

```powershell
docker compose ps -a
```

Контейнеры `kafka-init`, `nifi-setup`, `producer` должны завершиться со статусом `Exited (0)`; Kafka, PostgreSQL, NiFi и Metabase должны работать (Up).

Если `nifi-setup` ещё выполняется, смотреть прогресс:

```powershell
docker compose logs -f nifi-setup
```

Остановить просмотр логов — `Ctrl+C`, контейнеры продолжат работу.

Проверка NiFi-потока:

```powershell
docker compose logs --tail=100 nifi-setup
```

Ожидаемый финал:

```text
SUCCESS: NiFi flow created and running.
Flow: ConsumeKafkaRecord_2_6 -> PutDatabaseRecord (UPSERT) -> PostgreSQL
```

Проверка Producer:

```powershell
docker compose logs --tail=100 producer
```

Ожидаемый финал:

```text
SUCCESS: produced 10000 JSON messages to Kafka
```

Важное различие: `SUCCESS` Producer означает только отправку в Kafka. Следующий запрос доказывает, что их сохранил NiFi в PostgreSQL.

## Проверка данных в PostgreSQL

После завершения Producer запросите количество записей:

```powershell
docker compose exec -T postgres psql -U student -d bigdata_nifi -c 'SELECT COUNT(*) FROM public.sales_raw;'
```

Проверка всех данных и SQL-отчётов:

```powershell
docker compose exec -T postgres psql -U student -d bigdata_nifi -v ON_ERROR_STOP=1 -f /sql/03_check_lab5.sql
```

Должно быть **10 000 строк**, **10 000 уникальных `event_id`**, **10 исходных файлов × 1000 записей**. Если количество меньше, проверьте NiFi UI, очередь `Parsed Kafka records` и журналы:

```powershell
docker compose logs --tail=150 nifi
```

SQL-представления (`CREATE OR REPLACE VIEW`) создаются скриптом `postgres/init/02_reports.sql`, а не самим NiFi. Сырые записи в `sales_raw` остаются без агрегации.

## Просмотр NiFi

Откройте http://localhost:8085/nifi (без логина: только локальное тестовое окружение).

На главном canvas найдите группу `Lab 5 - Kafka JSON to PostgreSQL`; дважды щёлкните по ней.

Внутри должна быть схема:

```text
[01 - Consume Kafka JSON events] ----success----> [02 - UPSERT raw records into PostgreSQL]
             |                                              |
       parse.failure                                   failure/retry
             |                                              |
             +----------> [03 - Log failed events] <---------+
```

У процессоров должны быть зелёные индикаторы (Running). У успешной ветки после обработки очередь должна опустеть; счётчики сообщений и ошибок можно посмотреть через NiFi UI.

Controller Services внутри группы: `JsonTreeReader`, `JsonRecordSetWriter`, `PostgreSQL DBCP Connection Pool`.

## Визуализация в Metabase

1. Откройте http://localhost:3005; при первом открытии создайте **свой локальный администраторский аккаунт Metabase**.
2. Добавьте базу данных: *Admin → Databases → Add database → PostgreSQL* (названия пунктов могут немного отличаться).
3. Укажите **Host `postgres`**, **Port `5432`**, **Database `bigdata_nifi`**, **User `student`**, **Password `student123`**. **Не используйте `localhost:5437` внутри Metabase**: контейнер видит PostgreSQL по Docker-имени `postgres`.
4. Выполните синхронизацию схемы. Откройте *New → SQL query*. Выберите эту базу.
5. Из файла `metabase/queries.sql` выполняйте **по одному** каждый из шести `SELECT` (они разделены комментариями). Для каждого сохраните вопрос с понятным названием.
6. Выберите визуализацию: **Top products — горизонтальная столбчатая**, **Top customers — столбчатая**, **Time sales — линейная**, **Top stores — столбчатая**, **Top suppliers — столбчатая**, **Quality — точечная или столбчатая**.
7. Создайте новый Dashboard `BigDataNiFi — Sales Analytics` и добавьте все шесть вопросов.
8. Убедитесь, что на дашборде есть данные, и сделайте скриншот для отчёта/защиты.

Метрики, требуемые преподавателем, присутствуют в шести SQL-представлениях: рейтинги продаж, выручка по категориям, распределение по странам, динамика по месяцам, средний чек, отзывы и корреляция рейтингов с продажами.

Важно: CSV содержат продажи **только за 2021 год**, поэтому сравнить выручку между разными годами нельзя; временной отчёт показывает изменение **между месяцами** и итог за один год. Не следует выдумывать отсутствующие годы.

## Проверка подключения PostgreSQL в DBeaver

- Host: `localhost`
- Port: `5437`
- Database: `bigdata_nifi`
- User: `student`
- Password: `student123`

Проверить аналитические представления:

```sql
SELECT * FROM public.report_product_sales ORDER BY sales_rank LIMIT 10;
SELECT * FROM public.report_customer_sales ORDER BY spend_rank LIMIT 10;
SELECT * FROM public.report_time_sales ORDER BY year, month;
SELECT * FROM public.report_store_sales ORDER BY revenue_rank LIMIT 5;
SELECT * FROM public.report_supplier_sales ORDER BY revenue_rank LIMIT 5;
SELECT * FROM public.report_product_quality ORDER BY rating_rank_desc LIMIT 10;
```

## Повторная отправка данных

Если `producer` уже завершился, можно переотправить те же CSV:

```powershell
docker compose run --rm --no-deps producer
```

`event_id` и PostgreSQL UPSERT защищают `sales_raw` от дублирования записей. После повторной доставки должно остаться **10 000 записей**, а не 20 000.

## Остановка

Сохранить PostgreSQL, Metabase и NiFi-тома:

```powershell
docker compose down
```

**Важно:** Kafka в данном учебном Compose не имеет отдельного persistent volume. При удалении её контейнера сообщения могут быть потеряны, но их можно воспроизвести повторным запуском Producer. PostgreSQL и NiFi сохраняются в Docker volumes.

Полный сброс этой лабораторной (удалит данные и настройки Metabase!) — **только если это действительно необходимо**:

```powershell
docker compose down -v
```

## Безопасность

Эта сборка — локальная учебная демонстрация: NiFi работает без аутентификации по HTTP, тестовый пароль PostgreSQL указан в Compose. Все внешние порты привязаны к `127.0.0.1`, чтобы сервисы не были доступны с других компьютеров. **Не используйте эту конфигурацию в общедоступной сети/производственной среде без TLS, аутентификации и секретов.**
