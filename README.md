# Change Data Capture с помощью Spark и Delta Lake
Демонстрационный проект. Можно манипулировать строками в postgres таблице, и наблюдать в консоле Idea
результирующую Delta Lake таблицу.

У нас будут подняты PostgreSQL база данных с наблюдаемоей таблицей, узел kafka и Debezium.
При каждой манипуляции с данными (в виде crud-операций), изменение в виде json'а будет добавляться в топик kafka (например добавление новой строки).
Spark streaming будет читать этот топик, и применять эти изменения к целевой Delta Lake таблице (то есть тоже добавим эту строку), и таким образом наша целевая таблица
будет синхронизироваться с таблицей источника, минуя загрузку всей таблицы с источника.

# Запуск docker-контейнеров
Скачиваем проект и разархивируем его в удобную вам папку. 
У меня, этой папкой будет:

`C:/Users/danst/IdeaProjects/cdc_example`

В командной строке от имени администратора устанавливаем папку с образами
как текущую: 

`cd C:\Users\danst\IdeaProjects\cdc_example`

Строим и поднимаем контейнеры:

`docker-compose up -d --build`

Подождите пару минут, пока контейнеры запустятся.

## Настроим Debezium
Выполните post запрос по url-адресу `localhost:8083/connectors/`:
```
{
 "name": "product-connector",
 "config": {
 "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
 "tasks.max": "1",
 "topic.prefix": "cdc",
 "database.hostname": "postgres-cdc",
 "database.port": "5432",
 "database.user": "postgres",
 "database.password": "postgres",
 "database.dbname" : "product",
 "database.server.name": "dbserver1",
 "database.whitelist": "exampledb",
 "database.history.kafka.bootstrap.servers": "kafka:9092",
 "database.history.kafka.topic": "schema-changes.product",
 "table.include.list": "public.(.*)",
 "decimal.handling.mode":"string"
 }
}
```

Указываются параметры kafka и наблюдаемые таблицы (наьлюдаем за всеми общедоступными таблицами в схеме public)

`"decimal.handling.mode":"string"`: Decimal-значение столбца будет указано в string, а не в массиве байтов, как по умолчанию.

## Теперь настроим источник
Подключитесь к источнику, и создайте таблицу:
```
create table if not exists product
(
	id int PRIMARY key GENERATED ALWAYS AS IDENTITY,
	name varchar not null,
	price decimal(38,10) not null
);
```
Добавьте одну строку предпологая, что это начальное состояние таблицы:

`insert into product(name, price) values('монитор', 30000);select * from product;`


## Запустите проект в idea
Запустите проект локально, например нажатием на "зелёный треугольник"

По очереди выполняйте следующие запросы, и после каждого - итоговая целевая таблица будет видна в консоле Idea. 
К целевой(delta lake) таблице будет применяться каждое изменение,
и она будет соответствовать таблице источника postgres.

`insert into product(name, price) values ('умная колонка', 10000);select * from product;`

`update product set price = 11000 where id = 2;select * from product;`

`delete from product where id = 1;select * from product;`

`delete from product;select * from product;`

`insert into product(name, price) values ('компьютерный стол', 20000);select * from product;`


