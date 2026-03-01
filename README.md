## Сделано
1) Каталог initdb - DDL скрипты баз данных
2) Каталог haproxy - настройки ha-proxy для patroni
3) Кластер Patroni (master + replica) через etcd (3 хоста)
4) View v_cohort_mart - когортный анализ клиентов (inidb/20-order_service_db.sql)
5) scripts/csv-report.py - python скрипт вызывающий v_cohort_mart и сохраняющий результат в csv-файл в каталог report

### Сборка и запуск осуществляется через скрипт
Скрипт запускает сборку контейнеров и дожидается , когда реплика синхронизируется с мастером

```shell
bash ./build-and-start.sh
```

### Остановка
```shell
docker-compose down -v
```

### Генерация отчета
```shell
python scripts/csv_report.py
```

### Загрузка mock данных из файлов
```shell
python scripts/load_mock_data.py
```

### Master connection string 
1) jdbc:postgresql://localhost:5432/logistics_service_db
2) jdbc:postgresql://localhost:5432/order_service_db
3) jdbc:postgresql://localhost:5432/user_service_db

### Replica connection string 
1) jdbc:postgresql://localhost:6432/logistics_service_db
2) jdbc:postgresql://localhost:6432/order_service_db
3) jdbc:postgresql://localhost:6432/user_service_db