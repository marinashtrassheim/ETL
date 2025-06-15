# ETL
ETL Итоговая работа
Задание 1 
Работа с Yandex DataTransfer
Требуется перенести данные из Managed Service for YDB в объектное хранилище Object Storage. Выполнить необходимо с использованием сервиса Data Transfer.
Создать БД Yandex DataBase. 
Подготовить данные: 
transactions_v2 
Создать трансфер в Object Storage 
Проверить работоспособность трансфера 

1. Создаем ybd в Yandex Cloud + создаем таблицу transactions_v2

```
CREATE TABLE transactions_v2
(
    `msno` String,
    `payment_method_id` Int32,
    `payment_plan_days` Int32,
    `plan_list_price` Int32,
    `actual_amount_paid` Int32,
    `is_auto_renew` Int32,
    `transaction_date` Int32,
    `membership_expire_date` Int32,
    `is_cancel` Int32,
    PRIMARY KEY (`msno`)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = DISABLED,
    AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048,
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1,
    KEY_BLOOM_FILTER = DISABLED
);
```
