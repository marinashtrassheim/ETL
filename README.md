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
2. Скачиваем файл с данными и разбиваем его на части с помощью команды в консоли

```
split -l 10000 /Users/marinamarina/Downloads/transactions_v2.csv /Users/marinamarina/Downloads/transactions_part_
```
3. С помощью bash скрипта переносим данные в Yandex Cloud

```
#!/bin/bash

counter=0
for file in /Users/marinamarina/Downloads/transactions_part_*; do
  echo "Processing $file..."
  
  # Для первого файла пропускаем 1 строку (заголовок), для остальных - 0
  if [ $counter -eq 0 ]; then
    skip_rows=1
  else
    skip_rows=0
  fi
  
  ydb --endpoint "grpcs://ydb.serverless.yandexcloud.net:2135" \
      --database "/ru-central1/b1gpmcf3pbaq8qhmo2pu/etnss6b7au3eltqohr1v" \
      --sa-key-file "/Users/marinamarina/Downloads/authorized_key.json" \
      import file csv \
      --path transactions_v2 \
      --delimiter "," \
      --skip-rows $skip_rows \
      --null-value "" \
      "$file"
  
  if [ $? -eq 0 ]; then
    echo "Success: $file"
    ((counter++))
  else
    echo "Failed: $file"
    exit 1
  fi
done
```
Результат -> Данные в transactions_v2

4. Создаем Object Storage
5. Создаем Data Transfer, указывая источником ybd, приемником Object Storage и запускаем процесс трансфера. Результат -> Итог_трансфера_в_Object_Storage
