#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для выполнения двух основных задач:

1. Поиск информации об организациях по списку ИНН:
   a. Считывает список ИНН из файла traders.txt.
   b. Загружает данные организаций из файла traders.json.
   c. Находит записи организаций с этими ИНН и сохраняет ИНН, ОГРН и адрес в traders.csv.

2. Извлечение email-адресов из датасета сообщений ЕФРСБ:
   a. Определяет функцию для поиска email-адресов в произвольном тексте.
   b. Загружает набор данных (1000_efrsb_messages.json) с сообщениями, 
      включая поля publisher_inn, msg_text и другие вложенные поля.
   c. Рекурсивно обходит все строковые поля записи и ищет email-адреса.
   d. Собирает email-адреса по каждому publisher_inn в множество.
   e. Сохраняет результат в файл emails.json.

Перед запуском убедитесь, что в рабочей папке находятся файлы:
- traders.txt              (список ИНН, по одному ИНН в строке)
- traders.json             (JSON-массив объектов организаций, каждый объект содержит 
                            поля "inn", "ogrn", "address" и возможно другие)
- 1000_efrsb_messages.json (JSON-массив сообщений ЕФРСБ с ключами "publisher_inn",
                            "msg_text" и/или другими строковыми полями)

Код оформлен в соответствии с PEP 8, все функции снабжены подробными комментариями.
"""

import json
import re
import csv
import os
import sys


def process_traders(txt_filename: str, json_filename: str, output_csv_filename: str) -> None:
    """
    Считывает список ИНН из текстового файла, загружает данные организаций из JSON,
    ищет совпадения по ИНН и сохраняет ИНН, ОГРН и адрес организаций в CSV.

    :param txt_filename: имя файла с ИНН (traders.txt)
    :param json_filename: имя JSON-файла с данными организаций (traders.json)
    :param output_csv_filename: имя результирующего CSV-файла (traders.csv)
    """
    # Попытка открыть файл со списком ИНН
    try:
        with open(txt_filename, 'r', encoding='utf-8') as txt_file:
            # Предполагаем, что в каждой строке указан один ИНН.
            # Убираем пустые строки и пробельные символы по краям.
            inn_list = [line.strip() for line in txt_file if line.strip()]
    except FileNotFoundError:
        print(f"Ошибка: файл '{txt_filename}' не найден.", file=sys.stderr)
        return
    except Exception as e:
        print(f"Ошибка при чтении файла '{txt_filename}': {e}", file=sys.stderr)
        return

    # Если список ИНН пустой, сообщаем и завершаем функцию
    if not inn_list:
        print(f"Предупреждение: файл '{txt_filename}' не содержит ни одного ИНН.", file=sys.stderr)
        return

    # Загружаем данные организаций из JSON-файла
    try:
        with open(json_filename, 'r', encoding='utf-8') as json_file:
            organizations = json.load(json_file)
            # Ожидаем, что organizations — это список словарей вида:
            # [{"inn": "77010127248512", "ogrn": "1027700132195", 
            #   "address": "г. Москва, ул. ...", ...}, ...]
    except FileNotFoundError:
        print(f"Ошибка: файл '{json_filename}' не найден.", file=sys.stderr)
        return
    except json.JSONDecodeError as e:
        print(f"Ошибка при разборе JSON в файле '{json_filename}': {e}", file=sys.stderr)
        return
    except Exception as e:
        print(f"Ошибка при чтении файла '{json_filename}': {e}", file=sys.stderr)
        return

    # Проверяем, что загруженные данные действительно список
    if not isinstance(organizations, list):
        print(f"Ошибка: ожидается, что '{json_filename}' содержит JSON-массив.", file=sys.stderr)
        return

    # Строим индекс (словарь) для быстрого поиска организации по ИНН.
    # Если запись не содержит поля 'inn', пропускаем её.
    org_index = {}
    for org in organizations:
        inn_value = org.get('inn')
        if not inn_value:
            # Пропускаем записи без поля 'inn' или с пустым значением
            continue
        # В случае дублирования ИНН последняя встреченная запись перезапишет предыдущую
        org_index[inn_value] = org

    # Открываем CSV-файл для записи результатов
    try:
        with open(output_csv_filename, 'w', encoding='utf-8', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            # Записываем заголовок: inn, ogrn, address
            csv_writer.writerow(['inn', 'ogrn', 'address'])

            # Для каждого ИНН из списка ищем соответствующую запись в org_index
            for inn in inn_list:
                org = org_index.get(inn)
                if org:
                    # Если организация найдена, извлекаем поля 'ogrn' и 'address'
                    # Если какой-то из полей отсутствует, подставляем пустую строку
                    ogrn_value = org.get('ogrn', '')
                    address_value = org.get('address', '')
                    csv_writer.writerow([inn, ogrn_value, address_value])
                else:
                    # Если запись не найдена, выводим предупреждение и пропускаем
                    print(f"Предупреждение: организация с ИНН '{inn}' не найдена "
                          f"в файле '{json_filename}'.", file=sys.stderr)
    except Exception as e:
        print(f"Ошибка при записи в CSV-файл '{output_csv_filename}': {e}", file=sys.stderr)
        return

    print(f"Готово: информация об организациях сохранена в '{output_csv_filename}'.")


def find_emails_in_text(text: str) -> list:
    """
    Ищет в переданном тексте все email-адреса и возвращает список найденных совпадений.
    Если email-адреса не найдены, возвращает пустой список.

    :param text: строка, в которой нужно искать email-адреса
    :return: список всех найденных email-адресов (каждый встречается столько раз, сколько найден)
    """
    # Регулярное выражение для поиска email-адресов:
    # - Локальная часть: буквы, цифры, точки, подчёркивания, проценты, плюсы, дефисы
    # - Символ '@'
    # - Домен: буквы, цифры, точки, дефисы
    # - Точка и от 2 до более букв
    EMAIL_REGEX = re.compile(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}')
    return EMAIL_REGEX.findall(text)


def collect_strings(obj) -> list:
    """
    Рекурсивно обходит переданный объект (словарь, список или строку) и собирает все
    строковые значения в один список. Это позволяет искать email-адреса не только
    в полях верхнего уровня, но и в любых вложенных структурах.

    :param obj: объект, который может быть строкой, словарём, списком или иным типом
    :return: список всех строк, найденных внутри переданного объекта
    """
    strings = []

    if isinstance(obj, str):
        # Если объект — строка, добавляем его
        strings.append(obj)
    elif isinstance(obj, dict):
        # Если объект — словарь, рекурсивно обходим все значения
        for value in obj.values():
            strings.extend(collect_strings(value))
    elif isinstance(obj, list):
        # Если объект — список, рекурсивно обходим все элементы
        for item in obj:
            strings.extend(collect_strings(item))
    # Для других типов (int, float, None и т. д.) ничего не делаем

    return strings


def extract_emails(dataset_filename: str, output_json_filename: str) -> None:
    """
    Извлекает email-адреса из всех строковых полей записей JSON-датасета и
    собирает их в словарь: ключ — publisher_inn, значение — множество email-адресов.
    Результат сохраняется в файл emails.json.

    Возвращает None, но при ошибках печатает диагностические сообщения в stderr.

    :param dataset_filename: имя JSON-файла с данными сообщений 
                             (1000_efrsb_messages.json)
    :param output_json_filename: имя файла для сохранения результата (emails.json)
    """
    # Открываем JSON-файл с сообщениями
    try:
        with open(dataset_filename, 'r', encoding='utf-8') as dataset_file:
            messages = json.load(dataset_file)
            # Ожидаем, что messages — это список словарей, например:
            # [
            #   {
            #     "publisher_inn": "77010127248512",
            #     "msg_text": "Текст сообщения с email: example@mail.ru...",
            #     "contacts": {"support_email": "support@company.ru"},
            #     ...
            #   },
            #   ...
            # ]
    except FileNotFoundError:
        print(f"Ошибка: файл '{dataset_filename}' не найден.", file=sys.stderr)
        return
    except json.JSONDecodeError as e:
        print(f"Ошибка при разборе JSON в файле '{dataset_filename}': {e}", file=sys.stderr)
        return
    except Exception as e:
        print(f"Ошибка при чтении файла '{dataset_filename}': {e}", file=sys.stderr)
        return

    # Проверяем, что загруженные данные — это список
    if not isinstance(messages, list):
        print(f"Ошибка: ожидается, что '{dataset_filename}' содержит JSON-массив.", file=sys.stderr)
        return

    # Словарь для накопления email-адресов по каждому publisher_inn
    publishers_emails = {}

    # Проходим по каждой записи (сообщению) в списке
    for record in messages:
        # Извлекаем ИНН публикатора
        publisher_inn = record.get('publisher_inn')
        if not publisher_inn:
            # Пропускаем записи без поля publisher_inn или с пустым значением
            continue

        # Инициализируем пустое множество для данного publisher_inn, 
        # если ещё не создавали
        if publisher_inn not in publishers_emails:
            publishers_emails[publisher_inn] = set()

        # Рекурсивно собираем все строковые поля из записи
        all_strings = collect_strings(record)

        # Для каждой собранной строки ищем email-адреса
        for text in all_strings:
            found_emails = find_emails_in_text(text)
            # Добавляем найденные email-адреса в множество (дубликаты автоматически устраняются)
            for email in found_emails:
                publishers_emails[publisher_inn].add(email)

    # Преобразуем множества во вложенные списки для корректной сериализации в JSON
    serializable_dict = {
        inn: list(emails_set) for inn, emails_set in publishers_emails.items()
    }

    # Записываем результат в выходной JSON-файл
    try:
        with open(output_json_filename, 'w', encoding='utf-8') as out_file:
            json.dump(serializable_dict, out_file, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"Ошибка при записи в JSON-файл '{output_json_filename}': {e}", file=sys.stderr)
        return

    print(f"Готово: email-адреса собраны и сохранены в '{output_json_filename}'.")


def main() -> None:
    """
    Основная функция, выполняющая две части задания.

    1) Генерация файла traders.csv:
       - Берёт список ИНН из traders.txt
       - Сверяет с traders.json
       - Сохраняет соответствующие ИНН, ОГРН и адреса в traders.csv

    2) Извлечение email-адресов из датасета сообщений:
       - Обходит dataset_filename (1000_efrsb_messages.json)
       - Собирает все строковые поля, находит email-адреса
       - Сохраняет результат в emails.json

    Проверки наличия файлов для каждой части выполняются отдельно,
    чтобы отсутствие одного из файлов не останавливает выполнение другой части.
    """
    # Файлы для первой части
    traders_txt = 'traders.txt'
    traders_json = 'traders.json'
    traders_csv = 'traders.csv'

    # Файлы для второй части
    messages_json = '1000_efrsb_messages.json'
    emails_json = 'emails.json'

    # --- Первая часть: информация об организациях ---
    print("=== Часть 1: Обработка списка организаций ===")
    missing_for_traders = []
    for fname in (traders_txt, traders_json):
        if not os.path.isfile(fname):
            missing_for_traders.append(fname)

    if missing_for_traders:
        print("Ошибка: не найдены файлы для первой части:", 
              ', '.join(missing_for_traders), file=sys.stderr)
    else:
        # Если оба файла есть, запускаем процесс обработки организцаий
        process_traders(traders_txt, traders_json, traders_csv)

    # --- Вторая часть: извлечение email-адресов ---
    print("\n=== Часть 2: Извлечение email-адресов ===")
    if not os.path.isfile(messages_json):
        print(f"Ошибка: не найден файл '{messages_json}' для второй части.", 
              file=sys.stderr)
    else:
        extract_emails(messages_json, emails_json)


if __name__ == '__main__':
    main()
