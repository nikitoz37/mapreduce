import argparse
import re
import json
import os

import requests
from flask import Flask, request
from bs4 import BeautifulSoup
import asyncio
import aiohttp
import psycopg2


URL_TXT = 'urls.txt'
TOP_WORDS_COUNT = 30
FILES_COUNT = 8
SLAVES_COUNT = 2
SLAVES_ADDRESS = (
    'http://127.0.0.1:5001/slave/run',
    'http://127.0.0.1:5002/slave/run',
)
CACHE_SIZE = 1000

DB_NAME = "postgres", 
USER = "postgres", 
PASSWORD = "123456", 
HOST = "127.0.0.1", 
PORT = "5432"


parser = argparse.ArgumentParser()
parser.add_argument('--host', type=str, required=True)
parser.add_argument('--port', type=int, required=True)
args = parser.parse_args()


app = Flask(__name__)


def get_urls(file_name: str) -> tuple:
    urls = []
    with open(file_name, 'r', encoding='utf-8') as file:
        for line in file:
            urls.append(line[:-1])
    return tuple(urls)


def get_filename(word: str):
    hash_int = 0
    for ch in word:
        hash_int = (hash_int * 281 ^ ord(ch) * 997) & 0xFFFFFFFF
    return str(hash_int % FILES_COUNT) + ".json"


def write_word(key: str, value: int, cache_min_value: int):
    new_value = 0
    filename = get_filename(key)

    if not os.path.exists(filename) or os.path.getsize(filename) <= 2:
        if value > cache_min_value:
            new_value = value
        else:
            input_dict = {}
            input_dict.setdefault(key, value)
            with open(filename, 'w', encoding="utf-8") as file:
                try:
                    file.seek(0)
                    json.dump(input_dict, file, ensure_ascii=False)
                except Exception as e:
                    print('Error on writing empty JSON (', filename, ')')
    else:
        file_dict = {}
        with open(filename, 'r') as file:
            try:
                file_dict = json.load(file)
            except Exception as e:
                print('Error on reading existing JSON (', filename, ')')
        if file_dict:
            if file_dict.get(key) is None:
                if value > cache_min_value:
                    new_value = value
                else:
                    file_dict.setdefault(key, value)
                    with open(filename, 'w') as file:
                        try:
                            file.seek(0)
                            json.dump(file_dict, file, ensure_ascii=False)
                            file.truncate()
                        except Exception as e:
                            print('Error (', key, ' is None) on writing existing JSON (', filename, ')')
            else:
                new_value = file_dict[key] + value
                if new_value > cache_min_value:
                    try:
                        file_dict.pop(key)
                    except KeyError:
                        print('Error with .pop(' + key + ') in dict from existing JSON (', filename, ')')
                else:
                    file_dict[key] = new_value
                    new_value = 0
                with open(filename, 'w') as file:
                    try:
                        file.seek(0)
                        json.dump(file_dict, file, ensure_ascii=False)
                        file.truncate()
                    except Exception as e:
                        print('Error (', key, ' is not None) on writing existing JSON (', filename, ')')
    return new_value


def data_to_file(slave_words: dict[str, int]):
    cache = {}
    for key, value in slave_words.items():
        if cache.get(key) is not None:
            cache[key] += value
        else:
            if len(cache) < FILES_COUNT:
                cache.setdefault(key, value)
            else:
                cache_min_key = min(cache, key=cache.get)
                cache_min_value = cache.pop(cache_min_key)

                new_value = write_word(key, value, cache_min_value)
                if new_value > 0:
                    write_word(cache_min_key, cache_min_value, 0)
                    cache.setdefault(key, new_value)
                else:
                    cache.setdefault(cache_min_key, cache_min_value)

def get_top_from_files() -> dict:
    top_dict = {}
    for i in range(8):
        filename = str(i) + '.json'
        if (os.path.exists(filename)):
            with open(filename, 'r') as file:
                try:
                    file_dict = json.load(file)
                except Exception as e:
                    print(f'Ошибка чтения файла JSON {filename}')

            for j in range(5): # топ 5 слов
                max_key = max(file_dict, key=file_dict.get)
                max_value = file_dict.pop(max_key)
                top_dict[max_key] = max_value

    return top_dict
            


def data_to_db():
    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=USER, password=PASSWORD, host=HOST)
    except:
        print('Не возможно установить соединение')


    with conn.cursor as cursor:
        conn.autocommit = True
        
        query1 = "CREATE DATABASE metanit"
        cursor.execute(query1)

        query2 = "CREATE TABLE people (id SERIAL PRIMARY KEY, name VARCHAR(50),  age INTEGER)"
        cursor.execute(query2)
        
        all_users = cursor.fetchall()


def get_top_from_db() -> dict:
    pass


# ---------------------------------------------------


def get_data2(url: str):
    resp = requests.get(url)
    words = {}
    if (resp.status_code == 200):
        soup1 = BeautifulSoup(resp.text, 'html.parser')
        soup2 = soup1.find('body')
        match_list = re.findall(r'[А-Я][а-я]+', soup2.text.lower(), re.I)

        for item in match_list:
            if (item in words):
                words[item] += 1
            else:
                words[item] = 1
    return words



# Передача url-адресов ведомым
async def get_data(session, slave_addr: str, url: str) -> dict:
    async with session.post(url=slave_addr, json=json.dumps(url, ensure_ascii=False)) as resp:
        words_list = await resp.json(content_type=None)
        return words_list


async def main(work_list) -> list:
    async with aiohttp.ClientSession() as session:
        tasks = []
        for i in range(SLAVES_COUNT):
            print(work_list[i])
            tasks.append(asyncio.ensure_future(get_data(session, SLAVES_ADDRESS[0], work_list[i])))
        words = await asyncio.gather(*tasks)
        return words
        

# ---------------------------------------------------


@app.route('/master/info')
def master_info():
    return 'Hello, I am master-container!'


# Запустить MapReduce
@app.route('/master/run1', methods=['GET'])
def master_run1():
    cache = {}
    work_list = []
    url_tuple = get_urls(URL_TXT)
    for url in url_tuple:
        if (len(work_list) < SLAVES_COUNT):
            work_list.append(url)
        else:
            words = asyncio.run(main(work_list))
            work_list.clear()
            #print(words)

            for page in words:
                for key_page, value_page in page[0].items():
                    if (key_page in cache):
                        cache[key_page] += value_page
                    else:
                        cache[key_page] = value_page

            if (len(cache) > CACHE_SIZE):
                print('сброс')
                sorted_cache = dict(sorted(cache.items(), key=lambda item: item[1], reverse=True)) # сортировка значений словаря по убыванию
                # отправка в бд
                #cache.clear()
                break
            else:
                if (key_page in cache):
                    cache[key_page] += value_page
                else:
                    cache[key_page] = value_page

    return json.dumps(sorted_cache, indent = 4, ensure_ascii=False)


# Запустить MapReduce
@app.route('/master/run2', methods=['GET'])
def master_run2():
    cache = {}
    url_tuple = get_urls(URL_TXT)
    work_list = []
    for url in url_tuple:
        if (len(work_list) < SLAVES_COUNT):
            work_list.append(url)
        else:
            words = asyncio.run(main(work_list))
            work_list.clear()
            print(words)

            for page in words:
                for key_page, value_page in page.items():
                    if (len(cache) > CACHE_SIZE):
                        sorted_cache = dict(sorted(cache.items(), key=lambda item: item[1], reverse=True)) # сортировка значений словаря по убыванию
                        data_to_file(sorted_cache)
                        cache.clear()
                    if (key_page in cache):
                        cache[key_page] += value_page
                    else:
                        cache[key_page] = value_page

    top_words = get_top_from_files()
    sorted_top_words= dict(sorted(top_words.items(), key=lambda item: item[1], reverse=True)) # сортировка значений словаря по убыванию
    return json.dumps(sorted_top_words, indent = 4, ensure_ascii=False)



@app.route('/slave/info')
def slave_info():
    return 'Hello, I am slave-container!'


@app.route('/slave/run', methods=['POST'])
def slave_run():
    json_data = request.get_json()
    url_list = []
    url_list.append(json.loads(json_data))
    words = get_data2(url_list[0])
    return json.dumps(words, indent = 4, ensure_ascii=False)


# ---------------------------------------------------


app.run(host=args.host, port=args.port, debug=False)





