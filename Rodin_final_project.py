from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import pandas as pd
from collections import Counter
import asyncio
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import zipfile
import json
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from aiohttp import ClientSession
from collections import Counter

default_args = {
    'owner': 'rodin',
    'retries': 5,                        # Сколько раз перезапускать 
    'retry_delay': timedelta(minutes=2)  # Задержка на перезапуск 
}

def okved():
    df_telecom_companies=pd.DataFrame(columns=['name', 'full_name', 'inn', 'okved', 'kpp'])

    #Читаем список файлов в  egrul.json.zip
    with zipfile.ZipFile("/home/rtstudent/egrul.json.zip" , 'r') as zipobj:
        file_names = zipobj.namelist()
        for name in file_names:
            # Распаковываем один файл
            zipobj.extract(name)
            # Обрабатываем файл
            with open(name, 'r', encoding='UTF-8') as f:
                egrul=pd.read_json(f)
                for i in range(egrul.shape[0]):
                    try:
                        OKVED=egrul.iloc[i]['data']['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД']
                        if OKVED [:3]=='61.':
                            Name=egrul.iloc[i]['name']
                            Full_name=egrul.iloc[1]['full_name']
                            INN=egrul.iloc[i]['inn']
                            KPP=egrul.iloc[i]['kpp']
                            df_telecom_companies.loc[len(df_telecom_companies.index)]=[Name,Full_name,INN,OKVED,KPP]
                    except: pass
            # Удаляем файл
            path = Path(name)
            path.unlink()
    df_telecom_companies['name_new']=df_telecom_companies.name.apply(lambda x: x[5:-1:])
    print('Функция okved выполнена')
    return df_telecom_companies

async def vacancy():
    async def get_vacancy(url, session): # Делаем асинхронную функцию для загрузки данных по вакансии 
        async with session.get(url=url) as response:
            vacancy_json = await response.json()
            return vacancy_json

# Функция парсинга hh и формирование датафрейма с вакансиями 

    async def main(link_to_vacancies_0):          # Создаем функцию по формированию списка задач для асинхронного IO 
        async with ClientSession() as session: 
            tasks = []
            for url in link_to_vacancies_0:
                tasks.append(asyncio.create_task(get_vacancy(url, session)))

            results = await asyncio.gather(*tasks)
                        
            for job_details in results:                
                try:                           # делаем исключение на проверку что все ключи присутствуют 
                    company_name=job_details['employer']['name']  # берем название компании 
                    position=job_details['name']                  # берем название вакансии
                    job_description=job_details['description']    # берем описание вакансии
                    key_skills=[]                                 # формируем список skills 
                    for i in job_details['key_skills']:
                        key_skills.append(i['name'])
                    if len(key_skills)==0:                         # Проверяем что список skills не пустой 
                        print('У компании %s нет key_skills, её пропускаем'%(company_name))
                    else:
                        key_skills=', '.join(key_skills)
                        df_vacancies.loc[len(df_vacancies.index)]=[company_name, position, job_description, key_skills] # Записываем данные в dataframe
                except:
                    pass
                    print('Произошло исключение по запросу данных о вакансии по ссылке', url) 


    df_vacancies=pd.DataFrame(columns=['company_name','position','job_description','key_skills']) # создаем dataframe для данных по вакансиям
    page=0 # Задаем нуливую страницу для запроса 

    while df_vacancies.shape[0]<100:  # Перебераем страницы пока не получим 100 вакансий 
        url_0='https://api.hh.ru/vacancies' # Записываем ссылку на API HH
        params={
            'text':'middle python developer', #Текст фильтра
            'search_field':'name',
            'per_page':100, #Количество вакансий на одной странице
            'areas':[1,2], # Поиск по факансиям Москвы
            'page': page, # Индекс страницы поиска на HH
         }
        result = requests.get(url_0, params) # Получаем данные по ссылке 
        vacancies=result.json() # записываем полученные данные в переменную, обозначания что данные в формате json 
        if vacancies['items']==[]: # проверяем что есть данные по вакансии, в на последней странице (пустой) не будет данных
            break
     
        list_of_vacancies=[]    # Создаем список ссылок на вакансии с загруженной страницы 
        for vacancy in vacancies['items']:
            list_of_vacancies.append(vacancy['url'])

        await main(list_of_vacancies)
        page+=1
    print('Функция vacancy выполнена')
    return df_vacancies

def insert_okved():
    df = okved()
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')
    connection = sqlite_hook.get_conn()    
    df.to_sql('telecom_companies', con=connection, if_exists='append', index=False)

def insert_vacancy():
    df = asyncio.run(vacancy())
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')
    connection = sqlite_hook.get_conn()    
    df.to_sql('vacancie', con=connection, if_exists='append', index=False)

def result(df_1,df_2):
    res_df = pd.merge(df_1[['company_name', 'key_skills', 'position']],
                      df_2[['name', 'name_new']],
                      left_on='company_name', right_on='name_new')
    print('Функция result выполнена')
    return res_df

def top_skills(df):
    df['key_skills']=df['key_skills'].apply(lambda x: x.split(', '))
    skills= Counter()                        # создаем счетсик skills
    for i in df['key_skills']:               # Пробегаемся по колонке key_skills 
        skills.update(i)                     # Обновляем данные в счетчике
    print('Функция top_skills выполнена') 
    return skills.most_common(10)            # выводим топ 10 

def print_skills (res):
    for i in range(len(res)):
        print(f'На {i+1} месте идет навык {res[i][0]} он встречается {res[i][1]} раз!')
    print('Функция print_skills выполнена')

def final_project (): 
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')
    connection = sqlite_hook.get_conn()
    df_1 = pd.read_sql("SELECT * FROM vacancie", connection)
    df_2 = pd.read_sql("SELECT * FROM telecom_companies", connection)    
    print_skills(top_skills(result(df_1,df_2)))


with DAG(                                                           # Описание графа 
    dag_id='rodin_final_project',                                  # Указывваем название которое будет в Airflow 
    default_args=default_args,                                      # агрументы по умолчанию смотри выше 
    description='This is final prodject middle python developer',   # Описание 
    start_date=datetime(2023, 7, 15, 8),                            # Когда запускается (старта) 
    schedule_interval='@once'                                      # Когда запускаем: ежедневно 
) as dag:
    
    task01 = SqliteOperator(
        task_id='create_table_telecom_companies',
        # Название соединения в AirFlow
        sqlite_conn_id='sqlite_default',
        sql="""
                CREATE TABLE IF NOT EXISTS telecom_companies(
                 name TEXT,
                 full_name TEXT,
                 inn TEXT,
                 okved TEXT,
                 kpp TEXT,
                 name_new TEXT
                 )
                 """
    )

    task02 = SqliteOperator(
        task_id='create_table_vacancie',
        sqlite_conn_id='sqlite_default',
        sql="""
            CREATE TABLE IF NOT EXISTS vacancie(
             company_name TEXT,
             position TEXT,
             job_description TEXT,
             key_skills TEXT
             )
             """
    )
    task1 = PythonOperator(    
        task_id='insert_okved',
        python_callable=insert_okved,    
    )
   
    task2 = PythonOperator(     
        task_id='insert_vacancy',
        python_callable=insert_vacancy,    
    )

    task3 = PythonOperator(    
        task_id='final_project',
        python_callable=final_project    
    )


    task01 >> task1 >> task3
    task02 >> task2 >> task3