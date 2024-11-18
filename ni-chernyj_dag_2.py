import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import requests
import json
from urllib.parse import urlencode


from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

link = "http://dl.dropboxusercontent.com/scl/fi/w0dj3eipa5zxex6d0d2v5/vgsales.csv?rlkey=p0swaljqdf52adlychaf3n5dl&dl=0"


default_args = {
    'owner': 'ni-chernyj',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 10, 31)
}

chat_id = 1038448919
bot_token = '7567701918:AAHq1r9f4GvJjn1KYUIawYY__FYx6PhEH1E'

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Даг {dag_id} успешно выполнен! Дата выполнения: {date}'
    params = {'chat_id': chat_id, 'text': message}
    base_url = f'https://api.telegram.org/bot{bot_token}/'
    url = base_url + 'sendMessage?' + urlencode(params)
    resp = requests.get(url)
    
@dag(default_args=default_args,schedule_interval='0 13 * * *', catchup=False)
def ni_chernyj_second_dag():
    @task(retries=3)
    def get_data():
        sales = pd.read_csv(link)
        return sales

    @task(retries=4, retry_delay=timedelta(10))
    def get_biggest_sales(sales):
        biggest_sales = sales.query('Year == 2008').groupby('Name',as_index=False).agg({'Global_Sales':'sum'})\
        .sort_values('Global_Sales',ascending=False).reset_index(drop=True).iloc[:1]
        return biggest_sales

    @task(retries=4, retry_delay=timedelta(10))
    def get_most_popular_genres_EU(sales):
        most_popular_genres_EU =sales.query('Year == 2008').groupby('Genre',as_index=False).agg({'EU_Sales':'sum'})\
        .sort_values('EU_Sales',ascending=False).reset_index(drop=True).iloc[:5]
        return most_popular_genres_EU

    @task(retries=4, retry_delay=timedelta(10))
    def get_more_million_sales_top(sales):
        more_million_sales_top=sales.query('Year == 2008').groupby(['Genre','Name'],as_index=False).agg({'NA_Sales':'sum'})\
        .sort_values('NA_Sales', ascending=False)
        more_million_sales_top['more_than_billion'] = more_million_sales_top.NA_Sales > 1
        more_million_sales_top.query("more_than_billion == True").groupby('Genre',as_index=False)\
        .agg({'more_than_billion':'count'}).sort_values('more_than_billion',ascending=False).reset_index(drop=True).iloc[:3]
        return more_million_sales_top

    @task(retries=4, retry_delay=timedelta(10))
    def get_highest_average(sales):
        highest_average=sales.query('Year == 2008').groupby('Publisher',as_index=False).agg({'JP_Sales':'mean'})\
        .sort_values('JP_Sales',ascending=False).reset_index(drop=True).iloc[:3]
        return highest_average

    @task(retries=4, retry_delay=timedelta(10))
    def get_diff_EU_JP(sales):
        diff_EU_JP = sales.query("Year == 2008").groupby('Name',as_index=False).agg({'EU_Sales':'sum','JP_Sales':'sum'})
        diff_EU_JP['diff'] =diff_EU_JP.EU_Sales - diff_EU_JP.JP_Sales
        diff_EU_JP = diff_EU_JP.sort_values('diff',ascending=False).reset_index(drop=True).iloc[:5][['Name','diff']]
        return diff_EU_JP
    
    
    @task(on_success_callback=send_message)
    def print_data(biggest_sales, most_popular_genres_EU,more_million_sales_top,highest_average,diff_EU_JP):
        context = get_current_context()
        date = context['ds']
        
        print(f'===================1. Самая продаваемая игра в мире на {date}:')
        print(biggest_sales)
        
        print(f'===================2. Самые продаваемые жанры в Европе на {date}')
        print(most_popular_genres_EU)
        
        print(f'============3.Платформа с наибольшим количеством игр, проданных более чем миллионным тиражом в Северной Америке на {date}:')
        print(more_million_sales_top)
        
        print(f'===================4.Издатель с самыми высокими средними продажами в Японии на {date}:')
        print(highest_average)
        
        print(f'===================5. Количество игр, проданных лучше в Европе, чем в Японии на {date}:')
        print(diff_EU_JP)
    
    sales = get_data()
    biggest_sales = get_biggest_sales(sales)
    most_popular_genres_EU = get_most_popular_genres_EU(sales)
    more_million_sales_top = get_more_million_sales_top(sales)
    highest_average = get_highest_average(sales)
    diff_EU_JP = get_diff_EU_JP(sales)
    
    print_data(biggest_sales, most_popular_genres_EU,more_million_sales_top,highest_average,diff_EU_JP) 
    
ni_chernyj_second_dag = ni_chernyj_second_dag()

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    