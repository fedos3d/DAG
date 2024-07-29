import time
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

"""
Test documentation
"""
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'tutorial',  # имя дага
default_args={
'depends_on_past': False,
'email': ['hdbda@yandex.ru'],
'email_on_failure': False,
'email_on_retry': False,
'retries': 1,
'retry_delay': timedelta(minutes=5)
},
description = 'a simple tutorial DAG',
schedule_interval = timedelta(days=1),
start_date = datetime(2024, 7, 27),
catchup = False,
tags = ['example'],
) as dag:
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )
    t2 = BashOperator(
        task_id='sleep',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )
    t1.doc_md = dedent(
        """\
    ### Task Documentation
    You can bla bla
    """
    )
    dag.doc_md = __doc__
    dag.doc_md = """
        This is a documentation placed anywhere
        """
    templated_command = dedent(
        """
        {% for i in range(5) %}
            echo "{{ ds }}"
            echo "{{ macros.ds_add(ds, 7)}}"
        {% endfor %}
        """ )
    # здесь используется шаблонизация через Jinja
    t3 = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command,
    )

    def print_context(ds, **kwargs):
        """Primer PythonOperator"""
        # через синтаксис **kwargs можно получить словарь
        print(kwargs)
        print(ds)
        return 'Whatever you return gets printed in the logs'

    t4=PythonOperator(
        task_id = 'print_the_context',
        python_callable=print_context,
    )

    def my_sleeping_function(random_base):
        """Заснуть на рандом_басе секунд"""
        time.sleep(random_base)

    #Генерируем таски в цикле
    for i in range(5):
        task = PythonOperator(
            task_id='sleep_for_'+ str(i),
            python_callable=my_sleeping_function,
            #передаем в аргумент рандом_басе значение float(i)/10
            op_kwargs={'random_base': float(i)/10},
        )
        t4 >> task
    # последовательность задач
    t1 >> [t2, t3] >> t4 # t2 и t3 после t1( то же самое что t2<< t1, t3<<t1)
