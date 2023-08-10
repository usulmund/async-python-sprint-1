import threading
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Queue, Process
import queue
import csv
import logging
import json
import subprocess
from time import sleep, time

from utils import get_url_by_city_name
from external.client import YandexWeatherAPI
from utils import CITIES

AGGREGATION_TABLE = 'aggregate_data.csv'
ANALYSER_DIR = 'analyzer_result'
DATA_DIR = 'data_for_analysis'

city_for_calculation_queue: Queue = Queue()


class Service:
    @staticmethod
    def init_logger():
        logging.basicConfig(
            level=logging.DEBUG,
            filename='app-log.log',
            filemode='w',
            format='%(asctime)s: %(name)s - %(levelname)s - %(message)s'
        )

    @staticmethod
    def start_threads_for_data_fetching():
        city_names = [city_name for city_name in CITIES]
        with ThreadPoolExecutor() as thread_pool:
            thread_pool.map(
                DataFetchingTask.get_data_by_city_name,
                city_names,
                timeout=20
            )

    @staticmethod
    def start_processes_for_calculation_and_agregation():
        city_for_aggregation_queue = Queue()
        data_producer = Process(
            target=DataCalculationTask.analyze_data_by_city_name,
            args=(city_for_aggregation_queue,)
        )

        data_rater = Process(
            target=DataAggregationTask.aggregate_data,
            args=(city_for_aggregation_queue,)
        )

        data_producer.start()
        data_rater.start()

        processes = [data_producer, data_rater]
        TIMEOUT = 10
        start = time()
        while time() - start <= TIMEOUT:
            if not any(proc.is_alive() for proc in processes):
                break
            sleep(.1)
        else:
            logging.error(
                'TIMEOUT'
            )
            for proc in processes:
                proc.terminate()
                proc.join()

        data_producer.join()
        data_rater.join()


class DataFetchingTask:  # получение данных через API
    @staticmethod
    def get_data_by_city_name(city_name: str):
        thread = threading.current_thread()

        url_with_data = get_url_by_city_name(city_name)
        logging.debug(
            f'{city_name} -> {url_with_data}'
        )
        resp = YandexWeatherAPI.get_forecasting(url_with_data)
        logging.info(
            f'{city_name} is ready by thread {thread.name}\n'
        )
        city_for_calculation_queue.put(city_name)
        with open(
            f'{DATA_DIR}/{city_name}_response.json', 'w'
        ) as data_file:
            json.dump(resp, data_file)


class DataCalculationTask:  # вычисление погодных параметров
    @staticmethod
    def analyze_data_by_city_name(data_to_send_queue: Queue):
        while not city_for_calculation_queue.empty():
            city_name = city_for_calculation_queue.get()
            analysis_command = 'python3 external/analyzer.py ' \
                f'-i {DATA_DIR}/{city_name}_response.json ' \
                f'-o {ANALYSER_DIR}/{city_name}_output.json'
            analysis_command_list = analysis_command.split()
            analysis_process = subprocess.Popen(
                analysis_command_list,
                stdout=subprocess.PIPE,
                universal_newlines=True
            )
            output, error = analysis_process.communicate()
            logging.info(
                f'analyser put {city_name}'
            )
            data_to_send_queue.put(city_name)


class DataAggregationTask:  # объединение вычисленных данных
    @staticmethod
    def get_new_format_data(city_name: str):
        relevant_data = []
        json_file = f'{ANALYSER_DIR}/{city_name}_output.json'
        with open(json_file) as json_data:
            data = json.load(json_data)
        try:
            relevant_data = [
                [
                    day['date'],
                    day['temp_avg'],
                    day['relevant_cond_hours']
                ] for day in data['days']
            ]
        except KeyError:
            relevant_data = [[None, None, 0]]

        return relevant_data

    @staticmethod
    def get_avg_data(days: list):
        sum_temp = 0
        sum_cond = 0
        cnt_non_zero_days = 0
        for day in days:
            if day[1] is not None:
                sum_temp += day[1]
                cnt_non_zero_days += 1
            sum_cond += day[2]

        if cnt_non_zero_days != 0:
            avg_temp = sum_temp / cnt_non_zero_days
            avg_cond = sum_cond / cnt_non_zero_days
            return avg_temp, avg_cond
        else:
            logging.warning(
                'data for the city is not exist'
            )
            return None, 0

    @staticmethod
    def create_avg_data(city_name: str):
        relevant_data = DataAggregationTask.get_new_format_data(city_name)
        avg_temp, avg_cond = DataAggregationTask.get_avg_data(relevant_data)
        return [city_name, avg_temp, avg_cond]

    @staticmethod
    def catch_data_from_calculator(caught_data_queue: Queue):
        avg_data = []
        while True:
            try:
                city_name = caught_data_queue.get(timeout=2)
                logging.info(
                    f'aggregator get {city_name}'
                )
                if city_name != 'incorrect data':
                    avg = DataAggregationTask.create_avg_data(city_name)
                else:
                    avg = [None, None, None]
                if avg[1] is not None:
                    avg_data.append(avg)
                else:
                    logging.warning(
                        f'data for {city_name} is incorrect'
                    )
            except queue.Empty:
                break

        return avg_data

    @staticmethod
    def create_rating(avg_data: list):
        avg_data.sort(
            key=lambda data: (data[1], data[2]),
            reverse=True
        )
        city_for_record = []
        rating_place = 1
        for i in range(len(avg_data) - 1):
            city = avg_data[i][0]
            city_for_record.append([city, rating_place])
            cur_temp = avg_data[i][1]
            next_temp = avg_data[i + 1][1]
            cur_cond = avg_data[i][2]
            next_cond = avg_data[i + 1][2]
            if cur_temp != next_temp or cur_cond != next_cond:
                rating_place += 1
        try:
            city = avg_data[-1][0]
        except IndexError:
            logging.error(
                'data in incorrect'
            )
            city = None
        city_for_record.append([city, rating_place])
        return city_for_record

    @staticmethod
    def create_table_header(relevant_data: list, data_writer):
        dates = [item[0] for item in relevant_data]
        header = ['City/day', ''] + dates + ['avg', 'rating']
        data_writer.writerow(header)

    @staticmethod
    def put_temp_and_cond_into_table(
            city_name: str, priority: int,
            relevant_data: list, data_writer):
        avg_temp, avg_cond = DataAggregationTask.get_avg_data(
            relevant_data
        )
        temps = [item[1] for item in relevant_data]
        temp_line = [city_name, 'temp_avg'] + temps + [avg_temp, priority]

        conds = [item[2] for item in relevant_data]
        cond_line = ['', 'relevant_cond_hours'] + conds + [avg_cond]

        data_writer.writerow(temp_line)
        data_writer.writerow(cond_line)

    @staticmethod
    def fill_table_by_city_name(data: list):
        city_name = data[0]
        priority = data[1]
        relevant_data = DataAggregationTask.get_new_format_data(city_name)
        with open(AGGREGATION_TABLE, 'a', encoding='utf-8') as aggreg_table:
            data_writer = csv.writer(
                aggreg_table,
                delimiter=";",
                lineterminator="\r",
                quoting=csv.QUOTE_NONE
            )
            DataAggregationTask.create_table_header(
                relevant_data,
                data_writer
            )
            DataAggregationTask.put_temp_and_cond_into_table(
                city_name,
                priority,
                relevant_data,
                data_writer
            )

    @staticmethod
    def aggregate_data(queue: Queue):
        with open(AGGREGATION_TABLE, 'w', encoding='utf-8'):
            logging.info(f'clear {AGGREGATION_TABLE}')

        avg_data = DataAggregationTask.catch_data_from_calculator(queue)
        avg_data.sort(key=lambda data: (data[1], data[2]), reverse=True)
        logging.debug(avg_data)

        city_for_record = DataAggregationTask.create_rating(avg_data)

        with ThreadPoolExecutor() as thread_pool:
            thread_pool.map(
                DataAggregationTask.fill_table_by_city_name,
                city_for_record,
                timeout=20
            )


class DataAnalyzingTask:    # финальный анализ и получение результата
    @staticmethod
    def get_perfect_cities():
        isCorrect = False
        with open(AGGREGATION_TABLE, encoding='utf-8') as aggreg_table:
            table = csv.reader(aggreg_table, delimiter=';')
            for row in table:
                city = row[0]
                rating = row[-1]
                if rating == '1':
                    return f'Best choice:    {city}'
                    isCorrect = True
        if not isCorrect:
            return 'ERROR'
