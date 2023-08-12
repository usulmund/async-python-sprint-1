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
from settings import Settings
from utils import CITIES

settings = Settings()
AGGREGATION_TABLE = settings.AGGREGATION_TABLE
ANALYSER_DIR = settings.ANALYSER_DIR
DATA_DIR = settings.DATA_DIR

city_for_calculation_queue: Queue = Queue()

'''

Service - класс с обсуживающими функциями, созданный для
лаконичности функции forecast_weather.

Включает в себя:
    --  init_logger - настройка логирования.

    --  start_threads_for_data_fetching - запуск пула
        потоков для получения данных через API.

    --  start_processes_for_calculation_and_agregation - запуск процессов,
        подвязанных на одну очередь: один вычисляет погодные параметры,
        другой объединяет данные в таблицу.

'''


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
        calculator = Process(
            target=DataCalculationTask.analyze_data_by_city_name,
            args=(city_for_aggregation_queue,)
        )

        aggregator = Process(
            target=DataAggregationTask.aggregate_data,
            args=(city_for_aggregation_queue,)
        )

        calculator.start()
        aggregator.start()

        processes = [calculator, aggregator]
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

        calculator.join()
        aggregator.join()


'''

DataFetchingTask - класс, ответственный за получение данных через API.

Включает в себя:
    --  get_data_by_city_name - по названию города получает URL,
        с которого берет сырую информацию о погоде.
        Данные сохраняются в директории DATA_DIR.
        Название обработанного города кладется в
        city_for_calculation_queue - очередь данных, готовых к вычислению.

'''


class DataFetchingTask:
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


'''

DataCalculationTask - класс, вычисляющий погодные параметры.

Включает в себя:
    --  analyze_data_by_city_name - вызывает подпроцесс для
        каждого элемента city_for_calculation_queue,
        в результате которого сырые данные из DATA_DIR
        перерабатываются в данные, с которыми удобно работать.
        Храним обработанные данные в ANALYSER_DIR.
        Аргументом функции является data_to_send_queue - очередь
        с данными, готовыми к отправке в процесс-агрегатор.
        Как только данные переработаны, в data_to_send_queue
        кладется название города.

'''


class DataCalculationTask:
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
            analysis_process.communicate()
            logging.info(
                f'analyser put {city_name}'
            )
            data_to_send_queue.put(city_name)


'''

DataAggregationTask - класс, объединяющий вычисленные данные.
По техническому заданию в результате этого этапа получаем таблицу.

Включает в себя:
    --  convert_json_to_list - переводит данные из json-файла
        в более удобный формат:
        список из троек [дата, средняя температура, часы без осадков].
        В случае, если json не соответствует формату,
        функция вернет тройку None, None, 0.

    --  get_avg_temp_and_cond - подсчитывает средние значения
        для температуры и часов без осадков,
        обрабатывает случай отсутствия данных.

    --  create_stats_from_json - вызывает предыдущие две функции:
        в результате из каждого json файла получаем тройкy
        [город, средняя температура, среднее время без осадков].

    --  catch_data_from_calculator - отлавливает данные, попадающие в
        очередь из процесса, вычисляющего погодные параметры.
        В результате обработки очереди получаем список из троек
        [город, средняя температура, среднее время без осадков].

    --  create_rating - на основе списка троек вида
        [город, средняя температура, среднее время без осадков]
        строится список из пар [город, место в рейтинге] с учетом,
        что города с одинаковыми показателями занимают одинаковое место.

    --  create_table_header - по списку
        [дата, средняя температура, часы без осадков]
        выстраивается заголовок в таблице.
        Для каждой записи в таблице формируется собственный
        заголовок во избежание несовпадения дат для разных городов.

    --  put_temp_and_cond_into_table - заполняются основные данные
        для города: название, показатели по датам,
        средние значения показателей за весь период, место в рейтинге.

    --  fill_table_by_city_name - объединяет все функции выше
        по взаимодействию с таблицей.
        ВНИМАНИЕ: если вы используете Microsoft Excel,
        он может "помочь" вам, и переписать числа с плавающей точкой в даты.

    --  aggregate_data - создает/очищает AGGREGATION_TABLE для записи
        данных, производит отлов данных из вычисляющего процесса,
        составляет рейтинг, запускает пул потоков на заполнение таблицы.

'''


class DataAggregationTask:  # объединение вычисленных данных
    @staticmethod
    def convert_json_to_list(city_name: str):
        day_temp_cond_list = []
        json_file = f'{ANALYSER_DIR}/{city_name}_output.json'
        with open(json_file) as json_data:
            data = json.load(json_data)
        try:
            day_temp_cond_list = [
                [
                    day['date'],
                    day['temp_avg'],
                    day['relevant_cond_hours']
                ]
                for day in data['days']
            ]
        except KeyError:
            day_temp_cond_list = [[None, None, 0]]

        return day_temp_cond_list

    @staticmethod
    def get_avg_temp_and_cond(day_temp_cond_list: list):
        sum_temp = 0
        sum_cond = 0
        cnt_non_zero_days = 0
        for data in day_temp_cond_list:
            if data[1] is not None:
                sum_temp += data[1]
                cnt_non_zero_days += 1
            sum_cond += data[2]

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
    def create_stats_from_json(city_name: str):
        day_temp_cond_list = DataAggregationTask.convert_json_to_list(
            city_name
        )
        avg_temp, avg_cond = DataAggregationTask.get_avg_temp_and_cond(
            day_temp_cond_list
        )
        return [city_name, avg_temp, avg_cond]

    @staticmethod
    def catch_data_from_calculator(caught_data_queue: Queue):
        city_temp_cond_list = []
        while True:
            try:
                city_name = caught_data_queue.get(timeout=2)
                logging.info(
                    f'aggregator get {city_name}'
                )
                city_temp_cond = DataAggregationTask.create_stats_from_json(
                    city_name
                )
                if city_temp_cond[1] is not None:
                    city_temp_cond_list.append(city_temp_cond)
                else:
                    logging.warning(
                        f'data for {city_name} is incorrect'
                    )
            except queue.Empty:
                break

        return city_temp_cond_list

    @staticmethod
    def create_rating(city_temp_cond_list: list):
        city_temp_cond_list.sort(
            key=lambda data: (data[1], data[2]),
            reverse=True
        )
        logging.debug(city_temp_cond_list)

        city_for_record = []
        rating_place = 1
        for i in range(len(city_temp_cond_list) - 1):
            city = city_temp_cond_list[i][0]
            city_for_record.append([city, rating_place])
            cur_temp = city_temp_cond_list[i][1]
            next_temp = city_temp_cond_list[i + 1][1]
            cur_cond = city_temp_cond_list[i][2]
            next_cond = city_temp_cond_list[i + 1][2]
            if cur_temp != next_temp or cur_cond != next_cond:
                rating_place += 1
        try:
            city = city_temp_cond_list[-1][0]
        except IndexError:
            logging.error(
                'data in incorrect'
            )
            city = None
        city_for_record.append([city, rating_place])
        return city_for_record

    @staticmethod
    def create_table_header(day_temp_cond_list: list, data_writer):
        dates = [item[0] for item in day_temp_cond_list]
        header = ['City/day', ''] + dates + ['avg', 'rating']
        data_writer.writerow(header)

    @staticmethod
    def put_temp_and_cond_into_table(
            city_name: str, rating: int,
            day_temp_cond_list: list, data_writer):
        avg_temp, avg_cond = DataAggregationTask.get_avg_temp_and_cond(
            day_temp_cond_list
        )
        temps = [item[1] for item in day_temp_cond_list]
        temp_line = [city_name, 'temp_avg'] + temps + [avg_temp, rating]

        conds = [item[2] for item in day_temp_cond_list]
        cond_line = ['', 'relevant_cond_hours'] + conds + [avg_cond]

        data_writer.writerow(temp_line)
        data_writer.writerow(cond_line)

    @staticmethod
    def fill_table_by_city_name(city_rating: list):
        city_name = city_rating[0]
        rating = city_rating[1]
        day_temp_cond_list = DataAggregationTask.convert_json_to_list(
            city_name
        )
        with open(AGGREGATION_TABLE, 'a', encoding='utf-8') as aggreg_table:
            data_writer = csv.writer(
                aggreg_table,
                delimiter=";",
                lineterminator="\r",
                quoting=csv.QUOTE_NONE
            )
            DataAggregationTask.create_table_header(
                day_temp_cond_list,
                data_writer
            )
            DataAggregationTask.put_temp_and_cond_into_table(
                city_name,
                rating,
                day_temp_cond_list,
                data_writer
            )

    @staticmethod
    def aggregate_data(queue: Queue):
        with open(AGGREGATION_TABLE, 'w', encoding='utf-8'):
            logging.info(f'clear {AGGREGATION_TABLE}')

        city_temp_cond_list = DataAggregationTask.catch_data_from_calculator(
            queue
        )
        city_for_record = DataAggregationTask.create_rating(
            city_temp_cond_list
        )

        with ThreadPoolExecutor() as thread_pool:
            thread_pool.map(
                DataAggregationTask.fill_table_by_city_name,
                city_for_record,
                timeout=20
            )


'''

DataAnalyzingTask - класс для финального анализа и получения результата.

Включает в себя:
    --  get_perfect_cities - выбирает из таблицы города, стоящие
        на первом месте в рейтинге, а в случае отсутствия таковых
        возвращает сообщение об ошибке.

'''


class DataAnalyzingTask:    # финальный анализ и получение результата
    @staticmethod
    def get_perfect_cities():
        with open(AGGREGATION_TABLE, encoding='utf-8') as aggreg_table:
            table = csv.reader(aggreg_table, delimiter=';')
            for row in table:
                city = row[0]
                rating = row[-1]
                if rating == '1':
                    return f'Best choice:    {city}'
        return 'ERROR'
