import unittest
from multiprocessing import Queue
import subprocess
from concurrent.futures import ThreadPoolExecutor

from tasks import (
    Service,
    DataFetchingTask,
    DataAggregationTask,
    DataAnalyzingTask,
)

from utils import CITIES

GOOD_CITIES = [
    'CAIRO',
    'ABUDHABI',
    'BEIJING',
    'BUCHAREST',
    'ROMA',
    'LONDON',
    'NOVOSIBIRSK',
    'PARIS',
    'WARSZAWA',
    'BERLIN',
    'VOLGOGRAD',
    'MOSCOW',
    'KALININGRAD',
    'KAZAN',
    'SPETERSBURG',
]

BAD_CITIES_LIST = [
    ['A', 'B', 'C'],
    {'a': 'b', 'c': 'd'},
    [1],
    ['a'],
]


def prepare_thread_pool(cities: list[str]):
    city_names = [city_name for city_name in cities]
    with ThreadPoolExecutor() as thread_pool:
        thread_pool.map(
            DataFetchingTask.get_data_by_city_name,
            city_names,
            timeout=20
        )


def app_with_bed_data(bad_cities: list[str]):
    Service.init_logger()
    prepare_thread_pool(bad_cities)
    Service.start_processes_for_calculation_and_agregation()
    return DataAnalyzingTask.get_perfect_cities()


def app_with_good_data():
    Service.init_logger()
    prepare_thread_pool(CITIES)
    Service.start_processes_for_calculation_and_agregation()
    return DataAnalyzingTask.get_perfect_cities()


class AppTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        start_app_command = 'python3 forecasting.py'
        start_app_command_list = start_app_command.split()
        app_process = subprocess.Popen(
            start_app_command_list,
            stdout=subprocess.PIPE,
            universal_newlines=True
        )
        output, error = app_process.communicate()
        ret_code = app_process.returncode
        print(
            f'Application has finished with output:\n'
            f'* * * * * * * * * * * * * * * * * * *\n'
            f'\t{output}\n'
            f'* * * * * * * * * * * * * * * * * * *\n'
            f'Error: {error}\n'
            f'Return code: {ret_code}'
        )

    def test_convert_json_to_list(self):
        for city in GOOD_CITIES:
            self.assertNotEqual(
                DataAggregationTask.convert_json_to_list(city),
                [[None, None, 0]]
            )

    def test_create_stats_from_json(self):
        for city in GOOD_CITIES:
            self.assertNotEqual(
                DataAggregationTask.create_stats_from_json(city),
                [city, None, 0]
            )

    def test_catch_data_from_calculator(self):
        test_queue = Queue()
        for city in GOOD_CITIES:
            test_queue.put(city)
        self.assertNotEqual(
            DataAggregationTask.catch_data_from_calculator(test_queue),
            []
        )

    def test_create_rating(self):
        data_1 = [['a', 1, 1], ['b', 1, 1], ['c', 1, 1]]
        data_2 = [['a', 1, 1], ['b', 5, 1], ['c', 2, 1]]
        data_3 = [['a', 3, 3], ['b', 5, 1], ['c', 3, 7]]

        correct_output_1 = [['a', 1], ['b', 1], ['c', 1]]
        correct_output_2 = [['b', 1], ['c', 2], ['a', 3]]
        correct_output_3 = [['b', 1], ['c', 2], ['a', 3]]

        self.assertEqual(
            DataAggregationTask.create_rating(data_1),
            correct_output_1
        )
        self.assertEqual(
            DataAggregationTask.create_rating(data_2),
            correct_output_2
        )
        self.assertEqual(
            DataAggregationTask.create_rating(data_3),
            correct_output_3
        )

    def test_app_with_bed_data(self):
        for bad_cities in BAD_CITIES_LIST:
            self.assertEqual(
                app_with_bed_data(bad_cities),
                'ERROR'
            )

    def test_app_with_good_data(self):
        self.assertIn(
            'ABUDHABI',
            app_with_good_data(),
        )


if __name__ == "__main__":
    unittest.main()
