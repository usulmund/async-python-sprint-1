import logging
import time

from tasks import (
    Service,
    DataAnalyzingTask,
)


'''

Если вы запускаете под MacOS и у вас выдает ошибку,
запустите проект в Docker.

Сборка контейнера с запуском программы:
docker build -t async1 . --build-arg arg=run_app

Сборка контейнера с тестированием:
docker build -t async1 . --build-arg arg=test

Старт контейнера:
docker run async1

'''


def forecast_weather():
    Service.init_logger()

    time_s = time.time()
    Service.start_threads_for_data_fetching()
    logging.info(
        f"spent time on DataFetchingTask: {time.time() - time_s:.6f}s"
    )

    time_s = time.time()
    Service.start_processes_for_calculation_and_agregation()
    logging.info(
        f"spent time on DataCalculationTask and DataAggregationTask: ' \
        f'{time.time() - time_s:.6f}s"
    )

    time_s = time.time()
    print(DataAnalyzingTask.get_perfect_cities())
    logging.info(
        f"spent time on DataAnalyzingTask: {time.time() - time_s:.6f}s"
    )


if __name__ == "__main__":
    forecast_weather()
