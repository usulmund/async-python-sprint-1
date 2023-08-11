ARG arg

FROM python AS app
COPY . .

FROM app AS mode_run_app
ENV file_name="./forecasting.py"

FROM app AS mode_test
ENV file_name="./tests.py"

FROM mode_$arg as start
CMD python ${file_name}

