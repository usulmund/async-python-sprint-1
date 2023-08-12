from pydantic import BaseModel


class Settings(BaseModel):
    AGGREGATION_TABLE: str = 'aggregate_data.csv'
    ANALYSER_DIR: str = 'analyzer_result'
    DATA_DIR: str = 'data_for_analysis'
