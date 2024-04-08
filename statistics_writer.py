import csv
import logging
import os
import random
import threading
from typing import Any, Mapping
import faker
from pandas import DataFrame, concat
from Enums import FilterMode, ProcessingMode
from arguments import Args
from chunks_processing_info import ChunkFilteringInfo, ChunkInfo, elapsed

logging.basicConfig(
    filename="logfile.log",
    format=" %(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
)


class StatisticsWriter:
    def __init__(self, args: Args) -> None:
        self.args = args

    def convert_to_data_frame(self, chunks_info: list[ChunkInfo]) -> DataFrame:
        return DataFrame(
            [
                {
                    "chunk_size": self.args.chunk_size,
                    "chunk_number": i,
                    "healthy_records": chunk.number_of_healthy,
                    "unhealthy_records": chunk.number_of_unhealthy,
                    "reading_time": chunk.reading_time,
                    "filtering_time": chunk.filtering_time,
                    "frame_total_time": round(
                        chunk.reading_time + chunk.filtering_time, 4
                    ),
                }
                for i, chunk in enumerate(chunks_info, start=1)
            ]
        )

    def calculate_aggregation_values(self, df: DataFrame) -> dict:
        agg_values = {}
        for column in df.columns[2:4]:
            agg_values[column] = {"sum": round(df[column].sum(), 4)}
        for column in df.columns[4:]:
            agg_values[column] = {
                "sum": round(df[column].sum(), 4),
                "average": round(df[column].mean(), 4),
                "max": df[column].max(),
                "min": df[column].min(),
            }
        return agg_values

    def write_csv(self, df: DataFrame, aggregation_values: dict[str, dict]):
        aggregation_rows = list[list[str | None]]()
        for key in ["sum", "average", "max", "min"]:
            label = key if key != "sum" else "total"
            fields = len(df.columns) - sum(
                key in dict for dict in aggregation_values.values()
            )
            aggregation_rows.append(
                [None] * fields
                + [
                    f"{label}:{aggregation_values[column][key]}"
                    for column in df.columns[fields:]
                ]
            )
        csv_df = concat(
            [df, DataFrame(aggregation_rows, columns=df.columns, index=None)], axis=0
        )
        # replace csv columns
        csv_df.columns = [
            "chunk_size",
            "chunk_number",
            "No of healthy records",
            "No of unhealthy records",
            "reading_time",
            "filtering_time",
            "frame_Total_Time",
        ]
        csv_df.to_csv(
            path_or_buf=f"output/Detailed_Log_{self.args.filter_mode.name}.csv",
            mode="a",
            index=False,
        )

    def start(self, chunks_info: list[ChunkInfo]) -> None:
        logging.info(
            "%s  Writer:starting write excel and csv files statistics ....",
            elapsed(self.args.starting_time),
        )
        df = self.convert_to_data_frame(chunks_info)
        aggregation_values = self.calculate_aggregation_values(df)
        if not os.path.exists("output"):
            os.makedirs("output")
        csv_thread = threading.Thread(
            target=lambda: self.write_csv(df, aggregation_values),
        )
        csv_thread.start()
        csv_thread.join()
        logging.info(
            "%s  Writer:finish of write excel and csv files statistics .",
            elapsed(self.args.starting_time),
        )


if __name__ == "__main__":
    faker = faker.Faker()
    chunks_info = list[ChunkInfo]()
    chunk_size = 1000
    for _ in range(10):
        healthy_number = random.randint(0, chunk_size)
        unhealthy_number = chunk_size - healthy_number
        filtering_time = round(random.uniform(0.01, 2), 4)
        reading_time = round(random.uniform(0.01, 2), 4)
        chunks_info.append(
            ChunkInfo(
                reading_time,
                ChunkFilteringInfo(filtering_time, healthy_number, unhealthy_number),
            )
        )
    statistics_writer = StatisticsWriter(
        Args(
            data_file="",
            bad_words_file="",
            head_columns=[],
            specify_columns=[],
            processing_mode=ProcessingMode.ProcessesPool,
            filter_mode=FilterMode.AhoCorasick,
            chunk_size=1000,
        )
    )
    statistics_writer.start(chunks_info)

# def excel_writer(self, sheet_name):

#     # measure reading total time and avg time
#     total_time_of_read = sum(self.__statistics_dictionary["reading"])
#     avg_time_of_read = (
#         total_time_of_read / self.__statistics_dictionary["number of chunks"]
#     )

#     # measure filtering total time and avg time
#     total_time_of_filter = sum(self.__statistics_dictionary["filtering"])
#     avg_time_of_filter = (
#         total_time_of_filter / self.__statistics_dictionary["number of chunks"]
#     )
#     # measure writing total time and avg time
#     total_time_of_writing = sum(self.__statistics_dictionary["writing"])
#     avg_time_of_writing = (
#         total_time_of_writing / self.__statistics_dictionary["number of chunks"]
#     )

#     # measure processing total time and avg time
#     total_time_of_processing = (
#         sum(self.__statistics_dictionary["reading"])
#         + sum(self.__statistics_dictionary["filtering"])
#         + sum(self.__statistics_dictionary["writing"])
#     )
#     avg_time_of_processing = (
#         total_time_of_processing / self.__statistics_dictionary["number of chunks"]
#     )

#     data = {
#         "D.frame size": self.__statistics_dictionary["chunksize"],
#         "avg_Reading Time": avg_time_of_read,
#         "avg_filtering Time": avg_time_of_filter,
#         "Total_processing_Time": total_time_of_processing,
#         "avg_processing_Time": avg_time_of_processing,
#     }

#     headers = list(data.keys())
#     # check if the file exists
#     try:
#         workbook = openpyxl.load_workbook("./output/output.xlsx")
#         if sheet_name in workbook.sheetnames:
#             worksheet = workbook[sheet_name]
#         else:
#             worksheet = workbook.create_sheet(sheet_name)
#             worksheet.append(headers)
#     except FileNotFoundError:
#         # create a new workbook and worksheet if the file doesn't exist
#         workbook = openpyxl.Workbook()
#         worksheet = workbook.create_sheet(sheet_name)
#         worksheet.append(headers)

#     # delete the default 'Sheet' worksheet
#     if "Sheet" in workbook.sheetnames:
#         del workbook["Sheet"]

#     # append data to the worksheet
#     values = [data[header] for header in headers]
#     worksheet.append(values)
#     # save the workbook
#     workbook.save("./output/output.xlsx")
