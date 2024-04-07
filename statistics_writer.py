import csv
import logging
import os
import random
import threading
from time import time
from typing import Any, Mapping
import faker

from arguments import Args
from time_helper import ChunkInfo, elapsed

logging.basicConfig(
    filename="logfile.log",
    format=" %(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
)


def setup_row(
    chunk_size="",
    chunk_number="",
    healthy_number="",
    unhealthy_number="",
    reading="",
    filtering="",
    frame_total="",
) -> Mapping[str, str]:
    return {
        "chunk_size": chunk_size,
        "chunk_number": chunk_number,
        "No of healthy records": healthy_number,
        "No of unhealthy records": unhealthy_number,
        "reading_time": reading,
        "filtering_time": filtering,
        "frame_Total_Time": frame_total,
    }


class StatisticsWriter:
    def __init__(self, args: Args) -> None:
        self.args = args
        self.chunks_info: list[ChunkInfo] = None  # type: ignore

    def statistics_csv(self):
        log_path = f"output/Detailed_Log_{self.args.filter_mode.name}.csv"
        with open(log_path, "a", newline="") as csv_file:
            fieldnames = [
                "chunk_size",
                "chunk_number",
                "No of healthy records",
                "No of unhealthy records",
                "reading_time",
                "filtering_time",
                "frame_Total_Time",
            ]
            writer = csv.DictWriter(csv_file, fieldnames)
            # writing columns names
            writer.writeheader()
            for index, chunk_info in enumerate(self.chunks_info):
                writer.writerow(
                    setup_row(
                        self.args.chunk_size,  # type: ignore
                        index + 1,  # type: ignore
                        chunk_info.number_of_healthy,  # type: ignore
                        chunk_info.number_of_unhealthy,  # type: ignore
                        chunk_info.reading_time,  # type: ignore
                        chunk_info.filtering_time,  # type: ignore
                        chunk_info.reading_time + chunk_info.filtering_time,  # type: ignore
                    )
                )
            # region columns subtotal calculations
            reading_list = list(map(lambda ci: ci.reading_time, self.chunks_info))
            filtering_list = list(
                map(lambda ci: ci.filtering_info.filtering_time, self.chunks_info)
            )
            frame_time_list = [r + f for r, f in zip(reading_list, filtering_list)]
            healthy_total = sum(
                map(lambda ci: ci.filtering_info.number_of_healthy, self.chunks_info)
            )
            unhealthy_total = sum(
                map(lambda ci: ci.filtering_info.number_of_unhealthy, self.chunks_info)
            )
            reading_total = round(sum(reading_list), 4)
            filtering_total = round(sum(filtering_list), 4)
            frame_time_total = round(reading_total + filtering_total, 4)
            writer.writerow(
                setup_row(
                    "",
                    "",
                    f"Total:{healthy_total}",
                    f"Total:{unhealthy_total}",
                    f"Total:{reading_total}",
                    f"Total:{filtering_total}",
                    f"Total:{frame_time_total}",
                )
            )

            reading_average = round(reading_total / len(reading_list), 4)
            filtering_average = round(filtering_total / len(filtering_list), 4)
            frame_total_average = round(frame_time_total / len(frame_time_list), 4)
            writer.writerow(
                setup_row(
                    reading=f"Average:{reading_average}",
                    filtering=f"Average:{filtering_average}",
                    frame_total=f"Average:{frame_total_average}",
                )
            )

            reading_max = max(reading_list)
            filtering_max = max(filtering_list)
            frame_total_max = max(frame_time_list)
            writer.writerow(
                setup_row(
                    reading=f"Max:{reading_max}",
                    filtering=f"Max:{filtering_max}",
                    frame_total=f"Max:{frame_total_max}",
                )
            )

            reading_min = min(reading_list)
            filtering_min = min(filtering_list)
            frame_total_min = min(frame_time_list)
            # endregion
            writer.writerow(
                setup_row(
                    reading=f"Min:{reading_min}",
                    filtering=f"Min:{filtering_min}",
                    frame_total=f"Min:{frame_total_min}",
                )
            )

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

    def start(self, chunks_info: list[ChunkInfo]) -> None:
        logging.info(
            "%s  Writer:starting write excel and csv files statistics ....",
            elapsed(self.args.starting_time),
        )
        self.chunks_info = chunks_info
        if not os.path.exists("output"):
            os.makedirs("output")
        # excel_thread = threading.Thread(target=self.excel_writer)
        # excel_thread.start()
        # excel_thread.join()
        csv_thread = threading.Thread(target=self.statistics_csv)
        csv_thread.start()
        csv_thread.join()
        logging.info(
            "%s  Writer:finish of write excel and csv files statistics .",
            elapsed(self.args.starting_time),
        )


# if __name__ == "__main__":
#     faker = faker.Faker()
#     dict = generate_statistics_dict()
#     dict["start_time"] = time()
#     dict["chunk_size"] = 1000
#     dict["filtering_algorithm"] = "Khaled_saeed"
#     dict["number_of_chunks"] = 10
#     for _ in range(dict["number_of_chunks"]):
#         chunks_info: list[ChunkInfo] = dict["chunks_info"]
#         healthy_number = random.randint(0, 100)
#         unhealthy_number = 100 - healthy_number
#         filtering_time = round(random.uniform(0.01, 2), 4)
#         reading_time = round(random.uniform(0.01, 2), 4)
#         chunks_info.append(
#             ChunkInfo(reading_time, filtering_time, healthy_number, unhealthy_number)
#         )
#     statistics_writer = StatisticsWriter(dict)
#     statistics_writer.start()
