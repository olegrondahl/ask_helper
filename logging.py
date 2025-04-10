import os
from datetime import datetime
import env_data as env
import math
import pandas as pd


temp_folder_name = datetime.now().strftime("%Y%m%d_%H%M%S")


def log_to_file(
    heading: str,
    data_changes: list[str | pd.DataFrame],
    change_text: str = "",
) -> None:
    padding_size = math.floor((40 - len(heading)) / 2)
    title = f"#{' '* padding_size}{heading}{' '* (padding_size + len(heading) % 2)}#"
    heading = f"{'#' * 42}\n#{' ' * 40}#\n{title}\n#{' ' * 40}#\n{'#' * 42}\n"

    with open(f"{env.log_folder}{temp_folder_name}/log.txt", "a") as file:
        file.write(heading)
        for change in data_changes:
            if isinstance(change, pd.DataFrame):
                file.write(f"\n{change_text}")
                change.to_csv(file, sep=";", index=False)
            else:
                file.write(f"\n{change_text} {change}")
        if len(data_changes) == 0:
            file.write("\nNo change made")
        file.write("\n\n\n")


def create_folder_file() -> None:
    os.mkdir(f"{env.log_folder}{temp_folder_name}")


def save_new_file(data: pd.DataFrame, file_type: str) -> None:
    file_name = f"{file_type}_{data.iloc[0,4]}_{temp_folder_name}"
    os.rename(f"{env.log_folder}{temp_folder_name}", f"{env.log_folder}{file_name}")
    data.to_csv(f"{env.log_folder}{file_name}/{file_name}.csv", sep=";", index=False, decimal=',')
    data.to_csv(f"{env.download_folder}{file_name}.txt", sep="\t", index=False, decimal=',')
    print(f"File saved to {env.download_folder}{file_name}.txt")
