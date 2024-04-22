import pandas as pd
from io import StringIO

import static_data.headers as headers
import static_data.conversion_data as conversion_data
import logging as logg

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)


def get_separator(ask_data: str, debug: bool) -> str:
    first_line = ask_data.split("\n")[0]

    first_semicolon = first_line.find(";")
    first_tab = first_line.find("\t")
    first_comma = first_line.find(",")

    sep_indices = {"semicolon": first_semicolon, "tab": first_tab, "comma": first_comma}

    if debug:
        print(sep_indices)

    # filter out not found
    sep_indices = {
        separator: index for separator, index in sep_indices.items() if index != -1
    }

    if sep_indices:
        separator = min(sep_indices, key=sep_indices.get)

        logg.log_to_file(
            heading="SEPARATOR",
            data_changes=[f"Separator identified to be: {separator}"],
        )

        match separator:
            case "semicolon":
                return ";"  # type: str
            case "tab":
                return "\t"  # type: str
            case "comma":
                return ","  # type: str
            case _:
                raise Exception("Was unable to identify separator used in the file")
    else:
        raise Exception("Was unable to identify separator used in the file")


def convert_data_to_df(separator: str, data: str, debug: bool = False) -> pd.DataFrame:
    io_data = StringIO(data)
    df = pd.read_csv(io_data, sep=separator, dtype=str).dropna(how="all")

    # remove_all_empty_columns
    for col in df.columns:
        if debug:
            print(
                f"column: {col}, type: {df[col].dtype}, is empty: {df[col].isna().all()}"
            )
        if df[col].isna().all() and "Unnamed" in col:
            df.drop(col, axis=1, inplace=True)
    df.fillna("", inplace=True)

    if debug:
        print(df)

    logg.log_to_file(
        heading="CONVERT TO DATA FRAME",
        data_changes=[df],
    )

    return df


def indentify_file_type(data: pd.DataFrame, debug: bool = False) -> str:
    # Check number of columns
    ncols = len(data.columns)
    file_type_est = {"RFH": 0, "RHC": 0, "PTOI": 0, "PTOC": 0, "NTO": 0}

    if ncols == 9:
        file_type_est["RFH"] += 10
        logg.log_to_file(
            heading="IDENTIFY FILE TYPE",
            data_changes=["File identified as: RFH"],
        )

        print("File identified as: RFH")
        return "RFH"
    elif ncols <= 12:
        file_type_est["RFH"] += 2
    elif ncols == 13:
        file_type_est["RFH"] += 10
    elif ncols == 17:
        file_type_est["RHC"] += 10
        file_type_est["PTOI"] += 10
    elif ncols <= 20:
        file_type_est["RHC"] += 2
        file_type_est["PTOI"] += 2
    elif ncols == 27:
        file_type_est["PTOC"] += 10
    elif ncols <= 30:
        file_type_est["PTOC"] += 2

    # check if numeric in col 10 (ANTALL_ANDELER)

    score_currency = 4
    score_isin = 4
    score_units = 4

    isin_rfh = 0
    isin_ptoi = 0
    isin_ptoc = 0
    currency_rfh = 0
    currency_ptoi = 0
    units_rhc = 0

    for index, row in data.iterrows():
        if len(row[data.columns[6]]) == 12 and str(row[data.columns[6]][:2]).isalpha():
            isin_rfh += 1

        if (
            len(row[data.columns[11]]) == 12
            and str(row[data.columns[11]][:2]).isalpha()
        ):
            isin_ptoi += 1

        if (
            len(row[data.columns[10]]) == 12
            and str(row[data.columns[10]][:2]).isalpha()
        ):
            isin_ptoc += 1

        if ncols > 9 and (
            ("," in str(row[data.columns[9]]) or "." in str(row[data.columns[9]]))
            or (
                str(row[data.columns[9]]) == "NO0000000000"
                and "," in str(row[data.columns[10]])
                or "." in str(row[data.columns[10]])
            )
        ):
            units_rhc += 1

        if len(row[data.columns[7]]) == 3:
            currency_rfh += 1

        if len(row[data.columns[12]]) == 3:
            currency_ptoi += 1

    if currency_rfh >= len(data) * 2 / 3 and currency_rfh >= currency_ptoi:
        file_type_est["RHC"] += score_currency
    elif currency_ptoi >= len(data) * 2 / 3 and currency_ptoi >= currency_rfh:
        file_type_est["PTOI"] += score_currency

    if (
        isin_rfh >= len(data) * 2 / 3
        and isin_rfh >= isin_ptoi
        and isin_rfh >= isin_ptoc
    ):
        file_type_est["RHC"] += score_isin
    elif (
        isin_ptoi >= len(data) * 2 / 3
        and isin_ptoi >= isin_rfh
        and isin_ptoi >= isin_ptoc
    ):
        file_type_est["PTOI"] += score_isin
    elif (
        isin_ptoc >= len(data) * 2 / 3
        and isin_ptoc >= isin_rfh
        and isin_ptoc >= isin_ptoi
    ):
        file_type_est["PTOC"] += score_isin

    if units_rhc >= len(data) * 2 / 3:
        file_type_est["RHC"] += score_units

    max_key = max(file_type_est, key=file_type_est.get)

    if debug:
        print(f"file_type_est: {file_type_est}")

    logg.log_to_file(
        heading="IDENTIFY FILE TYPE",
        data_changes=[f"File identified as: {max_key}"],
    )

    print(f"File identified as: {max_key}")

    return max_key


def update_header(data: pd.DataFrame, file_type: str) -> pd.DataFrame:
    match file_type:
        case "RFH":
            header = headers.rfh_header
        case "RHC":
            header = headers.rhc_header
        case "PTOI":
            header = headers.ptoi_header
        case "PTOC":
            header = headers.ptoc_header
        case "NTO":
            header = headers.nto_header

    if len(data.columns) == len(header):
        logg.log_to_file(
            heading="UPDATE HEADER",
            change_text="Updated header:",
            data_changes=[
                f"{data.columns[index]} -> {header}"
                for index, header in enumerate(header)
                if header != data.columns[index]
            ],
        )

        data.columns = header
    else:
        raise Exception("Unable to update header, number of columns does not match")

    return data


def pad_customer_number(data: pd.DataFrame) -> None:
    logg_msg = []
    for index, row in data.iterrows():
        if row["KUNDENR"] != row["KUNDENR"].zfill(11):
            logg_msg.append(
                f"{index +1} {row['KUNDENR']} -> {row['KUNDENR'].zfill(11)}"
            )
            row["KUNDENR"] = row["KUNDENR"].zfill(11)

    logg.log_to_file(
        heading="PAD CUSTOMER NUMBER",
        change_text="Added 0 to customer number on row: ",
        data_changes=logg_msg,
    )


def convert_distributor(data: pd.DataFrame, file_type: str) -> None:
    # Convert dict keys to uppercase
    dist_dict = {
        key.upper(): value for key, value in conversion_data.distributors.items()
    }

    logg_msg = []

    for index, row in data.iterrows():
        if file_type in ["RHC", "PTOC"]:
            distributor_key = str(row.iloc[1]).upper()
            if distributor_key in dist_dict.keys():
                logg_msg.append(
                    f"{index +1} [{data.columns[1]}] {row[1]} -> {dist_dict[distributor_key]}"
                )
                row[1] = dist_dict[distributor_key]
        elif file_type in ["RFH", "PTOI"]:
            distributor_key = str(row[0]).upper()
            if distributor_key in dist_dict.keys():
                logg_msg.append(
                    f"{index +1} [{data.columns[1]}] {row[0]} -> {dist_dict[distributor_key]}"
                )
                row[0] = dist_dict[distributor_key]

    logg.log_to_file(
        heading="CONVERT DISTRIBUTOR",
        change_text="Converted distributor on row: ",
        data_changes=logg_msg,
    )


def check_for_units_in_ptoi(data: pd.DataFrame) -> None:
    logg_msg = []
    for index, row in data.iterrows():
        if row["ANTALL_ANDELER"] != "":
            logg_msg.append(
                f"{index +1} [TRANSFER_REF: {row['TRANSFER_REF']}, value removed: {row['ANTALL_ANDELER']}]"
            )
            row["ANTALL_ANDELER"] = ""

    logg.log_to_file(
        heading="REMOVE UNITS PTOI",
        change_text="Removed units on row: ",
        data_changes=logg_msg,
    )


def set_tax_value_per_isin(data: pd.DataFrame) -> None:
    logg_msg = []
    for index, row in data.iterrows():
        if row["KOSTPRIS_PR_ISIN"] == "" and row["ISIN"] != "":
            logg_msg.append(
                f"{index +1} [ISIN: {row['ISIN']}, TRANSFER_REF: {row['TRANSFER_REF']}]"
            )
            row["KOSTPRIS_PR_ISIN"] = "0"

    logg.log_to_file(
        heading="SET TAX VALUE PER ISIN",
        change_text="Changed blank to 0 on row: ",
        data_changes=logg_msg,
    )


def update_tax_indetifier(data: pd.DataFrame) -> None:
    logg_msg = []
    for index, row in data.iterrows():
        if str(row["VERDIPAPIRNAVN"]).upper() in [
            tax.upper() for tax in conversion_data.tax_key
        ]:
            logg_msg.append(
                f"{index +1} [MTR: {row['MASTERTRANSFERREF_(FULLMAKTSNR)']}] changed '{row['VERDIPAPIRNAVN']}' -> 'Skatteopplysninger'"
            )
            row["VERDIPAPIRNAVN"] = "Skatteopplysninger"

    logg.log_to_file(
        heading="UPDATE TAX IDENTIFIER",
        change_text="Changed tax identifier on row: ",
        data_changes=logg_msg,
    )


def set_account_on_tax_data(data: pd.DataFrame) -> None:
    logg_msg = []
    for index, row in data.iterrows():
        if (
            row["VERDIPAPIRNAVN"] == "Skatteopplysninger"
            and row["TIL_ASK_KONTO_KUNDE_TILBYDER"] == ""
        ):
            logg_msg.append(
                f"{index +1} [MTR: {row['MASTERTRANSFERREF_(FULLMAKTSNR)']}] TIL_ASK_KONTO_KUNDE_TILBYDER sat to {data.iloc[index - 1, 6]}"
            )

            row["TIL_ASK_KONTO_KUNDE_TILBYDER"] = data.iloc[index - 1, 7]

    logg.log_to_file(
        heading="SET ACCOUNT ON TAX DATA",
        change_text="Set account number on row: ",
        data_changes=logg_msg,
    )


def check_valid_error_code(data: pd.DataFrame) -> None:
    for index, row in data.iterrows():
        if row["FEILKODE"] not in ["A", "B", "C", "D", "E", "F", "G", "H", ""]:
            print(f"\n\nInvalid error code was given in file {row['FEILKODE']}")
            raise Exception(f"Invalid error code on row {index + 1}: {row['FEILKODE']}")


def move_tax_data(data: pd.DataFrame) -> None:
    logg_msg = []
    for index, row in data.iterrows():
        if (
            row["VERDIPAPIRNAVN"] == "Skatteopplysninger"
            and row["KOSTPRIS_PR_ISIN"] != ""
            and row["FLYTTET_KOSTPRIS_ASK"] == ""
        ):
            logg_msg.append(
                f"{index +1} [MTR: {row['MASTERTRANSFERREF_(FULLMAKTSNR)']}] KOSTPRIS_PR_ISIN moved to FLYTTET_KOSTPRIS_ASK for Skatteopplysninger"
            )
            row["FLYTTET_KOSTPRIS_ASK"] = row["KOSTPRIS_PR_ISIN"]
            row["KOSTPRIS_PR_ISIN"] = ""
    logg.log_to_file(
        heading="MOVE TAX DATA",
        change_text="Moved tax data on row: ",
        data_changes=logg_msg,
    )


def convert_to_numeric(data: pd.DataFrame, file_type: str) -> pd.DataFrame:
    logg_msg = []
    columns_to_convert = []
    match file_type:
        case "RHC":
            columns_to_convert = ["ANTALL_ANDELER", "VERDI"]
        case "PTOC":
            columns_to_convert = [
                "ANTALL_FLYTTET",
                "KOSTPRIS_PR_ISIN",
                "FLYTTET_KOSTPRIS_ASK",
                "MINSTE_INNSKUDD_HIA",
                "UBENYTTET_SKJERMING_31.12",
                "BENYTTET_SKJERMING_HIA",
            ]
        case _:
            return

    count_comma = 0
    count_period = 0

    for index, row in data.iterrows():
        for field in columns_to_convert:
            data[field] = data[field].str.strip().str.replace(" ", "")
            comma = row[field].find(",")
            period = row[field].find(".")

            if comma != -1 or period != -1:
                if comma > period:
                    count_comma += 1
                else:
                    count_period += 1

    sep_to_drop = "." if count_comma > count_period else ","

    for field in columns_to_convert:
        data[field] = data[field].str.replace(sep_to_drop, "").str.replace(",", ".")
        data[field] = pd.to_numeric(data[field]).fillna(0)
        logg_msg.append(
            f'"{field}" converted to numeric with separator assumed to be: "{sep_to_drop}"'
        )

    logg.log_to_file(
        heading="CONVERT TO NUMERIC",
        change_text="Field:",
        data_changes=logg_msg,
    )


def group_same_fund(
    data: pd.DataFrame,
    file_type: str,
) -> None:
    columns_to_sum = []
    match file_type:
        case "RHC":
            columns_to_sum = ["ANTALL_ANDELER", "VERDI"]
        case "PTOC":
            columns_to_sum = [
                "ANTALL_FLYTTET",
                "KOSTPRIS_PR_ISIN",
                "FLYTTET_KOSTPRIS_ASK",
                "MINSTE_INNSKUDD_HIA",
                "UBENYTTET_SKJERMING_31.12",
                "BENYTTET_SKJERMING_HIA",
            ]
        case _:
            return

    current_header = data.columns.tolist()

    if file_type == "RHC":
        data = (
            data.groupby(
                [field for field in current_header if field not in columns_to_sum]
            )[columns_to_sum]
            .sum()
            .round(8)
            .reset_index()
        )
    else:
        data = (
            data.groupby(
                [
                    field
                    for field in current_header
                    if field not in columns_to_sum + ["TRANSFER_REF"]
                ]
            )
            .agg(
                {
                    **{
                        col: "first"
                        for col in current_header
                        if col in ["TRANSFER_REF"]
                    },
                    **{col: "sum" for col in columns_to_sum},
                }
            )
            .round(8)
            .reset_index()
        )

    data = data[current_header]

    logg.log_to_file(
        heading="GROUPPING DATA",
        change_text="Grouped data by ISIN:\n",
        data_changes=[data],
    )

    return data
