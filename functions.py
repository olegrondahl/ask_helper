import pandas as pd
from io import StringIO
from colorama import Fore, Style
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
    if ncols < 6:
        raise ValueError(f"DataFrame has only {ncols} columns. Not enough to identify file type.")
    
    file_type_est = {"RFH": 0, "RHC": 0, "PTOI": 0, "PTOC": 0, "NTO": 0}


    if ncols == 9:
        file_type_est["RFH"] += 10
    elif ncols == 13:
        file_type_est["NTO"] += 5  
    elif ncols == 17:
        file_type_est["RHC"] += 10
        file_type_est["PTOI"] += 10
    elif ncols == 27:
        file_type_est["PTOC"] += 10

    columns_set = set(data.columns)

    if set(headers.RFH_header) == columns_set:
        file_type_est["RFH"] += 20  
    if set(headers.NTO_header) == columns_set:
        file_type_est["NTO"] += 20  

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
            ncols > 11 and len(row[data.columns[11]]) == 12
            and str(row[data.columns[11]][:2]).isalpha()
        ):
            isin_ptoi += 1

        if (
            ncols > 10 and len(row[data.columns[10]]) == 12
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

        if ncols > 7 and len(row[data.columns[7]]) == 3:
            currency_rfh += 1

        if ncols > 12 and len(row[data.columns[12]]) == 3:
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
            header = headers.RFH_header
        case "RHC":
            header = headers.RHC_header
        case "PTOI":
            header = headers.PTOI_header
        case "PTOC":
            header = headers.PTOC_header
        case "NTO":
            header = headers.NTO_header

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
    logg_msg = []
    if file_type.upper() in {"RFH", "RHC", "PTOI", "PTOC"}:
        dist_dict = {
            key.upper(): value for key, value in conversion_data.distributors.items()
        }
        cols_conv = [0, 1]
    elif file_type.upper() == "NTO":
        dist_dict = {
            key.upper(): value for key, value in conversion_data.NTO_distributors.items()
        }
        cols_conv = [1, 4]

    for index, row in data.iterrows():
        for col in cols_conv:
            orig_val = str(row.iloc[col]).upper()
            if orig_val in dist_dict:
                logg_msg.append(
                    f"{index +1} [{data.columns[col]}] {row.iloc[col]} -> {dist_dict[orig_val]}"
                )
                row.iloc[col] = dist_dict[orig_val]

    for col in cols_conv:
        data.iloc[:, col] = data.iloc[:, col].str.upper()
    
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


def update_cash_identifier(data: pd.DataFrame) -> None:
    logg_msg = []
    for index, row in data.iterrows():
        verdipapirnavn = str(row["VERDIPAPIRNAVN"]).strip()
        if verdipapirnavn.upper().startswith("CASH") and verdipapirnavn != "Cash":
            logg_msg.append(
                f"{index + 1} [MTR: {row['MASTERTRANSFERREF_(FULLMAKTSNR)']}] changed '{row['VERDIPAPIRNAVN']}' -> 'Cash'"
            )
            data.at[index, "VERDIPAPIRNAVN"] = "Cash"

    if logg_msg:
        logg.log_to_file(
            heading="UPDATE CASH IDENTIFIER",
            change_text="Changed cash identifier on row: ",
            data_changes=logg_msg,
        )


def set_tax_and_cash_account(data: pd.DataFrame) -> None:
    logg_msg = []
    unique_mtrs = data["MASTERTRANSFERREF_(FULLMAKTSNR)"].unique()

    for mtr in unique_mtrs:
        mtr_rows = data[data["MASTERTRANSFERREF_(FULLMAKTSNR)"] == mtr]
        unique_accounts = mtr_rows["TIL_ASK_KONTO_KUNDE_TILBYDER"].dropna().unique()
        unique_accounts = [acc for acc in unique_accounts if acc.strip()]

        if len(unique_accounts) > 1:
            error_msg = f"Error: More than one account number for {mtr}: {unique_accounts}. Account number for MTR is excluded from the file."
            print(Fore.RED + "!!! - " + error_msg + Style.RESET_ALL)
            logg_msg.append(error_msg)
            continue
        
        elif len(unique_accounts) == 0:
            error_msg = f"Warning: No account number found for {mtr}. No updates applied for this MTR."
            print(Fore.RED + "!!! - " + error_msg + Style.RESET_ALL)
            logg_msg.append(error_msg)
            continue
        
        correct_account = unique_accounts[0]
        updated_entries = []

        for category in ["Cash", "Skatteopplysninger"]:
            mask = (data["MASTERTRANSFERREF_(FULLMAKTSNR)"] == mtr) & \
                   (data["VERDIPAPIRNAVN"] == category) & \
                   (data["TIL_ASK_KONTO_KUNDE_TILBYDER"] == "")
            
            if mask.any():
                data.loc[mask, "TIL_ASK_KONTO_KUNDE_TILBYDER"] = correct_account
                updated_entries.append(category)

        if updated_entries:
            categories_updated = " and ".join(updated_entries)
            logg_msg.append(f"Updated account number {correct_account} for {categories_updated} in MTR {mtr}.")

    if logg_msg:
        logg.log_to_file(
            heading="UPDATE TAX AND CASH ACCOUNT",
            change_text="Updated Cash and Tax accounts:",
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
        case "NTO":
            columns_to_convert = ["ANTALL_ANDELER"]
        case "RHC":
            columns_to_convert = ["ANTALL_ANDELER", "VERDI"]
        case "PTOC":
            columns_to_convert = [
                "ANTALL_FLYTTET",
                "KOSTPRIS_PR_ISIN",
                "FLYTTET_KONTANTER",
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
            data[field] = data[field].str.strip().str.replace(r'\s+', '', regex=True)
            comma = row[field].find(",")
            period = row[field].find(".")

            if comma != -1 or period != -1:
                if comma > period:
                    count_comma += 1
                else:
                    count_period += 1

    sep_to_drop = "." if count_comma > count_period else ","
    sep_used = "," if count_comma > count_period else "."

    for field in columns_to_convert:
        data[field] = data[field].str.replace(sep_to_drop, "").str.replace(",", ".")
        data[field] = pd.to_numeric(data[field]).fillna(0)
        logg_msg.append(
            f'"{field}" converted to numeric with separator assumed to be: "{sep_used}"'
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


def add_distributor(data: pd.DataFrame, file_type: str) -> None:
    logg_msg = []

    data.iloc[:, 0] = data.iloc[:, 0].astype(str).str.strip()
    data.iloc[:, 1] = data.iloc[:, 1].astype(str).str.strip()

    for mtr, mtr_rows in data.groupby("MASTERTRANSFERREF_(FULLMAKTSNR)"):
        mottakende = set(mtr_rows.iloc[:, 0].dropna()) - {""}
        avleverende = set(mtr_rows.iloc[:, 1].dropna()) - {""}

        if len(mottakende) > 1 or len(avleverende) > 1:
            msg = f"Error: More than one distributor for {mtr}. Could not fill empty distributor for this MTR"
            print(Fore.RED + "!!! - " + msg + Style.RESET_ALL)
            logg_msg.append(msg)
            raise Exception(msg)

        if not mottakende or not avleverende:
            msg = f"Warning: No distributor found for {mtr}. Could not fill empty distributor for this MTR."
            print(Fore.RED + "!!! - " + msg + Style.RESET_ALL)
            logg_msg.append(msg)
            raise Exception(msg)

        for index, value in [(0, next(iter(mottakende))), (1, next(iter(avleverende)))]:
            mask = (data["MASTERTRANSFERREF_(FULLMAKTSNR)"] == mtr) & (data.iloc[:, index] == "")
            if mask.any():
                data.loc[mask, data.columns[index]] = value
                for row in data.index[mask]:
                    logg_msg.append(f"{row+1} [{data.columns[index]}] Empty -> {value}")

        logg.log_to_file(
            heading="ADD DISTRIBUTOR",
            change_text="Added distributor on row: ",
            data_changes=logg_msg,
        )


def remove_duplicate(data: pd.DataFrame) -> pd.DataFrame:
    logg_msg = []
    duplicate_rows = data[data.duplicated(keep='first')]
    data.drop_duplicates(inplace=True)
    
    if not duplicate_rows.empty:
        for index, row in duplicate_rows.iterrows():
            row_values = ', '.join(str(value) for value in row.values)
            logg_msg.append(f"{index+1} {row_values}")

        error_msg = f"Warning: Removed a duplicate row, please verify."
        print(Fore.RED + "!!! - " + error_msg + Style.RESET_ALL)

        logg.log_to_file(
            heading="REMOVE DUPLICATES",
            change_text="Removed duplicate on row: ",
            data_changes=logg_msg,
        )
    
    return data


def remove_negative_cash(data: pd.DataFrame) -> None:
    logg_msg = []
    for index, row in data.iterrows():
        if row["VERDI"].startswith("-") and row["FEILKODE"]=="G" and row["VERDIPAPIRNAVN"].upper()=="CASH":
            logg_msg.append(
                f"{index+1} [{row['VERDI']}] -> 0"
            )
            row["VERDI"] = "0"

    logg.log_to_file(
        heading="REMOVE NEGATIVE CASH",
        change_text="Changed negative cash on row: ",
        data_changes=logg_msg,
    )


def move_misplaced_accountno(data: pd.DataFrame) -> None:
    logg_msg=[]
    mask = (
        data.apply(lambda r: str(r["VERDIPAPIRNAVN"]).startswith(str(r["MOTTAKENDE_TILBYDER"])), axis=1) &
        (data["SELG_ALLE_ANDELER"] == "N") &
        (data["TIL_KONTO_NOMINEE_TILBYDER"].str.strip() == "") & 
        (data["TIL_ASK_KONTO_KUNDE_TILBYDER"]!="")
    )

    row = data.index[mask]
    for index in row:
        logg_msg.append(
        f"{index+1} from TIL_ASK_KONTO_KUNDE_TILBYDER -> TIL_KONTO_NOMINEE_TILBYDER"
        )
    data.loc[mask, "TIL_KONTO_NOMINEE_TILBYDER"] = data.loc[mask, "TIL_ASK_KONTO_KUNDE_TILBYDER"]
    data.loc[mask, "TIL_ASK_KONTO_KUNDE_TILBYDER"] = ""
    
    logg.log_to_file(
        heading="MOVED MISPLACED ACCOUNT NUMBER",
        change_text="Moved misplaced account number on row: ",
        data_changes=logg_msg,
    )

def fix_sell_cash(data: pd.DataFrame) ->None:
    logg_msg=[]

    selg_column = "SELG_ALLE_ANDELER" if "SELG_ALLE_ANDELER" in data.columns else "SELG_ANDELER"
    
    for index, row in data.iterrows():
        if row[selg_column].strip() == "" and row["VERDIPAPIRNAVN"].upper() == "CASH":
            logg_msg.append(
                f"{index+1} [{row[selg_column]}] -> N"
            )
            row[selg_column] = "N"
        if row["STOPP_SPAREAVTALE"].strip() =="":
            logg_msg.append(
                f"{index+1} [{row["STOPP_SPAREAVTALE"]}] -> J"
            )
            row["STOPP_SPAREAVTALE"] = "J"

    logg.log_to_file(
        heading="FIX J/N ON SELL ALL UNITS AND STOP AGREEMENT",
        change_text="Added J/N to row : ",
        data_changes=logg_msg,
    )