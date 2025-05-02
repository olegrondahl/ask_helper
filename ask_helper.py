import functions as f
import logging as logg





def get_data(debug: bool = False):
    # Get data from user
    print("Enter input:")

    data_from_user = ""
    while True:
        input_line = input().replace("−", "-")
        if input_line.strip().upper() == "END":
            break
        elif input_line.strip() != "":
            data_from_user += input_line + "\n"

    logg.create_folder_file()

    logg.log_to_file(
        heading="User input",
        data_changes=[data_from_user],
    )

    if debug:
        print("#######\nYOUR INPUT: \n\n")
        print(data_from_user)
        print("\n#######\n")


    separator = f.get_separator(data_from_user, debug=debug)
    data_frame = f.convert_data_to_df(separator, data_from_user, debug=debug)
    file_type = f.indentify_file_type(data_frame, debug=debug)
    f.update_header(data=data_frame, file_type=file_type)
    f.convert_distributor(data=data_frame, file_type=file_type)
    f.add_distributor(data=data_frame, file_type=file_type)
    f.remove_duplicate(data=data_frame)
    if file_type in ["RFH", "RHC", "PTOI", "PTOC"]:
        f.pad_customer_number(data=data_frame)
    if file_type in ["RHC", "PTOC"]:
        f.check_valid_error_code(data=data_frame)

    if file_type == "PTOI":
        f.check_for_units_in_ptoi(data=data_frame)
        f.move_misplaced_accountno(data=data_frame)
        f.fix_sell_cash(data=data_frame)
    
    if file_type=="RHC":
        f.remove_negative_cash(data=data_frame)

    if file_type == "PTOC":
        f.set_tax_value_per_isin(data=data_frame)
        f.update_tax_indetifier(data=data_frame)
        f.update_cash_identifier(data=data_frame)
        f.set_tax_and_cash_account(data=data_frame)
        f.move_tax_data(data=data_frame)
        f.fix_sell_cash(data=data_frame)

    if file_type in ["RHC", "PTOC", "NTO"]:
        f.convert_to_numeric(data=data_frame, file_type=file_type)
        # data_frame = f.group_same_fund(data=data_frame, file_type=file_type)

    logg.save_new_file(data=data_frame, file_type=file_type)


get_data(debug=False)
