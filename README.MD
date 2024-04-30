## INSTALATION 
```
pip install -r requirements.txt
```
or 
```
pip3 install -r requirements.txt
```

## SETUP / ENV VARiABLES

The script requirest to folder paths, one for where to log the changes and a back-up file and one to preffered file destination of the output file. 
Both of these variables should be sat in `./env_data.py`. Remeber the `/` at the end of the files 

env_data.py:
```py
download_folder = "C:/Users/**SIGNATUR**/Downloads/"
log_folder = "C:/Users/**SIGNATUR**/**/ask_helper/test_log/"
```

## USE
```
python3 ask_helper.py
```

paste file content after `Enter input:`
when you want the system to handle the content write `end` on a new line by itself 


## MAINTAIN
There are two static data files that needs to be maintained `./static_data/conversion_data.py` & `./static_data/headers.py`.
The first used to convert to convert know input issues to valid Wolf values. 
First is distributor names to Wolf know codes and new distirbutors are added inside of `distributors` dict on a new line with convesion being "TRANSLATE_FORM" : "TRANSLATE_TO".
The second translate know invalid key for tax data to "Skatteopplysninger". Values to be translated are inserted into the `tax_key` list

./static_data/conversion_data.py:
```py
distributors = {
    "Søderberg og Partners Wealth Management AS": "SP",
    "Sparebank1 Nord-Norge": "SB1",
    "Søderberg og Partners": "SP",
}

tax_data = ["skattedata", "skatedata", "kateopplysninger"]
```