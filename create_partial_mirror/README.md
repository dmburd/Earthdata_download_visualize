## Download selected arrays

1. `cd` to this folder, create a virtual environment and activate it:
```bash
conda env create -f environment.yml
conda activate pygeo2
```

2. Rename the config file (`config_example.yaml` -> `config.yaml`) and edit it (specify your Earthdata login and password, select the dates and the names of the datasets you want to download and save).

3. Run the script:
```bash
python save_to_netcdf.py
```
If your Earthdata credentials are correct, you will see lines like
```text
Authenticated? True
session=<earthaccess.auth.Auth object at 0x...>
session.get_session()=<earthaccess.auth.SessionWithHeaderRedirection object at 0x...>
```
at the beginning of the output.
