## Download selected arrays

1. `cd` to this folder, create a virtual environment and activate it:
```bash
conda env create -f environment.yml
conda activate pygeo2
```

1. Rename the config file (`config_example.yaml` -> `config.yaml`) and edit it (specify your Earthdata login and password, select the dates and the names of the datasets you want to download and save).

1. Run the script:
```bash
python save_to_netcdf.py
```

