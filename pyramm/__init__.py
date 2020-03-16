
from os import getenv
from pathlib import Path
from configparser import ConfigParser


CONFIG_FILE = Path(getenv("HOME")).joinpath("pyramm.ini")
config = ConfigParser()
config.read(CONFIG_FILE)
