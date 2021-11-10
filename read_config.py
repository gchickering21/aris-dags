import os
import configparser

# set-up parsers and read 
config = configparser.ConfigParser()
config.read("config.ini")

# read git path 
SERVICE_GIT_DIR = config.get('DIRECTORIES', 'SERVICE_GIT_DIR')
