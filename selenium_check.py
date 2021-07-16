import selenium


import selenium
from selenium import webdriver
from selenium.webdriver.common.keys import Keys

from mintapi import api

a = api.get_latest_chrome_driver_version()

print(a)

print("Selenium Version is: " + selenium.__version__)


