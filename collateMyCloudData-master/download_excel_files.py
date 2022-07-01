import json
import logging
import os
import time
from datetime import datetime
from threading import Thread

from dateutil.relativedelta import relativedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class MyCloudWorker(Thread):
    logger = logging.getLogger(__name__)

    def __init__(self, data, property_name, property_id, date_list):
        Thread.__init__(self)
        self.data = data
        self.property_name = property_name
        self.property_id = property_id
        self.date_list = date_list

    def run(self):
        start = time.perf_counter()
        options = webdriver.ChromeOptions()
        options.headless = True
        options.add_argument('log-level=3')
        download_dir = os.path.join(self.data["download_dir"], "tmp", self.property_name)
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)
        
        prefs = {"download.default_directory": download_dir}
        options.add_experimental_option("prefs", prefs)
        driver = webdriver.Chrome(executable_path=self.data["chrome_driver_path"], options=options)
        driver.maximize_window()

        self.logger.info("Started working on %s" % self.property_name)
        try:
            self.setup(driver, self.data["username"], self.data["password"], self.property_id)
        
            for (start_date, end_date) in self.date_list:
                start_download = time.perf_counter()
                
                # Trying 3 times to download
                for x in range(3):
                    self.download_report(driver, start_date, end_date)
                    downloaded_file = os.path.join(download_dir, "Guest_Transaction_History.xlsx")
                    
                    # Moving and renaming downloaded file
                    if os.path.exists(downloaded_file):
                        # To move file to date name folder
                        path_name = os.path.join(self.data["download_dir"], datetime.today().strftime("%Y-%m-%d"))
                        new_filename = self.property_name + "_" + datetime.strptime(start_date, '%d/%m/%Y').strftime("%Y-%m") + ".xlsx"
                        
                        if not os.path.exists(path_name):
                            os.makedirs(path_name)
                        # Move to file to new dir and break out of loop
                        os.rename(downloaded_file, os.path.join(path_name, new_filename))
                        break
                    else:
                        logger.info("Could not download %s for %s to %s. Tries: %s" % (
                            self.property_name, start_date, end_date, x))

                self.logger.info('Completed from %s to %s for %s time taken %s seconds' % (
                    self.property_name, start_date, end_date, round((time.perf_counter() - start_download), 2)))
            
            driver.execute_script('doLogout();')
            self.logger.info("Logging out of %s" % self.property_name)
        finally:
            driver.close()
            self.logger.info(f"Closing Driver for {self.property_name}. The whole process took {round(time.perf_counter() - start, 2)} seconds")

    @staticmethod
    def setup(driver: webdriver, username: str, password: str, prop_id: str):
        # Set Wait Timeout to 400 Seconds
        wait = WebDriverWait(driver, 400)
        # Getting Home Page
        driver.get("https://live.mycloudhospitality.com/Login/Common/Index.aspx")
        # Wait until txtImageVerification is clickable
        wait.until(ec.element_to_be_clickable((By.ID, "txtImageVerification")))
        # Disabling Captcha
        driver.execute_script('$("#lblHiddenSuppressCaptchaValidation")[0].value = "Y"')
        # Fill Login and Passsword
        driver.find_element_by_name('txtUserNameEmailId').send_keys(username)
        driver.find_element_by_name('txtPassword').send_keys(password)
        # Click Login
        driver.find_element_by_name('btnLogin').click()
        # Select First Property
        # Wait till selecet property button is clickable and click
        wait.until(ec.element_to_be_clickable((By.ID, "dgChangeProperty_btnOpenChangeProperty_" + prop_id))).click()
        # Wait PMA application Button to be clickable and then click
        wait.until(ec.element_to_be_clickable((By.ID, "imgCPMS"))).click()
        # Wait Report Button to be clikable and then click
        wait.until(ec.element_to_be_clickable((By.ID, "btnReportsWelcome"))).click()
        # Wait until Report Search Box is clickable and then type Guest Transactions
        wait.until(ec.element_to_be_clickable((By.ID, "txtSearchReports"))).send_keys('Guest Transaction')
        # Wait until Search Button is clickable and then click Search button
        wait.until(ec.element_to_be_clickable((By.ID, "btnSearchReports"))).click()
        # Wait for invisibility of Overlay Process
        wait.until(ec.invisibility_of_element((By.ID, "overlayProcess")))
        # Wait till the Guest Transaction Report Link is present
        wait.until(ec.presence_of_element_located((By.XPATH, '//*[@id="dgReportsList"]/tbody/tr[3]/td[4]')))
        # Wait till Guest Transaction Report Link is clickable and then click Guest Transaction Report Link
        element = wait.until(ec.element_to_be_clickable((By.XPATH, '//*[@id="dgReportsList"]/tbody/tr[3]/td[4]')))
        driver.execute_script("arguments[0].click();", element)
        # Wait for invisibility of Overlay Process
        wait.until(ec.invisibility_of_element((By.ID, "overlayProcess")))
        # Wait till From Date Textbox is clickable
        wait.until(ec.element_to_be_clickable((By.ID, "cmbDatefrom")))
        return

    @staticmethod
    def download_report(driver, start_date, end_date):
        wait = WebDriverWait(driver, 900)
        # Wait for invisibility of Overlay Process
        wait.until(ec.invisibility_of_element((By.ID, "overlayProcess")))
        # Wait till From Date Textbox is clickable
        wait.until(ec.element_to_be_clickable((By.ID, "cmbDatefrom")))
        # Set the Start Date using Javascript
        driver.execute_script('$("#cmbDatefrom")[0].value = "%s"' % start_date)
        # Set the End Date using Javascript
        driver.execute_script('$("#cmbDateto")[0].value = "%s"' % end_date)

        # Wait for invisibility of Overlay Process
        wait.until(ec.invisibility_of_element((By.ID, "overlayProcess")))
        # Wait until Download Report Button is clickable and then click Download Report Button
        element = wait.until(ec.element_to_be_clickable((By.ID, "btnOkReports")))
        driver.execute_script("arguments[0].click();", element)
        # Wait for invisibility of Overlay Process
        wait.until(ec.invisibility_of_element((By.ID, "overlayProcess")))
        # Wait for invisibility of Overlay Process (Not sure why it is being done twice but it works)
        wait.until(ec.invisibility_of_element((By.ID, "overlayProcess")))
        # End of Loop
        time.sleep(2)
        # Wait till From Date Textbox is clickable
        wait.until(ec.element_to_be_clickable((By.ID, "cmbDatefrom")))
        return


def get_date_strings(start_date: datetime, start_month: int, end_month: int) -> list:

    start_date = start_date.replace(day=1)
    rv = []
    for x in range(start_month, end_month + 1):
        s_delta = relativedelta(months=x)
        e_delta = relativedelta(months=x + 1) + relativedelta(days=-1)
        rv.append(
            ((start_date + s_delta).strftime('%d/%m/%Y'), (start_date + e_delta).strftime('%d/%m/%Y'))
        )
    return list(rv)


def main(date_str: str):
    # Reading download config file
    with open('config.json') as f:
        data = json.load(f)
    
    start_date = datetime.strptime(date_str, '%Y-%m-%d')
    date_list = get_date_strings(start_date, data["start_month_offset"], data["end_month_offset"])
    workers = []
    
    for (key, value) in data["properties"].items():
        worker = MyCloudWorker(data, key, value, date_list)
        worker.start()
        workers.append(worker)
    
    for worker in workers:
        worker.join()
