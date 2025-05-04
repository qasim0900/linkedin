import os
import time
import logging
import pymongo
import logging
from dotenv import load_dotenv
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException, NoSuchElementException
from selenium.common.exceptions import WebDriverException, TimeoutException


# ----------------------------------
# :: ENV Variable Loader
# ----------------------------------

""" 
The load_dotenv() function loads environment variables from a .env file into the environment, allowing you to access them using os.getenv() in your code.
"""

load_dotenv()


# ----------------------------------
# :: Logging Variable
# ----------------------------------

""" 
This code sets the logging level of the "undetected_chromedriver" logger to ERROR, suppressing less severe log messages.
"""

logging.getLogger("undetected_chromedriver").setLevel(logging.ERROR)


# ----------------------------------
# :: Xpath Paths
# ----------------------------------

""" 
This code retrieves environment variable values and assigns them to respective variables such as DIR, PHONE, WEBSITE, ADDRESS, etc.
"""


base_url = os.getenv("BASE_URL")
email = os.getenv("LINKED_IN_EMAIL")
password = os.getenv("LINKED_IN_PASSWORD")
mongo_connection = os.getenv("MONGO_CONNECTION")


# ---------------------------------------
# :: Google Map List Class
# ---------------------------------------

"""
The GoogleMapList class automates retrieving and updating company details from Google Maps based on data in a MongoDB database.
"""


class GoogleMapList:
    # ---------------------------------------
    # :: Constructor Function
    # ---------------------------------------

    """
    This __init__ method initializes a GoogleMap object by setting up a web driver for headless
    browsing and connecting to a MongoDB database to access the "company_details" collection within the
    "estimation_db" database.

    """

    def __init__(self):
        self.driver = None
        self.client = None
        try:
            self.driver = self.google_chrome_function()
            database = self.mongodb_connection_function(
                "linkedin_estimation", "profiles")
            self.client = database["client"]
            self.collection = database["collection"]
            self.driver.get(base_url)
        except Exception as e:
            logging.error(f"Failed to initialize BlueBook: {e}")
            raise

    # ---------------------------------------
    # :: Start Requests Function
    # ---------------------------------------

    """
    The Start Requests method navigates to Google Maps and retrieves documents from the MongoDB collection
    where the address is "Not found," then calls the parse method for each relevant document
    to process the company's details.

    """

    def start_requests(self):
        try:
            while True:
                href_data_list = []
                try:
                    linkedin_profiles = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_all_elements_located(
                            (By.XPATH, '//div[@class="dURPMd"]//a')
                        )
                    )
                    if linkedin_profiles:
                        href_data_list = [
                            {"href": profile.get_attribute("href")}
                            for profile in linkedin_profiles
                            if profile.get_attribute("href")
                        ]

                        if href_data_list:
                            operations = []
                            for data in href_data_list:
                                operation = pymongo.UpdateOne(
                                    {"linkedin_profile": data["href"]},
                                    {"$setOnInsert": {**data, "send_email": False}},
                                    upsert=True,
                                )
                                operations.append(operation)

                            if operations:
                                try:
                                    result = self.collection.bulk_write(
                                        operations)
                                    logging.info(
                                        f"Bulk write operation completed: {result.upserted_count} inserted."
                                    )
                                except Exception as db_error:
                                    logging.error(
                                        f"Database insertion error: {db_error}",
                                        exc_info=True,
                                    )

                        else:
                            logging.warning("No new hrefs to insert.")
                except WebDriverException as e:
                    logging.error(
                        f"Error finding LinkedIn profiles: {e}", exc_info=True
                    )

                try:
                    time.sleep(2)
                    next_link_element = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable(
                            (
                                By.XPATH,
                                '//*[@id="pnnext"]',
                            )
                        )
                    )
                    next_link = next_link_element.get_attribute("href")
                    if next_link:
                        self.driver.get(next_link)
                        logging.info("Navigated to next page.")
                    else:
                        logging.info("No next page link found. Stopping.")
                        break
                except NoSuchElementException:
                    logging.info("Reached last page. Exiting.")
                    break
                except WebDriverException as nav_error:
                    logging.error(
                        f"Error navigating to next page: {nav_error}", exc_info=True
                    )
                    break

        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}", exc_info=True)

    def login(self):
        try:
            # Wait for email input field
            email_input = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(
                    (By.XPATH,
                     "/html/body/div[1]/main/div[2]/div[1]/form/div[1]/input")
                )
            )
            email_input.clear()
            email_input.send_keys(email)

            # Wait for password input field
            password_input = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(
                    (By.XPATH,
                     "/html/body/div[1]/main/div[2]/div[1]/form/div[2]/input")
                )
            )
            password_input.clear()
            password_input.send_keys(password)

            # Wait for and click the submit button
            submit_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable(
                    (
                        By.XPATH,
                        "/html/body/div[1]/main/div[2]/div[1]/form/div[4]/button",
                    )
                )
            )
            submit_button.click()

            # Wait for successful login (adjust the condition as needed)
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(
                    (By.XPATH, "//div[@class='application-outlet']")
                )  # Change this to a valid element after login
            )

            print("Login successful!")
            return True

        except TimeoutException:
            print("Login failed: Element not found or timeout.")
            return False

    # ---------------------------------------
    # :: Parse Fetch Function
    # ---------------------------------------

    """
    The data_fetch method retrieves and validates business information from a webpage using Selenium, storing the results in a structured format.
    """

    def parse(self):
        try:
            flag = self.login()
            if flag:
                for profile in self.collection.find({"Name": {"$exists": False}}):
                    linkedin_url = profile.get("linkedin_profile")
                    if not linkedin_url:
                        logging.warning(
                            "Skipping profile with no LinkedIn URL.")
                        continue

                    time.sleep(3)
                    try:
                        self.driver.get(linkedin_url)
                        time.sleep(1)
                    except TimeoutException:
                        logging.warning(
                            f"Skipping {linkedin_url} due to slow loading (timeout after 10 sec)."
                        )
                        continue

                    try:
                        follow_button = WebDriverWait(self.driver, 5).until(
                            EC.element_to_be_clickable(
                                (
                                    By.XPATH,
                                    "/html/body/div[6]/div[3]/div/div/div[2]/div/div/main/section[1]/div[2]/div[3]/div/button/span",
                                )
                            )
                        )

                        if follow_button and follow_button.text.lower() == "follow":
                            follow_button.click()
                            logging.info(
                                f"Clicked 'Follow' button for {linkedin_url}")
                            self.collection.update_many(
                                {"linkedin_url": linkedin_url},
                                {"$set": {"send_email": "Yes"}},
                            )

                        else:
                            self.collection.update_many(
                                {"linkedin_url": linkedin_url},
                                {"$set": {"send_email": "Yes"}},
                            )
                            logging.info(f"Already following {linkedin_url}")

                    except TimeoutException:
                        logging.warning(
                            f"'Follow' button not found or already clicked for {linkedin_url}"
                        )

                    name_text = None
                    try:
                        name_element = WebDriverWait(self.driver, 5).until(
                            EC.presence_of_element_located(
                                (
                                    By.XPATH,
                                    "/html/body/div[6]/div[3]/div/div/div[2]/div/div/main/section[1]/div[2]/div[2]/div[1]/div[1]/span[1]/a/h1",
                                )
                            )
                        )
                        name_text = name_element.text.strip()
                    except Exception as e:
                        logging.warning(
                            f"Name not found for {linkedin_url}: {e}")

                    # Extract Skills
                    skill_text = None
                    try:
                        skill_element = WebDriverWait(self.driver, 5).until(
                            EC.presence_of_element_located(
                                (
                                    By.XPATH,
                                    "/html/body/div[6]/div[3]/div/div/div[2]/div/div/main/section[1]/div[2]/div[2]/div[1]/div[2]",
                                )
                            )
                        )
                        skill_text = skill_element.text.strip()
                    except Exception as e:
                        logging.warning(
                            f"Skills not found for {linkedin_url}: {e}")

                    # Prepare data to update
                    update_data = {}
                    if name_text:
                        update_data["Name"] = name_text
                    if skill_text:
                        update_data["Skill"] = skill_text

                    # Update MongoDB for this specific LinkedIn profile
                    if update_data:
                        self.collection.update_one(
                            {"linkedin_profile": linkedin_url},  # Filter
                            # Update only extracted fields
                            {"$set": update_data},
                        )
                        logging.info(f"Updated profile: {linkedin_url}")

                    # Additional delay before processing the next profile
                    time.sleep(3)  # Wait longer to avoid detection

        except Exception as e:
            logging.error(
                f"An error occurred in parse function: {e}", exc_info=True)

    # ----------------------------------
    # :: Google Chrome Function
    # ----------------------------------

    """ 
    This function initializes a Chrome WebDriver with custom options and handles any errors during setup.
    """

    def google_chrome_function(self):
        try:
            chrome_driver_path = os.getenv("CHROME_DRIVER")
            chrome_options = Options()
            chrome_options.add_argument("--v=1")
            # chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--enable-logging")
            chrome_options.add_argument("--disable-dev-shm-usage")
            service = Service(executable_path=chrome_driver_path)
            driver = uc.Chrome(service=service, options=chrome_options)
            return driver

        except EnvironmentError as env_err:
            logging.error(str(env_err))
            raise

        except WebDriverException as wd_err:
            logging.error(f"Failed to initialize WebDriver: {str(wd_err)}")
            raise

        except Exception as e:
            logging.error(
                f"An unexpected error occurred while initializing Chrome WebDriver: {str(e)}"
            )
            raise

    # ----------------------------------
    # :: Google Chrome Function
    # ----------------------------------

    """ 
    This function connects to a MongoDB database and collection, logging success or error messages based on the outcome.
    """

    def mongodb_connection_function(self, db_name, collection_name):
        try:
            client = pymongo.MongoClient()
            db = client[db_name]
            collection = db[collection_name]
            return {"client": client, "collection": collection}
        except Exception as e:
            logging.error(
                f"An unexpected error occurred while connecting to MongoDB: {str(e)}"
            )
            raise

    # ----------------------------------------
    # :: Destructor Function
    # ---------------------------------------

    """
    The __del__ method ensures that the web driver is properly closed when the GoogleMap object is deleted.
    """

    def __del__(self):
        try:
            logging.info("Calling excel file save function...")
            if self.driver:
                self.driver.quit()
            if self.client:
                self.client.close()
        except Exception as e:
            logging.error(f"Error while saving Excel file: {e}")


# ---------------------------------------
# :: Mian List Function
# ---------------------------------------

"""
The main_list function initializes a GoogleMapList spider and iterates through its start_requests,
passing each request's parameters to the parse method for processing. It then calls main_list to execute this workflow.
"""


def main_list():
    spider_list = GoogleMapList()
    spider_list.parse()


main_list()
