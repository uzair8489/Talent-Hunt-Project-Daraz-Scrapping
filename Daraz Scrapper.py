import logging
import time
import re
import pandas as pd
import mysql.connector
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
from openpyxl import Workbook
import schedule

# Configure logging
logging.basicConfig(filename='daraz scrapping logs.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def daraz_scrapping_script():
    # Initialize the Chrome webdriver
    chrome_options = Options()
    # chrome_options.add_argument("--start-maximized")
    chrome_options.add_experimental_option("detach", True)

    driver = webdriver.Chrome(options=chrome_options)

    try:
        # Open the Daraz website
        driver.get("https://www.daraz.pk/")

        # Find the "Electronic devices" element
        electronics_menu = driver.find_element("xpath", "//li[@id='Level_1_Category_No1']/a")
        laptops_submenu_locator = driver.find_element("xpath", "//li[@data-cate='cate_7_5']/a")

        # Create an instance of ActionChains
        actions = ActionChains(driver)

        # Perform a hover action on the "Electronic devices" element
        actions.move_to_element(electronics_menu).move_to_element(laptops_submenu_locator).click().perform()

        # Assuming you have located the "Next" button element
        next_button = driver.find_element("xpath", "//li[@title='Next Page']")
        page_count = 0
        product_count = 0

        laptop_name = []
        laptop_price = []
        laptop_url = []
        laptop_rating = []
        laptop_review = []
        class_to_value = {
            "star-icon--k88DV star-10--UQtQk": 10,
            "star-icon--k88DV star-9--yMyuX": 9,
            "star-icon--k88DV star-8--lQLaV": 8,
            "star-icon--k88DV star-7--UNNG4": 7,
            "star-icon--k88DV star-6--ezGMb": 6,
            "star-icon--k88DV star-5--hXNSC": 5,
            "star-icon--k88DV star-4--hM0en": 4,
            "star-icon--k88DV star-3--zmTQe": 3,
            "star-icon--k88DV star-2--fBIsH": 2,
            "star-icon--k88DV star-1--Do7NZ": 1,
            "star-icon--k88DV star-0--WgmCt": 0,
        }

        logging.info('Laptops Data Scrapping Started....\n')
        while True:
            laptops = driver.find_elements("xpath", "//div[@class='inner--SODwy']")
            product_count += len(laptops)
            page_count += 1
            logging.info(f'Scrapping Data from page {page_count}....')
            logging.info(f"Products found on page {page_count}: {len(laptops)}\n")

            # Scrapping laptop's title
            for laptop in laptops:
                laptop_names = laptop.find_elements("xpath", ".//div[@class='title--wFj93']/a")
                for name in laptop_names:
                    laptop_name.append(name.text)

                # Scrapping laptop's price
                try:
                    if len(laptop.find_elements("xpath", ".//div[@class='price--NVB62']/span[@class='currency--GVKjl']")) > 0:
                        prices = laptop.find_elements("xpath", ".//div[@class='price--NVB62']/span[@class='currency--GVKjl']")
                        for price in prices:
                            laptop_price.append(price.text)
                    else:
                        laptop_price.append("0")
                except Exception as e:
                    logging.error(f"Error occurred while scraping price: {str(e)}")

                # Scrapping laptop's url
                try:
                    if len(laptop.find_elements("xpath", ".//div[@class='title--wFj93']/a")) > 0:
                        urls = laptop.find_elements("xpath", ".//div[@class='title--wFj93']/a")
                        for url in urls:
                            laptop_url.append(url.get_attribute("href"))
                    else:
                        laptop_url.append("NULL")
                except Exception as e:
                    logging.error(f"Error occurred while scraping price: {str(e)}")

                # Scrapping laptop's reviews
                try:
                    if len(laptop.find_elements("xpath", ".//div[@class='rating--ZI3Ol rate--DCc4j']/span[@class= 'rating__review--ygkUy']")) > 0:
                        reviews = laptop.find_elements("xpath", ".//div[@class='rating--ZI3Ol rate--DCc4j']/span[@class= 'rating__review--ygkUy']")
                        for review in reviews:
                            review_text = re.sub(r'[()]', '', review.text)  # Remove brackets from review count
                            laptop_review.append(int(review_text))
                    else:
                        laptop_review.append(int("0"))
                except Exception as e:
                    logging.error(f"Error occurred while scraping review: {str(e)}")
                
                # Scrapping laptop's rating
                try:
                    stars = laptop.find_elements("xpath", ".//div/span/i[contains(@class, 'star-icon--k88DV')]")
                    rating = 0
                    if len(laptop.find_elements("xpath", ".//div/span/i[contains(@class, 'star-icon--k88DV')]")) > 0:
                        for star in stars:
                            class_name = star.get_attribute("class")
                            if class_name in class_to_value:
                                value = class_to_value[class_name]
                                rating += value/10.0
                    else:
                        rating = 0.0
                    laptop_rating.append(rating)
                except Exception as e:
                    logging.error(f"Error occurred while scraping ratings: {str(e)}")


            if next_button.get_attribute("aria-disabled") == 'true':
                break
            
            next_button.click()
            time.sleep(5)  # Adding a delay to allow the next page to load properly

            # Locate the next button again for the next iteration
            next_button = driver.find_element("xpath", "//li[@title='Next Page']")
            

        logging.info('Finishing the scrapping....\n')
        logging.info(f"Total products found: {product_count}\n")

        # Storing the laptop data in MySQL database
        # Database integration

        connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='lakeview',
        database='first_api_db'
        )
        logging.info('Connecting to the database......\n')

        #checking if the database is connected
        if connection.is_connected():
            logging.info('Database connected successfully.\n')
            logging.info('Storing data in database......\n')

        cursor = connection.cursor()

        # Inserting laptop data into the table
        for name, price, url, rating, review in zip(laptop_name, laptop_price, laptop_url, laptop_rating, laptop_review):
            insert_query = "INSERT INTO laptops_name_price (laptop_name, laptop_price, laptop_url) VALUES (%s, %s, %s)"
            cursor.execute(insert_query, (name, price, url))

             # Retrieve the last inserted laptop_id
            laptop_id = cursor.lastrowid

            # Insert the rating and review into the laptop_ratings_reviews table
            rating_review_query = "INSERT INTO laptops_ratings_reviews (id, laptop_ratings, laptop_reviews) VALUES (%s, %s, %s)"
            cursor.execute(rating_review_query, (laptop_id, rating, review))

        # Committing the changes to the database
        connection.commit()

        # Closing the cursor and the connection
        cursor.close()
        connection.close()
        logging.info('Data stored in Database Successfully.')

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    
    finally:
        # Closing the browser window
        driver.quit()
        logging.info('Browser Closed....')


# Schedule the script to run every Tuesday at a specific time
schedule.every().tuesday.at("09:00").do(daraz_scrapping_script)

while True:
    schedule.run_pending()
    time.sleep(1)
