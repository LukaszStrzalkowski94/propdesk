import time
import csv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options

def get_max_pages_manually():
    while True:
        try:
            max_pages = int(input("Enter the maximum number of pages to scrape (integer value): "))
            if max_pages <= 0:
                print("Please enter a positive integer greater than zero.")
            else:
                return max_pages
        except ValueError:
            print("Invalid input. Please enter a valid integer.")

def get_max_pages():
    url = "https://www.morizon.pl/mieszkania/warszawa/"
    
    options = Options()
    options.headless = True
    driver = webdriver.Firefox(options=options)

    try:
        driver.get(url)
        time.sleep()

        # Find the maximum number of pages in pagination
        max_pages_element = driver.find_element(By.XPATH, '//*[@id="__layout"]/div/div[2]/div[3]/div/div/div[40]/div/div[2]/ul/li[5]/div/button/span')
        max_pages = int(max_pages_element.text)

        return max_pages

    except Exception as e:
        print(f"Error while processing the page: {e}")
        return 0

    finally:
        driver.quit()

def get_all_links(max_pages):
    base_url = "https://www.morizon.pl/mieszkania/warszawa/?page={}"
    options = Options()
    options.headless = True
    driver = webdriver.Firefox(options=options)

    all_links = []

    try:
        for page_number in range(1, max_pages + 1):
            url = base_url.format(page_number)
            driver.get(url)
            time.sleep(5)

            # Execute JavaScript query to get all links on the page
            javascript_query = 'return Array.from(document.querySelectorAll(".offer a")).map((node) => node.href)'
            links = driver.execute_script(javascript_query)
            all_links.extend(links)

            # Print current page number
            print(f"Processing page {page_number}...")

    except Exception as e:
        print(f"Error while processing the page: {e}")

    finally:
        driver.quit()

    return all_links

def main():
    # Ask user for the maximum number of pages
    max_pages = get_max_pages_manually()

    # Get the maximum number of pages in pagination if not provided by the user
    if max_pages <= 0:
        max_pages = get_max_pages()

    if max_pages > 0:
        # Get all the links from the pages
        all_links = get_all_links(max_pages)

        # Print all the links
        for link in all_links:
            print(link)

        # Save all the links to a CSV file
        with open("morizon_all_links.csv", "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Links"])
            writer.writerows([[link] for link in all_links])

if __name__ == "__main__":
    main()