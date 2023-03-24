


import requests, pickle, json, asyncio, aiohttp
from bs4 import BeautifulSoup


import psycopg2 as ps
from psycopg2.extras import RealDictCursor
from psycopg2 import sql as s
import traceback

from selenium.webdriver import Firefox
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
import time, random

end = time.time()
start = time.time() - (60 * 60 * 24 * 7)


con = ps.connect(host='localhost', port=5432, dbname="wallets", user="postgres", password="1Proffblyth")
cur = con.cursor()

class baseGrabber:
    def __init__(self):
        self.headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

    def requestorGet(self, resource, info=None, headers=None):
        if info == None:
            if headers == None:
                res = requests.get(resource)
            else:
                res = requests.get(resource, headers=headers)
        else:
            if headers == None:
                res = requests.get(resource, info)
            else:
                res = requests.get(resource, info, headers=headers)
        return self.decodeRequest(res)

    def decodeRequest(self, res):
        if res.content.startswith(b'\x80'):
            return pickle.loads(res.content)
        else:
            return json.loads(res.content)

    async def asyncRequestorGet(self, url):
        async with aiohttp.ClientSession(headers=self.headers) as session:
            status = 0
            while status != 200:
                async with session.get(url) as response:
                    status = response.status
                    if response.status == 200:
                        return await response.json()
                    else:
                        await asyncio.sleep(0.1)

    async def bulk_func_asyncer(self, func, data):
        return await asyncio.gather(*map(func, data))


    async def bulk_fetch(self, url_list):
        return await self.bulk_func_asyncer(self.asyncRequestorGet, url_list)


def jDumper(name, data):
    with open(f'{name}.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
        f.close()

def jGrabber(file):
    with open(file, 'r', encoding='utf-8') as f:
        data = json.load(f)
        f.close()
    return data



async def asyncRequestorGet(url):
    async with aiohttp.ClientSession(headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}) as session:
        status = 0
        while status != 200:
            async with session.get(url) as response:
                status = response.status
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, "html.parser")
                    return soup
                else:
                    await asyncio.sleep(0.1)

def find365LI(browser):
    lis = browser.find_elements(By.TAG_NAME, "li")
    for i in lis:
        if i.text == "Last 365 days":
            return i

def findContinue(browser):
    buts = browser.find_elements(By.CLASS_NAME, 'glxMF')
    for i in buts:
        if i.text == "Continue":
            return i

def findLoadMore(browser):
    buts = browser.find_elements(By.CLASS_NAME, 'ccMCCm')
    for i in buts:
        if i.text == "Load More":
            return i
    buts = browser.find_elements(By.CLASS_NAME, 'izpqHR')
    for i in buts:
        if i.text == "Load More":
            return i
    browser.execute_script("window.scrollTo(200,document.body.scrollHeight)")
    buts = browser.find_elements(By.CLASS_NAME, 'ccMCCm')
    time.sleep(0.5)
    for i in buts:
        if i.text == "Load More":
            return i
    buts = browser.find_elements(By.CLASS_NAME, 'izpqHR')
    for i in buts:
        if i.text == "Load More":
            return i


def scrollToEle(browser, element):
    browser.execute_script("arguments[0].scrollIntoView();", element)

def getAddress(browser):
    x = browser.find_elements(By.CLASS_NAME, "mainChainAddress")
    for e, i in enumerate(x):
        if i.text.startswith("0x"):
            print(e, i, i.find_element(By.XPATH, "./..").get_attribute('href'))
            return i.find_element(By.XPATH, "./..").get_attribute('href')





def getCoinData(url):
    browser = Firefox(options=opts)
    browser.get(url)
    # z = browser.find_elements("class name", "div.history")
    # time.sleep(random.randint(2700, 3000) / 1000)

    try:
        addressURL = getAddress(browser)
        print("addressURL = ", addressURL)
        if addressURL == None:
            print("FAILED TO FIND ADDRESSURL = ", url)
            browser.close()
            return False
        address = addressURL.split("/")[-1]
        sqlFind = """ SELECT 1 FROM {table} WHERE waddress=%s""".format(table="cmcrates")
        cur.execute(sqlFind, (address,))
        found = cur.fetchone()
        if found != None:
            browser.close()
            return False

        browser.execute_script("window.scrollTo(200,document.body.scrollHeight)")
        # datedrop = browser.find_element(By.CLASS_NAME, 'kgeBdS')
        # browser.execute_script("arguments[0].scrollIntoView();", datedrop)
        # datedrop.click()
        # last365 = find365LI(browser)
        # last365.click()
        # continueButton = findContinue(browser)
        # continueButton.click()
        height = browser.execute_script("return document.body.scrollHeight")

        for i in range(80):
            lm = findLoadMore(browser)
            scrollToEle(browser, lm)
            try:
                lm.click()
            except:
                browser.execute_script("window.scrollBy(0,-50)")
                lm.click()

            # time.sleep(random.randint(1200, 1800) / 1000)
            old_height = height
            height = browser.execute_script("return document.body.scrollHeight")
            if old_height == height:
                break

        table = browser.find_element(By.CLASS_NAME, 'cxLkYn')
        text = table.text
        lists = []
        for e, i in enumerate(text.split('\n')):
            dt = time.time()
            if e == 0:
                pass
            else:
                dt = dt - ((e-1)*60*60*24)
                t = i.split(' $')
                t[0] = dt
                for e2, i2, in enumerate(t):
                    if e2 == 0:
                        pass
                    else:
                        i2 = i2.replace(",", "")
                        t[e2] = float(i2)

                if len(t) == 6:
                    t.append(0.0)
                lists.append(t)

        browser.close()
        return [address, lists]
    except Exception as e:
        print(e)
        print(str(traceback.format_exc(10)))
        browser.close()
        return False

def getPageLinks(browser):
    z = browser.find_elements(By.CLASS_NAME, "cmc-link")
    ans = []
    for i in z:
        if i.get_attribute('href') == None:
            pass
        elif i.get_attribute('href').startswith("https://coinmarketcap.com/currencies/") and i.get_attribute('href') not in ans and "/markets/" not in i.get_attribute('href') and "?" not in i.get_attribute('href'):
            ans.append(i.get_attribute('href'))
    stored = jGrabber("links.json")
    stored = stored + ans
    jDumper("links", stored)
    return ans

def nextTransition(browser):
    for retry in range(3):
        z = browser.find_elements(By.CLASS_NAME, "chevron")
        try:
            browser.execute_script("arguments[0].scrollIntoView();", z[-1])
            # time.sleep(random.randint(300, 800) / 1000)
            z[-1].click()
        except Exception as e:
            print(e)
        try:
            browser.execute_script("arguments[0].scrollIntoView();", z[1])
            # time.sleep(random.randint(300, 800) / 1000)
            z[1].click()
        except Exception as e:
            print(e)
        # time.sleep(0.2)
    return False

def grabAllLinks(browser):
    totalLinks = []
    for i in range(2, 90):
        # time.sleep(random.randint(500, 900) / 1000)
        tmpList = getPageLinks(browser)
        if isinstance(tmpList, list):
            totalLinks = totalLinks + tmpList
        newUrl = "https://coinmarketcap.com/?page="+str(i)
        browser.get(newUrl)
        # time.sleep(random.randint(200, 700) / 1000)
    browser.close()
    return totalLinks


if __name__=="__main__":
    ########################################
    ######## Change these Settigs ##########
    ########################################
    opts = Options()
    opts.headless = True
    opts.binary_location = r'C:\Program Files\WindowsApps\Mozilla.Firefox_111.0.1.0_x64__n80bbvh6b1yt2\VFS\ProgramFiles\Firefox Package Root\firefox.exe'
    ########################################
    ########################################

    url = "https://coinmarketcap.com/"
    browser = Firefox(options=opts)
    browser.get(url)
    time.sleep(random.randint(2700, 3000) / 1000)
    z = grabAllLinks(browser)

    sqlInsert = """INSERT INTO {table} (waddress, timestamp_, openprice, highprice, lowprice, closeprice, volume, marketcap) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""".format(table="cmcrates")
    links = jGrabber("links.json")
    for link in links:
        ready = link + "historical-data/"
        try:
            data = getCoinData(ready)
            if data == False:
                pass
            else:
                for row in data[1]:
                    cur.execute(sqlInsert, (data[0],  *row))
                    con.commit()
        except Exception as e:
            print("245 = ", e)
            print(str(traceback.format_exc(10)))
            opened = jGrabber("failed.json")
            opened.append(link)
            jDumper("failed", opened)

    con.close()


















