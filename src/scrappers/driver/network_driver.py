from selenium import webdriver
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chromium.remote_connection import ChromiumRemoteConnection
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.remote.remote_connection import LOGGER
from selenium.webdriver.chrome.service import Service
from scrappers.driver.smart_proxy_extension import proxies
from abc import ABC, abstractmethod
import time, json, os, logging, ua_generator, requests


logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)


class NetworkDriver(ABC):
    """
    Base class for network drivers. It is an abstract class that defines
    a driver with capabilities to get network events from a browser.
    """

    def __init__(self, headless: bool = True, proxy: str = None) -> None:
        chrome_options = Options()
        # user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
        user_agent = ua_generator.generate(browser="chrome")
        # Notice that when setting User Agent, we shuold also set Client Hints CH that adhere to the proposet UA and reduce fingerprinting issues
        # Reference on how to scrape with reduced fingerprints: https://www.zenrows.com/blog/selenium-python-web-scraping#save-resources
        # Here some sample of how to generate user_agents and all other headers (including CHs): https://anyip.io/blog/python-requests-user-agent
        if headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument(f"user-agent={user_agent}")
        # chrome_options.add_argument("--start-maximized") # This for when not using headless mode
        # chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        # Performance configs
        prefs = {"profile.managed_default_content_settings.images": 2}
        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-renderer-backgrounding")
        chrome_options.add_argument("--disable-background-timer-throttling")
        chrome_options.add_argument("--disable-backgrounding-occluded-windows")
        chrome_options.add_argument("--disable-client-side-phishing-detection")
        chrome_options.add_argument("--disable-crash-reporter")
        chrome_options.add_argument("--disable-oopr-debug-crash-dump")
        chrome_options.add_argument("--no-crash-upload")
        chrome_options.add_argument("--disable-low-res-tiling")
        chrome_options.add_argument("--disable-preload")
        chrome_options.add_argument("--disable-predictive-services")
        chrome_options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
        # if proxy:
        #     username, password, endpoint, port = (
        #         proxy["username"],
        #         proxy["password"],
        #         proxy["endpoint"],
        #         proxy["port"],
        #     )
        #     proxy_url = f"http://{username}:{password}@{endpoint}:{port}"
        #     proxies_extension = proxies(username, password, endpoint, port)
        #     chrome_options.add_extension(proxies_extension)
        #     chrome_options.add_argument(f"--proxy-server={proxy}")
        self.options = chrome_options
        self.driver = self._get_driver()
        self.driver.execute(
            "executeCdpCommand", {"cmd": "Network.enable", "params": {}}
        )
        self.raw_events = []
        self.events = []
        self.selected_events = []

    @abstractmethod
    def _get_driver(self) -> WebDriver:
        pass

    # def __del__(self):
    #     """
    #     Ensures Chrome process is closed when the class instance is garbage collected.
    #     """
    #     self.driver.quit()

    def get(self, url: str):
        self.driver.get(url)

    def get_network_events(self) -> None:
        logging.debug("Getting network logs ...")
        self.raw_events = self.driver.get_log("performance")
        logging.debug(f"Got {len(self.raw_events)} raw events")

        self.events = [
            {
                "event_timestamp": event["timestamp"],
                "level": event["level"],
                "message": event["message"],
                "method": json.loads(event["message"])["message"].get("method", None),
                "webview": json.loads(event["message"])["message"].get("webview", None),
                **{
                    key: value
                    for key, value in json.loads(event["message"])["message"][
                        "params"
                    ].items()
                },
            }
            for event in self.raw_events
        ]
        logging.debug(f"Got {len(self.raw_events)} filtered events")
        return self.events

    def get_network_responses(
        self, url_to_find: list, method: str = "Network.responseReceived"
    ) -> None:

        self.selected_events = []

        logging.debug("Getting body messages from selected network messages ...")
        logging.debug(
            f"Looking for events with method = {method} and base_url = {url_to_find}"
        )
        for event in self.events:
            if event["method"] == method and event["response"]["url"] in (url_to_find):
                logging.debug(
                    f"Found event with requestId = {event['requestId']} and url = {event['response']['url']}"
                )
                try:
                    response_body = self.driver.execute(
                        "executeCdpCommand",
                        {
                            "cmd": "Network.getResponseBody",
                            "params": {"requestId": event["requestId"]},
                        },
                    )
                    self.selected_events.append(
                        {
                            **event,
                            "response": {
                                "responseBody": response_body["value"][
                                    "body"
                                ],  # Para uma função mais generica, talvez faça sentido não realizar a transformação para dict aqui, mas sim no scrapper do site.
                                **event["response"],
                            },
                        }
                    )

                except WebDriverException as err:
                    if "No data found for resource" in err.msg:
                        pass
                    else:
                        raise err


class ChromeNetworkDriver(NetworkDriver):
    """
    Usefull class for remote controlling custom docker image with chrome
    and chromedriver installed with adition of functionality to get network
    events of interaction with web APIs accessed through the browser
    """

    def __init__(self, headless: bool = True, proxy: str = None) -> None:
        super().__init__(headless=headless, proxy=proxy)

    def _get_driver(self) -> WebDriver:
        executable_path = os.environ.get("CHROME_DRIVER", "/usr/bin/chromedriver")
        service = Service(executable_path=executable_path)
        driver = webdriver.Chrome(service=service, options=self.options)
        return driver


class RemoteNetworkDriver(NetworkDriver):
    """
    Usefull class for remote controlling selenium-standalone docker image
    with adition of functionality to get network events of interaction with
    web APIs accessed through the browser
    """

    def __init__(
        self,
        remote_server_addr: str = "http://localhost:4444/wd/hub",
        vendor_prefix: str = "goog",
        browser_name: str = "chrome",
        keep_alive: bool = True,
        headless: bool = False,
        proxy: str = None,
    ):
        self.remote_server_addr = remote_server_addr
        self.vendor_prefix = vendor_prefix
        self.browser_name = browser_name
        self.keep_alive = keep_alive
        super().__init__(headless=headless, proxy=proxy)

    def _get_driver(self) -> WebDriver:
        retries = 5
        for _ in range(retries):
            try:
                return webdriver.Remote(
                    command_executor=ChromiumRemoteConnection(
                        remote_server_addr=self.remote_server_addr,
                        vendor_prefix=self.vendor_prefix,
                        browser_name=self.browser_name,
                        keep_alive=self.keep_alive,
                        ignore_proxy=self.options._ignore_local_proxy,
                    ),
                    options=self.options,
                )
            except Exception as e:
                logging.info(f"Failed to connect to Selenium server: {e}")
                time.sleep(5)
        raise Exception("Could not connect to Selenium server after several retries")


## Exemplo 1: Random site
# network_driver = RemoteNetworkDriver()
# network_driver.get("https://gerg.dev/2021/06/making-chromedriver-and-chrome-versions-match-in-a-docker-image/")
# logging.info(f"Title is: {network_driver.driver.title}")

# logging.info("Starting to get events ...")
# time.sleep(5)
# events = network_driver.get_network_events()
# # logging.info(f"Events: {events}")
# # logging.info("\nDone getting events ...")
# time.sleep(200)

## Exemplo 2: SkyScanner
# origin='gyn'
# destination='gru'
# date='240113'

# network_driver = RemoteNetworkDriver()

# logging.info("Connecting to skyscanner url ...")
# network_driver.get(f"https://www.skyscanner.com.br/transporte/passagens-aereas/{origin}/{destination}/{date}")

# time.sleep(5)

# network_driver.get_network_events()
# url_to_find = 'https://www.skyscanner.com.br/g/conductor/v1/fps3/search/'
# network_driver.get_network_responses(url_to_find=url_to_find)

# stop=True
