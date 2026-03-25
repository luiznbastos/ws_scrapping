import json, time, re, logging, os, pytz, boto3, pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from scrappers.driver.network_driver import RemoteNetworkDriver, NetworkDriver
from scrappers.utils.database import DatabaseClient
from scrappers.utils.aws import create_prefix, object_exists

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ScrappingTask:
    def __init__(
        self,
        network_driver: NetworkDriver = None,
        s3: boto3.client = None,
        database_client: DatabaseClient = None,
    ):
        self.network_driver = network_driver
        self.s3 = s3
        self.database_client = database_client

    def dismiss_overlays(self):
        """Dismiss cookie banners, ads, and other overlays that might block interactions"""
        overlays_to_dismiss = [
            "//button[contains(text(), 'Accept')]",
            "//button[contains(text(), 'Agree')]",
            "//button[contains(text(), 'OK')]",
            "//button[contains(text(), 'Aceitar')]",
            "//button[contains(text(), 'Allow all')]",
            "//button[@id='onetrust-accept-btn-handler']",
            "//button[contains(@class, 'cookie')]",
            "/html/body/div[8]/div/div[1]/button",
            "//button[contains(@class, 'close')]",
            "//button[contains(@aria-label, 'Close')]",
            "//div[contains(@class, 'Row-buoy')]//button",
            "//a[contains(text(), 'Accept')]",
            "//div[contains(@class, 'consent')]//button",
        ]
        
        for xpath in overlays_to_dismiss:
            try:
                overlay = WebDriverWait(self.network_driver.driver, 2).until(
                    EC.element_to_be_clickable((By.XPATH, xpath))
                )
                overlay.click()
                time.sleep(0.5)
                logger.debug(f"Dismissed overlay: {xpath}")
            except:
                pass
        
        try:
            self.network_driver.driver.execute_script("""
                var overlays = document.querySelectorAll('[class*="Row-buoy"]');
                overlays.forEach(function(el) { el.remove(); });
            """)
            logger.debug("Removed Row-buoy overlays with JavaScript")
        except:
            pass
        
        time.sleep(1)

    def run(self, *args, **kwargs):
        raise NotImplementedError

    def save(self, *args, **kwargs):
        raise NotImplementedError

class ScrapeSeasons(ScrappingTask):
    def __init__(
        self,
        network_driver=None,
        s3=None,
        database_client=None,
        tournament_name=None,
        tournament_url=None,
        s3_bucket=None,
    ):
        super().__init__(network_driver=network_driver, s3=s3, database_client=database_client)
        self.tournament_name = tournament_name
        self.tournament_url = tournament_url
        self.bucket = s3_bucket

    @property
    def _ctx(self):
        return f"[step=seasons league={self.tournament_name}]"

    def click_buttons(self, xpath, timeout=5):
        try:
            button = WebDriverWait(self.network_driver.driver, timeout).until(
                EC.element_to_be_clickable((By.XPATH, xpath))
            )
            
            self.network_driver.driver.execute_script(
                "arguments[0].scrollIntoView({block: 'center', behavior: 'instant'});", 
                button
            )
            time.sleep(0.3)
            
            try:
                button.click()
            except Exception:
                logger.debug(f"Normal click failed for {xpath}, trying JavaScript click")
                self.network_driver.driver.execute_script("arguments[0].click();", button)
            time.sleep(3)
        except Exception as e:
            self.network_driver.driver.save_screenshot(
                "/app/screenshots/screenshot.png"
            )
            raise e

    def _perform_tournaments_clicks(self):
        try:
            ad_xpath = "/html/body/div[8]/div/div[1]/button"
            ad_element = WebDriverWait(self.network_driver.driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, ad_xpath))
            )
            self.click_buttons(ad_xpath)
        except:
            pass
        self.click_buttons("//*[@id='sub-navigation']/ul/li[2]/a")

    @property
    def season_is_scrapped(self):
        seasons = "seasons"
        check_table_exists = f"""
        SELECT EXISTS (
            SELECT * FROM information_schema.Tables
            WHERE table_name = '{seasons}'
        );
        """
        resultado = self.database_client.fetch_one(check_table_exists)
        resultado = resultado[0] if resultado else False
        if resultado == True:
            self.scrapped_seasons = self.database_client.read_sql(
                "SELECT id FROM seasons"
            )
            if not self.scrapped_seasons["id"].empty:
                return True
        return False

    def save(self):
        seasons = []
        tournament_prefix = f"{self.tournament_name}/"

        for id in self.season_id:
            season_prefix = f"{tournament_prefix}{id}/"
            create_prefix(
                bucket_name=self.bucket, prefix=season_prefix, s3_client=self.s3
            )
            seasons.append(
                {
                    "id": id,
                    "url": f"{self.tournament_url}/Seasons/{id}",
                    "season_prefix": season_prefix,
                    "tournament_prefix": tournament_prefix,
                }
            )
        seasons_df = pd.DataFrame(seasons)
        self.database_client.write_df(seasons_df, "seasons", if_exists="append")
        logger.info(f"{self._ctx} Saved count={len(self.season_id)} seasons={self.season_id}")


    def run(self):
        self.season_id = list()

        if self.season_is_scrapped:
            season_ids = self.scrapped_seasons["id"].tolist()
            logger.info(f"{self._ctx} Skipped: already_scraped count={len(season_ids)} seasons={season_ids}")
            return "skipped"

        logger.info(f"{self._ctx} Navigating url={self.tournament_url}")
        self.network_driver.get(self.tournament_url)
        time.sleep(3)
        self.dismiss_overlays()
        self._perform_tournaments_clicks()
        time.sleep(2)

        logger.info(f"{self._ctx} Extracting season data from network events")
        self.network_driver.get_network_events()
        filtered_season_url = [
            event["response"]["url"]
            for event in self.network_driver.events
            if "response" in event
            and event["response"]["url"].startswith(
                "https://1xbet.whoscored.com/regions/"
            )
            and len(event["response"]["url"]) > 100
        ]
        self.network_driver.get_network_responses(url_to_find=filtered_season_url)
        html_doc = self.network_driver.selected_events[0]["response"]["responseBody"]
        soup = BeautifulSoup(html_doc, "html.parser")
        seasons_select = soup.find("select", id="seasons")
        for option in seasons_select.find_all("option"):
            value = option.get("value")
            start_index = value.find("Seasons/")
            season_value = value[start_index:].split("/")[1]
            season_text = option.text.strip()
            year = int(season_text.split("/")[0])
            if year >= 2013:
                self.season_id.append(season_value)

        logger.info(f"{self._ctx} Found count={len(self.season_id)} seasons={self.season_id}")
        self.save()
        return "saved"


class ScrapeMatches(ScrappingTask):

    def __init__(
        self,
        tournament_directory,
        season_url,
        season_directory,
        is_current_season=False,
        update_season=False,
        network_driver=None,
        database_client=None,
        s3=None,
        s3_bucket=None,
    ):
        super().__init__(network_driver=network_driver, s3=s3, database_client=database_client)
        self.tournament_directory = tournament_directory
        self.season_url = season_url
        self.season_directory = season_directory
        self.season_id = season_directory.split("/")[-2]
        self.is_current_season = is_current_season
        self.update_season = update_season
        self.bucket = s3_bucket

    @property
    def _ctx(self):
        return f"[step=matches season={self.season_id}]"

    def click_buttons(self, xpath, timeout=5):
        try:
            button = WebDriverWait(self.network_driver.driver, timeout).until(
                EC.element_to_be_clickable((By.XPATH, xpath))
            )
        except:
            button = WebDriverWait(self.network_driver.driver, timeout).until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "td.datePicker_selectable")
                )
            )
        
        self.network_driver.driver.execute_script(
            "arguments[0].scrollIntoView({block: 'center', behavior: 'instant'});", 
            button
        )
        time.sleep(0.3)
        
        try:
            button.click()
        except Exception:
            logger.debug(f"Normal click failed for {xpath}, trying JavaScript click")
            self.network_driver.driver.execute_script("arguments[0].click();", button)
        time.sleep(2)

    def _select_date(self, row, column):
        xpath = f"//*[@id='datePicker']/div/div[1]/table/tbody/tr[{row}]/td[{column}]"
        self.click_buttons(xpath)

    def _perform_matchs_clicks(self):
        try:
            ad_xpath = "/html/body/div[8]/div/div[1]/button"
            self.click_buttons(ad_xpath, timeout=5)
        except:
            pass

        calendar = "//*[@id='toggleCalendar']/span[2]"
        self.click_buttons("//*[@id='sub-navigation']/ul/li[2]/a")

        dates_to_select = [
            (3, 2),
            (3, 3),
            (4, 1),
            (4, 2),
            (4, 3),
            (1, 1),
            (1, 2),
            (1, 3),
            (2, 1),
            (2, 2),
        ]

        for i, (row, column) in enumerate(dates_to_select):
            self.click_buttons(calendar)

            if i == 5:
                self.click_buttons(
                    "//*[@id='datePicker']/div/div[1]/div/div/button/span[2]"
                )
                self.click_buttons(
                    "//*[@id='datePicker']/div/div[1]/table/tbody/tr/td[1]"
                )

            self._select_date(row, column)

        self.click_buttons(calendar)
        self.click_buttons("//*[@id='datePicker']/div/div[1]/div/div/button/span[2]")

    def _perform_current_matchs_clicks(self):
        try:
            self.click_buttons("/html/body/div[8]/div/div[1]/button", timeout=5)
        except:
            pass

        calendar = "//*[@id='toggleCalendar']/span[2]"
        self.click_buttons("//*[@id='sub-navigation']/ul/li[2]/a")

        dates_current_month = [(1, 1), (1, 2), (1, 3), (2, 1), (2, 2)]
        for row, column in dates_current_month:
            self.click_buttons(calendar)
            self._select_date(row, column)

        self.click_buttons(calendar)
        self.click_buttons("//*[@id='datePicker']/div/div[1]/div/div/button/span[2]")
        self.click_buttons("//*[@id='datePicker']/div/div[1]/table/tbody/tr/td[2]")

        dates_next_month = [(3, 2), (3, 3), (4, 1), (4, 2), (4, 3)]
        for row, column in dates_next_month:
            self._select_date(row, column)
            self.click_buttons(calendar)

    def _matches_pandemic(self):
        try:
            self.click_buttons("/html/body/div[8]/div/div[1]/button", timeout=5)
        except:
            pass

        calendar = "//*[@id='toggleCalendar']/span[2]"
        self.click_buttons("//*[@id='sub-navigation']/ul/li[2]/a")

        dates_current_month = [(1, 1), (1, 2), (1, 3), (2, 1), (2, 2)]
        for row, column in dates_current_month:
            self.click_buttons(calendar)
            self._select_date(row, column)

        self.click_buttons(calendar)
        self.click_buttons("//*[@id='datePicker']/div/div[1]/div/div/button/span[2]")
        self.click_buttons("//*[@id='datePicker']/div/div[1]/table/tbody/tr/td[2]")

        dates_next_month = [(3, 3), (4, 1), (4, 2), (4, 3)]
        for row, column in dates_next_month:
            self._select_date(row, column)
            self.click_buttons(calendar)

    def _perform_matches_pandemic(self):
        try:
            self.click_buttons("/html/body/div[8]/div/div[1]/button", timeout=5)
        except:
            pass

        calendar = "//*[@id='toggleCalendar']/span[2]"
        self.click_buttons("//*[@id='sub-navigation']/ul/li[2]/a")

        dates_current_month = [(1, 1), (1, 2), (1, 3), (2, 3), (3, 1)]
        for row, column in dates_current_month:
            self.click_buttons(calendar)
            self._select_date(row, column)

        self.click_buttons(calendar)
        self.click_buttons("//*[@id='datePicker']/div/div[1]/div/div/button/span[2]")
        self.click_buttons("//*[@id='datePicker']/div/div[1]/table/tbody/tr/td[2]")

        dates_next_month = [(3, 2), (3, 3), (4, 1), (4, 2), (4, 3)]
        for row, column in dates_next_month:
            self._select_date(row, column)
            self.click_buttons(calendar)

    def extract_date_from_url(self, url):
        match = re.search(r"d=(\d{6})", url)
        return match.group(1) if match else None

    @property
    def match_is_scrapped(self):
        matches = "season_matches"
        check_table_exists = f"""
        SELECT EXISTS (
            SELECT * FROM information_schema.Tables
            WHERE table_name = '{matches}'
        );
        """
        exist_check_result = self.database_client.fetch_one(check_table_exists)
        exist_check_result = exist_check_result[0] if exist_check_result else False
        if exist_check_result == True:
            self.match_id = self.database_client.read_sql(
                f"SELECT match_id FROM season_matches WHERE season_id = '{self.season_id}'"
            )["match_id"]
            if not self.match_id.empty:
                return True
        return False

    def save(self):
        for matches in self.monthly_matches:
            create_prefix(
                bucket_name=self.bucket,
                prefix=matches["month_path"],
                s3_client=self.s3,
            )
            self.database_client.write_df(
                matches["df"], "monthly_matches", if_exists="append"
            )
            for match_id in matches["match_ids"]:
                match_path = f"{matches['month_path']}/{str(match_id)}"
                create_prefix(
                    bucket_name=self.bucket, prefix=match_path, s3_client=self.s3
                )
                self.matches.append(
                    {
                        "match_id": match_id,
                        "match_path": match_path,
                        "match_url": f"https://1xbet.whoscored.com/matches/{str(match_id)}/live",
                        "date": matches["date"],
                        "season_id": self.season_id,
                    }
                )
        seasons_matches_df = pd.DataFrame(self.matches)
        self.database_client.write_df(seasons_matches_df, "season_matches", if_exists="append")
        months = [m["date"] for m in self.monthly_matches]
        logger.info(
            f"{self._ctx} Saved matches={len(self.matches)} months={months}"
        )

    def run(self):
        if self.match_is_scrapped:
            logger.info(f"{self._ctx} Skipped: already_scraped count={len(self.match_id)}")
            return "skipped"

        a_elements = self.network_driver.driver.find_elements(By.TAG_NAME, "a")
        visible_a_elements = list(filter(lambda e: (e.is_displayed()), a_elements))
        logger.debug(f"Found {len(visible_a_elements)} visible links")

        logger.info(f"{self._ctx} Navigating url={self.season_url}")
        url = self.season_url
        self.network_driver.get(url)
        time.sleep(3)
        self.dismiss_overlays()

        if self.is_current_season:
            now = datetime.now()
            year = now.year
            if year == 2026: 
                self._perform_current_matchs_clicks()
            else:
                self._perform_matchs_clicks()
        elif self.season_id in ["8321"]:
            self._matches_pandemic()
        elif self.season_id in ["7889"]:
            self._perform_matches_pandemic()
        else:
            self._perform_current_matchs_clicks()

        self.network_driver.get_network_events()

        filtered_urls = [
            event["response"]["url"]
            for event in self.network_driver.events
            if "response" in event
            and event["response"]["url"].startswith(
                "https://1xbet.whoscored.com/tournaments/"
            )
            and event["response"]["url"].endswith("&isAggregate=false")
        ]
        seen_dates = set()
        self.monthly_matches = []
        for url in filtered_urls:
            date = self.extract_date_from_url(url)
            if date and date not in seen_dates:
                seen_dates.add(date)
                self.monthly_matches.append({
                    "url": url,
                    "date": date,
                    "month_path": os.path.join(self.season_directory, date),
                })
        logger.info(
            f"{self._ctx} Found months={len(self.monthly_matches)} "
            f"(from {len(filtered_urls)} network responses, {len(filtered_urls) - len(self.monthly_matches)} duplicates dropped)"
        )
        self.matches = []
        for matches in self.monthly_matches:
            self.network_driver.get_network_responses(url_to_find=matches["url"])
            jsondata = json.loads(
                self.network_driver.selected_events[0]["response"]["responseBody"]
            )

            monthly_matches_df = pd.DataFrame(
                jsondata.get("tournaments", [{}])[0].get("matches", {})
            )
            matches["df"] = monthly_matches_df.drop(columns=["incidents"])
            matches["df"]["bets"] = matches["df"]["bets"].apply(lambda x: str(x))

            matches["match_ids"] = [
                match.get("id", None)
                for match in jsondata.get("tournaments", [{}])[0].get("matches", {})
            ]
        self.save()
        return "saved"


class ScrapeEvents(ScrappingTask):
    def __init__(
        self,
        match_id: int,
        match_url: str,
        match_prefix: str,
        run_context: dict,
        network_driver: RemoteNetworkDriver = None,
        database_client=None,
        s3=None,
        s3_bucket=None,
    ):
        super().__init__(
            network_driver=network_driver, s3=s3, database_client=database_client
        )
        self.match_url = match_url
        self.match_prefix = match_prefix
        self.match_id = match_id
        self.run_context = run_context
        self.bucket = s3_bucket

    @property
    def _ctx(self):
        return (
            f"[step=events season={self.run_context.get('season_id')} "
            f"match={self.match_id}]"
        )

    def click_buttons(self, xpath, timeout=5):
        button = WebDriverWait(self.network_driver.driver, timeout).until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )
        
        self.network_driver.driver.execute_script(
            "arguments[0].scrollIntoView({block: 'center', behavior: 'instant'});", 
            button
        )
        time.sleep(0.3)
        
        try:
            button.click()
        except Exception:
            logger.debug(f"Normal click failed for {xpath}, trying JavaScript click")
            self.network_driver.driver.execute_script("arguments[0].click();", button)
        time.sleep(2)

    def _perform_events_clicks(self):
        try:
            ad_xpath = "/html/body/div[7]/div/div[1]/button"
            ad_element = WebDriverWait(self.network_driver.driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, ad_xpath))
            )
            self.click_buttons(ad_xpath)
        except:
            pass

    @property
    def match_has_data(self):
        return object_exists(self.s3, self.bucket, f"{self.match_prefix}/events.json")

    @property
    def match_has_happened(self):
        now = datetime.now()
        london_tz = pytz.timezone("UTC")
        london_datetime = now.astimezone(london_tz)
        monthly_matches_df = self.database_client.read_sql(
            "SELECT * FROM monthly_matches"
        )
        match_date = monthly_matches_df[monthly_matches_df["id"] == self.match_id][
            "starttime"
        ].values[0]
        match_date = datetime.strptime(match_date, "%Y-%m-%dT%H:%M:%S")
        match_date = match_date.replace(tzinfo=london_tz)
        return True if match_date < london_datetime else False

    def save(self):
        self.s3.put_object(
            Bucket=self.bucket,
            Key=f"{self.match_prefix}/events.json",
            Body=json.dumps(self.events),
            ContentType="application/json",
        )

        self.run_context["match_id"] = self.match_id
        self.run_context["is_scrapped"] = True
        run_context_df = pd.DataFrame([self.run_context])
        self.database_client.write_df(
            run_context_df, "scrape_runs", if_exists="append"
        )
        logger.info(
            f"{self._ctx} Saved path={self.match_prefix}/events.json"
        )

    def run(self):
        if self.match_has_happened and self.match_has_data:
            logger.debug(f"{self._ctx} Skipped: already_scraped")
            return "skipped"

        if not self.match_has_happened and not self.match_has_data:
            logger.debug(f"{self._ctx} Skipped: not_yet_played")
            return "skipped"

        logger.info(f"{self._ctx} Navigating url={self.match_url}")
        self.network_driver.get(self.match_url)
        time.sleep(2)
        self.dismiss_overlays()
        events = self.network_driver.get_network_events()

        filtered_urls = [
            event["response"]["url"]
            for event in events
            if "response" in event
            and event["response"]["url"].startswith(
                "https://1xbet.whoscored.com/matches/"
            )
            and event["response"]["url"].endswith("/live")
        ]

        if not filtered_urls:
            total_events = len(events) if events else 0
            logger.warning(
                f"{self._ctx} No matching network events found "
                f"total_raw_events={total_events} — nothing saved"
            )
            return "failed"

        for new_url in filtered_urls:
            self.network_driver.get_network_responses(url_to_find=new_url)

            soup = BeautifulSoup(
                self.network_driver.selected_events[0]["response"]["responseBody"],
                "html.parser",
            )
            script_tags = soup.find_all("script")
            script_contents = [tag.string for tag in script_tags]
            data = (
                [
                    content
                    for content in script_contents
                    if (
                        content is not None
                        and 'require.config.params["args"]' in content
                    )
                ][0]
                .replace('require.config.params["args"] = ', "")
                .replace(";\r\n    ", "")
            )
            data = (
                re.sub(r"\s+", " ", data)
                .replace("'", '"')
                .replace("formationIdNameMappings", '"formationIdNameMappings"')
                .replace("matchId", '"matchId"')
                .replace("matchCentreData", '"matchCentreData"')
                .replace("matchCentreEventTypeJson", '"matchCentreEventTypeJson"')
                .replace('Etienne Eto"o Pineda', "Etienne Eto'o Pineda")
                .replace('Alfred N"Diaye', "Alfred N'Diaye")
                .replace('Mu"nas Dabbur', "Mu'nas Dabbur")
            )

            self.events = json.loads(data)
            self.save()
        return "saved"
