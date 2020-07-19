import psycopg2
import csv
from requests_html import HTMLSession
from bs4 import BeautifulSoup
import urllib.request, json
from datetime import datetime
from fake_useragent import UserAgent

CREDENTIALS = ("""
                host = 'localhost'
                dbname = 'investments'
                user = 'invest_admin'
                password = 'invest_pass'
                """)


class InvestmentIndicesSpider():
    """This class instantiates all the features and methods of spider required for PMI indices scraping
    from www.investing.com"""
    def __init__(self):
        self.base_url = "https://www.investing.com"
        self.start_page = "https://www.investing.com/search/?q=Manufacturing%20Purchasing%20Managers&tab=ec_event"
        self.json_base_url = 'https://sbcharts.investing.com/events_charts/us/'
        self.start_page_selector = 'div[class="js-section-content newResultsContainer economicEvents"]'
        self.link_selector = 'head>link[hreflang="x-default"]'
        self.series_list = []
        self.series_csv = "timeseries.csv"
        self.json_links = []
        self.json_links_csv = "json_links.csv"
        self.pmi_table_values = []
        self.pmi_values_csv = "pmi_values.csv"
        self.connection = psycopg2.connect(CREDENTIALS)
        self.cursor = self.connection.cursor()
        self.db_timeseries_table = "timeseries"
        self.db_pmi_values_table = "timeseries_value"

    def get_response(self, link):
        """This method emulates new session with randomized user agent and obtains response from url requested"""
        try:
            ua = UserAgent()
            header = {'User-Agent': str(ua.random)}
            session = HTMLSession()
            response = session.get(link, headers=header)
            return response

        except Exception:
            print("Response from url hasn't been obtained")

    def render_and_select(self, response, selector):
        """This method artificially 'renders' the webpage with response obtained and selects definite area
        for further parsing"""
        response.html.render(scrolldown=500)
        passage = response.html.find(selector, first=True)
        return passage

    def find_series_events(self, passage):
        """This method scrapes names and links to the pages of PMIs listed at www.investing.com. It also produces
        list of links for futher timeseries data scraping"""
        soup = BeautifulSoup(passage.html, "html.parser")
        try:
            for item in soup.select('a', class_='row'):
                name = item.select_one(".fourth").text
                print(name)
                sublink = item["href"]
                link_response = self.get_response(self.base_url + sublink)
                link = self.render_and_select(link_response, self.link_selector).attrs['href']
                timeseries = [name, link]
                self.series_list.append(timeseries)
                json_set = [name, self.json_base_url + sublink.split("-")[-1] + ".json"]
                self.json_links.append(json_set)
            # The following csv-writing scripts are not actually part of the task but are rather made
            # for scraping results back-up and  demonstration
            with open(self.series_csv, 'w') as series_file:
                wr = csv.writer(series_file)
                wr.writerows(self.series_list)
            with open(self.json_links_csv, 'w') as links_file:
                wr = csv.writer(links_file)
                wr.writerows(self.json_links)
        except Exception as e:
            print(e)

    def add_results_to_db(self, list_of_lists, table_name, table_columns):
        """This method prepares and executes insert queries into the database"""
        signs = '(' + ('%s,' * len(list_of_lists[0]))[:-1] + ')'
        try:
            args_str = b','.join(self.cursor.mogrify(signs, x) for x in list_of_lists)
            args_str = args_str.decode()
            insert_statement = """INSERT INTO %s (%s) VALUES """ % (table_name, ",".join(table_columns))
            # The following database-approaching script is not actually part of the task but is rather made
            # for scraping results testing in local database
            self.cursor.execute(insert_statement + args_str)
            self.connection.commit()
        except Exception as e:
            print(e)
            self.connection.rollback()

    def parse_json(self):
        """This method scrapes timeseries data of PMIs listed at www.investing.com."""
        for link in self.json_links:
            with urllib.request.urlopen(link[1]) as url:
                json_doc = json.loads(url.read().decode())
                time_series = json_doc["data"]
                for item in time_series:
                    human_date = datetime.fromtimestamp(item[0]/1000).strftime("%d %b %Y")
                    pmi_value = item[1]
                    pmi_table_entry = [link[0], human_date, pmi_value]
                    self.pmi_table_values.append(pmi_table_entry)
                # The following csv-writing script is not actually part of the task but is rather made
                # for scraping results back-up and  demonstration
                with open(self.pmi_values_csv, 'w') as pmi_file:
                    wr = csv.writer(pmi_file)
                    wr.writerows(self.pmi_table_values)

    def execute_scraping(self):
        """This function puts together the whole scraping logic"""
        self.find_series_events(
            self.render_and_select(
                self.get_response(self.start_page), self.start_page_selector
            ))

        self.add_results_to_db(self.series_list, self.db_timeseries_table, table_columns=['name', 'link'])

        self.parse_json()

        chunks_pmi_values = [self.pmi_table_values[x:x + 100] for x in range(0, len(self.pmi_table_values), 100)]
        chunk_count = 0
        for chunk in chunks_pmi_values:
            chunk_count += 1
            print(chunk_count)
            self.add_results_to_db(chunk, self.db_pmi_values_table, table_columns=['timeseries_name', 'date', 'value'])


spider = InvestmentIndicesSpider()
spider.execute_scraping()