import random
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
import pipeline
import eliteproxy


def scrap_agent():
	""" Scrap user agent from 'https://developers.whatismybrowser.com/' using high anonymous proxy."""
	try:
		i_url = pipeline.select(database="ETL_Config", column="NextPageUrl", table="NavigationUrl",
		                        condition={"UrlCategory": "User Agent"}, operator="AND"
		                        )
		i = int((i_url[0][0])[-1])
		if i == 1:
			pipeline.truncate(database="ETL_Config", table="UserAgent")
		else:
			pass
		while i < 11:

			url = pipeline.select(database="ETL_Config", column="NextPageUrl", table="NavigationUrl",
			                      condition={"UrlCategory": "User Agent"}, operator="AND"
			                      )
			proxy_header = eliteproxy.get_proxy(url=url[0][0])
			req = requests.get(url[0][0], headers=proxy_header[1], proxies=proxy_header[0], timeout=(5, 10))
			soup = BeautifulSoup(req.content, 'html5lib')
			user_agents = list(map(lambda x: x.text, soup.findAll('td')[::5]))
			software = list(map(lambda x: x.text, soup.findAll('td')[1::5]))
			software_type = list(map(lambda x: x.text, soup.findAll('td')[2::5]))
			os = list(map(lambda x: x.text, soup.findAll('td')[3::5]))
			popularity = list(map(lambda x: x.text, soup.findAll('td')[4::5]))
			data_dictionary = {
				'User Agent': user_agents,
				'Software': software,
				'Software Type': software_type,
				'OS': os,
				'Popularity': popularity
			}
			data_frame = pd.DataFrame(data_dictionary)
			data_filter = data_frame['Popularity'] == 'Very common'
			df_agents = data_frame[data_filter]
			if int(len(df_agents.index)) == 0:
				print("[{}] Scraper crawled [{}] items from <{}> and ready to crawl same URL."
				      .format(time.strftime("%I:%M:%S %p", time.localtime()), len(df_agents.index), url[0][0]))
				scrap_agent()
			else:
				print("[{}] Scraper crawled [{}] items from <{}> and ready to insert scraped items."
				      .format(time.strftime("%I:%M:%S %p", time.localtime()), len(df_agents.index), url[0][0]))
				for row in range(len(df_agents.index)):
					pipeline.insert(database="ETL_Config", table="UserAgent",
					                values={"UserAgent": df_agents.iloc[row]['User Agent'],
					                        "Software": df_agents.iloc[row]['Software'],
					                        "SoftwareType": df_agents.iloc[row]['Software Type'],
					                        "OS": df_agents.iloc[row]['OS'],
					                        "Popularity": df_agents.iloc[row]['Popularity']
					                        }
					                )
				pipeline.update(database="ETL_Config", table="NavigationUrl",
				                values={
					                "PreviousPageUrl": (
						                (url[0][0].replace((url[0][0])[-2:], str(int(i - 1)))) if i >= 10 else url[0][
							                0].replace(
							                (url[0][0])[-1], str(int(i - 1)))),
					                "CurrentPageUrl": url[0][0],
					                "NextPageUrl": (
						                (url[0][0].replace((url[0][0])[-2:], str(int(i - 9)))) if i >= 10 else url[0][
							                0].replace(
							                (url[0][0])[-1], str(int(i + 1))))
				                },
				                condition={"UrlCategory": "User Agent"},
				                operator="AND"
				                )
			i = i + 1
		pipeline.call(database="ETL_Config", procedure="SP_UpdateUserAgent")
		print("[{}] User Agent Scraped and loaded Successfully..."
		      .format(time.strftime("%I:%M:%S %p", time.localtime())))
	except Exception as e:
		print("[{}] Exception Occurs and retries scrap_agent Method. Error: {}"
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), e))
		scrap_agent()
	except KeyboardInterrupt as e:
		print("[{}] Someone Forced Program to EXIT - KeyboardInterrupt at scrap_agent Method. Error: {}"
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), e))
		exit()


def get_agent():
	try:
		ua_count = pipeline.select(database="ETL_Config", table="UserAgent", column="COUNT(*)")
		headers = requests.utils.default_headers()
		if int(ua_count[0][0]) <= 10:
			ua_count_bkp = pipeline.select(database="ETL_Config", table="UserAgent_BKP", column="COUNT(*)")
			if int(ua_count_bkp[0][0]) <= 10:
				print(
					"[{}] Advise you to Scrap UserAgent and Sync UserAgent. Only [{}] items exist in Table[UserAgent_BKP] and Database[ETL_Config]."
						.format(time.strftime("%I:%M:%S %p", time.localtime()), ua_count_bkp[0][0]))
				user_agent = temp_agent()
				headers.update({"User-Agent": user_agent})
				return headers
			else:
				ua = random.choice([x for x in range(1, int(ua_count_bkp[0][0]))])
				user_agent = pipeline.select(database="ETL_Config", table="UserAgent_BKP", column="UserAgent",
				                             condition={"UserAgentID": ua}
				                             )
				print("[{}] Backup UserAgent is Choosed. Which mean UserAgent is not available in Database [{}]."
				      .format(time.strftime("%I:%M:%S %p", time.localtime()), ua))
				headers.update({"User-Agent": user_agent[0][0]})
				return headers
		else:
			ua = random.choice([x for x in range(1, int(ua_count[0][0]))])
			user_agent = pipeline.select(database="ETL_Config", table="UserAgent", column="UserAgent",
			                             condition={"UserAgentID": ua}
			                             )
			headers.update({"User-Agent": user_agent[0][0]})
			return headers
	except requests.Timeout as e:
		print("[{}] Exception Occurs - requests.Timeout at get_agent Method. Error: {}"
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), e))
		temp_agent()
	except requests.ConnectionError as e:
		print("[{}] Exception Occurs - request.ConnectionError at get_agent Method. Error: {}"
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), e))
		temp_agent()
	except requests.RequestException as e:
		print("[{}] Exception Occurs - request.RequestException(GeneralException) at get_agent Method. Error: {}"
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), e))
		temp_agent()
	except KeyboardInterrupt as e:
		print("[{}] Someone Forced Program to EXIT - KeyboardInterrupt at get_agent Method. Error: {}"
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), e))
		exit()


def temp_agent():
	""" Stores the list of temporary UserAgent and returns random one."""
	try:
		user_agent = [
			'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',
			'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36',
			'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
			'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.157 Safari/537.36',
			'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
			'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
			'Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
			'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
			'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36',
			'Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1; 125LA; .NET CLR 2.0.50727; .NET CLR 3.0.04506.648; .NET CLR 3.5.21022)',
			'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.71 Safari/537.36',
			'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
			'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36',
			'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.83 Safari/537.1',
			'Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
			'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
			'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
			'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'
		]

		ua = random.choice(user_agent)
		print("[{}] Temporary UserAgent is Choosed. Which mean UserAgent is not available in Database [{}]."
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), ua))
		return ua
	except Exception as e:
		print("[{}] Exception Occurs and retries temp_agent Method. Error: {}"
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), e))
		temp_agent()
	except KeyboardInterrupt as e:
		print("[{}] Someone Forced Program to EXIT - KeyboardInterrupt at temp_agent Method. Error: {}"
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), e))
		exit()


if __name__ == '__main__':
	scrap_agent()
