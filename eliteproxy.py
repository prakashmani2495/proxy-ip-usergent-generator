import random
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
import pipeline
import useragent


def scrap_proxy():
	""" Scrap proxy from desired websites and filter high anonymity level proxies """
	checker = [
		'https://www.google.com/', 'https://in.yahoo.com/', 'https://www.bing.com/',
		'https://duckduckgo.com/', 'https://www.dogpile.com/', 'https://scholar.google.com/'
	]
	try:
		pipeline.truncate(database="ETL_Config", table="EliteProxy")
		url = pipeline.select(database="ETL_Config", table="NavigationUrl", column="NextPageUrl",
		                      condition={"UrlCategory": "Proxy"}, operator="AND"
		                      )
		req = requests.get(url[0][0], headers=useragent.get_agent(), timeout=(5, 10))
		soup = BeautifulSoup(req.text, 'html5lib')
		ip = list(map(lambda x: x.text, soup.findAll('td')[::8]))
		port = list(map(lambda x: x.text, soup.findAll('td')[1::8]))
		anonymity = list(map(lambda x: x.text, soup.findAll('td')[4::8]))
		data_dictionary = {'IP': ip, 'PORT': port, 'ANONYMITY': anonymity}
		data_frame = pd.DataFrame(data_dictionary)
		data_filter = data_frame['ANONYMITY'] == 'elite proxy'
		elite_data = data_frame[data_filter]
		print("[{}] [{}] items scraped from <{}> successfully."
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), len(elite_data.index), url[0][0]))
		process = []
		for i in range(len(elite_data.index)):
			ip = elite_data.iloc[i]['IP']
			port = elite_data.iloc[i]['PORT']
			proxies = ('http://' + ip + ':' + port)
			proxy = {
				'http': proxies,
				'https': proxies,
			}
			print("[{}] Evaluating Proxy <{}> that scraped from [{}]"
			      .format(time.strftime("%I:%M:%S %p", time.localtime()), proxies, url[0][0]))
			result = check_proxy(proxy=proxy, url=random.choice(checker), ip=ip)
			if result is True:
				p_count = pipeline.select(database="ETL_Config", table="EliteProxy", column="COUNT(*)")
				if int(p_count[0][0]) >= 10:
					pipeline.call(database="ETL_Config", procedure="SP_UpdateProxy")
					db_result = pipeline.call(database="ETL_Config", procedure="SP_NavigationUrl_Sync",
					                          parameter={"category": "Proxy"}
					                          )
					if db_result is True:
						print("[{}] Elite Proxy Scraper successfully completed and Synchronized."
						      .format(time.strftime("%I:%M:%S %p", time.localtime())))
						return True
					else:
						print("[{}] Elite Proxy Scraper successfully completed and 'NOT' Synchronized."
						      .format(time.strftime("%I:%M:%S %p", time.localtime())))
						return False
					pass
				else:
					pipeline.insert(database="ETL_Config", table="EliteProxy",
					                values={"IP": ip, "Port": port, "Anonymity": "High", "IsAlive": "Y",
					                        "LastUpdate": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}
					                )
			else:
				pass
		pipeline.call(database="ETL_Config", procedure="SP_UpdateProxy")
		pipeline.call(database="ETL_Config", procedure="SP_NavigationUrl_Sync",
		              parameter={"category": "Proxy"}
		              )
	except Exception as e:
		print("[{}] Exception Occurs and retries Scrap_Proxy Method. Error: {}"
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), e))
		scrap_proxy()
	except KeyboardInterrupt as e:
		print("[{}] Someone Forced Program to EXIT - KeyboardInterrupt at Scrap_Proxy Method. Error: {}"
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), e))
		exit()


def check_proxy(proxy, url, ip):
	""" Check proxy working status for current scraping URL """

	try:
		req = requests.get(url, proxies=proxy, headers=useragent.get_agent(), timeout=(5, 10))

		response_code = req.status_code
		print("[{}] Got response from <{}> while using Proxy: {} and returned response code [{}]"
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), url, proxy, response_code))

		if response_code == 200:
			print("[{}] Proxy {} is validated and proceed for anonymity check"
			      .format(time.strftime("%I:%M:%S %p", time.localtime()), proxy))

			result = elite_proxy(proxy=proxy, ip=ip)

			if result is True:
				return True
			else:
				return False
		else:
			print("[{}] Bad response from <{}> while using proxy {} and returning to Scrap Proxy"
			      .format(time.strftime("%I:%M:%S %p", time.localtime()), url, proxy))
	except Exception as e:
		print("[{}] Exception Occurs at Check_Proxy Method. Error: {}"
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), e))
	except KeyboardInterrupt as e:
		print("[{}] Someone Forced Program to EXIT - KeyboardInterrupt at Check_Proxy Method. Error: {}"
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), e))
		exit()


def elite_proxy(proxy, ip):
	""" Check proxy for high anonymity level (Elite Proxy) """
	try:
		url = 'https://httpbin.org/ip'
		req = requests.get(url, proxies=proxy, headers=useragent.get_agent(), timeout=(5, 10))
		soup = BeautifulSoup(req.content, 'html5lib')
		origin_ip = soup.text[15:int((((len(soup.text[15:-4])) / 2) - 1) + 15)]
		if origin_ip == ip:
			print("[{}] Request originated(while using proxy) from {}"
			      .format(time.strftime("%I:%M:%S %p", time.localtime()), soup.text))
			return True
		else:
			return False
	except Exception as e:
		print("[{}] Exception Occurs at Elite_Proxy Method. Error: {}"
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), e))
	except KeyboardInterrupt as e:
		print("[{}] Someone Forced Program to EXIT - KeyboardInterrupt at Elite_Proxy Method. Error: {}"
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), e))
		exit()


def get_proxy(url):
	""" Get suitable proxy from database, if proxy is restricted proxy delete from database. Also call
	    eliteproxy.py when the proxy is not available in ETL_Config database """
	c_url = url.split('/')[0] + "//" + url.split('/')[2]
	database = "ETL_Config"
	table = "EliteProxy"
	proxy_id = pipeline.select(database="ETL_Config", table="EliteProxy", column="ProxyID")
	i_d = ""
	if not proxy_id or int(len(proxy_id)) <= 3:
		scrap_proxy()
		get_proxy(url=c_url)
	else:
		i_d = proxy_id[random.randrange(int(len(proxy_id)))][0]
	headers = useragent.get_agent()
	proxy = validate_proxy(database=database, table=table, url=c_url, i_d=i_d, header=headers)
	return proxy, headers


def validate_proxy(database, table, url, i_d, header):
	""" Validate proxy working status for the given domain"""
	try:
		ip_port = pipeline.select(database=database, table=table, column="IP, Port", condition={"ProxyID": i_d}
		                          )
		proxies = ('http://' + ip_port[0][0] + ":" + ip_port[0][1])
		proxy = {
			'http': proxies,
			'https': proxies,
		}
		req = requests.get(url, proxies=proxy, headers=header,
		                   timeout=(5, 10))
		status = req.status_code
		if status == 200:
			print("[{}] Right Proxy and User Agent choosed for scrapping {}."
			      .format(time.strftime("%I:%M:%S %p", time.localtime()), proxy))
			return proxy
		else:
			print("[{}] Bad response from {}."
			      .format(time.strftime("%I:%M:%S %p", time.localtime()), proxy))
			pipeline.delete(database=database, table=table, condition={"ProxyID": i_d})
			get_proxy(url=url)
	except requests.Timeout as e:
		print("[{}] Exception Occurs - requests.Timeout at validate_proxy Method. Error: {}"
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), e))
		pipeline.delete(database="ETL_Config", table="EliteProxy",
		                condition={"ProxyID": i_d}
		                )
		get_proxy(url=url)
	except requests.ConnectionError as e:
		print("[{}] Exception Occurs - request.ConnectionError at validate_proxy Method. Error: {}"
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), e))
		pipeline.delete(database="ETL_Config", table="EliteProxy",
		                condition={"ProxyID": i_d}
		                )
		get_proxy(url=url)
	except requests.RequestException as e:
		print("[{}] Exception Occurs - request.RequestException(GeneralException) at validate_proxy Method. Error: {}"
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), e))
		pipeline.delete(database="ETL_Config", table="EliteProxy",
		                condition={"ProxyID": i_d}
		                )
		get_proxy(url=url)
	except KeyboardInterrupt as e:
		print("[{}] Someone Forced Program to EXIT - KeyboardInterrupt at validate_proxy Method. Error: {}"
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), e))
		exit()
		pass


if __name__ == "__main__":
	scrap_proxy()
