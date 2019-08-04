import bs4
from bs4 import BeautifulSoup
import requests
import pandas as pd
import random
import time


ua = [
	'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
	'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:68.0) Gecko/20100101 Firefox/68.0',
	'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0',
	'Mozilla/5.0 (X11; Linux x86_64; rv:45.0) Gecko/20100101 Thunderbird/45.8.0',
	'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) HeadlessChrome/74.0.3729.157 Safari/537.36',
	'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/534.24 (KHTML, like Gecko) Chrome/11.0.696.3 Safari/534.24',
	'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/605.1.15 (KHTML, like Gecko)'
	]


def scrap_proxy():
	try:
		url_list = ['https://www.sslproxies.org/', 'https://www.us-proxy.org/', 'https://free-proxy-list.net/']
		url = random.choice(url_list)
		user_agent = random.choice(ua)
		req = requests.get(url, headers={"User-Agent": user_agent}, timeout=(2, 3))
		r_t = time.localtime()
		req_time = time.strftime("%I:%M:%S %p", r_t)
		soup = BeautifulSoup(req.text, 'html5lib')
		ip = list(map(lambda x: x.text, soup.findAll('td')[::8]))
		port = list(map(lambda x: x.text, soup.findAll('td')[1::8]))
		anonymity = list(map(lambda x: x.text, soup.findAll('td')[4::8]))
		data_dictionary = {
			'IP': ip,
			'PORT': port,
			'ANONYMITY': anonymity
		}
		data_frame = pd.DataFrame(data_dictionary)
		data_filter = data_frame['ANONYMITY'] == 'elite proxy'
		elite_data = data_frame[data_filter]
		proxies = random.choice(list(map(lambda x: x[0]+':'+x[1], list(zip(list(elite_data['IP']), list(elite_data['PORT']))))))
		proxy = {
			'http': proxies,
			'https': proxies
		}
		print("Proxy scraped at [{}] from <{}> and evaluating scraped proxy [{}]".format(req_time, url, proxies))
		return proxy
	except Exception as e:
		print("Error: {}".format(e))
		callable(elite_proxy())


def get_proxy():
	while 1:
		try:
			proxy = scrap_proxy()
			urls = [
				'https://www.google.com', 'https://www.yahoo.com', 'https://www.duckduckgo.com', 'https://www.bing.com'
				]
			url = random.choice(urls)
			user_agent = random.choice(ua)
			r_t = time.localtime()
			req_time = time.strftime("%I:%M:%S %p", r_t)
			req = requests.request('get', url, proxies=proxy, headers={"User-Agent": user_agent}, timeout=(2, 3))
			response_code = req.status_code
			print("Got response at time [{}] from <{}> while using Proxy: {} User-Agent: [{}] and returned response code [{}]".format(req_time, url, proxy, user_agent, response_code))
			if response_code == 200:
				break
			else:
				print("Bad response from <{}> while using proxy {}".format(url, proxy))
				pass
		except Exception as e:
			print("Error: {}".format(e))
			pass
	return proxy


def elite_proxy():
	try:
		proxy = get_proxy()
		url = 'http://httpbin.org/ip'
		user_agent = random.choice(ua)
		r_t = time.localtime()
		req_time = time.strftime("%I:%M:%S %p", r_t)
		req = requests.request('get', url, proxies=proxy, headers={"User-Agent": user_agent}, timeout=(2, 3))
		soup = BeautifulSoup(req.content, 'html5lib')
		print("Request originated(while using proxy) from {}".format(soup.text))
		print("Finally validated Elite Proxy from <{}> User-Agent used [{}] and chooses proxy for further process {}".format(url, user_agent, proxy))
		print("Elite Proxy Scraper successfully completed [{}]...".format(req_time))
		return proxy
	except Exception as e:
		print("Error: {}".format(e))
		callable(elite_proxy())


if __name__ == '__main__':
	b_t = time.localtime()
	begin_time = time.strftime("%I:%M:%S %p", b_t)
	req_url = 'http://httpbin.org/ip'
	req_req = requests.request('get', req_url, timeout=(2, 3))
	req_soup = BeautifulSoup(req_req.content, 'html5lib')
	print("Request originated(without using proxy) from {}".format(req_soup.text))
	print("Elite Proxy crawler and validator initialized [{}]......".format(begin_time))
	elite_proxy()
