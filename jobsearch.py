import os
import re
import math
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
import random
import logging
import sqlite3
from datetime import datetime
import json
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cv_downloader.log'),
        logging.StreamHandler()
    ]
)

class RozeeCVDownloader:
    def __init__(self):
        self.session = self._configure_session()
        self.city_map = self._build_city_map()
        self.db_conn = self._init_database()
        self.config = self._load_config()
        
    def _configure_session(self):
        """Configure HTTP session with headers and cookies"""
        session = requests.Session()
        session.cookies.update({
            'PHPSESSID': 'itdpm4v7mfvrgdku1rl311omht',
            'rozeeId': '268838a99a8f55722659feab9ef11ab9',
            'session_lb': '059bd7e612e3842d'
        })
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://hiring.rozee.pk/',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        })
        return session

    def _build_city_map(self):
        """Build mapping of city names to IDs"""
        original_url = ("https://hiring.rozee.pk/cv/csearch/q/Manager/fc/1180:1184:1185:1190:2505:2010:2011:2012:2013:3192:110768:2014:2015:2664:110838:2016:2017:2663:2018:110985:110857:2019:2020:2021:2507:2022:2023:2827:110856:2024:2025:2026:110732:2027:2028:2529:110912:2548:2530:2030:2031:2032:2531:2033:2034:2035:2036:2665:2038:110734:110827:2039:2519:2037:110855:2040:2766:2041:2042:2043:2044:2045:2046:2047:2048:2517:2049:2050:2051:2845:2052:110730:2053:2054:2538:2204:2055:2056:2057:2058:2059:2060:2061:2062:2063:110914:2064:110832:2065:110808:2066:2067:110776:2068:2069:2070:2071:2072:110530:2073:2666:2074:2075:110805:2076:2508:2077:110728:2078:2079:2080:2081:2661:2082:2083:2084:2085:110858:2086:110916:2087:1181:2089:2093:2090:2091:2092:2660:110063:110344:2768:110346:110348:110524:2767:2769:2094:2095:110833:2096:2097:2098:2099:2100:2101:2884:2103:2104:110126:2105:2516:2106:2107:2108:110837:2109:2110:2111:1182:2112:2102:2113:2114:2115:110828:2116:2117:110744:2118:2119:2120:2121:2503:3282:2122:2123:2124:110775:2125:2126:110918:2528:2127:110738:110124:1183:110859:110946:2128:2129:110528:2130:2131:2132:3006:2133:2134:2135:2136:2137:3194:2138:2139:2140:110120:2142:2143:2144:2141:2146:110810:2147:2148:110122:110993:2165:2149:2150:2151:2152:2153:2550:2154:2155:110532:2156:2157:2158:2159:2160:2161:2162:2163:2164:2166:2497:2167:2168:2169:110538:2171:2170:2172:110839:2174:2175:2176:2177:2527:2178:2179:2770:2180:2181:110834:2182:2591:2183:2184:2185:2186:110841:110860:110073:110518:2722:2187:2543:110853:110852:110526:110851:2188:2189:110829:2190:2191:2192:2193:2194:2195:2196:2552:2197:3198:2198:2199:2200:2201:2202:110342:2746:110830:2203:110698:2205:2542:2206:2207:2847:2208:2779:2209:2210:2553:2211:2212:2511:2521:2213:3200:2214:2215:2216:2218:2724:2545:2219:2220:2221:2544:2222:2223:2224:2225:2226:110842:110920:2227:2228:2229:1186:2230:2231:110854:2232:2233:110840:2234:2235:2236:2237:2494:110740:2238:2239:2240:2241:2242:2243:110799:110861:3270:2244:2555:2245:2246:2247:2248:1187:2249:2250:2251:2771:110836:2252:2253:2254:110922:2255:110815:2256:2257:2258:2259:2260:2498:2268:110947:2261:3292:2262:2592:2263:110831:2264:2778:2265:2772:2515:2266:2267:110987:2269:2773:110924:110991:2270:110929:2272:2273:2776:2275:110299:2276:2277:2504:2278:1188:2279:2280:2281:2029:2282:2284:2536:2283:2285:3190:2286:2537:2287:110700:2593:2288:2556:2777:2289:2290:2291:1189:2292:2293:2294:2295:2296:2297:110926:2298:2299:110072:110835:2300:2301:2302:2303:2304:2305:110989:2306:2307:2308:2309:2310:110702:2311:2312:2313:2314:2315:2316:2317:2318:2319:2321:2324:1191:2322:2323:110764:2325:2327:2328:2723:2326:2329:2532:2331:2546:2332:2333:2334:2335:110736:2336:3002:2337:2338:1192:2339:2594:2340:2341:2342:110891:2343:2344:2345:2346:110806:2774:2595:1193:110928:2347:2554:2348:2349:2350:2351:2352:2846:2353:2354:2355:2356:2357:2358:2359:2509:2360:2361:110766:3188:2362:2534:2363:2364:110077:2366:2662:2367:2368:110847:2369:2370:2371:2372:2373:2374:110083:2376:2520:110843:2377:110930:2551:2378:2501:2379:2380:2381:3004:2382:2383:2775:2384:110932:2385:2386:2765:2506/fco/79/fpp/20/fsrt/score"
        )
        city_names = [
    'Islamabad', 'Karachi', 'Lahore', 'Rawalpindi', 'Abbaspur', 'Abbottabad',
    'Abdul Hakim', 'Adda Jahan Khan', 'Adda Shaiwala', 'Ahmadpur East',
    'Ahmed pur Sial', 'Akhora Khattak', 'Ali Chak', 'Alipur', 'Alipur Chatta',
    'Allahabad', 'Amangarh', 'Ambela', 'Arifwala', 'Arzanipur', 'Astore',
    'Attock', 'Babri Banda', 'Badin', 'Bagh', 'Bahawalnagar', 'Bahawalpur',
    'Bajaur', 'Bajour', 'Balakot', 'Bannu', 'Barbar Loi', 'Barkhan', 'Baroute',
    'Bat Khela', 'Battagram', 'Bela', 'Besham', 'Bewal', 'Bhakkar', 'Bhalwal',
    'Bhan Saeedabad', 'Bhara Kahu', 'Bhera', 'Bhimbar', 'Bhirya Road',
    'Bhuawana', 'Bisham', 'Blitang', 'Bolan', 'Bonga Hayat', 'Buchay Key',
    'Bunner', 'Burewala', 'CDA', 'Chacklala', 'Chaghi', 'Chaininda',
    'Chak 4 b c', 'Chak 46', 'Chak Jamal', 'Chak Jhumra', 'Chak Sawara',
    'Chak Shezad', 'Chakwal', 'Chaman', 'Charsada', 'Chashma', 'Chawinda',
    'Cherat', 'Chicha watni', 'Chilas', 'Chiniot', 'Chishtian', 'Chitral',
    'Choa Saiden Shah', 'Chohar Jamali', 'Choppar Hatta', 'Chowk Azam',
    'Chowk Maitla', 'Chowk Munda', 'Chunian', 'Dadakhel', 'Dadu', 'Daharki',
    'Dalbandin', 'Dandot', 'Danyor', 'Dargai', 'Darra Pezu', 'Darya Khan',
    'Daska', 'Dassu', 'Daud Khel', 'Daulat Pur', 'Daur', 'Deh Pathaan',
    'Depal Pur', 'Dera Bugti', 'Dera Ghazi Khan', 'Dera Ismail Khan',
    'Dera Murad Jamali', 'Dera Nawab Sahib', 'Dhabeji', 'Dhatmal', 'Dhirkot',
    'Dhoun Kal', 'Diamer', 'Digri', 'Dijkot', 'Dina', 'Dinga', 'Dir',
    'Doaaba', 'Doltala', 'Domeli', 'Dudial', 'Dukki', 'Dunyapur', 'Dureji',
    'Eminabad', 'Faisalabad', 'Farooqabad', 'Fateh Jang', 'Fateh Pur',
    'Feroz Walla', 'Feroz Watan', 'Fizagat', 'Fort Abbas', 'FR Bannu',
    'FR Bannu / Lakki', 'FR DI Khan', 'FR Kohat', 'FR Peshawar',
    'FR Peshawar / Kohat', 'FR Tank', 'Gadoon Amazai', 'Gaggo Mandi',
    'Gahkuch', 'Gakhar Mandi', 'Gambet', 'Garh Maharaja', 'Garh More',
    'Gari Habibullah', 'Gari Mori', 'Ghari Dupatta', 'Gharo', 'Ghazi',
    'Ghizer', 'Ghotki', 'Ghuzdar', 'Gilgit', 'Gohar Ghoushti', 'Gojra',
    'Golarchi', 'Goular Khel', 'Guddu', 'Gujar Khan', 'Gujranwala', 'Gujrat',
    'Gwadar', 'Hafizabad', 'Hala', 'Hangu', 'Harappa', 'Hari Pur', 'Hariwala',
    'Harnai', 'Haroonabad', 'Hasilpur', 'Hassan Abdal', 'Hattar', 'Hattian',
    'Haveli Kahuta', 'Haveli Lakha', 'Havelian', 'Hayatabad', 'Hazara',
    'Hazro', 'Head Marala', 'Hub', 'Hub Chowki', 'Hub Inds Estate',
    'Hujra Shah Muqeem', 'Hunza Nagar', 'Hyderabad', 'ICT', 'Islamkot',
    'Issa Khel', 'Jacobabad', 'Jaffarabad', 'Jaja Abasian', 'Jalal Pur Jatan',
    'Jalal Pur Priwala', 'Jalozai', 'Jampur', 'Jamrud Road', 'Jamshoro',
    'Jandanwala', 'Jaranwala', 'Jatoi', 'Jauharabad', 'Jehangira',
    'Jehanian', 'Jhal Magsi', 'Jhand', 'Jhang', 'Jhatta Bhutta', 'Jhelum',
    'Jhudo', 'Jutial', 'Kabir Wala', 'Kacha Khooh', 'Kachhi/Bolan', 'Kahna',
    'Kahror Pacca', 'Kahuta', 'Kakul', 'Kakur Town', 'Kala Bagh',
    'Kala Shah Kaku', 'Kalam', 'Kalar Syedian', 'Kalaswala', 'Kalat',
    'Kallur Kot', 'Kamalia', 'Kamalia Musa', 'Kamber Ali Khan', 'Kamoke',
    'Kamra', 'Kandhkot', 'Kandiaro', 'Karak', 'Karore Lalisan', 'Kashmir',
    'Kashmore', 'Kasur', 'Kazi Ahmed', 'Kech', 'Khair Pur Mir', 'Khairpur',
    'Khairpur Nathan Shah', 'Khairpur Tamaiwali', 'Khanbela', 'Khandabad',
    'Khanewal', 'Khangarh', 'Khanpur', 'Khanqah Dogran', 'Khanqah Sharif',
    'Kharan', 'Kharian', 'Khewra', 'Khipro', 'Khoski', 'Khuiratta',
    'Khurian wala', 'Khushab', 'Khushal Kot', 'Khuzdar', 'Khwazakhela',
    'Khyber', 'Khyber Agency', 'Killa Abdullah', 'Killa Saifullah', 'Kohat',
    'Kohistan', 'Kohistan Lower', 'Kohistan Upper', 'Kohlu', 'Kolaipalas',
    'Kot Addu', 'Kot Bunglow', 'Kot Chutta', 'Kot Ghulam Mohd', 'Kot Mithan',
    'Kot Radha Kishan', 'Kotla', 'Kotla Arab Ali Khan', 'Kotla Jam',
    'Kotla Pathan', 'Kotli', 'Kotli Loharan', 'Kotmomin', 'Kotri', 'Kumbh',
    'Kundina', 'Kunjah', 'Kunri', 'Kurram', 'Kurram Agency',
    'Kuthiala Sheikhan', 'Lakimarwat', 'Lakki Marwat', 'Lala rukh',
    'Lalamusa', 'Laliah', 'Lalshanra', 'Landi Kotal', 'Larkana', 'Lasbela',
    'Lawrence pur', 'Layyah', 'Leepa', 'Liaquat Pur', 'Lodhran', 'Loralai',
    'Lower Dir', 'Ludhan', 'Machh', 'Machi Goth', 'Madinah', 'Mailsi',
    'Makli', 'Makran', 'Malakand', 'Malakwal', 'Mamu kunjan', 'Mandi Bahauddin',
    'Mandi Faizabad', 'Mandra', 'Manga Mandi', 'Mangal Sada', 'Mangi',
    'Mangla', 'Manglor', 'Mangochar', 'Mangowal', 'Manoabad', 'Mansehra',
    'Mardan', 'Mari Indus', 'Mastoi', 'Mastung', 'Matiari', 'Matli',
    'Matta', 'Mehar', 'Mehmood Kot', 'Mehrab Pur', 'Mian Chunnu', 'Mian Walli',
    'Minchanabad', 'Mingora', 'Mir Ali', 'Miran Shah', 'Mirpur  (AJK)',
    'Mirpur Khas', 'Mirpur Mathelo', 'Mirpur Sakro', 'Mishti Mela', 'Mithi',
    'Mohen Jo Daro', 'Mohmand', 'More kunda', 'Morgah', 'Moro', 'Mubarik Pur',
    'Multan', 'Muridkay', 'Murree', 'Musafir Khana', 'Musakhel', 'Muslim Bagh',
    'Mustang', 'Muzaffarabad', 'Muzaffargarh', 'Nall', 'Nankana Sahib',
    'Naran', 'Narang Mandi', 'Narowal', 'Naseerabad', 'Naudero', 'Naukot',
    'Naukundi', 'Naushahro Feroze', 'Nawab Wali Muhammad', 'Nawabshah',
    'Neelam', 'New Saeedabad', 'Nilam', 'Nilore', 'Nokhar', 'Noor kot',
    'Nooriabad', 'Noorpur nooranga', 'North Wazirstan', 'Noshki',
    'Nowshera', 'Nowshera Cantt', 'Oderolal', 'Okara', 'Orakzai', 'Ormara',
    'Pabbi', 'Padidan', 'Painsra', 'Pak Pattan Sharif', 'Panjan Kisan',
    'Panjgur', 'Pannu Aqil', 'Parachinar', 'Pasni', 'Pasroor', 'Patika',
    'Patoki', 'Peshawar', 'Phagwar', 'Phalia', 'Phool nagar',
    'Phoolnagar (Bhai Pheru)', 'Piaro goth', 'Pind Dadan Khan',
    'Pindi Bhattian', 'Pindi bhohri', 'Pindi gheb', 'Piplan', 'Pir Mahal',
    'Pirpai', 'Pishin', 'Poonch', 'Punch', 'Qalandarabad', 'Qambar',
    'Qambar Shahdatkot', 'Qasba Gujrat', 'Qazi Ahmed', 'Quaidabad', 'Quetta',
    'Rabwah', 'Rahimyar Khan', 'Rahwali', 'Raiwind', 'Rajana', 'Rajanpur',
    'Rakhni', 'Rangoo', 'Ranipur', 'Rashidabad', 'Rashkai', 'Ratto Dero',
    'Rawala Kot', 'Rawat', 'Renala Khurd', 'Risalpur', 'Rohri', 'Sabu Rahu',
    'Sadiqabad', 'Sagri', 'Sahiwal', 'Saidu Sharif', 'Sajawal', 'Sakardu',
    'Sakrand', 'Sambrial', 'Samma Satta', 'Samundri', 'Sanghar', 'Sanghi',
    'Sangla Hill', 'Sangote', 'Sanjwal', 'Sara e Naurang', 'Sarai Alamgir',
    'Sargodha', 'Satyana Bangla', 'Sehar Baqlas', 'Sehwan', 'Shadiwal',
    'Shahdad Kot', 'Shahdad Pur', 'Shaheed Benazirabad', 'Shahkot',
    'Shahpur Chakar', 'Shakargarh', 'Shamsabad', 'Shangla', 'Shankiari',
    'Shedani sharif', 'Sheikhupura', 'Shemier', 'Sherani', 'Shikarpur',
    'Shogram', 'Shorkot', 'Shujabad', 'Sialkot', 'Sibi', 'Sidhnoti',
    'Sihala', 'Sikandarabad', 'Silanwala', 'Sillanwali', 'Sita Road',
    'Skardu', 'Sohawa District Daska', 'Sohawa District Jelum', 'Sohbatpur',
    'South Wazirstan', 'Sudhnoti', 'Sukkur', 'Surab', 'Swabi', 'Swat',
    'Swatmingora', 'Takhtbai', 'Talagang', 'Talamba', 'Talhur', 'Tall',
    'Tando Adam', 'Tando Allahyar', 'Tando Jam', 'Tando Mohd Khan', 'Tank',
    'Tarbela', 'Tarmatan', 'Tarnol', 'Taunsa sharif', 'Taxila',
    'Thana Bula Khan', 'Thari Mirwah', 'Tharo Shah', 'Tharparkar', 'Thatta',
    'Theing Jattan More', 'Thul', 'Tibba Sultanpur', 'Timergara',
    'Tobatek Singh', 'Topi', 'Torghar', 'Toru', 'Trinda Mohd Pannah',
    'Turbat', 'Ubaro', 'Ugoki', 'Ukba', 'Umer Kot', 'Upper Deval',
    'Upper Dir', 'Upper Swat', 'Usta Mohammad', 'Uthal', 'Utror', 'Vehari',
    'Village Sunder', 'Wah Cantt', 'Wahi hassain', 'Wan Radha Ram', 'Wana',
    'Warah', 'Warburton', 'Washuk', 'Wazirabad', 'Winder', 'Yazman Mandi',
    'Zahir Pir', 'Zhob', 'Ziarat'
]
        
        match = re.search(r"/fc/([^/]+)/fco", original_url)
        if not match:
            raise RuntimeError("Couldn't find the /fc/.../fco segment.")
        city_ids = match.group(1).split(":")

        if len(city_ids) != len(city_names):
            raise ValueError(f"Mismatch: found {len(city_ids)} IDs but {len(city_names)} names.")

        return dict(zip(city_names, city_ids))

    def _init_database(self):
        """Initialize SQLite database for tracking downloads"""
        conn = sqlite3.connect('cv_downloader.db')
        conn.execute('''CREATE TABLE IF NOT EXISTS downloads
                     (url TEXT PRIMARY KEY, filename TEXT, status TEXT, timestamp DATETIME)''')
        return conn

    def _load_config(self):
        """Load configuration with intelligent defaults and validation"""
        defaults = {
            'max_retries': 3,
            'backoff_factor': 1,
            'timeout': 30,
            'save_dir': 'downloaded_cvs',
            'skip_existing': True,
            'duplicate_handling': 'rename',
            'min_disk_space': 100,
            'results_per_page': 50
        }
        
        config_path = Path('config.json')
        
        if not config_path.exists():
            logging.info("No config file found - creating default config.json")
            try:
                with open(config_path, 'w') as f:
                    json.dump(defaults, f, indent=2)
                return defaults
            except IOError as e:
                logging.warning(f"Couldn't create config file: {e}. Using defaults")
                return defaults
        
        try:
            with open(config_path) as f:
                user_config = json.load(f)
                
                # Validate critical values
                if not isinstance(user_config.get('max_retries'), int) or user_config['max_retries'] < 1:
                    logging.warning("Invalid max_retries in config - using default")
                    user_config['max_retries'] = defaults['max_retries']
                    
                return {**defaults, **user_config}
                
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON in config file: {e}. Using defaults")
            return defaults
        except Exception as e:
            logging.error(f"Error loading config: {e}. Using defaults")
            return defaults

    def _check_internet(self):
        """Verify internet connectivity"""
        try:
            response = self.session.get(
                'https://www.google.com',
                timeout=5,
                allow_redirects=False
            )
            return response.status_code < 400
        except requests.RequestException:
            return False

    def _wait_for_internet(self):
        """Block until internet connection is restored"""
        while not self._check_internet():
            logging.warning("Internet connection lost - waiting to reconnect...")
            time.sleep(60)

    def _get_file_extension(self, content_type, content_start):
        """Determine file extension based on content"""
        if 'application/pdf' in content_type or content_start.startswith(b'%PDF'):
            return '.pdf'
        elif 'application/msword' in content_type or content_start.startswith(b'\xD0\xCF\x11\xE0'):
            return '.doc'
        elif 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' in content_type or content_start.startswith(b'PK\x03\x04'):
            return '.docx'
        return '.bin'

    def _create_folder(self, city):
        """Create folder for saving CVs with validation"""
        folder_name = os.path.join(self.config['save_dir'], city)
        try:
            os.makedirs(folder_name, exist_ok=True)
            return folder_name
        except OSError as e:
            logging.error(f"Failed to create folder {folder_name}: {e}")
            raise

    def _extract_city_count(self, base_url, cid):
        """Get count of CVs for a city with error handling"""
        try:
            res = self.session.get(base_url, timeout=self.config['timeout'])
            res.raise_for_status()
            soup = BeautifulSoup(res.text, "html.parser")
            input_tag = soup.find("input", {"id": f"fc_{cid}"})
            if input_tag:
                small_tag = input_tag.find_next("small", class_="s-14")
                if small_tag:
                    return int(small_tag.get_text(strip=True).replace(",", ""))
            return 0
        except Exception as e:
            logging.warning(f"Failed to get count for city {cid}: {e}")
            return 0

    def _extract_cv_info(self, url):
        """Extract CV information from listing page with robust error handling"""
        try:
            logging.info(f"Fetching page: {url}")
            self._wait_for_internet()
            
            response = self.session.get(url, timeout=self.config['timeout'])
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, "html.parser")

            # Extract names
            names = [a.get_text(strip=True) for a in soup.select("div.cvappname.s-18 a")]
            
            # Extract CV numbers
            cv_numbers = []
            for div in soup.find_all("div", class_="cvsdatetxt"):
                text = div.get_text(strip=True)
                if text.startswith("CV Number:"):
                    cv_numbers.append(text.split(":")[1].strip())
            
            # Extract download links
            download_links = []
            for a in soup.select("a[href*='cvdownload']"):
                download_url = urljoin(url, a['href'])
                download_links.append(download_url)

            logging.info(f"Found {len(download_links)} download links")
            
            # Verify counts
            min_count = min(len(names), len(cv_numbers), len(download_links))
            if min_count == 0:
                logging.warning("No complete CV records found")
                return [], [], []
                
            if len(names) != len(cv_numbers) or len(names) != len(download_links):
                logging.warning(f"Count mismatch - using first {min_count} records")
                
            return names[:min_count], cv_numbers[:min_count], download_links[:min_count]
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Network error extracting CV info: {e}")
            return [], [], []
        except Exception as e:
            logging.error(f"Unexpected error extracting CV info: {e}", exc_info=True)
            return [], [], []

    def _download_cv(self, download_url, save_path):
        """Download individual CV with comprehensive error handling"""
        retry_count = 0
        max_retries = self.config['max_retries']
        
        while retry_count < max_retries:
            try:
                logging.info(f"Attempting download (try {retry_count+1}/{max_retries}): {download_url}")
                
                # Random delay to avoid rate limiting
                time.sleep(random.uniform(2, 5))
                
                headers = {
                    'Referer': download_url,
                    'Accept': 'application/pdf, application/msword, */*',
                }
                
                response = self.session.get(
                    download_url,
                    headers=headers,
                    stream=True,
                    timeout=self.config['timeout']
                )
                response.raise_for_status()
                
                # Check for CAPTCHA
                if 'captcha' in response.text.lower():
                    raise RuntimeError("CAPTCHA detected - manual intervention required")
                
                # Determine file extension
                content_start = response.content[:8]
                content_type = response.headers.get('Content-Type', '').lower()
                file_ext = self._get_file_extension(content_type, content_start)
                
                # Ensure proper file extension
                base_path, current_ext = os.path.splitext(save_path)
                if current_ext.lower() != file_ext:
                    save_path = base_path + file_ext
                
                # Save file
                with open(save_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                # Verify download
                if os.path.exists(save_path) and os.path.getsize(save_path) > 1024:
                    logging.info(f"Successfully saved: {os.path.basename(save_path)}")
                    return True
                else:
                    raise IOError("Downloaded file is too small or invalid")
                    
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:  # Rate limited
                    wait_time = 30
                    logging.warning(f"Rate limited - waiting {wait_time} seconds")
                    time.sleep(wait_time)
                    retry_count += 1
                    continue
                logging.error(f"HTTP error downloading CV: {e}")
                retry_count += 1
                
            except Exception as e:
                logging.error(f"Error downloading CV: {e}")
                retry_count += 1
                
            # Clean up failed download
            if os.path.exists(save_path):
                try:
                    os.remove(save_path)
                except OSError:
                    pass
        
        logging.error(f"Failed to download after {max_retries} attempts: {download_url}")
        return False

    def _record_download(self, url, filename, status):
        """Record download attempt in database"""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(
                '''INSERT OR REPLACE INTO downloads 
                (url, filename, status, timestamp) VALUES (?, ?, ?, ?)''',
                (url, filename, status, datetime.now().isoformat())
            )
            self.db_conn.commit()
        except sqlite3.Error as e:
            logging.error(f"Database error recording download: {e}")

    def _is_already_downloaded(self, url):
        """Check if URL was already processed"""
        if not self.config['skip_existing']:
            return False
            
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("SELECT status FROM downloads WHERE url = ?", (url,))
            result = cursor.fetchone()
            return result is not None and result[0] == 'success'
        except sqlite3.Error as e:
            logging.error(f"Database error checking download status: {e}")
            return False

    def _generate_save_path(self, folder, cv_id, name):
        """Generate safe save path handling duplicates"""
        clean_name = re.sub(r'[\\/*?:"<>|]', "", name)[:50]
        base_filename = f"{cv_id}_{clean_name.replace(' ', '_')}"
        base_path = os.path.join(folder, base_filename)
        
        # Check for existing files
        existing_files = [
            f for f in os.listdir(folder) 
            if f.startswith(base_filename) and f.lower().endswith(('.pdf', '.doc', '.docx'))
        ]
        
        if existing_files and self.config['skip_existing']:
            return None  # Skip this file
            
        if existing_files and self.config['duplicate_handling'] == 'rename':
            counter = 1
            while True:
                new_path = f"{base_path}_{counter}"
                if not any(f.startswith(os.path.basename(new_path)) for f in existing_files):
                    return new_path
                counter += 1
                
        return base_path

    def process_city(self, city, cid, keyword):
        """Process all CVs for a single city"""
        logging.info(f"Processing {city} ({cid})")
        
        base_url = f"https://hiring.rozee.pk/cv/csearch/q/{requests.utils.quote(keyword)}/fc/{cid}/fco/79/fpp/{self.config['results_per_page']}/fsrt/score"
        total = self._extract_city_count(base_url, cid)
        pages = math.ceil(total / self.config['results_per_page']) if total > 0 else 0
        logging.info(f"Found {total} CVs | Pages to scrape: {pages}")

        if total == 0:
            return 0

        try:
            folder = self._create_folder(city)
        except OSError:
            return 0

        downloaded_count = 0

        for page in range(pages):
            offset = page * self.config['results_per_page']
            url = base_url if page == 0 else f"{base_url}/?fpn={offset}"
            logging.info(f"Processing page {page+1}/{pages}: {url}")
            
            # Add delay between pages
            time.sleep(random.uniform(3, 7))

            names, cv_ids, links = self._extract_cv_info(url)
            
            if not names:
                logging.warning("No CVs found on this page")
                continue

            for name, cv_id, link in zip(names, cv_ids, links):
                if self._is_already_downloaded(link):
                    logging.info(f"Skipping previously downloaded CV: {link}")
                    downloaded_count += 1
                    continue
                
                save_path = self._generate_save_path(folder, cv_id, name)
                if not save_path:  # Skip existing files if configured
                    downloaded_count += 1
                    continue
                
                if self._download_cv(link, save_path):
                    self._record_download(link, os.path.basename(save_path), 'success')
                    downloaded_count += 1
                else:
                    self._record_download(link, None, 'failed')
        
        logging.info(f"Finished {city}. Downloaded {downloaded_count}/{total} CVs")
        return downloaded_count

    def run(self, keyword):
        """Main execution method"""
        try:
            logging.info(f"Starting CV download for keyword: {keyword}")
            start_time = time.time()
            total_downloaded = 0
            
            for city, cid in self.city_map.items():
                total_downloaded += self.process_city(city, cid, keyword)
                
        except KeyboardInterrupt:
            logging.info("\nReceived keyboard interrupt - shutting down gracefully...")
        except Exception as e:
            logging.error(f"Fatal error in main execution: {e}", exc_info=True)
        finally:
            elapsed = time.time() - start_time
            logging.info(
                f"\n🏁 Process completed. Total CVs downloaded: {total_downloaded} "
                f"in {elapsed:.2f} seconds"
            )
            self.db_conn.close()

if __name__ == "__main__":
    downloader = RozeeCVDownloader()
    keyword = input("Enter your search keyword: ").strip()
    downloader.run(keyword)