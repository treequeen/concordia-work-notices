# COPYRIGHT LILY COWPER 2024

#------------------------------------------------------------
# SEARCH FOR WORDS IN STRING
#------------------------------------------------------------
# 'matches' can be existing list or else define as empty list 
# before running define 'wordlist' before running

def search_keys(string, wordlist, matches):
  string = string.lower()
  for word in wordlist:
    if word in string:
      matches.append(word)
  return matches

def search_bldgs(string, wordlist):
  for word in wordlist:
    matches = re.findall(word, string)
  return matches

#------------------------------------------------------------
# SCRAPE POST
#------------------------------------------------------------
def scrape_post(entry, df):
  from datetime import datetime as dt
  import dateparser
  import sys

  # GET FEED TITLES & LINKS
  title = entry.title
  link = entry.link

  parsed_title = re.split(r'\s*:\s|(?:\s\-|\-\s)|\(|\)', title)

  # remove elements in list that do not contain letters
  letters = re.compile('[a-zA-Z]+')
  parsed_title = list(filter(lambda x: letters.search(x), parsed_title))

  #filter note
  note = None
  pat1 = r'^[A-Z]+$'
  for i in parsed_title:
    match = re.search(pat1, i)
    if match != None:
      idx = parsed_title.index(i)
      note = parsed_title[idx]
      break

  #filter summary
  summary = None
  pat2 = r'[Bb][uilding]{6,}'
  for i in parsed_title:
    match = re.search(pat2, i)
    if match != None:
      idx = parsed_title.index(i) + 1
      try:
        summary = parsed_title[idx]
      except IndexError:
        pass
      break

  if summary is None:
    summary = ', '.join(parsed_title[:])

  # parse post text
  response = requests.get(link) #get html from post link
  soup = BeautifulSoup(response.content, 'html.parser') #make soup from html
  details = soup.find_all('div', {'class': ['date','rte']})
  fulltext = details[1].get_text() #get full post text

  # get date published
  dateclass = soup.find_all('div', {'class': ['date']})
  pubdate = dateclass[0].string
  pubdate = dateparser.parse(pubdate).date()

  # search post text for regex patterns
  dates = re.findall(r'(\d{4}\-\d{2}\-\d{2})', fulltext) #date format (ex: 2024-02-01)
  times = re.findall(r'(\d{2}:\d{2})', fulltext) #time format (ex: 06:00)
  projectno = re.findall(r'(?:[\D\W])(\d{2}\-\d{3})(?:[\D\W])', fulltext) #construction project number (12-123)
  floor = re.findall(r'([A-Z]+\-?[A-Z]*\d\d?)(?:\D)', fulltext) #floor (ex: H7)
  location = re.findall(r'([A-Z]+\-?\d\d\d\d*\.?\d*)', fulltext) #room number (ex: S115.30)

  # separate start/end dates
  if len(dates)>0:
    startdate = dateparser.parse(dates[0]).date()
  else:
    startdate = None
  if len(dates)>1:
    enddate = dateparser.parse(dates[1]).date()
  else:
    enddate = None

  # separate start/end times
  if len(times)>0:
    starttime = times[0]
  else:
    starttime = None
  if len(times)>1:
    endtime = times[1]
  else:
    endtime = None

  #  SEARCH FOR: buildings
  bldgs = r'\b[\s\W]*([A-Z]+|Hall)[\s\W]*\b'
  building = re.findall(bldgs, title)
  building = ['H' if x=='Hall' else x for x in building]
  while note in building: building.remove(note)

  #  SEARCH FOR: locations
  locations = ['elevator', 'escalator', 'terrace', 'roof', 'door', 'entrance', 'porte', 'loading dock', 'stair', 'mezzanine', 'vestibule', 'lobby', 'tunnel', 'scaffolding',
               'facade', 'sidewalk', 'Mackay', 'Guy St', 'Bishop', 'Catherine', 'Maisonneuve', 'street', 'alley', 'tree', 'ground floor', 'first floor', 'hallway', 'corridor',
               'security desk', 'lounge', 'basement', 'parking']
  location = search_keys(summary, locations, location)

  #  SEARCH FOR: directional location
  direction=[]
  directions = ['north', 'east', 'south', 'west', 'main']
  direction = search_keys(summary, directions, direction)

  new_row = pd.Series({'pubdate': pubdate, 'note': note, 'summary': summary, 'building': list(set(building)), 'floor': list(set(floor)), 'location': list(set(location)), 'direction': list(set(direction)), 'startdate': startdate, 'enddate': enddate, 'starttime': starttime, 'endtime': endtime, 'tags': None, 'fulltext': fulltext, 'link': link})
  new_row = new_row.replace({None: ''}) #replace None with empty string
  page_posts = pd.concat([df, pd.DataFrame([new_row], columns=new_row.index)]).reset_index(drop=True)
  return page_posts

#------------------------------------------------------------
# GET PAGES IN RANGE (based on pagination)
#------------------------------------------------------------
def getpostsinrange(url, firstpage, lastpage, df):
  print(f'Scraping pages {firstpage} to {lastpage} in reverse order...\n--------------------------------------------')

  lastpage = lastpage + 1         #range() starts from zero, so need to add 1
  num_rows = (lastpage - firstpage)*10        #10 rows per page

  posts = df

  for page in reversed(range(firstpage, lastpage)):
    page_posts = pd.DataFrame(columns=('pubdate', 'note', 'summary', 'building', 'floor', 'location', 'direction', 'startdate', 'enddate', 'starttime', 'endtime', 'tags', 'fulltext', 'link'))

    pageurl = url + f'?page={page}'

    #parse feed
    feed = feedparser.parse(pageurl)
    for entry in feed.entries:
      page_posts = scrape_post(entry, page_posts)

    #add page to posts df
    posts =  pd.concat([page_posts, posts]).reset_index(drop=True)

    print(f'Page {page} successfully scraped')

  posts = posts.drop_duplicates(subset=['pubdate', 'fulltext'])
  print('\n----PULL COMPLETE----')
  print('Rows scraped: ', num_rows)
  return posts

#------------------------------------------------------------
# UPDATE CSV IN GITHUB
#------------------------------------------------------------
def updatenotices(filename, df, commit_message =""):
  from datetime import datetime as dt
  import dateparser
  if commit_message == "":
      commit_message = "Last updated - "+ dt.now().strftime('%Y-%m-%d %H:%M:%S')
  notices = df.to_csv(sep=',', index=False)
  g = Github(${{ secrets.SuperSecret }})
  repo = g.get_user().get_repo("concordia-work-notices")
  master_ref = repo.get_git_ref('heads/main')
  master_sha = master_ref.object.sha
  base_tree = repo.get_git_tree(master_sha)
  element_list = list()
  element = InputGitTreeElement(filename, '100644', 'blob', notices)
  element_list.append(element)
  tree = repo.create_git_tree(element_list, base_tree)
  parent = repo.get_git_commit(master_sha)
  commit = repo.create_git_commit(commit_message, tree, [parent])
  master_ref.edit(commit.sha)
  print('Last updated @', dt.now().strftime('%Y-%m-%d %H:%M:%S'))

#------------------------------------------------------------
# COMPILED FUNCTION
#------------------------------------------------------------
def RSS_scraper(url, firstpage, lastpage):
  from datetime import datetime as dt
  import dateparser

  csv_url = "https://raw.githubusercontent.com/treequeen/concordia-work-notices/main/notices.csv"
  # Make sure the url is the raw version of the file on GitHub
  # Remember githraw urls only update every 5 minutes, so need to wait between tests
  download = requests.get(csv_url).content

  # Reading the downloaded content and turning it into a pandas dataframe
  posts = pd.read_csv(io.StringIO(download.decode('utf-8')))
  posts = posts.fillna('')

  posts = getpostsinrange(url, firstpage, lastpage, posts)
  posts = posts.drop_duplicates(subset=['fulltext'])

  print('UPLOAD:\n', posts.loc[1:10, ['pubdate', 'summary']])
  updatenotices('notices.csv', posts)
  print('Total row count: ', len(posts))
  return posts

#------------------------------------------------------------
# RUN SCRAPER
#------------------------------------------------------------
url = 'https://www.concordia.ca/content/concordia/en/offices/facilities/news/_jcr_content/content-main/news_list.xml'
posts = RSS_scraper(url, 1, 3)

#---------------------------------------------------------------------
# RESET CSV !! DELETE ALL DATA !! (for starting over and re-scraping)
#---------------------------------------------------------------------
#posts = pd.DataFrame(columns=('pubdate', 'note', 'summary', 'building', 'floor', 'location', 'direction', 'startdate', 'enddate', 'starttime', 'endtime', 'tags', 'fulltext', 'link'))
#updatenotices('notices.csv', posts)
