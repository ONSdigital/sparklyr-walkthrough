
# coding: utf-8

# In[1]:


import sys
import requests


def download(url, filename):
    with open(filename, 'wb') as f:
        response = requests.get(url, stream=True)
        total = response.headers.get('content-length')

        if total is None:
            f.write(response.content)
        else:
            downloaded = 0
            total = int(total)
            for data in response.iter_content(chunk_size=max(int(total / 1000), 1024 * 1024)):
                downloaded += len(data)
                f.write(data)
                done = int(50 * downloaded / total)
                sys.stdout.write('\r[{}{}]'.format('█' * done, '.' * (50 - done)))
                sys.stdout.flush()
    sys.stdout.write('\n')


# In[5]:


from pathlib import Path

root_url = 'http://data.dft.gov.uk/anonymised-mot-test/test_data'

download_location = Path('../data/raw/')

if not download_location.exists():
    download_location.mkdir(parents=True)

years = range(2010, 2012)

files_to_download = []

for year in years:
    if year == 2017:
        ext = '.zip'
    else:
        ext = '.txt.gz'
    
    files_to_download.append(root_url + f'/test_result_{year}{ext}')
    files_to_download.append(root_url + f'/test_item_{year}{ext}')

    
for candidate in files_to_download:
    filename = candidate.split('/')[-1]
    destination = download_location / filename
    if destination.exists():
        print(f'File already present: {filename}')
    else:
        print(f'Downloading {filename}...')
        download(candidate, destination)
    


# In[3]:


candidate


# In[ ]:


supporting_docs = [
    'http://data.dft.gov.uk/anonymised-mot-test/lookup/item_detail.txt', 
    'http://data.dft.gov.uk/anonymised-mot-test/lookup.zip',
    'http://data.dft.gov.uk/anonymised-mot-test/MOT_user_guide_v4.docx',
    'http://data.dft.gov.uk/anonymised-mot-test/lookup/mdr_rfr_location.txt',
    'http://data.dft.gov.uk/anonymised-mot-test/lookup/item_group.txt'
]

