#! /usr/local/bin/python3 
from pathlib import Path
import sys

import requests


ROOT_URL = 'http://data.dft.gov.uk/anonymised-mot-test/test_data'


def download_all_mot_data(start_year, end_year, output_directory):
        
    download_location_data = Path(output_directory) / 'raw'
    download_location_docs = Path(output_directory) / 'docs'

    years = range(start_year, end_year+1)
    
    for year in years:
        download_mot_data(year, download_location_data)    
        
    download_supporting_docs()
        
            
def download_mot_data(year, output_directory):

    if not output_directory.exists():
        output_directory.mkdir(parents=True)

    ext = '.txt.gz' if year <= 2016 else '.zip'

    for fname_prefix in ['test_result', 'test_item']:
        
        filename = f'{fname_prefix}_{year}{ext}'
        url = ROOT_URL + '/' + filename
        destination = output_directory / filename

        if destination.exists():
            print(f'File already present: {filename}')
        else:
            print(f'Downloading {filename} to {output_directory} ...')
            download(url, destination)


def download_supporting_docs(output_directory):

    if not output_directory.exists():
        output_directory.mkdir(parents=True)
    
    supporting_docs = [
        'http://data.dft.gov.uk/anonymised-mot-test/lookup/item_detail.txt', 
        'http://data.dft.gov.uk/anonymised-mot-test/lookup.zip',
        'http://data.dft.gov.uk/anonymised-mot-test/MOT_user_guide_v4.docx',
        'http://data.dft.gov.uk/anonymised-mot-test/lookup/mdr_rfr_location.txt',
        'http://data.dft.gov.uk/anonymised-mot-test/lookup/item_group.txt'
    ]

    for url in supporting_docs:

        filename = url.split('/')[-1]
        destination = output_directory / filename

        if destination.exists():
            print(f'File already present: {filename}')
        else:
            print(f'Downloading {filename} to {output_directory} ...')
            download(url, destination)
            

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

    
if __name__ == '__main__':
    
    from argparse import ArgumentParser
    
    parser = ArgumentParser()
    parser.add_argument('start_year', type=int, help='Start year for downloaded data.')
    parser.add_argument('end_year', type=int, help='End year (inclusive) for downloaded data.')
    parser.add_argument('-o', '--output_directory', default='./data', help='Root directory to download data to.')
    args = parser.parse_args()
    
    download_mot_data(args.start_year, args.end_year, args.output_directory)
