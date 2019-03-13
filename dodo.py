from pathlib import Path
from lib.source import download_mot_data, download_supporting_docs


START = 2010
END = 2011
DATA_DIR = Path('./data')


def task_download_docs():
    """Download the supporting documentation for the datasets."""
    
    fnames = [
        'item_detail.txt', 
        'lookup.zip',
        'MOT_user_guide_v4.docx',
        'mdr_rfr_location.txt',
        'item_group.txt',
    ]
    targets = [DATA_DIR / 'docs' / t for t in fnames]
    
    def download():
        download_supporting_docs(output_directory=DATA_DIR / 'docs')
    
    return {
        'targets': targets,
        'actions': [download],
        'verbosity': 2,
    }


def task_download_data():
    """Download data fro an individual year."""

    for year in range(START, END+1):
        ext = 'txt.gz' if year <= 2016 else '.zip'
        fnames = [
            f'test_result_{year}{ext}',
            f'test_item_{year}{ext}',
        ]
        targets = [DATA_DIR / 'raw' / t for t in fnames]

        def download():
            download_mot_data(year, output_directory=DATA_DIR / 'raw')
        
        yield {
            'name': f'data_{year}',
            'targets': targets,
            'actions': [download],
            'verbosity': 2,
        }

        
def task_extract_and_load_result_data():
    
    raw_data_dir = DATA_DIR / 'raw' 
    
    for g_zipped_filepath in raw_data_dir.glob('test_result_*txt.gz'):
        print(g_zipped_filepath)
    
    yield {
        'name': g_zipped_filepath.name,
        'file_deps': [g_zipped_filepath],
        'targets': [f'{g_zipped_filepath.name}_loaded'],
        'actions': [
            f'gunzip -c {g_zipped_filepath} | hdfs dfs -appendToFile - /user/dte_chrism/mot/results.txt', 
            'touch {targets}'
        ]
    }

    
def task_extract_and_load_item_data():
    
    raw_data_dir = DATA_DIR / 'raw' 
    
    for g_zipped_filepath in raw_data_dir.glob('test_item_*txt.gz'):
        print(g_zipped_filepath)
    
    yield {
        'name': g_zipped_filepath.name,
        'file_deps': [g_zipped_filepath],
        'targets': [f'{g_zipped_filepath.name}_loaded'],
        'actions': [
            f'gunzip -c {g_zipped_filepath} | hdfs dfs -appendToFile - /user/dte_chrism/mot/items.txt', 
            'touch {targets}'
        ]
    }

    
    
