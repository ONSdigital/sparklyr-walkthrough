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
    output_dir = DATA_DIR / 'docs'
    targets = [output_dir / t for t in fnames]

    return {
        'targets': targets,
        'actions': [(download_supporting_docs, [output_dir])],
        'verbosity': 2,
    }


def task_download_data():
    """Download data for an individual year."""

    for year in range(START, END+1):
        ext = 'txt.gz' if year <= 2016 else '.zip'
        fnames = [
            f'test_result_{year}{ext}',
            f'test_item_{year}{ext}',
        ]

        output_dir = DATA_DIR / 'raw'
        targets = [output_dir / t for t in fnames]

        yield {
            'name': f'data_{year}',
            'targets': targets,
            'actions': [(download_mot_data, [year, output_dir])],
            'verbosity': 2,
        }

        
def task_extract_and_load_result_data():
    
    raw_data_dir = DATA_DIR / 'raw' 
    
    for g_zipped_filepath in raw_data_dir.glob('test_result_*txt.gz'):
    
        yield {
            'name': g_zipped_filepath.name,
            'file_dep': [g_zipped_filepath],
            'targets': [f'{g_zipped_filepath.name}_loaded'],
            'actions': [
                f'gunzip -c {g_zipped_filepath} | hdfs dfs -appendToFile - /user/dte_chrism/mot/results.txt', 
                f'touch {g_zipped_filepath.with_suffix(".loaded")}'
            ]
        }

    
def task_extract_and_load_item_data():
    
    raw_data_dir = DATA_DIR / 'raw' 
    
    for g_zipped_filepath in raw_data_dir.glob('test_item_*txt.gz'):
    
        yield {
            'name': g_zipped_filepath.name,
            'file_dep': [g_zipped_filepath],
            'targets': [f'{g_zipped_filepath.name}_loaded'],
            'actions': [
                f'gunzip -c {g_zipped_filepath} | hdfs dfs -appendToFile - /user/dte_chrism/mot/items.txt', 
                f'touch {g_zipped_filepath.with_suffix(".loaded")}'
            ]
        }

    
    
