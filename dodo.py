from pathlib import Path
from lib.source import download_mot_data, download_supporting_docs


START = 2005
END = 2017
DATA_DIR = Path('./data')
DATA_DIR_HDFS = Path('/tmp/mot')


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
        ext = '.txt.gz' if year <= 2016 else '.zip'
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
    hdfs_filepath = DATA_DIR_HDFS / 'results.csv'
    
    for year in range(START, END+1):
        ext = '.txt.gz' if year <= 2016 else '.zip'        
        g_zipped_filepath = raw_data_dir / f'test_result_{year}{ext}'
        target = g_zipped_filepath.with_suffix(".loaded")
    
        yield {
            'name': g_zipped_filepath.name,
            'file_dep': [g_zipped_filepath],
            'targets': [target],
            'actions': [
                f'gunzip -c {g_zipped_filepath} | hdfs dfs -appendToFile - {hdfs_filepath}', 
                f'touch {target}'
            ]
        }


def task_extract_and_load_item_data():
    
    raw_data_dir = DATA_DIR / 'raw' 
    hdfs_filepath = DATA_DIR_HDFS / 'items.csv'
        
    for year in range(START, END+1):
        ext = '.txt.gz' if year <= 2016 else '.zip'        
        g_zipped_filepath = raw_data_dir / f'test_item_{year}{ext}'
        target = g_zipped_filepath.with_suffix(".loaded")

        yield {
            'name': g_zipped_filepath.name,
            'file_dep': [g_zipped_filepath],
            'targets': [target],
            'actions': [
                f'gunzip -c {g_zipped_filepath} | hdfs dfs -appendToFile - {hdfs_filepath}', 
                f'touch {target}'
            ]
        }


def task_create_impala_table():
    
    sql_schema = DATA_DIR / 'mot-impala-schema.sql'
    assert sql_schema.exists()
    
    impala_host = 'cdhwn-04682bdf.odts-sandpit.internal'
    
    return {
        'actions': [
            f'impala-shell -i {impala_host}  -f {sql_schema} --var=data_location={DATA_DIR_HDFS}'
        ]
    }
