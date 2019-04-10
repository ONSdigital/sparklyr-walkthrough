"""Setup the training material and resources ready for teaching. 

Steps:
    Clean datasets
    Transfer to HDFS
    Strip out the exercise andswers and save seperate to walkthough

"""
from pathlib import Path
import sys
sys.path.append('./src/scripts')

from scripts.clean import clean_rescue_data, clean_population_data
from scripts.process import filter_out_answers
from scripts.transfer import transfer_to_hdfs

# Paths
ROOT_DIR = Path(__file__).parent
DATA_DIR = ROOT_DIR / 'src' / 'data'

# Clean 
# =====
def task_clean_rescue_data():
    return {
        'actions': [
            (clean_rescue_data, [DATA_DIR])
        ],
        'target': [
            DATA_DIR / 'clean' / 'animal-rescue.csv',   
        ] 
    }


def task_clean_population_data():

    return {
        'actions': [
            (clean_population_data, [DATA_DIR])
        ],
        'target': [
            DATA_DIR / 'clean' / 'population-by-postcode.csv',   
        ] 
    }

# Load 
# =====

def task_transfer_data():
    """Transfer all data files to HDFS"""

    filenames = [
        'population-by-postcode.csv',
        'animal-rescue.csv',
    ]

    sources = [DATA_DIR / 'clean' / f for f in filenames]

    for source_file in sources:
        destination_file = Path('/tmp/training') / source_file.name    
        cmd = (transfer_to_hdfs, [source_file, destination_file])

        yield {
            'name': source_file.name
            'file_deps': [source_file],
            'actions': [cmd],
        }


# Filter out Answers
# ==================

def task_filter_answers():
    """Converts full material to seperate files for material + exercise answers"""

    src_path = ROOT_DIR / 'src' / 'material' / 'full_walkthrough.py'
    material_dest_path = ROOT_DIR / 'material' / 'walkthrough.py'
    exercixe_dest_path = ROOT_DIR / 'material' / 'exercise' / 'answers.py'

    cmd = (
        filter_out_answers, [src_path, material_dest_path, exercixe_dest_path]
    )

    return {
        'actions': [cmd],
        'targets': [material_dest_path, exercixe_dest_path]
    }

    