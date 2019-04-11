"""Setup the training material and resources ready for teaching. 

Steps:
    Clean datasets
    Transfer to HDFS
    Strip out the exercise andswers and save seperate to walkthough

"""
from pathlib import Path
import sys
sys.path.append('src')

from scripts.clean import clean_rescue_data, clean_population_data
from scripts.process import filter_out_answers
from scripts.transfer import transfer_to_hdfs

# Paths
ROOT_DIR = Path(__file__).parent
MATERIAL_PATH = ROOT_DIR / 'material'
EXERCISE_PATH = MATERIAL_PATH / 'exercise'

DATA_DIR = ROOT_DIR / 'src' / 'data'

# Check all exist 
for p in [ROOT_DIR, MATERIAL_PATH, EXERCISE_PATH, DATA_DIR]:
    assert p.exists()

# Clean 
# =====
cleaned_rescue_src = clean_rescue_data(DATA_DIR)
cleaned_pop_src = clean_population_data(DATA_DIR)

print('Cleaned data')

# Load 
# =====

# rescue data
cleaned_rescue_dest = Path('/tmp/training') / cleaned_rescue_src.name
transfer_to_hdfs(cleaned_rescue_src, cleaned_rescue_dest)

# population data
cleaned_pop_dest = Path('/tmp/training') / cleaned_pop_src.name
transfer_to_hdfs(cleaned_pop_src, cleaned_pop_dest)

print('Loaded data to HDFS')


# Filter out Answers
# ==================
src_path = ROOT_DIR / 'src' / 'material' / 'full_walkthrough.py'
material_dest_path = ROOT_DIR / 'material' / 'walkthrough.py'
exercixe_dest_path = ROOT_DIR / 'material' / 'exercise' / 'answers.py'

filter_out_answers(src_path, material_dest_path, exercixe_dest_path)

print(f'Saved walkthrough to {material_dest_path}')
print(f'Saved exercise answers to {exercixe_dest_path}')