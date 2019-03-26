import sys
import re
import shutil
from pathlib import Path


def filter_out_answers(input_path, output_path):

    exercise_start_pattern = re.compile(r'## Exercise.+$')
    exercise_end_pattern = re.compile(r'^#{3,}$')
    blank_line_pattern = re.compile(r'^\s*$')

    exercise_started = False

    new_content = []
    for line in input_path.open():

        if exercise_start_pattern.match(line):
            exercise_started = True
            new_content.append(line)
            continue
        elif exercise_end_pattern.match(line):
            exercise_started = False
            new_content.append(line)
            continue
        if exercise_started:
            if line.startswith('#>') or blank_line_pattern.match(line):            
                new_content.append(line)
        else:
            new_content.append(line)
    
    new_content = ''.join(new_content)

    output_path.write_text(new_content)

if __name__ == '__main__':
        
    # Read in file and check it exists
    input_filepath = Path(sys.argv[1])
    assert input_filepath.exists()

    # Copy input file to have with version with answers
    new_filename = 'exercise_answers.py'
    training_script_with_answers = input_filepath.with_name(new_filename)
    shutil.copyfile(input_filepath, training_script_with_answers)
    
    filter_out_answers(input_filepath, input_filepath)
    
    print('Success!')
    