import sys
import re
import shutil
from pathlib import Path


def filter_out_answers(input_path, material_output_path, answers_output_path):

    # Regexes to match begining and end of exercises 
    # blank lines, and lines that are quoted text in markdown 
    exercise_start_pattern = re.compile(r'## Exercise.+$')
    exercise_end_pattern = re.compile(r'^#{3,}$')
    blank_line_pattern = re.compile(r'^\s*$')
    quote_line_pattern = re.compile(r'^# *> *.*')

    exercise_started = False
    
    exercise_answers_content = []
    material_content_without_answers = []

    for line in input_path.open():

        if exercise_start_pattern.match(line):
            exercise_started = True
            exercise_answers_content.append(line)
            material_content_without_answers.append(line)
            continue
        
        elif exercise_end_pattern.match(line):
            exercise_started = False
            material_content_without_answers.append(line)
            exercise_answers_content.append(line)
            exercise_answers_content.append('\n\n\n')
            continue

        if exercise_started:
            if quote_line_pattern.match(line) or blank_line_pattern.match(line):            
                exercise_answers_content.append(line)
                material_content_without_answers.append(line)
            else: 
                exercise_answers_content.append(line)
        else:
            material_content_without_answers.append(line)
    
    material_content_without_answers = ''.join(material_content_without_answers)
    exercise_answers = ''.join(exercise_answers_content)
    
    # Write outputs
    material_output_path.write_text(material_content_without_answers)
    answers_output_path.write_text(exercise_answers)
        