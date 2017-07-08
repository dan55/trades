import pdb

tot_lines = 0

with open('pitch_example_data', 'r') as f:
    line = f.readline()

    # remove the newline char and, per the instructions, 
    # ignore the initial 'S'
    line.rstrip()[1:]

    pdb.set_trace()