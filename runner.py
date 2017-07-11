import sys

from PitchParser import PitchParser

if __name__ == '__main__':
    try:
        data_file = sys.argv[1]
    except IndexError:
        print('Usage: challenge_2.py data_file')
        sys.exit(-1)


    parser = PitchParser(data_file)
    parser.parse()
    parser.print_total_volume()