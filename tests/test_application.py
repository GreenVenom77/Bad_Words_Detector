import os

os.chdir('..')
from main import main


def test_application(test_data, args):
    args = test_data['args']
    main(args)
