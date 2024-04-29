import os

os.chdir('..')
from main import main


def test_application(test_data, ahoargs, regexargs):
    ahoargs = test_data['ahoargs']
    regexargs = test_data['regexargs']

    main(ahoargs)
    main(regexargs)
