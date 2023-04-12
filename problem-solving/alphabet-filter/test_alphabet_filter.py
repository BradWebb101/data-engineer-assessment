import pytest
from alphabet_filter import LetterFilter

@pytest.fixture
def empty_input():
    return ''

@pytest.fixture
def valid_input():
    return 'abcde'

@pytest.fixture
def all_vowels():
    return 'aaaaa'

@pytest.fixture
def all_consonants():
    return 'bbbbb'

@pytest.fixture
def special_characters_input():
    return 'abcd*'

@pytest.fixture
def upper_case_input():
    return 'ABCDE'

@pytest.fixture
def readme_example_1():
    return 'onomatopoeia'

#Testing setter properties
def test_setters_validate(empty_input, all_vowels, all_consonants, special_characters_input, upper_case_input):
    with pytest.raises(ValueError):
        LetterFilter(empty_input)  # invalid input containing no characters

    with pytest.raises(ValueError):
        LetterFilter(all_vowels)  # invalid input containing only vowels

    with pytest.raises(ValueError):
        LetterFilter(all_consonants)  # invalid input containing only consonants

    with pytest.raises(ValueError):
        LetterFilter(special_characters_input)  # invalid input containing a special character
    
    with pytest.raises(ValueError):
        LetterFilter(upper_case_input)  # invalid input containing upper case values

def test_vowels_out(valid_input):
    assert LetterFilter(valid_input).filter_vowels() == 'bcd'

def test_constanants_out(valid_input):
    assert LetterFilter(valid_input).filter_consonants() == 'ae'

def test_readme_example(readme_example_1):
    assert LetterFilter(readme_example_1).filter_vowels() == 'nmtp'
    assert LetterFilter(readme_example_1).filter_consonants() == 'ooaooeia'