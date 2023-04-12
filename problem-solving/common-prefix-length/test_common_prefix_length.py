from common_prefix_filter import commonPrefix

def test_default_string():
    inputs = ['abcabcd']
    expected_output = [10]
    assert commonPrefix(inputs) == expected_output

def test_increased_recururing_sets():
    inputs = ['abcabcabcd']
    expected_output = [19]
    assert commonPrefix(inputs) == expected_output

def test_non_recuring_string():    
    inputs = ['abc', 'abcd', 'abcde', 'abcdef']
    expected_output = [3, 4, 5, 6]
    assert commonPrefix(inputs) == expected_output

def test_single_entry_string():
    inputs = ['a', 'b', 'c', 'd', 'e']
    expected_output = [1, 1, 1, 1, 1]
    assert commonPrefix(inputs) == expected_output

def test_min_value_is_string_length():
    inputs = ['a', 'ab', 'abc']
    expected_output = [1, 2, 3]
    assert commonPrefix(inputs) == expected_output


