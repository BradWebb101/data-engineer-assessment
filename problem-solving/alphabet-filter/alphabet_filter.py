import re

class LetterFilter():

    def __init__(self, s:str|None):
        self.s = s
        self.vowel_list = ['a','e','i','o','u']
    
    @property
    def s(self):
        return self._s

    @s.setter
    def s(self, value):
        if not re.match("^[a-z]+$", value):
            raise ValueError("String must contain only lowercase letters in the range ascii[a-z]")
        if not re.match("^(?=.*[aeiou])(?=.*[^aeiou])[a-z]+$", value):
            raise ValueError("String must contain at least one vowel and at least one consonant")
        self._s = value

    def update_vowel_list(self, new_vowel_list: list) -> None:
        self.vowel_list = new_vowel_list

    def filter_vowels(self) -> str: 
        consonants_list = [i.lower() for i in self.s if i not in self.vowel_list]
        return "".join(consonants_list)

    def filter_consonants(self) -> str:
        vowel_list = [i.lower() for i in self.s if i in self.vowel_list]
        return "".join(vowel_list)
    
