from setuptools import find_packages, setup
from typing import List 


HYPEN_E_DOT = "-e ."
def get_requirements(path : str ) -> List[str] :
    requirements = [] ; 
    with open(path) as file_obj :
        requirements = file_obj.readlines() ;

    requirements = [ req.replace("\n" ,"") for req in requirements ]
    if HYPEN_E_DOT in requirements : 
        requirements.remove(HYPEN_E_DOT)


    print(requirements) 
    return requirements

get_requirements("requirements.txt")
setup(
  name = "Solution_hacked".upper(),
  version = "0.0.1",
  author="Aryan Goel",
  author_email="aryangoel971@gmail.com",
  packages=[]

 )