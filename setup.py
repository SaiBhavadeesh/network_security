from setuptools import setup, find_packages
from typing import List


def get_requirements() -> List[str]:
    """
    This function will return the list of requirements
    """
    requirement_list: List[str] = []
    try:
        with open("requirements.txt", "r") as file:
            # Read lines from the requirements file
            lines = file.readlines()
            # Process each line
            for line in lines:
                requirement = line.strip()
                ## ignore empty lines and -e .
                if requirement and not requirement.startswith("-e"):
                    requirement_list.append(requirement)
    except FileNotFoundError:
        print(
            "requirements.txt file not found. Please ensure it exists in the current directory."
        )
    return requirement_list


setup(
    name="network_security",
    version="0.0.1",
    author="Sai Bhavadeesh Yarlagadda",
    author_email="saibhavadeesh@gmail.com",
    packages=find_packages(),
    install_requires=get_requirements(),
)
