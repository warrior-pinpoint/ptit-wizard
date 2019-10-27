from setuptools import setup, find_packages

requirements = [
    "paho-mqtt==1.3.1",
    "requests==2.22.0"
]

setup(
    name="ptit-wizard",
    version="0.0.6",
    author="louis",
    author_email="vuductaiptit@gmail.com",
    description="Wizard for final project at school",
    long_description_content_type="text/markdown",
    url="https://github.com/warrior-pinpoint/ptit-wizard",
    packages=find_packages(),
    install_requires=requirements,
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
    ],
)