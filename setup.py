from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.readlines()

long_description = ''

setup(
    name='mason',
    version='1.0.0',
    author='Kyle Prifogle',
    author_email='kyle.prifogle@samtec.com',
    url='',
    description='',
    long_description=long_description,
    long_description_content_type="text/markdown",
    license='',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'mason = cli.cli:main'
        ]
    },
    install_requires=requirements,
    zip_safe=False
)

