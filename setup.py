from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.readlines()

with open('VERSION') as f:
    version = f.read()

long_description = 'Mason Data Operators Framework'

setup(
    name='mason',
    version=version,
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
            'mason = mason.cli:cli'
        ]
    },
    install_requires=requirements,
    zip_safe=False,
    include_package_data=True
)

