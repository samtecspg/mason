from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.readlines()

with open('VERSION') as f:
    VERSION = float(f.readlines()[0])

long_description = 'Mason Data Operators Framework'

setup(
    name='mason',
    version=VERSION,
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
            'mason = cli:main'
        ]
    },
    install_requires=requirements,
    zip_safe=False,
    include_package_data=True
)

