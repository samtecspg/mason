from setuptools import setup, find_packages, find_namespace_packages

with open('requirements.txt') as f:
    requirements = f.readlines()

long_description = 'Mason Data Operators Framework'

setup(
    name='mason',
    version='1.04',
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
            'mason = mason.cli:main'
        ]
    },
    install_requires=requirements,
    zip_safe=False,
    include_package_data=True
)

