from setuptools import setup, find_packages

with open('README.rst') as f:
    readme = f.read()

INSTALL_REQUIRES = [
      'azure-core>=1.3.0',
      'azure-storage-blob>=12.3.0',
      'aiohttp'
]

setup(
    name='txpy-azurehelper',
    version='0.1.4',
    description='A wrapper around the Azure SDK for Python',
    long_description=readme,
    long_description_content_type='text/x-rst',
    author='Philippe Huet',
    author_email='philhu@tyxio.com',
    url='https://github.com/tyxio/txpy-azurehelper',
    license='MIT License',
    install_requires=INSTALL_REQUIRES,
    packages=find_packages(exclude=('tests', 'docs'))
)