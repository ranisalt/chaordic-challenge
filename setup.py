import setuptools

def read(filename):
    with open(filename) as fp:
        content = fp.read()
    return content

setuptools.setup(
    name='Chaordic Challenge',
    url='https://github.com/ranisalt/chaordic-challenge.git',
    author='Ranieri Althoff',
    author_email='ranisalt@gmail.com',
    install_requires=read('requirements.txt')
)
