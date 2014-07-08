from setuptools import setup, find_packages

version = '0.2.0'

setup(name='txamqp_ext',
      version=version,
      description="txAMQP extended",
      long_description="""Custom classes for work with txamqp""",
      classifiers=[],
      keywords='twisted amqp txamqp',
      author='cybergrind',
      author_email='cybergrind@gmail.com',
      url='https://github.com/cybergrind/txamqp_ext',
      license='LGPL',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests', 'old*']),
      zip_safe=False,
      install_requires=["Twisted>=10.0",
                        "txAMQP>=0.6.1",
                        "msgpack-python"]
      )
