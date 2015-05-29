import os
import sys
from setuptools import setup, find_packages
from setuptools.command.install import install


version = '0.2.4'


class InstallCheckTxamqp(install):
    def run(self):
        install.run(self)
        try:
            import txamqp  # noqa
        except:
            pp = sys.executable.replace('python', 'pip')
            c0 = '{} uninstall -y txamqp'.format(pp)
            c1 = '{} install txamqp'.format(pp)
            os.system(c0)
            os.system(c1)

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
                        "msgpack-python"],
      cmdclass={
          'install': InstallCheckTxamqp,
      }
      )
