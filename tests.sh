#!/bin/bash

#script for TC build agent test running
#System dependencies: python-virtualenv, virtualenv, python-setuptools, PEP8

VENV_PATH="../venv"
PYPI_MIRROR="http://pydev.iv/pypi/"
ARTIFACT_PATH="../artifacts"
TX_EXT="txamqp_ext"
TX_TEST="txamqp_ext/test"

PEP8_IGNORE_LIST="E202,E702,W291,E301,W292,E501"
PEP8_IGNORE_LIST="E202,E702,W291,E301,W292,E501,E225,W391,E251,E222,E221,E203,E231,E201,E111,E111"

function install_env {
        ln -sf $VENV_PATH/bin .
        ln -sf $VENV_PATH/lib .
        ln -sf $VENV_PATH/include .
	virtualenv --no-site-packages --python=python2.6 $VENV_PATH
	$VENV_PATH/bin/pip install -r .meta/packages --index-url $PYPI_MIRROR --find-links $PYPI_MIRROR
}

function update_packages {
	$VENV_PATH/bin/pip install -r .meta/packages --index-url $PYPI_MIRROR --find-links $PYPI_MIRROR --upgrade
}

function create_artifact {
	mkdir -p $ARTIFACT_PATH
	tar --exclude-vcs --exclude=*.pyc --exclude=tests.sh --exclude=settings_build_agent.py -czf $ARTIFACT_PATH/$TEAMCITY_BUILDCONF_NAME-$BUILD_NUMBER.tar.gz *
}

function cleanup {
        rm -rf $VENV_PATH
	rm -rf bin lib include
	rm ~/logs/debug.log ~/logs/error.log
}

function run_trial_tests {
    ./$VENV_PATH/bin/trial $@ $TX_TEST/test_factory.py
    ./$VENV_PATH/bin/trial $@ $TX_TEST/test_protocol.py
}
function run_test {

        ln -sf $VENV_PATH/bin .
        ln -sf $VENV_PATH/lib .
        ln -sf $VENV_PATH/include .

        export PYTHONPATH=.

        echo "check PEP8 rules"
        pep8 --filename=*.py --ignore=$PEP8_IGNORE_LIST .

	echo "pylint checker"
	pylint -f parseable -r n --rcfile ../.meta/pylintrc $TX_EXT

        echo "Starting trial"
        run_trial_tests

}


function agent_test_run {

        ln -sf $VENV_PATH/bin .
        ln -sf $VENV_PATH/lib .
        ln -sf $VENV_PATH/include .

        export PYTHONPATH=.
	
#        echo "check PEP8 rules"
#        ../$VENV_PATH/bin/pep8_teamcity --filename=*.py --ignore=$PEP8_IGNORE_LIST .
#        ../$VENV_PATH/bin/python ./pep8_teamcity --filename=*.py --ignore=$PEP8_IGNORE_LIST .

        echo "Starting trial"
        run_trial_tests --reporter=teamcity

}

case $1 in
    install)
	install_env
        ;;

    update_packages)
	update_packages
        ;;

    test)

	#checking for first run and create infra
	if [ ! -d $VENV_PATH ]; then
	    install_env
	fi

	#update python libs if needed
	if [ -n "${UPDATE_PACKAGES:+x}" ] && [ $UPDATE_PACKAGES = "True" ]; then
	    update_packages
	fi

	#create artifact
	if [ -n "${CREATE_ARTIFACT:+x}" ] && [ $CREATE_ARTIFACT = "True" ]; then
	    create_artifact
	fi

	run_test
        ;;

    agent_test_run)
        echo "Build number: $BUILD_NUMBER \n Rev: $BUILD_VCS_NUMBER" > .meta/revision

	#checking for first run and create infra
	if [ ! -d $VENV_PATH ]; then
	    install_env
	fi

	#update python libs if needed
	if [ -n "${UPDATE_PACKAGES:+x}" ] && [ $UPDATE_PACKAGES = "True" ]; then
	    update_packages
	fi

	#create artifact
	if [ -n "${CREATE_ARTIFACT:+x}" ] && [ $CREATE_ARTIFACT = "True" ]; then
	    create_artifact
	fi

	agent_test_run
        ;;

    start)
	start_workers
	;;

    stop)
	stop_workers
	;;

    clean)
	cleanup
        ;;

    *)
        echo "Usage: $0 {install|test|agent_test_run|update_packages|clean}"
        exit 1
esac
